from __future__ import annotations

import collections
import json
import os
import statistics
import time
import tempfile
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from . import analyzer
from .capacity import build_capacity_manager
from .execution_control import (
    AnalysisCancelled,
    analysis_cancel_requested,
    close_analysis_cancel_window,
    exclusive_analysis_execution,
)
from .scoring import (
    estimate_nominal_throughput_kbps,
    parse_fps,
    parse_resolution,
    throughput_ttl_with_jitter,
)
from .reliability import RELIABILITY_PATH, load_reliability_cache
from .throughput import (
    DEFAULT_USER_AGENT,
    LEGACY_CACHE_PATH,
    capture_stream_sample,
    load_cache as load_throughput_cache,
    probe_stream,
)


ANALYSIS_HEALTH_REPORT_PATH = "/data/dispatcharr_stream_sort_health_report.json"
MEDIA_CHECK_HISTORY_RETENTION_DAYS = 90
MEDIA_CHECK_HISTORY_MAX_ROWS = 10000
MEDIA_CHECK_ROLLUP_RETENTION_DAYS = 365
FFPROBE_STATS_HISTORY_MAX_ROWS = 7
HEALTH_REPORT_TIMEZONE = ZoneInfo("America/New_York")
MEDIA_BITRATE_RELATIVE_TOLERANCE = 0.30
SUSTAINED_PLAYBACK_HEALTHY_RATIO = 1.10
SUSTAINED_PLAYBACK_MINIMUM_RATIO = 1.00


def _normalize_throughput_check_reason(
    reason: str | None,
    *,
    media_changed: bool,
) -> str | None:
    if reason in {"missing", "throughput_missing"}:
        return "throughput_missing"
    if media_changed:
        return "media_changed"
    return reason


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _datetime_iso(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed is not None else None


def _age_hours(value: Any, now: datetime) -> float | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return max(0.0, (now - parsed).total_seconds() / 3600.0)


def _ttl_with_jitter(ttl_hours: float, *, url_hash: str, jitter_percent: float) -> float:
    return throughput_ttl_with_jitter(
        ttl_hours,
        identity=url_hash,
        jitter_percent=jitter_percent,
    )


def _effective_dead_ttl_hours(entry: Mapping[str, Any] | None, base_ttl_hours: float) -> float:
    if base_ttl_hours <= 0 or not entry:
        return base_ttl_hours
    if str(entry.get("error_type") or "") == "placeholder_file":
        return base_ttl_hours
    try:
        streak = max(1, int(entry.get("consecutive_dead_results") or 1))
    except (TypeError, ValueError):
        streak = 1
    if streak <= 2:
        return base_ttl_hours
    if streak <= 5:
        return base_ttl_hours * 4.0
    return base_ttl_hours * 12.0


def _percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    normalized: list[float] = [float(value) for value in values]
    normalized = [value for value in normalized if value is not None]
    if not normalized:
        return None
    normalized.sort()
    quantile = min(max(float(quantile), 0.0), 1.0)
    if len(normalized) == 1:
        return normalized[0]
    index = (len(normalized) - 1) * quantile
    lower = int(index)
    upper = min(lower + 1, len(normalized) - 1)
    if lower == upper:
        return normalized[lower]
    weight = index - lower
    return normalized[lower] * (1.0 - weight) + normalized[upper] * weight


def _round_if_present(value: float | None, digits: int) -> float | None:
    return round(value, digits) if value is not None else None


def _save_json(path: str, data: Mapping[str, Any]) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, temporary_path = tempfile.mkstemp(prefix=".stream-sort-report-", suffix=".json", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, sort_keys=True, indent=2, default=str)
            handle.write("\n")
        os.replace(temporary_path, path)
    except Exception:
        try:
            os.unlink(temporary_path)
        except OSError:
            pass
        raise


def health_check_reason(
    entry: Mapping[str, Any] | None,
    *,
    url_hash: str,
    ttl_hours: float,
    dead_ttl_hours: float | None = None,
    content_ttl_hours: float | None = None,
    ttl_jitter_percent: float = 0.0,
    now: datetime,
) -> str | None:
    if not entry:
        return "missing"
    if str(entry.get("url_hash") or "") != url_hash:
        return "url_changed"
    status = str(entry.get("status") or "unknown").strip().lower()
    if status != "alive":
        if status == "dead" and dead_ttl_hours is not None:
            if entry.get("retry_pending"):
                return "status_dead_retry_pending"
            checked_at = (
                entry.get("health_checked_at")
                or entry.get("playback_health_checked_at")
                or entry.get("media_checked_at")
                or entry.get("tested_at")
            )
            age = _age_hours(checked_at, now)
            if age is None:
                return "missing_timestamp"
            effective_dead_ttl = _effective_dead_ttl_hours(entry, dead_ttl_hours)
            if effective_dead_ttl > 0 and age < effective_dead_ttl:
                return "status_dead_ttl"
        return f"status_{status or 'unknown'}"
    if ttl_hours <= 0:
        return "ttl_forced"
    checked_at = (
        entry.get("playback_health_checked_at")
        or entry.get("health_checked_at")
        or entry.get("media_checked_at")
        or entry.get("tested_at")
    )
    age = _age_hours(checked_at, now)
    if age is None:
        return "missing_timestamp"
    effective_ttl_hours = _ttl_with_jitter(ttl_hours, url_hash=url_hash, jitter_percent=ttl_jitter_percent)
    if age >= effective_ttl_hours:
        return "ttl_expired"
    if content_ttl_hours is not None:
        content_checked_at = entry.get("content_checked_at")
        if not content_checked_at and entry.get("health_source") != "runtime_playback":
            content_checked_at = entry.get("health_checked_at") or entry.get("media_checked_at") or entry.get("tested_at")
        content_age = _age_hours(content_checked_at, now)
        # Recent successful playback defers the first content-only validation;
        # it does not claim that black/frozen/silent checks were performed.
        if content_age is None:
            playback_age = _age_hours(entry.get("playback_health_checked_at"), now)
            effective_content_ttl_hours = _ttl_with_jitter(
                content_ttl_hours,
                url_hash=url_hash,
                jitter_percent=ttl_jitter_percent,
            )
            if playback_age is None or playback_age >= effective_content_ttl_hours:
                return "content_missing"
        else:
            effective_content_ttl_hours = _ttl_with_jitter(
                content_ttl_hours,
                url_hash=url_hash,
                jitter_percent=ttl_jitter_percent,
            )
            if effective_content_ttl_hours <= 0 or content_age >= effective_content_ttl_hours:
                return "content_ttl_expired"
    return None


def metadata_check_reason(
    entry: Mapping[str, Any] | None,
    *,
    url_hash: str,
    ttl_hours: float,
    ttl_jitter_percent: float = 0.0,
    now: datetime,
) -> str | None:
    if not entry:
        return "missing"
    if str(entry.get("url_hash") or "") != url_hash:
        return "url_changed"
    stats = entry.get("stats")
    if not isinstance(stats, Mapping) or not stats:
        return "missing"
    if ttl_hours <= 0:
        return "ttl_forced"
    updated_at = entry.get("metadata_updated_at") or entry.get("media_checked_at") or entry.get("tested_at")
    age = _age_hours(updated_at, now)
    if age is None:
        return "missing_timestamp"
    effective_ttl_hours = _ttl_with_jitter(ttl_hours, url_hash=url_hash, jitter_percent=ttl_jitter_percent)
    if age >= effective_ttl_hours:
        return "ttl_expired"
    return None


media_check_reason = health_check_reason


def _provider_matches(expected, observed) -> bool:
    if expected in (None, "") or observed in (None, ""):
        return False
    return str(expected) == str(observed)


def ffprobe_check_reason(
    entry: Mapping[str, Any] | None,
    *,
    url_hash: str,
    ttl_hours: float,
    dead_ttl_hours: float,
    provider_id: Any = None,
    ttl_jitter_percent: float,
    now: datetime,
) -> str | None:
    if not entry:
        return "missing"
    if str(entry.get("url_hash") or "") != url_hash:
        return "url_changed"
    cached_provider = entry.get("m3u_account_id")
    if provider_id not in (None, "") and cached_provider not in (None, "") and not _provider_matches(provider_id, cached_provider):
        return "provider_changed"
    status = str(entry.get("status") or "unknown").lower()
    if status == "dead":
        checked_at = entry.get("dead_checked_at") or entry.get("health_checked_at") or entry.get("tested_at") or entry.get("ffprobe_checked_at")
        age = _age_hours(checked_at, now)
        if entry.get("retry_pending"):
            return "dead_retry_pending"
        effective_dead_ttl = _effective_dead_ttl_hours(entry, dead_ttl_hours)
        if age is not None and effective_dead_ttl > 0 and age < effective_dead_ttl:
            return None
        return "dead_ttl_expired"
    if status != "alive":
        return f"status_{status}"
    checked_at = entry.get("ffprobe_checked_at") or entry.get("media_checked_at")
    age = _age_hours(checked_at, now)
    if age is None:
        return "missing_timestamp"
    if ttl_hours <= 0:
        return "ttl_forced"
    effective_ttl = _ttl_with_jitter(ttl_hours, url_hash=url_hash, jitter_percent=ttl_jitter_percent)
    if age >= effective_ttl:
        return "ttl_expired"
    return None


def content_check_reason(
    entry: Mapping[str, Any] | None,
    *,
    url_hash: str,
    ttl_hours: float,
    dead_ttl_hours: float,
    provider_id: Any = None,
    ttl_jitter_percent: float = 0.0,
    now: datetime,
) -> str | None:
    if not entry:
        return "missing"
    if str(entry.get("url_hash") or "") != url_hash:
        return "url_changed"
    if entry.get("content_checked_at"):
        cached_provider = entry.get("content_m3u_account_id")
        if provider_id not in (None, "") and cached_provider in (None, ""):
            return "provider_missing"
        if provider_id not in (None, "") and not _provider_matches(provider_id, cached_provider):
            return "provider_changed"
    status = str(entry.get("status") or "unknown").lower()
    if status == "dead":
        checked_at = entry.get("dead_checked_at") or entry.get("health_checked_at") or entry.get("tested_at")
        age = _age_hours(checked_at, now)
        if entry.get("retry_pending"):
            return "dead_retry_pending"
        effective_dead_ttl = _effective_dead_ttl_hours(entry, dead_ttl_hours or 0.0)
        if age is not None and effective_dead_ttl > 0 and age < effective_dead_ttl:
            return None
        return "dead_ttl_expired"
    if status != "alive":
        return "health_revalidation"
    checked_at = entry.get("content_checked_at")
    age = _age_hours(checked_at, now)
    if age is None:
        return "missing"
    if ttl_hours <= 0:
        return "ttl_forced"
    effective_ttl_hours = _ttl_with_jitter(ttl_hours, url_hash=url_hash, jitter_percent=ttl_jitter_percent)
    if age >= effective_ttl_hours:
        return "ttl_expired"
    return None


def throughput_check_reason(
    entry: Mapping[str, Any] | None,
    *,
    url_hash: str,
    ttl_hours: float,
    dead_ttl_hours: float | None = None,
    degraded_ttl_hours: float | None = None,
    unknown_ttl_hours: float | None = None,
    provider_id: Any = None,
    ttl_jitter_percent: float = 0.0,
    now: datetime,
) -> str | None:
    if not entry:
        return "missing"
    throughput = entry.get("throughput")
    if not isinstance(throughput, Mapping):
        return "missing"
    if str(throughput.get("url_hash") or "") != url_hash:
        return "url_changed"
    cached_provider = throughput.get("m3u_account_id")
    if provider_id not in (None, "") and cached_provider in (None, ""):
        return "provider_missing"
    if provider_id not in (None, "") and not _provider_matches(provider_id, cached_provider):
        return "provider_changed"
    status = str(throughput.get("status") or "unknown").strip().lower()
    if status != "healthy":
        checked_at = throughput.get("checked_at") or throughput.get("tested_at")
        age = _age_hours(checked_at, now)
        fallback_ttl = ttl_hours if dead_ttl_hours is None else dead_ttl_hours
        status_ttl = unknown_ttl_hours if status == "unknown" else degraded_ttl_hours
        if status_ttl is None:
            status_ttl = fallback_ttl
        effective_status_ttl = _ttl_with_jitter(
            status_ttl,
            url_hash=url_hash,
            jitter_percent=ttl_jitter_percent,
        )
        if age is not None and effective_status_ttl > 0 and age < effective_status_ttl:
            return None
        return f"status_{status or 'unknown'}"
    if ttl_hours <= 0:
        return "ttl_forced"
    checked_at = throughput.get("checked_at") or throughput.get("tested_at")
    age = _age_hours(checked_at, now)
    if age is None:
        return "missing_timestamp"
    effective_ttl_hours = _ttl_with_jitter(ttl_hours, url_hash=url_hash, jitter_percent=ttl_jitter_percent)
    if age >= effective_ttl_hours:
        return "ttl_expired"
    return None


def _stats_signature(stats: Mapping[str, Any] | None) -> tuple[Any, ...]:
    stats = stats or {}
    width, height = parse_resolution(stats)
    fps = parse_fps(stats)
    return width, height, round(float(fps), 3) if fps is not None else None


def _extract_video_bitrate(stats: Mapping[str, Any] | None) -> float | None:
    if not stats:
        return None
    raw = stats.get("video_bitrate")
    if raw is None:
        raw = stats.get("video_bitrate_kbps")
    if raw is None:
        return None
    try:
        bitrate = float(raw)
    except (TypeError, ValueError):
        return None
    return bitrate if bitrate >= 0 else None


def _is_significant_bitrate_change(
    previous_bitrate: float | None,
    new_bitrate: float | None,
    *,
    relative_tolerance: float | None = None,
) -> bool:
    if new_bitrate is None and previous_bitrate is None:
        return False
    if previous_bitrate is None or new_bitrate is None:
        return False
    relative = MEDIA_BITRATE_RELATIVE_TOLERANCE if relative_tolerance is None else max(0.0, relative_tolerance)
    if previous_bitrate <= 0:
        return new_bitrate > 0
    return abs(new_bitrate - previous_bitrate) / previous_bitrate > relative


def _fps_family(value: Any) -> float | None:
    try:
        fps = float(value)
    except (TypeError, ValueError):
        return None
    for canonical, aliases in (
        (24.0, (23.976, 24.0)),
        (25.0, (25.0,)),
        (30.0, (29.97, 30.0)),
        (50.0, (50.0,)),
        (60.0, (59.94, 60.0)),
    ):
        if any(abs(fps - alias) <= 0.15 for alias in aliases):
            return canonical
    return round(fps, 1)


def _history_stats(media_history: Any) -> list[Mapping[str, Any]]:
    rows = []
    for row in media_history or []:
        stats = row.get("stats") if isinstance(row, Mapping) else None
        if isinstance(stats, Mapping) and stats:
            rows.append(stats)
    return rows[-FFPROBE_STATS_HISTORY_MAX_ROWS:]


def _persistent_fps_change(history: list[Mapping[str, Any]], new_stats: Mapping[str, Any]) -> bool:
    if len(history) < 2:
        return False
    baseline_families = [_fps_family(parse_fps(row)) for row in history[:-1]]
    baseline_families = [value for value in baseline_families if value is not None]
    previous_family = _fps_family(parse_fps(history[-1]))
    new_family = _fps_family(parse_fps(new_stats))
    if not baseline_families or previous_family is None or new_family is None:
        return False
    baseline_family = collections.Counter(baseline_families).most_common(1)[0][0]
    return previous_family == new_family and new_family != baseline_family


def _persistent_bitrate_change(
    history: list[Mapping[str, Any]],
    new_stats: Mapping[str, Any],
    *,
    relative_tolerance: float,
) -> bool:
    if len(history) < 4:
        return False
    baseline_values = [_extract_video_bitrate(row) for row in history[:-1]]
    baseline_values = [value for value in baseline_values if value is not None]
    previous_bitrate = _extract_video_bitrate(history[-1])
    new_bitrate = _extract_video_bitrate(new_stats)
    if not baseline_values or previous_bitrate is None or new_bitrate is None:
        return False
    baseline = float(statistics.median(baseline_values))
    deviations = [abs(value - baseline) for value in baseline_values]
    robust_standard_deviation = float(statistics.median(deviations)) * 1.4826
    minimum_delta = max(baseline * max(0.0, relative_tolerance), robust_standard_deviation)
    if abs(previous_bitrate - baseline) <= minimum_delta:
        return False
    if abs(new_bitrate - baseline) <= minimum_delta:
        return False
    return (previous_bitrate - baseline) * (new_bitrate - baseline) > 0


def _media_stats_changed_for_throughput(
    previous_stats: Mapping[str, Any] | None,
    new_stats: Mapping[str, Any] | None,
    *,
    media_bitrate_relative_tolerance: float = MEDIA_BITRATE_RELATIVE_TOLERANCE,
    media_history: Any = None,
) -> bool:
    if not previous_stats:
        return False
    if not new_stats:
        return False
    previous_width, previous_height = parse_resolution(previous_stats)
    new_width, new_height = parse_resolution(new_stats)
    if previous_width and previous_height and new_width and new_height and (previous_width, previous_height) != (new_width, new_height):
        return True
    history = _history_stats(media_history)
    if not history:
        history = [previous_stats]
    return _persistent_fps_change(history, new_stats) or _persistent_bitrate_change(
        history,
        new_stats,
        relative_tolerance=media_bitrate_relative_tolerance,
    )


def _status_counts(items, cache) -> collections.Counter[str]:
    counts: collections.Counter[str] = collections.Counter()
    for item in items:
        entry = cache.get(str(item["id"])) or {}
        counts[str(entry.get("status") or "unknown").lower()] += 1
    return counts


def _placeholder_dead_count(items, cache, results=None) -> int:
    results = results or {}
    count = 0
    for item in items:
        sid = int(item["id"])
        entry = results.get(sid) or cache.get(str(sid)) or {}
        if (
            str(entry.get("status") or "unknown").lower() == "dead"
            and str(entry.get("error_type") or "") == "placeholder_file"
        ):
            count += 1
    return count


def _throughput_counts(items, cache) -> collections.Counter[str]:
    counts: collections.Counter[str] = collections.Counter()
    for item in items:
        entry = cache.get(str(item["id"])) or {}
        if str(entry.get("status") or "unknown").lower() != "alive":
            continue
        throughput = entry.get("throughput") if isinstance(entry, Mapping) else None
        counts[str((throughput or {}).get("status") or "unknown").lower()] += 1
    return counts


def _overall_health_text(counts, *, placeholder_count: int = 0) -> str:
    dead_count = int(counts.get("dead", 0))
    placeholder_count = int(placeholder_count)
    if placeholder_count < 0 or placeholder_count > dead_count:
        raise ValueError("placeholder health count must be between zero and the aggregate dead count")
    other_dead_count = dead_count - placeholder_count
    return (
        f"alive={counts.get('alive', 0)} dead={dead_count} "
        f"(placeholder={placeholder_count} other_dead={other_dead_count}) "
        f"skipped={counts.get('skipped', 0)} unknown={counts.get('unknown', 0)}"
    )


def _overall_throughput_text(counts) -> str:
    return f"healthy={counts.get('healthy', 0)} marginal={counts.get('marginal', 0)} insufficient={counts.get('insufficient', 0)} unknown={counts.get('unknown', 0)}"


def _playback_throughput_report(items, cache) -> dict[str, Any]:
    ratios = []
    failures = 0
    streams = set()
    buckets = collections.Counter()
    for item in items:
        entry = cache.get(str(item["id"])) or {}
        for row in entry.get("playback_throughput_history") or []:
            if row.get("kind") == "delivery_failure":
                failures += 1
                streams.add(int(item["id"]))
                continue
            ratio = row.get("ratio")
            if not isinstance(ratio, (int, float)):
                continue
            ratio = float(ratio)
            ratios.append(ratio)
            streams.add(int(item["id"]))
            if ratio < 1.0:
                buckets["below_1_00"] += 1
            elif ratio < 1.03:
                buckets["1_00_to_1_03"] += 1
            elif ratio < 1.05:
                buckets["1_03_to_1_05"] += 1
            elif ratio < 1.07:
                buckets["1_05_to_1_07"] += 1
            elif ratio < 1.10:
                buckets["1_07_to_1_10"] += 1
            else:
                buckets["at_or_above_1_10"] += 1
    return {
        "clean_observations": len(ratios),
        "delivery_failures": failures,
        "streams_observed": len(streams),
        "ratio_percentiles": {
            "p10": _round_if_present(_percentile(ratios, 0.10), 4),
            "p25": _round_if_present(_percentile(ratios, 0.25), 4),
            "p50": _round_if_present(_percentile(ratios, 0.50), 4),
            "p75": _round_if_present(_percentile(ratios, 0.75), 4),
            "p90": _round_if_present(_percentile(ratios, 0.90), 4),
        },
        "ratio_buckets": dict(buckets),
        "initial_healthy_ratio": SUSTAINED_PLAYBACK_HEALTHY_RATIO,
    }


def _projected_status_counts(items, cache, media_results) -> collections.Counter[str]:
    counts = _status_counts(items, cache)
    for sid, result in media_results.items():
        previous = str((cache.get(str(sid)) or {}).get("status") or "unknown").lower()
        current = str(result.get("status") or "unknown").lower()
        counts[previous] -= 1
        counts[current] += 1
    return counts


def _health_class(result: Mapping[str, Any]) -> str:
    status = str(result.get("status") or "unknown").lower()
    if status == "dead":
        return "placeholder" if str(result.get("error_type") or "") == "placeholder_file" else "dead"
    return status


def _is_placeholder_result(result: Mapping[str, Any] | None) -> bool:
    return bool(
        isinstance(result, Mapping)
        and str(result.get("status") or "unknown").lower() == "dead"
        and str(result.get("error_type") or "") == "placeholder_file"
    )


def _is_confirmed_placeholder_result(result: Mapping[str, Any] | None) -> bool:
    return bool(_is_placeholder_result(result) and result.get("placeholder_confirmation"))


def _log_media_progress(
    logger,
    *,
    prefix: str,
    completed: int,
    phase_total: int,
    item: Mapping[str, Any],
    reason: str,
    result: Mapping[str, Any],
    items,
    cache,
    media_results,
    cached_media: int,
    pending_media: int,
    eta_seconds: float,
) -> None:
    stats = result.get("stats") or {}
    details = result.get("details") if isinstance(result.get("details"), Mapping) else {}
    projected_results = dict(media_results)
    health_counts = _projected_status_counts(items, cache, projected_results)
    placeholder_count = _placeholder_dead_count(items, cache, projected_results)
    logger.info(
        "%s %d%% (%d/%d) stream=%s reason=%s health=%s health_class=%s probe_mode=%s resolution=%s fps=%s bitrate=%skbps | overall %s cached_media=%d pending_media=%d | ETA=%s",
        prefix,
        int(round(completed / max(phase_total, 1) * 100)),
        completed,
        phase_total,
        item["id"],
        reason,
        str(result.get("status") or "unknown").lower(),
        _health_class(result),
        details.get("probe_mode") or "n/a",
        stats.get("resolution") or "n/a",
        f"{float(stats['source_fps']):.1f}" if stats.get("source_fps") is not None else "n/a",
        f"{float(stats['video_bitrate']):.0f}" if stats.get("video_bitrate") is not None else "n/a",
        _overall_health_text(health_counts, placeholder_count=placeholder_count),
        cached_media,
        pending_media,
        analyzer._format_eta(eta_seconds),
    )


def _append_health_history(
    entry: dict[str, Any],
    *,
    reason: str | None,
    previous_status: str,
    new_status: str,
    tested_at: str,
    result: Mapping[str, Any],
) -> None:
    history = list(entry.get("health_check_history") or [])
    retry = result.get("retry_telemetry")
    retry = dict(retry) if isinstance(retry, Mapping) else {}
    terminal = not bool(retry.get("retry_pending"))
    placeholder = str(result.get("error_type") or "") == "placeholder_file"
    row = {
        "checked_at": tested_at,
        "previous_status": previous_status,
        "status": new_status,
        "reason": reason or "unknown",
        "error_type": str(result.get("error_type") or ""),
        "error": str(result.get("error") or ""),
        "source": str(result.get("health_source") or entry.get("health_source") or "stream_sort_analyzer"),
        "retry": retry,
        "terminal": terminal,
    }
    history.append(row)
    observed_at = _parse_datetime(tested_at) or datetime.now(timezone.utc)
    cutoff = observed_at - timedelta(days=MEDIA_CHECK_HISTORY_RETENTION_DAYS)
    history = [
        row for row in history
        if (_parse_datetime(row.get("checked_at")) or observed_at) >= cutoff
    ]
    if len(history) > MEDIA_CHECK_HISTORY_MAX_ROWS:
        history = history[-MEDIA_CHECK_HISTORY_MAX_ROWS:]
    entry["health_check_history"] = history

    rollups = entry.get("health_daily_rollups")
    rollups = dict(rollups) if isinstance(rollups, Mapping) else {}
    day = observed_at.date().isoformat()
    bucket = dict(rollups.get(day) or {})
    prefix = "placeholder_" if placeholder else ""
    terminal_key = f"{prefix}completed_checks" if terminal else f"{prefix}provisional_checks"
    bucket[terminal_key] = int(bucket.get(terminal_key) or 0) + 1
    if terminal and not placeholder and new_status in {"alive", "dead"}:
        key = f"{new_status}_checks"
        bucket[key] = int(bucket.get(key) or 0) + 1
    if terminal and not placeholder and previous_status in {"alive", "dead"} and previous_status != new_status:
        key = f"{previous_status}_to_{new_status}"
        bucket[key] = int(bucket.get(key) or 0) + 1
    retry_prefix = "placeholder_" if placeholder else ""
    retry_attempts_key = f"{retry_prefix}retry_attempts"
    retry_recoveries_key = f"{retry_prefix}retry_assisted_recoveries"
    retry_exhaustions_key = f"{retry_prefix}retry_exhaustions"
    bucket[retry_attempts_key] = int(bucket.get(retry_attempts_key) or 0) + int(retry.get("retry_attempts") or 0)
    bucket[retry_recoveries_key] = int(bucket.get(retry_recoveries_key) or 0) + int(
        bool(retry.get("recovered_after_retries"))
    )
    bucket[retry_exhaustions_key] = int(bucket.get(retry_exhaustions_key) or 0) + int(
        bool(retry.get("retries_exhausted"))
    )
    rollups[day] = bucket
    rollup_cutoff = (observed_at - timedelta(days=MEDIA_CHECK_ROLLUP_RETENTION_DAYS)).date().isoformat()
    entry["health_daily_rollups"] = {
        key: value for key, value in sorted(rollups.items()) if key >= rollup_cutoff
    }


def _build_health_report(
    items,
    cache: Mapping[str, Any],
    *,
    now: datetime,
    media_reason_counts: Mapping[str, int],
    throughput_reason_counts: Mapping[str, int],
    channels_selected: int,
) -> dict[str, Any]:
    summary: collections.Counter[str] = collections.Counter()
    status_changes: dict[int, int] = {}
    dead_counts: dict[int, int] = {}
    hourly_dead_checks: collections.Counter[int] = collections.Counter()
    hourly_total_checks: collections.Counter[int] = collections.Counter()
    minute_check_counts: collections.Counter[str] = collections.Counter()
    transition_counts: collections.Counter[str] = collections.Counter()
    all_checked_at: list[datetime] = []
    check_intervals_hours: list[float] = []
    status_change_intervals_hours: list[float] = []
    dead_recovery_hours: list[float] = []
    alive_episode_hours: list[float] = []
    current_dead_episode_hours: list[float] = []
    retry_attempts = 0
    retry_assisted_recoveries = 0
    retry_exhaustions = 0
    retry_deferred = 0
    daily_rollup_dates = set()
    total_changes = 0
    total_dead_checks = 0
    placeholder_checks = 0
    placeholder_stream_ids: set[int] = set()
    placeholder_retry_attempts = 0
    placeholder_retry_assisted_recoveries = 0
    placeholder_retry_exhaustions = 0
    placeholder_retry_deferred = 0
    stream_reports = []

    for item in items:
        entry = cache.get(str(item["id"])) or {}
        status = str(entry.get("status") or "unknown").lower()
        summary[status] += 1
        history = entry.get("health_check_history") or []
        timeline: list[tuple[datetime, str, str]] = []
        valid_history = []
        raw_terminal_history = []
        dead = 0
        stream_retry_attempts = 0
        stream_retry_recoveries = 0
        stream_retry_exhaustions = 0
        stream_retry_deferred = 0
        daily_rollup_dates.update((entry.get("health_daily_rollups") or {}).keys())
        for row in history:
            row_status = str(row.get("status") or "").lower()
            checked_at = _parse_datetime(row.get("checked_at"))
            retry = row.get("retry") if isinstance(row.get("retry"), Mapping) else {}
            attempts = int(retry.get("retry_attempts") or 0)
            deferred = int(retry.get("retry_deferred") or 0)
            placeholder_row = str(row.get("error_type") or "") == "placeholder_file"
            if placeholder_row:
                placeholder_retry_attempts += attempts
                placeholder_retry_deferred += deferred
                placeholder_retry_assisted_recoveries += int(bool(retry.get("recovered_after_retries")))
                placeholder_retry_exhaustions += int(bool(retry.get("retries_exhausted")))
            else:
                stream_retry_attempts += attempts
                stream_retry_deferred += deferred
                stream_retry_recoveries += int(bool(retry.get("recovered_after_retries")))
                stream_retry_exhaustions += int(bool(retry.get("retries_exhausted")))
            if (
                row_status not in {"alive", "dead"}
                or checked_at is None
                or not bool(row.get("terminal", True))
            ):
                continue
            raw_terminal_history.append(row)
            if str(row.get("error_type") or "") == "placeholder_file":
                placeholder_checks += 1
                placeholder_stream_ids.add(int(item["id"]))
                continue
            valid_history.append(row)
            recorded_previous = str(row.get("previous_status") or "").lower()
            timeline.append((checked_at, row_status, recorded_previous))
            all_checked_at.append(checked_at)
            local_checked_at = checked_at.astimezone(HEALTH_REPORT_TIMEZONE)
            hourly_total_checks[local_checked_at.hour] += 1
            minute_check_counts[local_checked_at.strftime("%Y%m%d%H%M")] += 1
            if row_status == "dead":
                dead += 1
                total_dead_checks += 1
                hourly_dead_checks[local_checked_at.hour] += 1

        timeline.sort(key=lambda row: row[0])
        changes = 0
        dead_started_at: datetime | None = None
        alive_started_at: datetime | None = None
        prior_status = ""
        for index, (checked_at, row_status, recorded_previous) in enumerate(timeline):
            if index:
                check_intervals_hours.append(
                    (checked_at - timeline[index - 1][0]).total_seconds() / 3600.0
                )
            previous_status = (
                recorded_previous
                if recorded_previous in {"alive", "dead"}
                else prior_status
            )
            if previous_status in {"alive", "dead"} and previous_status != row_status:
                changes += 1
                total_changes += 1
                transition_counts[f"{previous_status}_to_{row_status}"] += 1
                if index:
                    status_change_intervals_hours.append(
                        (checked_at - timeline[index - 1][0]).total_seconds() / 3600.0
                    )
                if row_status == "dead":
                    if alive_started_at is not None:
                        alive_episode_hours.append(
                            (checked_at - alive_started_at).total_seconds() / 3600.0
                        )
                    alive_started_at = None
                    dead_started_at = checked_at
                elif row_status == "alive":
                    if dead_started_at is not None:
                        dead_recovery_hours.append(
                            (checked_at - dead_started_at).total_seconds() / 3600.0
                        )
                    dead_started_at = None
                    alive_started_at = checked_at
            elif index == 0:
                if row_status == "dead":
                    dead_started_at = checked_at
                elif row_status == "alive":
                    alive_started_at = checked_at
            prior_status = row_status

        if prior_status == "dead" and dead_started_at is not None:
            current_dead_episode_hours.append(
                max(0.0, (now - dead_started_at).total_seconds() / 3600.0)
            )

        last_record = valid_history[-1] if valid_history else {}
        raw_last_record = raw_terminal_history[-1] if raw_terminal_history else {}
        reported_status = str(raw_last_record.get("status") or last_record.get("status") or status).lower()
        reported_error_type = str(raw_last_record.get("error_type") or entry.get("error_type") or "")
        is_placeholder = reported_status == "dead" and reported_error_type == "placeholder_file"
        history_len = len(timeline)
        stream_history_span_hours = (
            (timeline[-1][0] - timeline[0][0]).total_seconds() / 3600.0
            if len(timeline) >= 2
            else 0.0
        )
        dead_ratio = dead / history_len if history_len else 0.0
        stream_id = int(item["id"])
        status_changes[stream_id] = changes
        dead_counts[stream_id] = dead
        stream_reports.append(
            {
                "stream_id": stream_id,
                "name": str(item.get("name") or ""),
                "m3u_account_id": item.get("account_id"),
                "source_name": str(item.get("account_name") or ""),
                "channels": [
                    dict(channel)
                    for channel in (item.get("channels") or [])
                    if isinstance(channel, Mapping)
                ],
                "last_status": reported_status,
                "last_reason": str(raw_last_record.get("reason") or last_record.get("reason") or "none"),
                "last_error_type": reported_error_type,
                "health_class": "placeholder" if is_placeholder else reported_status,
                "is_placeholder": is_placeholder,
                "raw_history_len": len(raw_terminal_history),
                "history_len": history_len,
                "status_changes": changes,
                "dead_checks": dead,
                "dead_check_ratio": round(dead_ratio, 4),
                "history_span_hours": round(stream_history_span_hours, 2),
                "retry_attempts": stream_retry_attempts,
                "retry_assisted_recoveries": stream_retry_recoveries,
                "retry_exhaustions": stream_retry_exhaustions,
                "retry_deferred": stream_retry_deferred,
                "last_checked_at": last_record.get("checked_at"),
                "age_hours": _age_hours(last_record.get("checked_at"), now),
            }
        )
        retry_attempts += stream_retry_attempts
        retry_assisted_recoveries += stream_retry_recoveries
        retry_exhaustions += stream_retry_exhaustions
        retry_deferred += stream_retry_deferred

    if all_checked_at:
        all_checked_at.sort()
        history_span_hours = (all_checked_at[-1] - all_checked_at[0]).total_seconds() / 3600.0
    else:
        history_span_hours = 0.0

    check_interval_p50 = _percentile(check_intervals_hours, 0.5)
    check_interval_p90 = _percentile(check_intervals_hours, 0.9)
    status_change_interval_p50 = _percentile(status_change_intervals_hours, 0.5)
    status_change_interval_p90 = _percentile(status_change_intervals_hours, 0.9)
    dead_recovery_p50 = _percentile(dead_recovery_hours, 0.5)
    dead_recovery_p90 = _percentile(dead_recovery_hours, 0.9)
    alive_episode_p25 = _percentile(alive_episode_hours, 0.25)
    alive_episode_p50 = _percentile(alive_episode_hours, 0.5)
    raw_total_history_rows = sum(len((cache.get(str(item["id"])) or {}).get("health_check_history") or []) for item in items)
    total_history_rows = sum(row["history_len"] for row in stream_reports)

    hourly = []
    for hour in range(24):
        dead = hourly_dead_checks.get(hour, 0)
        total = hourly_total_checks.get(hour, 0)
        ratio = dead / total if total else 0.0
        hourly.append(
            {
                "hour": hour,
                "dead_checks": dead,
                "total_checks": total,
                "dead_ratio": round(ratio, 4),
            }
        )

    unstable_top = sorted(
        stream_reports,
        key=lambda row: (row["dead_checks"], row["status_changes"], row["history_len"]),
        reverse=True,
    )[:20]
    problematic_streams = [
        row for row in stream_reports
        if not row["is_placeholder"]
        and row["history_len"] >= 20
        and row["history_span_hours"] >= 168.0
        and row["dead_check_ratio"] > 0.75
    ]
    problematic_streams.sort(
        key=lambda row: (row["dead_check_ratio"], row["dead_checks"], row["history_len"]),
        reverse=True,
    )
    current_dead_streams = [row for row in stream_reports if row["last_status"] == "dead"]
    current_dead_streams.sort(
        key=lambda row: (row["dead_check_ratio"], row["dead_checks"], row["name"]),
        reverse=True,
    )
    busiest_minute = max(minute_check_counts.values(), default=0)
    concentration_ratio = busiest_minute / total_history_rows if total_history_rows else 0.0

    return {
        "generated_at": now.isoformat(),
        "reporting_timezone": str(HEALTH_REPORT_TIMEZONE),
        "selected_streams": len(items),
        "channels_selected": channels_selected,
        "status_counts": {status: count for status, count in summary.items()},
        "observations": {
            "history_rows": total_history_rows,
            "raw_history_rows": raw_total_history_rows,
            "history_span_hours": round(history_span_hours, 4),
            "status_changes": total_changes,
            "dead_checks": total_dead_checks,
            "checks_per_status_change_ratio": (
                round(total_history_rows / total_changes, 4) if total_changes else None
            ),
            "status_changes_per_check_ratio": round(
                total_changes / max(1, total_history_rows), 4
            ),
            "dead_check_ratio": round(total_dead_checks / max(1, total_history_rows), 4),
            "check_interval_hours": {
                "p50": _round_if_present(check_interval_p50, 4),
                "p90": _round_if_present(check_interval_p90, 4),
            },
            "status_change_interval_hours": {
                "p50": _round_if_present(status_change_interval_p50, 4),
                "p90": _round_if_present(status_change_interval_p90, 4),
            },
            "dead_recovery_duration_hours": {
                "samples": len(dead_recovery_hours),
                "p50": _round_if_present(dead_recovery_p50, 4),
                "p90": _round_if_present(dead_recovery_p90, 4),
                "max": _round_if_present(max(dead_recovery_hours) if dead_recovery_hours else None, 4),
            },
            "alive_episode_duration_hours": {
                "samples": len(alive_episode_hours),
                "p25": _round_if_present(alive_episode_p25, 4),
                "p50": _round_if_present(alive_episode_p50, 4),
            },
            "current_dead_episodes": len(current_dead_episode_hours),
            "check_concentration": {
                "busiest_minute_checks": busiest_minute,
                "busiest_minute_ratio": round(concentration_ratio, 4),
            },
            "transition_counts": dict(transition_counts),
        },
        "reasons": {
            "media_due": dict(media_reason_counts),
            "throughput_due": dict(throughput_reason_counts),
        },
        "playback_throughput": _playback_throughput_report(items, cache),
        "ttl_tuning_guidance": {
            "suggested_health_ttl_hours": _round_if_present(alive_episode_p25, 2),
            "suggested_dead_ttl_hours": _round_if_present(dead_recovery_p50, 2),
        },
        "status_patterns": {
            "unstable_streams": unstable_top,
            "problematic_streams": problematic_streams,
            "current_dead_streams": current_dead_streams,
            "hourly_dead_ratio": hourly,
            "placeholders": {
                "checks": placeholder_checks,
                "streams": len(placeholder_stream_ids),
                "current_streams": [row for row in current_dead_streams if row["is_placeholder"]],
                "excluded_from_general_health_analysis": True,
                "retry_attempts": placeholder_retry_attempts,
                "retry_assisted_recoveries": placeholder_retry_assisted_recoveries,
                "retry_exhaustions": placeholder_retry_exhaustions,
                "retry_capacity_deferrals": placeholder_retry_deferred,
            },
        },
        "retry_reliability": {
            "retry_attempts": retry_attempts,
            "retry_assisted_recoveries": retry_assisted_recoveries,
            "retry_exhaustions": retry_exhaustions,
            "retry_capacity_deferrals": retry_deferred,
        },
        "daily_rollups": {
            "retention_days": MEDIA_CHECK_ROLLUP_RETENTION_DAYS,
            "days_present": len(daily_rollup_dates),
        },
        "stream_stats": {
            "dead_change_counts": dead_counts,
            "status_change_counts": status_changes,
        },
        "top_metrics": {
            "max_status_changes": max(status_changes.values()) if status_changes else 0,
            "max_dead_checks": max(dead_counts.values()) if dead_counts else 0,
            "dead_dominant_streams": [row["stream_id"] for row in problematic_streams],
        },
    }


_SHARED_MEMORY_CAPTURE_ROOT = "/dev/shm/stream-sorter"
_CAPTURE_BYTES_PER_WORKER = 256 * 1024 * 1024
_CAPTURE_HEADROOM_BYTES = 256 * 1024 * 1024


def _select_capture_temp_directory(
    worker_count: int,
    *,
    shared_memory_root: str = _SHARED_MEMORY_CAPTURE_ROOT,
    logger=None,
) -> str | None:
    """Use shared memory only when it has safe headroom and is writable."""
    parent = os.path.dirname(shared_memory_root) or shared_memory_root
    required = _CAPTURE_HEADROOM_BYTES + max(1, int(worker_count)) * _CAPTURE_BYTES_PER_WORKER
    probe_fd = None
    probe_path = None
    try:
        stats = os.statvfs(parent)
        block_size = stats.f_frsize or stats.f_bsize
        if stats.f_bavail * block_size < required:
            return None
        os.makedirs(shared_memory_root, mode=0o700, exist_ok=True)
        probe_fd, probe_path = tempfile.mkstemp(
            prefix=".stream-sort-write-test-",
            dir=shared_memory_root,
        )
        os.write(probe_fd, b"ok")
        os.close(probe_fd)
        probe_fd = None
        os.unlink(probe_path)
        probe_path = None
    except OSError as exc:
        if probe_fd is not None:
            try:
                os.close(probe_fd)
            except OSError:
                pass
        if probe_path:
            try:
                os.unlink(probe_path)
            except OSError:
                pass
        if logger is not None:
            logger.warning(
                "[Analyze Combined] shared-memory capture storage unavailable path=%s error=%s: %s; falling back to system temporary storage",
                shared_memory_root,
                type(exc).__name__,
                exc,
            )
        return None
    return shared_memory_root


def _analyze_local_capture(
    item: Mapping[str, Any],
    base_result: Mapping[str, Any],
    sample_path: str,
    *,
    settings: Mapping[str, Any],
    logger,
) -> dict[str, Any]:
    """Analyze one completed capture and always release its temporary storage."""
    try:
        return dict(
            analyzer.apply_content_analysis(
                base_result,
                sample_path,
                settings=settings,
                user_agent=str(item.get("user_agent") or DEFAULT_USER_AGENT),
                logger=logger,
                local_source=True,
            )
        )
    except Exception as exc:
        result = dict(base_result)
        result.update({"status": "dead", "error_type": "stream_unreachable", "error": str(exc)})
        return result
    finally:
        try:
            os.unlink(sample_path)
        except OSError:
            pass


def _account_key(item: Mapping[str, Any]) -> tuple[str, Any]:
    account_id = item.get("account_id")
    if account_id is not None:
        return "id", account_id
    return "unknown", str(item.get("account_name") or "unknown")


def _fair_account_futures(
    items,
    worker,
    *,
    max_workers: int,
    thread_name_prefix: str,
    capacity_manager=None,
    max_per_account: int | None = None,
):
    """Yield completed work while balancing active slots across M3U accounts."""
    queues: dict[tuple[str, Any], collections.deque] = {}
    account_order: list[tuple[str, Any]] = []
    for item in items:
        key = _account_key(item)
        if key not in queues:
            queues[key] = collections.deque()
            account_order.append(key)
        queues[key].append(item)

    running: collections.Counter = collections.Counter()
    launched: collections.Counter = collections.Counter()
    worker_count = max(1, int(max_workers))
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix=thread_name_prefix) as executor:
        futures = {}
        while futures or any(queues[key] for key in account_order):
            while len(futures) < worker_count:
                if analysis_cancel_requested():
                    break
                pending = [
                    key
                    for key in account_order
                    if queues[key]
                    and (max_per_account is None or running[key] < max_per_account)
                ]
                if not pending:
                    break
                # Prefer an account with fewer active slots; lifetime launch
                # counts make ties round-robin instead of favoring list order.
                candidates = sorted(
                    pending,
                    key=lambda candidate: (
                        running[candidate],
                        launched[candidate],
                        account_order.index(candidate),
                    ),
                )
                submitted = False
                for key in candidates:
                    item = queues[key][0]
                    acquired, reservation = (
                        capacity_manager.try_acquire(item)
                        if capacity_manager is not None
                        else (True, None)
                    )
                    if not acquired:
                        continue
                    item = queues[key].popleft()
                    worker_item = (
                        capacity_manager.prepare_item(item, reservation)
                        if capacity_manager is not None
                        and hasattr(capacity_manager, "prepare_item")
                        else item
                    )
                    try:
                        future = executor.submit(worker, worker_item)
                    except Exception:
                        if capacity_manager is not None:
                            capacity_manager.release(reservation)
                        raise
                    futures[future] = (item, key, reservation)
                    running[key] += 1
                    launched[key] += 1
                    submitted = True
                    break
                if not submitted:
                    break

            if not futures:
                if analysis_cancel_requested():
                    break
                # Every remaining limited source is currently occupied by
                # viewers or other reserved connections. Leave its cached
                # result unchanged and retry it on the next analysis run.
                for key in account_order:
                    while queues[key]:
                        yield queues[key].popleft(), None
                break

            done, _pending = wait(futures, return_when=FIRST_COMPLETED)
            completed_rows = []
            for future in done:
                item, key, reservation = futures.pop(future)
                running[key] -= 1
                if capacity_manager is not None:
                    capacity_manager.release(reservation)
                completed_rows.append((item, future))
            for item, future in completed_rows:
                yield item, future


def _item_from_stream(stream) -> dict[str, Any]:
    account = stream.m3u_account
    try:
        user_agent = account.get_user_agent_string() if account else DEFAULT_USER_AGENT
    except Exception:
        user_agent = DEFAULT_USER_AGENT
    stats_updated_at = getattr(stream, "stream_stats_updated_at", None)
    return {
        "id": stream.id,
        "name": stream.name or "",
        "url": stream.url or "",
        "account_id": getattr(stream, "m3u_account_id", None),
        "account_name": getattr(account, "name", "") if account else "",
        "user_agent": user_agent or DEFAULT_USER_AGENT,
        "dispatcharr_stats": dict(getattr(stream, "stream_stats", None) or {}),
        "dispatcharr_stats_updated_at": _datetime_iso(stats_updated_at),
    }


def _channel_attachments_by_stream(rows) -> dict[int, list[dict[str, Any]]]:
    attachments: dict[int, dict[int, dict[str, Any]]] = {}
    for row in rows:
        stream = getattr(row, "stream", None)
        stream_id = getattr(stream, "id", None)
        channel_id = getattr(row, "channel_id", None)
        if stream_id is None or channel_id is None:
            continue
        channel = getattr(row, "channel", None)
        per_stream = attachments.setdefault(int(stream_id), {})
        per_stream[int(channel_id)] = {
            "channel_id": int(channel_id),
            "channel_name": str(getattr(channel, "name", "") or ""),
            "channel_number": getattr(channel, "channel_number", None),
        }
    return {
        stream_id: list(per_stream.values())
        for stream_id, per_stream in attachments.items()
    }


def _merge_dispatcharr_metadata(
    item: Mapping[str, Any],
    previous: Mapping[str, Any],
    *,
    media_bitrate_relative_tolerance: float = MEDIA_BITRATE_RELATIVE_TOLERANCE,
) -> tuple[dict[str, Any], bool, bool]:
    merged = dict(previous)
    current_hash = analyzer._stream_url_hash(str(item.get("url") or ""))
    if str(merged.get("url_hash") or "") != current_hash:
        return merged, False, False
    dispatcharr_stats = item.get("dispatcharr_stats")
    if not isinstance(dispatcharr_stats, Mapping) or not dispatcharr_stats:
        return merged, False, False
    dispatcharr_updated_at = _parse_datetime(item.get("dispatcharr_stats_updated_at"))
    if dispatcharr_updated_at is None:
        return merged, False, False
    current_metadata_at = _parse_datetime(merged.get("metadata_updated_at") or merged.get("media_checked_at") or merged.get("tested_at"))
    if current_metadata_at is not None and dispatcharr_updated_at <= current_metadata_at:
        return merged, False, False
    previous_stats = dict(merged.get("stats") or {})
    stats = dict(merged.get("stats") or {})
    stats.update(dict(dispatcharr_stats))
    merged["stats"] = stats
    merged["metadata_updated_at"] = dispatcharr_updated_at.isoformat()
    merged["metadata_source"] = "dispatcharr_stream_stats"
    merged["dispatcharr_stats_updated_at"] = dispatcharr_updated_at.isoformat()
    merged["stream_id"] = item.get("id")
    merged["stream_name"] = item.get("name")
    merged["m3u_account_id"] = item.get("account_id")
    merged["m3u_account_name"] = item.get("account_name")
    merged["url_hash"] = current_hash
    changed = _media_stats_changed_for_throughput(
        previous_stats,
        stats,
        media_bitrate_relative_tolerance=media_bitrate_relative_tolerance,
    )
    return merged, True, changed


def _sync_dispatcharr_metadata(
    items,
    cache,
    *,
    media_bitrate_relative_tolerance: float = MEDIA_BITRATE_RELATIVE_TOLERANCE,
) -> tuple[int, set[int]]:
    refreshed = 0
    changed_ids: set[int] = set()
    for item in items:
        key = str(item["id"])
        previous = cache.get(key)
        if not isinstance(previous, Mapping):
            continue
        merged, did_refresh, signature_changed = _merge_dispatcharr_metadata(
            item,
            previous,
            media_bitrate_relative_tolerance=media_bitrate_relative_tolerance,
        )
        if not did_refresh:
            continue
        cache[key] = merged
        refreshed += 1
        if signature_changed:
            changed_ids.add(int(item["id"]))
    return refreshed, changed_ids


def _sync_runtime_playback_health(
    items,
    cache,
    reliability_cache,
    *,
    min_playback_seconds: float,
    min_clean_playback_seconds: float,
    ttl_hours: float,
    now: datetime,
) -> int:
    """Use fresh schema-2 playback evidence as a reachability observation."""
    refreshed = 0
    streams = reliability_cache.get("streams") if isinstance(reliability_cache, Mapping) else {}
    streams = streams if isinstance(streams, Mapping) else {}
    for item in items:
        telemetry = streams.get(str(item["id"]))
        evidence = telemetry.get("reliability_evidence") if isinstance(telemetry, Mapping) else None
        if not isinstance(evidence, Mapping):
            continue
        try:
            playback_seconds = float(evidence.get("playback_seconds") or 0.0)
        except (TypeError, ValueError):
            continue
        evidence_observed_at = _parse_datetime(evidence.get("updated_at"))
        clean_observed_at = _parse_datetime(telemetry.get("last_clean_playback_at"))
        try:
            clean_seconds = float(telemetry.get("last_clean_playback_seconds") or 0.0)
        except (TypeError, ValueError):
            clean_seconds = 0.0
        clean_qualifies = clean_seconds >= min_clean_playback_seconds and clean_observed_at is not None
        long_qualifies = playback_seconds >= min_playback_seconds and evidence_observed_at is not None
        if not clean_qualifies and not long_qualifies:
            continue
        observed_at = clean_observed_at if clean_qualifies else evidence_observed_at
        assert observed_at is not None
        if ttl_hours > 0 and (now - observed_at).total_seconds() / 3600.0 >= ttl_hours:
            continue

        key = str(item["id"])
        current_hash = analyzer._stream_url_hash(str(item.get("url") or ""))
        previous = cache.get(key)
        entry = dict(previous) if isinstance(previous, Mapping) and str(previous.get("url_hash") or "") == current_hash else {}
        previous_observation = _parse_datetime(entry.get("playback_health_checked_at"))
        if previous_observation is not None and previous_observation >= observed_at:
            continue
        latest_health_observation = max(
            (
                parsed for parsed in (
                    _parse_datetime(entry.get("health_checked_at")),
                    _parse_datetime(entry.get("media_checked_at")),
                    _parse_datetime(entry.get("tested_at")),
                )
                if parsed is not None
            ),
            default=None,
        )
        if str(entry.get("status") or "").lower() == "dead":
            continue
        if latest_health_observation is not None and latest_health_observation >= observed_at:
            continue
        previous_status = str(entry.get("status") or "unknown").lower()
        entry.update({
            "status": "alive",
            "error": "",
            "error_type": None,
            "stream_id": item.get("id"),
            "stream_name": item.get("name"),
            "m3u_account_id": item.get("account_id"),
            "m3u_account_name": item.get("account_name"),
            "url_hash": current_hash,
            "playback_health_checked_at": observed_at.isoformat(),
            "health_source": "runtime_playback",
            "playback_evidence_seconds": round(playback_seconds, 3),
            "clean_playback_seconds": round(clean_seconds, 3) if clean_qualifies else None,
        })
        dispatcharr_stats = item.get("dispatcharr_stats")
        dispatcharr_updated_at = _parse_datetime(item.get("dispatcharr_stats_updated_at"))
        if isinstance(dispatcharr_stats, Mapping) and dispatcharr_stats and dispatcharr_updated_at is not None:
            entry["stats"] = dict(dispatcharr_stats)
            entry["metadata_updated_at"] = dispatcharr_updated_at.isoformat()
            entry["metadata_source"] = "dispatcharr_stream_stats"
            entry["dispatcharr_stats_updated_at"] = dispatcharr_updated_at.isoformat()
        _append_health_history(
            entry,
            reason="runtime_playback",
            previous_status=previous_status,
            new_status="alive",
            tested_at=observed_at.isoformat(),
            result={"health_source": "runtime_playback"},
        )
        cache[key] = entry
        refreshed += 1
    return refreshed


def _sync_runtime_playback_evidence(
    items,
    cache,
    reliability_cache,
    *,
    min_clean_playback_seconds: float,
    min_throughput_playback_seconds: float,
) -> int:
    refreshed = 0
    streams = reliability_cache.get("streams") if isinstance(reliability_cache, Mapping) else {}
    streams = streams if isinstance(streams, Mapping) else {}
    for item in items:
        telemetry = streams.get(str(item["id"]))
        if not isinstance(telemetry, Mapping):
            continue
        key = str(item["id"])
        current_hash = analyzer._stream_url_hash(str(item.get("url") or ""))
        previous = cache.get(key)
        entry = dict(previous) if isinstance(previous, Mapping) and str(previous.get("url_hash") or "") == current_hash else {}
        changed = False

        clean_at = _parse_datetime(telemetry.get("last_clean_playback_at"))
        try:
            clean_seconds = float(telemetry.get("last_clean_playback_seconds") or 0.0)
        except (TypeError, ValueError):
            clean_seconds = 0.0
        clean_provider_matches = _provider_matches(
            item.get("account_id"),
            telemetry.get("last_clean_playback_m3u_account_id"),
        )
        current_content_at = _parse_datetime(entry.get("content_checked_at"))
        if (
            clean_at is not None
            and clean_seconds >= min_clean_playback_seconds
            and clean_provider_matches
            and (current_content_at is None or clean_at > current_content_at)
        ):
            entry["content_checked_at"] = clean_at.isoformat()
            entry["content_source"] = "dispatcharr_playback_assumed"
            entry["content_playback_seconds"] = round(clean_seconds, 3)
            entry["content_m3u_account_id"] = item.get("account_id")
            entry["content_validation_separated"] = True
            changed = True

        imported = list(entry.get("playback_throughput_history") or [])
        imported_keys = {
            (
                str(row.get("observed_at") or ""),
                str(row.get("kind") or ""),
                str(row.get("reason") or ""),
                str(row.get("m3u_account_id") or ""),
            )
            for row in imported if isinstance(row, Mapping)
        }
        stats = entry.get("stats") or item.get("dispatcharr_stats") or {}
        _width, height = parse_resolution(stats)
        nominal_kbps = estimate_nominal_throughput_kbps(height, parse_fps(stats))
        for raw in telemetry.get("playback_throughput_history") or []:
            if not isinstance(raw, Mapping):
                continue
            observed_at = _parse_datetime(raw.get("observed_at"))
            if observed_at is None:
                continue
            row_key = (
                observed_at.isoformat(),
                str(raw.get("kind") or ""),
                str(raw.get("reason") or ""),
                str(raw.get("m3u_account_id") or ""),
            )
            if row_key in imported_keys:
                continue
            row = dict(raw)
            row["observed_at"] = observed_at.isoformat()
            row["nominal_video_kbps"] = nominal_kbps
            attribution_valid = _provider_matches(item.get("account_id"), row.get("m3u_account_id"))
            row["provider_attribution_valid"] = attribution_valid
            if row.get("kind") == "delivery_failure":
                row["status"] = "insufficient"
                row["source"] = "dispatcharr_playback_failure"
                eligible = attribution_valid
            else:
                measured = row.get("measured_mbps")
                if not isinstance(measured, (int, float)) or nominal_kbps <= 0:
                    imported.append(row)
                    imported_keys.add(row_key)
                    changed = True
                    continue
                ratio = float(measured) / (float(nominal_kbps) / 1000.0)
                row["ratio"] = round(ratio, 4)
                row["source"] = "dispatcharr_playback"
                if ratio >= SUSTAINED_PLAYBACK_HEALTHY_RATIO:
                    row["status"] = "healthy"
                elif ratio >= SUSTAINED_PLAYBACK_MINIMUM_RATIO:
                    row["status"] = "marginal"
                else:
                    row["status"] = "insufficient"
                eligible = (
                    attribution_valid
                    and bool(row.get("eligible_for_throughput"))
                    and float(row.get("runtime_seconds") or 0.0) >= min_throughput_playback_seconds
                )
            imported.append(row)
            imported_keys.add(row_key)
            changed = True
            current_throughput_at = _parse_datetime((entry.get("throughput") or {}).get("checked_at"))
            if eligible and (current_throughput_at is None or observed_at > current_throughput_at):
                result = {
                    "status": row["status"],
                    "tested_at": observed_at.isoformat(),
                    "measured_mbps": row.get("measured_mbps"),
                    "nominal_video_kbps": nominal_kbps,
                    "capacity_ratio": row.get("ratio"),
                    "duration_seconds": row.get("runtime_seconds"),
                    "source": row.get("source"),
                    "reason": row.get("reason"),
                }
                entry = _merge_throughput_result(item, entry, result)

        cutoff = datetime.now(timezone.utc) - timedelta(days=MEDIA_CHECK_HISTORY_RETENTION_DAYS)
        entry["playback_throughput_history"] = [
            row for row in imported
            if (_parse_datetime(row.get("observed_at")) or cutoff) >= cutoff
        ][-MEDIA_CHECK_HISTORY_MAX_ROWS:]
        if changed:
            entry["stream_id"] = item.get("id")
            entry["stream_name"] = item.get("name")
            entry["url_hash"] = current_hash
            cache[key] = entry
            refreshed += 1
    return refreshed


def _merge_media_result(
    item,
    previous,
    result,
    *,
    analysis_reason: str | None = None,
    record_history: bool = True,
    history_previous_status: str | None = None,
) -> dict[str, Any]:
    merged = dict(previous or {})
    previous_throughput = merged.get("throughput")
    previous_stats = merged.get("stats")
    previous_status = str(
        history_previous_status
        if history_previous_status is not None
        else merged.get("status") or "unknown"
    ).lower()
    previous_url_hash = str(merged.get("url_hash") or "")
    previous_provider = merged.get("m3u_account_id")
    merged.update(dict(result))
    checked = result.get("tested_at") or analyzer._utc_now_iso()
    merged["health_checked_at"] = checked
    merged["media_checked_at"] = checked
    merged["ffprobe_checked_at"] = checked
    merged["content_validation_separated"] = True
    details = result.get("details")
    content = details.get("content") if isinstance(details, Mapping) else None
    if isinstance(content, Mapping) and content.get("measured"):
        merged["content_checked_at"] = content.get("tested_at") or checked
        merged["content_m3u_account_id"] = item.get("account_id")
    merged["health_source"] = "stream_sort_analyzer"
    merged["stream_id"] = item.get("id")
    merged["stream_name"] = item.get("name")
    merged["m3u_account_id"] = item.get("account_id")
    merged["m3u_account_name"] = item.get("account_name")
    merged["url_hash"] = analyzer._stream_url_hash(str(item.get("url") or ""))
    status = str(result.get("status") or "unknown").lower()
    if status == "dead":
        merged["dead_checked_at"] = checked
        if not result.get("retry_pending"):
            try:
                prior_streak = int(merged.get("consecutive_dead_results") or 0)
            except (TypeError, ValueError):
                prior_streak = 0
            merged["consecutive_dead_results"] = prior_streak + 1 if previous_status == "dead" else 1
    elif status == "alive":
        merged.pop("dead_checked_at", None)
        if record_history:
            merged.pop("consecutive_dead_results", None)
    result_stats = result.get("stats")
    if isinstance(result_stats, Mapping) and result_stats:
        merged["stats"] = dict(result_stats)
        merged["metadata_updated_at"] = checked
        merged["metadata_source"] = "stream_sort_analyzer"
        history = list(merged.get("ffprobe_stats_history") or [])
        same_identity = (
            previous_url_hash == merged["url_hash"]
            and (
                previous_provider in (None, "")
                or item.get("account_id") in (None, "")
                or _provider_matches(previous_provider, item.get("account_id"))
            )
        )
        if not same_identity:
            history = []
        if not history and isinstance(previous_stats, Mapping) and previous_stats:
            history.append({
                "checked_at": (previous or {}).get("ffprobe_checked_at") or (previous or {}).get("metadata_updated_at"),
                "stats": dict(previous_stats),
            })
        history.append({"checked_at": checked, "stats": dict(result_stats)})
        merged["ffprobe_stats_history"] = history[-FFPROBE_STATS_HISTORY_MAX_ROWS:]
    elif status == "skipped" and isinstance(previous_stats, Mapping) and previous_stats:
        merged["stats"] = dict(previous_stats)
    if status == "dead":
        if str(result.get("error_type") or "") == "placeholder_file":
            if isinstance(previous_throughput, Mapping):
                merged["throughput"] = dict(previous_throughput)
            else:
                merged.pop("throughput", None)
        else:
            merged["throughput"] = {
                "status": "unknown",
                "tested_at": checked,
                "checked_at": checked,
                "url_hash": merged["url_hash"],
                "error": "throughput invalidated because media analysis marked the stream dead",
            }
    elif isinstance(previous_throughput, Mapping):
        merged["throughput"] = dict(previous_throughput)
    else:
        merged.pop("throughput", None)
    if record_history:
        _append_health_history(
            merged,
            reason=analysis_reason,
            previous_status=previous_status,
            new_status=str(result.get("status") or "unknown").lower(),
            tested_at=checked,
            result=result,
        )
    return merged


def _content_checks_applicable(result: Mapping[str, Any], settings: Mapping[str, Any]) -> bool:
    if analyzer._as_bool(settings.get("black_screen_detection"), True):
        return True
    if analyzer._as_bool(settings.get("frozen_video_detection"), True):
        return True
    details = result.get("details") if isinstance(result.get("details"), Mapping) else {}
    stats = result.get("stats") if isinstance(result.get("stats"), Mapping) else {}
    has_audio = bool(details.get("has_audio") or stats.get("audio_codec"))
    return analyzer._as_bool(settings.get("silent_audio_detection"), True) and has_audio


def _content_skipped_result(result: Mapping[str, Any]) -> dict[str, Any]:
    updated = dict(result)
    details = dict(updated.get("details") or {})
    checked = analyzer._utc_now_iso()
    details["content"] = {
        "black": None,
        "frozen": None,
        "mean_volume_db": None,
        "measured": True,
        "skipped": True,
        "skip_reason": "no_applicable_detectors",
        "tested_at": checked,
    }
    updated.update({
        "status": "alive",
        "error_type": None,
        "error": "",
        "tested_at": checked,
        "details": details,
    })
    return updated


def _merge_content_result(
    item,
    previous,
    result,
    *,
    analysis_reason: str | None = None,
    history_previous_status: str | None = None,
) -> dict[str, Any]:
    merged = dict(previous or {})
    previous_status = str(
        history_previous_status
        if history_previous_status is not None
        else merged.get("status") or "unknown"
    ).lower()
    details = dict(result.get("details") or {})
    content = details.get("content") if isinstance(details.get("content"), Mapping) else {}
    checked = content.get("tested_at") or result.get("tested_at") or analyzer._utc_now_iso()
    for key in ("status", "error_type", "error", "retry_pending", "retry_telemetry"):
        if key in result:
            merged[key] = result.get(key)
    merged["details"] = details
    merged["tested_at"] = checked
    merged["health_source"] = "stream_sort_content"
    merged["content_validation_separated"] = True
    if content.get("measured"):
        merged["content_checked_at"] = checked
        merged["content_source"] = "stream_sort_content"
        merged["content_m3u_account_id"] = item.get("account_id")
    merged["stream_id"] = item.get("id")
    merged["stream_name"] = item.get("name")
    merged["m3u_account_id"] = item.get("account_id")
    merged["m3u_account_name"] = item.get("account_name")
    merged["url_hash"] = analyzer._stream_url_hash(str(item.get("url") or ""))
    if str(result.get("status") or "unknown").lower() == "dead":
        merged["dead_checked_at"] = checked
        if not result.get("retry_pending"):
            try:
                prior_streak = int(merged.get("consecutive_dead_results") or 0)
            except (TypeError, ValueError):
                prior_streak = 0
            merged["consecutive_dead_results"] = prior_streak + 1 if previous_status == "dead" else 1
        merged["throughput"] = {
            "status": "unknown",
            "tested_at": checked,
            "checked_at": checked,
            "url_hash": merged["url_hash"],
            "error": "throughput invalidated because content analysis marked the stream dead",
        }
    elif str(result.get("status") or "unknown").lower() == "alive":
        merged.pop("dead_checked_at", None)
        merged.pop("consecutive_dead_results", None)
    _append_health_history(
        merged,
        reason=f"content_{analysis_reason or 'unknown'}",
        previous_status=previous_status,
        new_status=str(result.get("status") or "unknown").lower(),
        tested_at=checked,
        result=result,
    )
    return merged


def _merge_throughput_result(item, entry, result) -> dict[str, Any]:
    merged = dict(entry)
    throughput = dict(result)
    checked_at = throughput.get("tested_at") or analyzer._utc_now_iso()
    throughput["checked_at"] = checked_at
    throughput["url_hash"] = analyzer._stream_url_hash(str(item.get("url") or ""))
    throughput["m3u_account_id"] = item.get("account_id")
    throughput["m3u_account_name"] = item.get("account_name")
    throughput.pop("expires_at", None)
    merged["throughput"] = throughput
    return merged


def _retained_throughput_measurement_ids(attempted_ids, cache: Mapping[str, Any]) -> set[int]:
    retained = set()
    for sid in attempted_ids:
        entry = cache.get(str(sid)) or {}
        throughput = entry.get("throughput") if isinstance(entry, Mapping) else None
        measured = throughput.get("measured_mbps") if isinstance(throughput, Mapping) else None
        if isinstance(measured, (int, float)) and not isinstance(measured, bool):
            retained.add(int(sid))
    return retained


def _migrate_legacy_throughput(items, cache) -> int:
    legacy = load_throughput_cache(LEGACY_CACHE_PATH)
    migrated = 0
    for item in items:
        key = str(item["id"])
        entry = cache.get(key)
        if not isinstance(entry, Mapping) or isinstance(entry.get("throughput"), Mapping):
            continue
        current_hash = analyzer._stream_url_hash(str(item.get("url") or ""))
        if str(entry.get("url_hash") or "") != current_hash:
            continue
        result = legacy.get(key)
        if not isinstance(result, Mapping):
            continue
        cache[key] = _merge_throughput_result(item, entry, result)
        migrated += 1
    return migrated


def _analysis_reason(
    entry: Mapping[str, Any] | None,
    *,
    url_hash: str,
    health_ttl_hours: float,
    content_ttl_hours: float,
    metadata_ttl_hours: float,
    provider_id: Any = None,
    dead_ttl_hours: float | None = None,
    ttl_jitter_percent: float = 0.0,
    now: datetime,
) -> str | None:
    reason = ffprobe_check_reason(
        entry,
        url_hash=url_hash,
        ttl_hours=health_ttl_hours,
        dead_ttl_hours=dead_ttl_hours,
        provider_id=provider_id,
        ttl_jitter_percent=ttl_jitter_percent,
        now=now,
    )
    if reason:
        return f"ffprobe_{reason}"
    return None


def _unique_items_from_rows(rows, channel_attachments) -> list[dict[str, Any]]:
    items = []
    seen = set()
    for row in rows:
        stream = row.stream
        if stream.id in seen:
            continue
        seen.add(stream.id)
        item = _item_from_stream(stream)
        item["channels"] = channel_attachments.get(int(stream.id), [])
        items.append(item)
    return items


@exclusive_analysis_execution
def analyze_assigned_streams(
    settings: Mapping[str, Any],
    *,
    logger,
    cache_path: str = analyzer.ANALYSIS_CACHE_PATH,
    health_report_path: str | None = None,
) -> dict[str, Any]:
    from apps.channels.models import ChannelStream
    from .sorter import resolve_channel_scope

    run_started = time.monotonic()
    if health_report_path is None:
        health_report_path = (
            ANALYSIS_HEALTH_REPORT_PATH
            if cache_path == analyzer.ANALYSIS_CACHE_PATH
            else os.path.join(
                os.path.dirname(cache_path) or ".",
                "dispatcharr_stream_sort_health_report.json",
            )
        )

    channel_ids, filter_summary = resolve_channel_scope(settings)
    workers = max(1, min(16, analyzer._as_int(settings.get("analysis_workers"), 2)))
    retries = max(0, min(5, analyzer._as_int(settings.get("analysis_retries"), 3)))
    account_delay = max(0.0, analyzer._as_float(settings.get("analysis_per_account_delay_seconds"), 1.0))
    ffprobe_ttl_hours = max(0.0, analyzer._as_float(settings.get("stream_data_ttl_hours"), 18.0))
    metadata_ttl_hours = ffprobe_ttl_hours
    health_ttl_hours = ffprobe_ttl_hours
    dead_ttl_hours = max(0.0, analyzer._as_float(settings.get("dead_content_ttl_hours"), 1.0))
    content_ttl_hours = max(0.0, analyzer._as_float(settings.get("content_validation_ttl_hours"), 168.0))
    media_bitrate_relative_tolerance = min(
        1.0,
        max(
            0.0,
            analyzer._as_float(settings.get("media_bitrate_relative_tolerance_percent"), 30.0) / 100.0,
        ),
    )
    playback_health_reuse = analyzer._as_bool(settings.get("playback_health_reuse"), True)
    playback_health_min_seconds = max(60.0, analyzer._as_float(settings.get("playback_health_min_seconds"), 300.0))
    playback_health_clean_min_seconds = max(30.0, analyzer._as_float(settings.get("playback_health_clean_min_seconds"), 60.0))
    throughput_ttl_hours = max(0.0, analyzer._as_float(settings.get("healthy_throughput_ttl_hours"), 48.0))
    degraded_throughput_ttl_hours = max(0.0, analyzer._as_float(settings.get("degraded_throughput_ttl_hours"), 24.0))
    unknown_throughput_ttl_hours = max(0.0, analyzer._as_float(settings.get("unknown_throughput_ttl_hours"), 4.0))
    analysis_ttl_jitter_percent = max(0.0, min(100.0, analyzer._as_float(settings.get("analysis_ttl_jitter_percent"), 30.0)))
    throughput_duration = max(1.0, analyzer._as_float(settings.get("probe_duration_seconds"), 6.0))
    throughput_timeout = max(throughput_duration + 2.0, analyzer._as_float(settings.get("probe_timeout_seconds"), 10.0))
    throughput_account_delay = max(0.0, analyzer._as_float(settings.get("probe_per_account_delay_seconds"), 1.0))

    queryset = ChannelStream.objects.select_related("channel", "stream", "stream__m3u_account").order_by("channel_id", "order", "id")
    if channel_ids is not None:
        queryset = queryset.filter(channel_id__in=channel_ids)
    rows = list(queryset)
    channel_attachments = _channel_attachments_by_stream(rows)

    items = _unique_items_from_rows(rows, channel_attachments)

    total = len(items)
    cache = analyzer.load_analysis_cache(cache_path)

    playback_health_refreshed = 0
    if playback_health_reuse:
        playback_health_refreshed = _sync_runtime_playback_evidence(
            items,
            cache,
            load_reliability_cache(RELIABILITY_PATH),
            min_clean_playback_seconds=playback_health_clean_min_seconds,
            min_throughput_playback_seconds=playback_health_min_seconds,
        )
        if playback_health_refreshed:
            analyzer.save_analysis_cache(cache, cache_path)
            logger.info(
                "[Analyze] Imported Dispatcharr playback content/throughput evidence for %d streams",
                playback_health_refreshed,
            )

    migrated = _migrate_legacy_throughput(items, cache)
    if migrated:
        analyzer.save_analysis_cache(cache, cache_path)
        logger.info("[Analyze] Migrated %d matching legacy throughput measurements into the unified cache", migrated)

    dispatcharr_metadata_refreshed, dispatcharr_metadata_changed_ids = _sync_dispatcharr_metadata(
        items,
        cache,
        media_bitrate_relative_tolerance=media_bitrate_relative_tolerance,
    )
    if dispatcharr_metadata_refreshed:
        analyzer.save_analysis_cache(cache, cache_path)
        logger.info("[Analyze] Refreshed basic metadata for %d streams from newer Dispatcharr stream_stats", dispatcharr_metadata_refreshed)

    scan_start_status_by_id = {
        int(item["id"]): str((cache.get(str(item["id"])) or {}).get("status") or "unknown").lower()
        for item in items
    }
    known_placeholder_ids = {
        int(item["id"])
        for item in items
        if _is_placeholder_result(cache.get(str(item["id"])))
    }
    now = datetime.now(timezone.utc)
    media_due = []
    content_reason_by_id = {}
    for item in items:
        sid = int(item["id"])
        entry = cache.get(str(sid))
        url_hash = analyzer._stream_url_hash(str(item.get("url") or ""))
        reason = _analysis_reason(
            entry,
            url_hash=url_hash,
            health_ttl_hours=health_ttl_hours,
            content_ttl_hours=content_ttl_hours,
            metadata_ttl_hours=metadata_ttl_hours,
            provider_id=item.get("account_id"),
            dead_ttl_hours=dead_ttl_hours,
            ttl_jitter_percent=analysis_ttl_jitter_percent,
            now=now,
        )
        content_reason = None
        if sid not in known_placeholder_ids:
            content_reason = content_check_reason(
                entry,
                url_hash=url_hash,
                ttl_hours=content_ttl_hours,
                dead_ttl_hours=dead_ttl_hours,
                provider_id=item.get("account_id"),
                ttl_jitter_percent=analysis_ttl_jitter_percent,
                now=now,
            )
        if content_reason:
            content_reason_by_id[sid] = content_reason
        if reason:
            media_due.append((item, reason))

    media_reason_counts = collections.Counter(reason for _, reason in media_due)

    media_due_ids = {int(item["id"]) for item, _ in media_due}
    initial_due_ids = media_due_ids | set(content_reason_by_id)
    initial_throughput_due = 0
    initial_fully_cached = 0
    for item in items:
        entry = cache.get(str(item["id"])) or {}
        status = str(entry.get("status") or "unknown").lower()
        if status == "alive":
            reason = throughput_check_reason(
                entry,
                url_hash=analyzer._stream_url_hash(str(item.get("url") or "")),
                ttl_hours=throughput_ttl_hours,
                dead_ttl_hours=dead_ttl_hours,
                degraded_ttl_hours=degraded_throughput_ttl_hours,
                unknown_ttl_hours=unknown_throughput_ttl_hours,
                provider_id=item.get("account_id"),
                ttl_jitter_percent=analysis_ttl_jitter_percent,
                now=now,
            )
            if reason or int(item["id"]) in dispatcharr_metadata_changed_ids:
                initial_throughput_due += 1
            elif int(item["id"]) not in initial_due_ids:
                initial_fully_cached += 1
        elif int(item["id"]) not in initial_due_ids:
            initial_fully_cached += 1

    logger.info(
        "[Analyze] Starting: streams=%d ffprobe_due=%d content_due=%d throughput_due=%d fully_cached=%d playback_evidence_refreshed=%d dispatcharr_metadata_refreshed=%d ffprobe_ttl=%.1fh dead_ttl=%.1fh content_ttl=%.1fh healthy_throughput_ttl=%.1fh degraded_throughput_ttl=%.1fh unknown_throughput_ttl=%.1fh ttl_jitter=%.1f%% workers=%d",
        total, len(media_due), len(content_reason_by_id), initial_throughput_due, initial_fully_cached, playback_health_refreshed, dispatcharr_metadata_refreshed,
        ffprobe_ttl_hours, dead_ttl_hours, content_ttl_hours, throughput_ttl_hours, degraded_throughput_ttl_hours, unknown_throughput_ttl_hours, analysis_ttl_jitter_percent, workers,
    )
    if not items:
        runtime_seconds = max(0.0, time.monotonic() - run_started)
        health_summary = _overall_health_text(collections.Counter(), placeholder_count=0)
        logger.info(
            "[Analyze] Complete: streams=0 media_checked=0 content_checked=0 throughput_attempted=0 throughput_checked=0 capacity_deferred=0 fully_cached=0 playback_health_refreshed=0 dispatcharr_metadata_refreshed=0 | health %s | throughput %s | runtime=%s",
            health_summary,
            _overall_throughput_text(collections.Counter()),
            analyzer._format_eta(runtime_seconds),
        )
        _save_json(
            health_report_path,
            _build_health_report(
                [],
                cache,
                now=datetime.now(timezone.utc),
                media_reason_counts=media_reason_counts,
                throughput_reason_counts={},
                channels_selected=len({row.channel_id for row in rows}),
            ),
        )
        return {
            "streams_analyzed": 0,
            "streams_selected": 0,
            "media_checked": 0,
            "content_checked": 0,
            "throughput_checked": 0,
            "throughput_attempted": 0,
            "capacity_deferred": 0,
            "fully_cached": 0,
            "dispatcharr_metadata_refreshed": 0,
            "playback_health_refreshed": 0,
            "channels_selected": len({row.channel_id for row in rows}),
            "filters": filter_summary,
            "status_counts": {},
            "throughput_status_counts": {},
            "placeholder_count": 0,
            "other_dead_count": 0,
            "health_summary": health_summary,
            "total_runtime_seconds": round(runtime_seconds, 3),
            "total_runtime": analyzer._format_eta(runtime_seconds),
            "cache_path": cache_path,
            "analysis_health_report_path": health_report_path,
        }

    capacity_manager = build_capacity_manager(items, logger=logger)

    canceled = False
    media_results = {}
    media_retry_telemetry = {}
    media_capacity_deferred_ids = set()
    previous_media_stats = {
        int(item["id"]): (cache.get(str(item["id"])) or {}).get("stats")
        for item, _ in media_due
    }
    previous_media_history = {
        int(item["id"]): list((cache.get(str(item["id"])) or {}).get("ffprobe_stats_history") or [])
        for item, _ in media_due
    }
    reason_by_id = {int(item["id"]): reason for item, reason in media_due}
    limiter = analyzer._PerAccountStartLimiter(account_delay)
    media_started = time.monotonic()

    def full_media_probe(item):
        result = analyzer.analyze_stream(
            str(item.get("url") or ""),
            stream_id=item.get("id"),
            stream_name=str(item.get("name") or ""),
            settings=settings,
            user_agent=str(item.get("user_agent") or DEFAULT_USER_AGENT),
            logger=logger,
            include_content=False,
        )
        details = dict(result.get("details") or {})
        details["probe_mode"] = "full_ffprobe_5s"
        result["details"] = details
        return result

    def run_media(item, *, force_full: bool = False):
        analyzer._RATE_LIMIT_GUARD.wait_if_throttled()
        limiter.wait(item.get("account_id"))
        sid = int(item["id"])
        known_placeholder = sid in known_placeholder_ids and not force_full
        if force_full or not known_placeholder:
            result = full_media_probe(item)
            if result.get("error_type") == "rate_limited":
                analyzer._RATE_LIMIT_GUARD.record_hit(logger)
            return result
        probe_settings = dict(settings)
        probe_settings["analysis_duration_seconds"] = 1
        result = analyzer.analyze_stream(
            str(item.get("url") or ""),
            stream_id=item.get("id"),
            stream_name=str(item.get("name") or ""),
            settings=probe_settings,
            user_agent=str(item.get("user_agent") or DEFAULT_USER_AGENT),
            logger=logger,
            include_content=False,
        )
        details = dict(result.get("details") or {})
        details["probe_mode"] = "placeholder_confirm_1s"
        result["details"] = details
        if str(result.get("error_type") or "") != "placeholder_file":
            result = full_media_probe(item)
            details = dict(result.get("details") or {})
            details["placeholder_recheck"] = "one_second_gate_not_placeholder_full_ffprobe_required"
            result["details"] = details
            result["placeholder_recovery_full_probe"] = True
        else:
            details = dict(result.get("details") or {})
            details["placeholder_recheck"] = "one_second_gate_confirmed_placeholder"
            result["details"] = details
            result["placeholder_confirmation"] = True
        if result.get("error_type") == "rate_limited":
            analyzer._RATE_LIMIT_GUARD.record_hit(logger)
        return result

    def run_media_retry(item):
        return run_media(item, force_full=True)

    if media_due:
        media_items = [item for item, _reason in media_due]
        for completed, (item, future) in enumerate(
            _fair_account_futures(
                media_items,
                run_media,
                max_workers=workers,
                thread_name_prefix="stream-sort-media",
                capacity_manager=capacity_manager,
            ),
            start=1,
        ):
            if future is None:
                sid = int(item["id"])
                media_capacity_deferred_ids.add(sid)
                elapsed = max(time.monotonic() - media_started, 0.001)
                eta = elapsed / completed * (len(media_due) - completed) if completed < len(media_due) else 0.0
                logger.info(
                    "[Analyze Media] %d%% (%d/%d) stream=%s reason=%s health=deferred_capacity | M3U connection limit is occupied; cached result preserved | ETA=%s",
                    int(round(completed / len(media_due) * 100)),
                    completed,
                    len(media_due),
                    sid,
                    reason_by_id[sid],
                    analyzer._format_eta(eta),
                )
                continue
            try:
                result = future.result()
            except Exception as exc:
                result = {
                    "tested_at": analyzer._utc_now_iso(),
                    "status": "dead",
                    "error_type": "other",
                    "error": str(exc),
                    "stats": {},
                    "details": {},
                }
            sid = int(item["id"])
            media_results[sid] = dict(result)
            initial_error_type = str(result.get("error_type") or "")
            media_retry_telemetry[sid] = {
                "initial_failed": str(result.get("status") or "unknown").lower() != "alive",
                "initial_status": str(result.get("status") or "unknown").lower(),
                "initial_error_type": initial_error_type,
                "retry_attempts": 0,
                "retry_deferred": 0,
                "failure_types": [initial_error_type] if initial_error_type else [],
                "current_retry_attempts": 0,
            }
            elapsed = max(time.monotonic() - media_started, 0.001)
            eta = elapsed / completed * (len(media_due) - completed) if completed < len(media_due) else 0.0
            _log_media_progress(
                logger,
                prefix="[Analyze Media]",
                completed=completed,
                phase_total=len(media_due),
                item=item,
                reason=reason_by_id[sid],
                result=result,
                items=items,
                cache=cache,
                media_results=media_results,
                cached_media=total - len(media_due),
                pending_media=len(media_due) - completed,
                eta_seconds=eta,
            )

        canceled = analysis_cancel_requested()

        by_id = {int(item["id"]): item for item, _ in media_due}
        for retry_pass in range(1, retries + 1):
            if canceled:
                break
            retry_ids = [
                sid for sid, result in media_results.items()
                if str(result.get("error_type") or "") in analyzer.RETRYABLE_ERROR_TYPES
                and not _is_confirmed_placeholder_result(result)
            ]
            if not retry_ids:
                break
            backoff = max(1.0, account_delay * 3.0)
            logger.info("[Analyze Retry %d/%d] waiting %.1fs before retrying %d media checks", retry_pass, retries, backoff, len(retry_ids))
            deadline = time.monotonic() + backoff
            while time.monotonic() < deadline:
                if analysis_cancel_requested():
                    canceled = True
                    break
                time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
            if canceled:
                break
            retry_items = [by_id[sid] for sid in retry_ids]
            retry_started = time.monotonic()
            for retry_completed, (item, future) in enumerate(
                _fair_account_futures(
                    retry_items,
                    run_media_retry,
                    max_workers=workers,
                    thread_name_prefix="stream-sort-media-retry",
                    capacity_manager=capacity_manager,
                    max_per_account=1,
                ),
                start=1,
            ):
                if future is None:
                    media_retry_telemetry[int(item["id"])]["retry_deferred"] += 1
                    logger.info(
                        "[Analyze Retry %d/%d] stream=%s deferred because its M3U connection limit is occupied",
                        retry_pass,
                        retries,
                        item["id"],
                    )
                    continue
                try:
                    retry_result = dict(future.result())
                except Exception as exc:
                    retry_result = {
                        "tested_at": analyzer._utc_now_iso(),
                        "status": "dead",
                        "error_type": "other",
                        "error": str(exc),
                        "stats": {},
                        "details": {},
                    }
                sid = int(item["id"])
                media_results[sid] = retry_result
                telemetry = media_retry_telemetry[sid]
                telemetry["retry_attempts"] += 1
                telemetry["current_retry_attempts"] += 1
                error_type = str(retry_result.get("error_type") or "")
                if error_type and error_type not in telemetry["failure_types"]:
                    telemetry["failure_types"].append(error_type)
                elapsed = max(time.monotonic() - retry_started, 0.001)
                eta = elapsed / retry_completed * (len(retry_items) - retry_completed) if retry_completed < len(retry_items) else 0.0
                _log_media_progress(
                    logger,
                    prefix=f"[Analyze Retry {retry_pass}/{retries}]",
                    completed=retry_completed,
                    phase_total=len(retry_items),
                    item=item,
                    reason=reason_by_id[sid],
                    result=retry_result,
                    items=items,
                    cache=cache,
                    media_results=media_results,
                    cached_media=total - len(media_due),
                    pending_media=len(retry_items) - retry_completed,
                    eta_seconds=eta,
                )
            canceled = analysis_cancel_requested()

    canceled = canceled or analysis_cancel_requested()
    media_changed_ids = {
        sid for sid, result in media_results.items()
        if str(result.get("status") or "unknown").lower() == "alive"
        and not _is_confirmed_placeholder_result(result)
        if _media_stats_changed_for_throughput(
            previous_media_stats.get(sid),
            result.get("stats"),
            media_bitrate_relative_tolerance=media_bitrate_relative_tolerance,
            media_history=previous_media_history.get(sid),
        )
    }
    media_changed_ids.update(dispatcharr_metadata_changed_ids)
    placeholder_recovered_ids = {
        sid
        for sid, result in media_results.items()
        if result.get("placeholder_recovery_full_probe")
        and str(result.get("status") or "unknown").lower() == "alive"
    }
    for sid in placeholder_recovered_ids:
        content_reason_by_id[sid] = "placeholder_recovered"

    now = datetime.now(timezone.utc)
    throughput_due = []
    for item in items:
        sid = int(item["id"])
        entry = cache.get(str(sid)) or {}
        effective = media_results.get(sid) or entry
        if str(effective.get("status") or "unknown").lower() != "alive":
            continue
        if sid in placeholder_recovered_ids:
            reason = "placeholder_recovered"
        else:
            reason = throughput_check_reason(
                entry,
                url_hash=analyzer._stream_url_hash(str(item.get("url") or "")),
                ttl_hours=throughput_ttl_hours,
                dead_ttl_hours=dead_ttl_hours,
                degraded_ttl_hours=degraded_throughput_ttl_hours,
                unknown_ttl_hours=unknown_throughput_ttl_hours,
                provider_id=item.get("account_id"),
                ttl_jitter_percent=analysis_ttl_jitter_percent,
                now=now,
            )
            reason = _normalize_throughput_check_reason(
                reason,
                media_changed=sid in media_changed_ids,
            )
        if reason:
            throughput_due.append((item, reason))
    throughput_reason_counts = collections.Counter(reason for _, reason in throughput_due)

    content_results = {}
    throughput_results = {}
    throughput_checked_ids = set()
    throughput_attempted_ids = set()
    throughput_capacity_deferred_ids = set()
    content_checked_ids = set()
    content_attempted_ids = set()
    content_capacity_deferred_ids = set()
    item_by_id = {int(item["id"]): item for item in items}
    throughput_reason_by_id = {int(item["id"]): reason for item, reason in throughput_due}
    content_candidate_ids = {
        sid for sid in content_reason_by_id
        if str((media_results.get(sid) or cache.get(str(sid)) or {}).get("status") or "unknown").lower() == "alive"
        and not _is_placeholder_result(media_results.get(sid) or cache.get(str(sid)))
    }

    def effective_result(sid):
        return content_results.get(sid) or media_results.get(sid) or cache.get(str(sid)) or {}

    def projected_results():
        return {**media_results, **content_results}

    for sid in content_candidate_ids:
        if sid in media_retry_telemetry:
            continue
        base_result = effective_result(sid)
        media_retry_telemetry[sid] = {
            "initial_failed": False,
            "initial_status": str(base_result.get("status") or "alive").lower(),
            "initial_error_type": "",
            "retry_attempts": 0,
            "retry_deferred": 0,
            "failure_types": [],
            "current_retry_attempts": 0,
        }

    for sid in list(content_candidate_ids):
        if _content_checks_applicable(effective_result(sid), settings):
            continue
        content_results[sid] = _content_skipped_result(effective_result(sid))
        content_checked_ids.add(sid)
        content_candidate_ids.remove(sid)

    combined_ids = content_candidate_ids & set(throughput_reason_by_id)

    def note_content_result(sid, result):
        content_attempted_ids.add(sid)
        if str(result.get("status") or "unknown").lower() == "alive":
            return
        telemetry = media_retry_telemetry[sid]
        if not telemetry["initial_failed"]:
            telemetry["initial_failed"] = True
            telemetry["initial_status"] = str(result.get("status") or "unknown").lower()
            telemetry["initial_error_type"] = str(result.get("error_type") or "")
        telemetry["current_retry_attempts"] = 0
        error_type = str(result.get("error_type") or "")
        if error_type and error_type not in telemetry["failure_types"]:
            telemetry["failure_types"].append(error_type)

    def run_content(item):
        sid = int(item["id"])
        return analyzer.apply_content_analysis(
            effective_result(sid),
            str(item.get("url") or ""),
            settings=settings,
            user_agent=str(item.get("user_agent") or DEFAULT_USER_AGENT),
            logger=logger,
        )

    content_only_items = [
        item_by_id[sid]
        for sid in content_candidate_ids - combined_ids
        if not _is_placeholder_result(effective_result(sid))
    ]
    if content_only_items and not canceled:
        content_started = time.monotonic()
        for completed, (item, future) in enumerate(
            _fair_account_futures(
                content_only_items,
                run_content,
                max_workers=workers,
                thread_name_prefix="stream-sort-content",
                capacity_manager=capacity_manager,
            ),
            start=1,
        ):
            sid = int(item["id"])
            if future is None:
                content_capacity_deferred_ids.add(sid)
                logger.info("[Analyze Content] stream=%s deferred because its M3U connection limit is occupied", sid)
                continue
            try:
                result = dict(future.result())
            except Exception as exc:
                result = dict(effective_result(sid))
                result.update({"status": "dead", "error_type": "stream_unreachable", "error": str(exc)})
            content_results[sid] = result
            content_checked_ids.add(sid)
            note_content_result(sid, result)
            elapsed = max(time.monotonic() - content_started, 0.001)
            eta = elapsed / completed * (len(content_only_items) - completed) if completed < len(content_only_items) else 0.0
            _log_media_progress(
                logger,
                prefix="[Analyze Content]",
                completed=completed,
                phase_total=len(content_only_items),
                item=item,
                reason=content_reason_by_id[sid],
                result=result,
                items=items,
                cache=cache,
                media_results=projected_results(),
                cached_media=total - len(content_candidate_ids),
                pending_media=len(content_only_items) - completed,
                eta_seconds=eta,
            )
        canceled = analysis_cancel_requested()

    combined_capture_retry_ids = set()
    throughput_retry_ids = set()
    combined_items = [
        item_by_id[sid]
        for sid in combined_ids
        if not _is_placeholder_result(effective_result(sid))
    ]
    if combined_items and not canceled:
        combined_started = time.monotonic()
        capture_temp_directory = _select_capture_temp_directory(workers, logger=logger)
        logger.info(
            "[Analyze Combined] capture temporary storage=%s workers=%s",
            capture_temp_directory or "system-default",
            workers,
        )

        def run_combined_capture(item):
            sid = int(item["id"])
            stats = effective_result(sid).get("stats") or {}
            _width, height = parse_resolution(stats)
            fps = parse_fps(stats)
            nominal = estimate_nominal_throughput_kbps(height, fps)
            result, sample_path = capture_stream_sample(
                str(item.get("url") or ""),
                nominal_video_kbps=nominal,
                duration_seconds=throughput_duration,
                timeout_seconds=throughput_timeout,
                user_agent=str(item.get("user_agent") or DEFAULT_USER_AGENT),
                ffmpeg_path=str(settings.get("analysis_ffmpeg_path") or "/usr/local/bin/ffmpeg"),
                temp_directory=capture_temp_directory,
            )
            return result, sample_path, nominal

        local_pending = {}
        local_completed = 0

        def record_local_result(item, result, *, checked):
            nonlocal local_completed
            sid = int(item["id"])
            content_results[sid] = dict(result)
            if checked:
                content_checked_ids.add(sid)
            note_content_result(sid, result)
            local_completed += 1
            elapsed = max(time.monotonic() - combined_started, 0.001)
            eta = (
                elapsed / local_completed * (len(combined_items) - local_completed)
                if local_completed < len(combined_items)
                else 0.0
            )
            _log_media_progress(
                logger,
                prefix="[Analyze Combined Content]",
                completed=local_completed,
                phase_total=len(combined_items),
                item=item,
                reason=content_reason_by_id[sid],
                result=result,
                items=items,
                cache=cache,
                media_results=projected_results(),
                cached_media=total - len(content_candidate_ids),
                pending_media=len(combined_items) - local_completed,
                eta_seconds=eta,
            )

        def finish_local_futures(done):
            for local_future in done:
                local_item = local_pending.pop(local_future)
                sid = int(item["id"])
                try:
                    result = dict(local_future.result())
                except Exception as exc:
                    sid = int(local_item["id"])
                    result = dict(effective_result(sid))
                    result.update({"status": "dead", "error_type": "stream_unreachable", "error": str(exc)})
                record_local_result(local_item, result, checked=True)

        with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="stream-sort-local-content",
        ) as local_executor:
            for completed, (item, future) in enumerate(
                _fair_account_futures(
                    combined_items,
                    run_combined_capture,
                    max_workers=workers,
                    thread_name_prefix="stream-sort-combined-capture",
                    capacity_manager=capacity_manager,
                ),
                start=1,
            ):
                sid = int(item["id"])
                if future is None:
                    content_capacity_deferred_ids.add(sid)
                    throughput_capacity_deferred_ids.add(sid)
                    logger.info("[Analyze Combined] stream=%s deferred because its M3U connection limit is occupied", sid)
                    continue
                throughput_attempted_ids.add(sid)
                try:
                    throughput_result, sample_path, nominal = future.result()
                except Exception as exc:
                    throughput_result = {
                        "status": "unknown",
                        "tested_at": analyzer._utc_now_iso(),
                        "error_type": "stream_unreachable",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                    sample_path = None
                    nominal = None
                if sample_path:
                    throughput_results[sid] = dict(throughput_result)
                    if throughput_result.get("measured_mbps") is not None:
                        throughput_checked_ids.add(sid)
                        throughput_retry_ids.discard(sid)
                    else:
                        throughput_retry_ids.add(sid)
                    try:
                        local_future = local_executor.submit(
                            _analyze_local_capture,
                            item,
                            effective_result(sid),
                            sample_path,
                            settings=settings,
                            logger=logger,
                        )
                        local_pending[local_future] = item
                    except Exception as exc:
                        try:
                            os.unlink(sample_path)
                        except OSError:
                            pass
                        failed = dict(effective_result(sid))
                        failed.update({"status": "dead", "error_type": "stream_unreachable", "error": str(exc)})
                        record_local_result(item, failed, checked=True)
                else:
                    combined_capture_retry_ids.add(sid)
                    logger.warning(
                        "[Analyze Combined] stream=%s capture failed error=%s; content and throughput remain incomplete",
                        sid,
                        throughput_result.get("error") or "Combined capture failed",
                    )
                    failed = dict(effective_result(sid))
                    failed.update({
                        "status": "dead",
                        "error_type": str(throughput_result.get("error_type") or "stream_unreachable"),
                        "error": str(throughput_result.get("error") or "Combined capture failed"),
                    })
                    record_local_result(item, failed, checked=False)

                ready = [local_future for local_future in local_pending if local_future.done()]
                if ready:
                    finish_local_futures(ready)
                while len(local_pending) >= workers:
                    done, _pending = wait(local_pending, return_when=FIRST_COMPLETED)
                    finish_local_futures(done)

                elapsed = max(time.monotonic() - combined_started, 0.001)
                eta = elapsed / completed * (len(combined_items) - completed) if completed < len(combined_items) else 0.0
                logger.info(
                    "[Analyze Combined] %d%% (%d/%d) stream=%s content=%s throughput=%s measured=%sMbps nominal=%skbps | ETA=%s",
                    int(round(completed / len(combined_items) * 100)), completed, len(combined_items), sid,
                    "captured" if sample_path else "capture_failed", throughput_result.get("status") or "unknown",
                    throughput_result.get("measured_mbps", "n/a"), nominal, analyzer._format_eta(eta),
                )

            while local_pending:
                done, _pending = wait(local_pending, return_when=FIRST_COMPLETED)
                finish_local_futures(done)
        canceled = analysis_cancel_requested()

    def note_retry_result(sid, result):
        telemetry = media_retry_telemetry[sid]
        telemetry["retry_attempts"] += 1
        telemetry["current_retry_attempts"] += 1
        error_type = str(result.get("error_type") or "")
        if error_type and error_type not in telemetry["failure_types"]:
            telemetry["failure_types"].append(error_type)

    def run_throughput_retry(item):
        sid = int(item["id"])
        stats = effective_result(sid).get("stats") or {}
        _width, height = parse_resolution(stats)
        fps = parse_fps(stats)
        nominal = estimate_nominal_throughput_kbps(height, fps)
        result = probe_stream(
            str(item.get("url") or ""),
            nominal_video_kbps=nominal,
            duration_seconds=throughput_duration,
            timeout_seconds=throughput_timeout,
            user_agent=str(item.get("user_agent") or DEFAULT_USER_AGENT),
        )
        return result, nominal

    for retry_pass in range(1, retries + 1):
        if canceled:
            break
        combined_retry_ids = sorted(
            sid for sid in combined_capture_retry_ids
            if str((content_results.get(sid) or {}).get("error_type") or "") in analyzer.RETRYABLE_ERROR_TYPES
        )
        content_retry_ids = sorted(
            sid for sid in content_attempted_ids
            if sid not in combined_capture_retry_ids
            if str((content_results.get(sid) or {}).get("error_type") or "") in analyzer.RETRYABLE_ERROR_TYPES
        )
        throughput_pass_ids = sorted(throughput_retry_ids)
        if not combined_retry_ids and not content_retry_ids and not throughput_pass_ids:
            break
        backoff = max(1.0, account_delay * 3.0)
        if combined_retry_ids:
            logger.info(
                "[Analyze Combined Retry %d/%d] waiting %.1fs before retrying %d combined checks",
                retry_pass,
                retries,
                backoff,
                len(combined_retry_ids),
            )
        if content_retry_ids:
            logger.info(
                "[Analyze Content Retry %d/%d] waiting %.1fs before retrying %d content checks",
                retry_pass,
                retries,
                backoff,
                len(content_retry_ids),
            )
        if throughput_pass_ids:
            logger.info(
                "[Analyze Throughput Retry %d/%d] waiting %.1fs before retrying %d throughput checks",
                retry_pass,
                retries,
                backoff,
                len(throughput_pass_ids),
            )
        deadline = time.monotonic() + backoff
        while time.monotonic() < deadline:
            if analysis_cancel_requested():
                canceled = True
                break
            time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
        if canceled:
            break

        if combined_retry_ids:
            retry_started = time.monotonic()
            retry_items = [item_by_id[sid] for sid in combined_retry_ids]
            for completed, (item, future) in enumerate(
                _fair_account_futures(
                    retry_items,
                    run_combined_capture,
                    max_workers=workers,
                    thread_name_prefix="stream-sort-combined-retry",
                    capacity_manager=capacity_manager,
                    max_per_account=1,
                ),
                start=1,
            ):
                sid = int(item["id"])
                telemetry = media_retry_telemetry[sid]
                if future is None:
                    telemetry["retry_deferred"] += 1
                    logger.info(
                        "[Analyze Combined Retry %d/%d] stream=%s deferred because its M3U connection limit is occupied",
                        retry_pass,
                        retries,
                        sid,
                    )
                    continue
                throughput_attempted_ids.add(sid)
                try:
                    throughput_result, sample_path, nominal = future.result()
                except Exception as exc:
                    throughput_result = {
                        "status": "unknown",
                        "tested_at": analyzer._utc_now_iso(),
                        "error_type": "stream_unreachable",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                    sample_path = None
                    nominal = None

                if sample_path:
                    combined_capture_retry_ids.discard(sid)
                    throughput_results[sid] = dict(throughput_result)
                    if throughput_result.get("measured_mbps") is not None:
                        throughput_checked_ids.add(sid)
                        throughput_retry_ids.discard(sid)
                    else:
                        throughput_retry_ids.add(sid)
                    result = _analyze_local_capture(
                        item,
                        effective_result(sid),
                        sample_path,
                        settings=settings,
                        logger=logger,
                    )
                    content_results[sid] = dict(result)
                    content_attempted_ids.add(sid)
                    content_checked_ids.add(sid)
                    note_retry_result(sid, result)
                else:
                    result = dict(effective_result(sid))
                    result.update({
                        "status": "dead",
                        "error_type": str(throughput_result.get("error_type") or "stream_unreachable"),
                        "error": str(throughput_result.get("error") or "Combined capture failed"),
                    })
                    content_results[sid] = result
                    content_attempted_ids.add(sid)
                    note_retry_result(sid, result)
                    logger.warning(
                        "[Analyze Combined Retry %d/%d] stream=%s capture failed error=%s; content and throughput remain incomplete",
                        retry_pass,
                        retries,
                        sid,
                        result.get("error"),
                    )

                elapsed = max(time.monotonic() - retry_started, 0.001)
                eta = elapsed / completed * (len(retry_items) - completed) if completed < len(retry_items) else 0.0
                _log_media_progress(
                    logger,
                    prefix=f"[Analyze Combined Retry {retry_pass}/{retries} Content]",
                    completed=completed,
                    phase_total=len(retry_items),
                    item=item,
                    reason=content_reason_by_id[sid],
                    result=result,
                    items=items,
                    cache=cache,
                    media_results=projected_results(),
                    cached_media=total - len(content_candidate_ids),
                    pending_media=len(retry_items) - completed,
                    eta_seconds=eta,
                )
                logger.info(
                    "[Analyze Combined Retry %d/%d] %d%% (%d/%d) stream=%s content=%s throughput=%s measured=%sMbps nominal=%skbps | ETA=%s",
                    retry_pass,
                    retries,
                    int(round(completed / len(retry_items) * 100)),
                    completed,
                    len(retry_items),
                    sid,
                    "captured" if sample_path else "capture_failed",
                    throughput_result.get("status") or "unknown",
                    throughput_result.get("measured_mbps", "n/a"),
                    nominal,
                    analyzer._format_eta(eta),
                )
            canceled = analysis_cancel_requested()
            if canceled:
                break

        if content_retry_ids:
            retry_started = time.monotonic()
            retry_items = [item_by_id[sid] for sid in content_retry_ids]
        else:
            retry_items = []
        for completed, (item, future) in enumerate(
            _fair_account_futures(
                retry_items,
                run_content,
                max_workers=workers,
                thread_name_prefix="stream-sort-content-retry",
                capacity_manager=capacity_manager,
                max_per_account=1,
            ),
            start=1,
        ):
            sid = int(item["id"])
            telemetry = media_retry_telemetry[sid]
            if future is None:
                telemetry["retry_deferred"] += 1
                logger.info("[Analyze Content Retry %d/%d] stream=%s deferred because its M3U connection limit is occupied", retry_pass, retries, sid)
                continue
            try:
                result = dict(future.result())
            except Exception as exc:
                result = dict(effective_result(sid))
                result.update({"status": "dead", "error_type": "stream_unreachable", "error": str(exc)})
            content_results[sid] = result
            content_checked_ids.add(sid)
            note_retry_result(sid, result)
            elapsed = max(time.monotonic() - retry_started, 0.001)
            eta = elapsed / completed * (len(retry_items) - completed) if completed < len(retry_items) else 0.0
            _log_media_progress(
                logger,
                prefix=f"[Analyze Content Retry {retry_pass}/{retries}]",
                completed=completed,
                phase_total=len(retry_items),
                item=item,
                reason=content_reason_by_id[sid],
                result=result,
                items=items,
                cache=cache,
                media_results=projected_results(),
                cached_media=total - len(content_candidate_ids),
                pending_media=len(retry_items) - completed,
                eta_seconds=eta,
            )

        if throughput_pass_ids:
            retry_started = time.monotonic()
            retry_items = [item_by_id[sid] for sid in throughput_pass_ids]
            for completed, (item, future) in enumerate(
                _fair_account_futures(
                    retry_items,
                    run_throughput_retry,
                    max_workers=workers,
                    thread_name_prefix="stream-sort-throughput-retry",
                    capacity_manager=capacity_manager,
                    max_per_account=1,
                ),
                start=1,
            ):
                sid = int(item["id"])
                if future is None:
                    logger.info(
                        "[Analyze Throughput Retry %d/%d] stream=%s deferred because its M3U connection limit is occupied",
                        retry_pass,
                        retries,
                        sid,
                    )
                    continue
                throughput_attempted_ids.add(sid)
                try:
                    result, nominal = future.result()
                except Exception as exc:
                    result = {
                        "status": "unknown",
                        "tested_at": analyzer._utc_now_iso(),
                        "error_type": "stream_unreachable",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                    nominal = None
                throughput_results[sid] = dict(result)
                if result.get("measured_mbps") is not None:
                    throughput_checked_ids.add(sid)
                    throughput_retry_ids.discard(sid)
                elapsed = max(time.monotonic() - retry_started, 0.001)
                eta = elapsed / completed * (len(retry_items) - completed) if completed < len(retry_items) else 0.0
                logger.info(
                    "[Analyze Throughput Retry %d/%d] %d%% (%d/%d) stream=%s throughput=%s measured=%sMbps nominal=%skbps | ETA=%s",
                    retry_pass,
                    retries,
                    int(round(completed / len(retry_items) * 100)),
                    completed,
                    len(retry_items),
                    sid,
                    result.get("status") or "unknown",
                    result.get("measured_mbps", "n/a"),
                    nominal,
                    analyzer._format_eta(eta),
                )
        canceled = analysis_cancel_requested()

    canceled = canceled or analysis_cancel_requested()
    terminal_result_ids = set(media_results) | set(content_results)
    for sid in terminal_result_ids:
        result = effective_result(sid)
        telemetry = media_retry_telemetry[sid]
        terminal_status = str(result.get("status") or "unknown").lower()
        confirmed_placeholder = _is_confirmed_placeholder_result(result)
        telemetry["terminal_status"] = terminal_status
        telemetry["recovered_after_retries"] = bool(
            telemetry["initial_failed"] and telemetry["retry_attempts"] and terminal_status == "alive"
        )
        telemetry["retries_exhausted"] = bool(
            not confirmed_placeholder
            and
            retries > 0
            and telemetry["current_retry_attempts"] >= retries
            and str(result.get("error_type") or "") in analyzer.RETRYABLE_ERROR_TYPES
        )
        telemetry["retry_pending"] = bool(
            not confirmed_placeholder
            and
            terminal_status == "dead"
            and retries > 0
            and telemetry["current_retry_attempts"] < retries
            and str(result.get("error_type") or "") in analyzer.RETRYABLE_ERROR_TYPES
        )
        result["retry_pending"] = telemetry["retry_pending"]
        result["retry_telemetry"] = telemetry
        if sid in content_results:
            content_results[sid] = result
        else:
            media_results[sid] = result

    for item, _ in media_due:
        sid = int(item["id"])
        if sid not in media_results:
            continue
        cache[str(sid)] = _merge_media_result(
            item,
            cache.get(str(sid)),
            media_results[sid],
            analysis_reason=reason_by_id.get(sid),
            record_history=sid not in content_results,
            history_previous_status=scan_start_status_by_id.get(sid, "unknown"),
        )

    for sid, result in content_results.items():
        item = item_by_id[sid]
        merged = _merge_content_result(
            item,
            cache.get(str(sid)),
            result,
            analysis_reason=content_reason_by_id.get(sid),
            history_previous_status=scan_start_status_by_id.get(sid, "unknown"),
        )
        if sid in combined_capture_retry_ids:
            merged.pop("throughput", None)
        cache[str(sid)] = merged

    if throughput_due and not canceled:
        throughput_started = time.monotonic()
        account_probe_limiter = analyzer._PerAccountStartLimiter(throughput_account_delay)

        def run_throughput(item):
            account_probe_limiter.wait(item.get("account_id"))
            entry = cache.get(str(item["id"])) or {}
            stats = (media_results.get(int(item["id"])) or entry).get("stats") or {}
            _width, height = parse_resolution(stats)
            fps = parse_fps(stats)
            nominal = estimate_nominal_throughput_kbps(height, fps)
            result = probe_stream(
                str(item.get("url") or ""),
                nominal_video_kbps=nominal,
                duration_seconds=throughput_duration,
                timeout_seconds=throughput_timeout,
                user_agent=str(item.get("user_agent") or DEFAULT_USER_AGENT),
            )
            return result, nominal

        throughput_items = [
            item
            for item, _reason in throughput_due
            if int(item["id"]) not in combined_ids
            and not _is_placeholder_result(effective_result(int(item["id"])))
        ]
        if not throughput_items:
            throughput_due = []
        for completed, (item, future) in enumerate(
            _fair_account_futures(
                throughput_items,
                run_throughput,
                max_workers=workers,
                thread_name_prefix="stream-sort-throughput",
                capacity_manager=capacity_manager,
            ),
            start=1,
        ):
            reason = throughput_reason_by_id[int(item["id"])]
            if future is None:
                sid = int(item["id"])
                throughput_capacity_deferred_ids.add(sid)
                elapsed = max(time.monotonic() - throughput_started, 0.001)
                eta = elapsed / completed * (len(throughput_items) - completed) if completed < len(throughput_items) else 0.0
                logger.info(
                    "[Analyze Throughput] %d%% (%d/%d) stream=%s reason=%s throughput=deferred_capacity measured=n/aMbps nominal=n/akbps | M3U connection limit is occupied; cached result preserved | ETA=%s",
                    int(round(completed / len(throughput_items) * 100)),
                    completed,
                    len(throughput_items),
                    sid,
                    reason,
                    analyzer._format_eta(eta),
                )
                continue
            sid = int(item["id"])
            throughput_attempted_ids.add(sid)
            try:
                result, nominal = future.result()
            except Exception as exc:
                result = {
                    "status": "unknown",
                    "tested_at": analyzer._utc_now_iso(),
                    "error": f"{type(exc).__name__}: {exc}",
                }
                nominal = None
            throughput_results[sid] = dict(result)
            if result.get("measured_mbps") is not None:
                throughput_checked_ids.add(sid)
            elapsed = max(time.monotonic() - throughput_started, 0.001)
            eta = elapsed / completed * (len(throughput_items) - completed) if completed < len(throughput_items) else 0.0
            counts = _throughput_counts(items, cache)
            alive_count = sum(
                1 for candidate in items
                if str((cache.get(str(candidate["id"])) or {}).get("status") or "unknown").lower() == "alive"
            )
            logger.info(
                "[Analyze Throughput] %d%% (%d/%d) stream=%s reason=%s throughput=%s measured=%sMbps nominal=%skbps | overall %s cached_throughput=%d pending_throughput=%d | ETA=%s",
                int(round(completed / len(throughput_items) * 100)), completed, len(throughput_items), item["id"], reason,
                result.get("status") or "unknown", result.get("measured_mbps", "n/a"), nominal,
                _overall_throughput_text(counts), max(0, alive_count - len(throughput_due)),
                len(throughput_items) - completed, analyzer._format_eta(eta),
            )
        canceled = analysis_cancel_requested()

    for sid, result in throughput_results.items():
        item = item_by_id[sid]
        terminal = content_results.get(sid) or media_results.get(sid) or {}
        if str(terminal.get("status") or "unknown").lower() == "dead":
            continue
        cache[str(sid)] = _merge_throughput_result(item, cache.get(str(sid)) or {}, result)

    throughput_checked_ids = _retained_throughput_measurement_ids(throughput_attempted_ids, cache)
    media_checked_ids = set(media_results)
    capacity_deferred_ids = media_capacity_deferred_ids | content_capacity_deferred_ids | throughput_capacity_deferred_ids
    fully_cached = sum(
        1 for item in items
        if int(item["id"]) not in media_checked_ids
        and int(item["id"]) not in content_attempted_ids
        and int(item["id"]) not in throughput_attempted_ids
        and int(item["id"]) not in capacity_deferred_ids
    )
    canceled = bool(close_analysis_cancel_window() or canceled)
    terminal_results = {**media_results, **content_results}
    for sid, result in terminal_results.items():
        analyzer._persist_dispatcharr_result(sid, result, logger)
    analyzer.save_analysis_cache(cache, cache_path)
    health_counts = _status_counts(items, cache)
    throughput_counts = _throughput_counts(items, cache)
    placeholder_count = _placeholder_dead_count(items, cache)
    dead_count = int(health_counts.get("dead", 0))
    other_dead_count = dead_count - placeholder_count
    health_summary = _overall_health_text(health_counts, placeholder_count=placeholder_count)
    runtime_seconds = max(0.0, time.monotonic() - run_started)
    logger.info(
        "[Analyze] Complete: streams=%d media_checked=%d content_checked=%d throughput_attempted=%d throughput_checked=%d capacity_deferred=%d fully_cached=%d playback_health_refreshed=%d dispatcharr_metadata_refreshed=%d | health %s | throughput %s | runtime=%s",
        total, len(media_checked_ids), len(content_checked_ids), len(throughput_attempted_ids), len(throughput_checked_ids), len(capacity_deferred_ids), fully_cached, playback_health_refreshed, dispatcharr_metadata_refreshed,
        health_summary, _overall_throughput_text(throughput_counts), analyzer._format_eta(runtime_seconds),
    )
    report = _build_health_report(
        items,
        cache,
        now=datetime.now(timezone.utc),
        media_reason_counts=media_reason_counts,
        throughput_reason_counts=throughput_reason_counts,
        channels_selected=len({row.channel_id for row in rows}),
    )
    report["filters"] = filter_summary
    _save_json(health_report_path, report)
    result = {
        "streams_analyzed": total,
        "streams_selected": total,
        "media_checked": len(media_checked_ids),
        "content_checked": len(content_checked_ids),
        "throughput_attempted": len(throughput_attempted_ids),
        "throughput_checked": len(throughput_checked_ids),
        "capacity_deferred": len(capacity_deferred_ids),
        "fully_cached": fully_cached,
        "dispatcharr_metadata_refreshed": dispatcharr_metadata_refreshed,
        "playback_health_refreshed": playback_health_refreshed,
        "channels_selected": len({row.channel_id for row in rows}),
        "filters": filter_summary,
        "status_counts": {key: value for key, value in health_counts.items() if value > 0},
        "throughput_status_counts": {key: value for key, value in throughput_counts.items() if value > 0},
        "placeholder_count": placeholder_count,
        "other_dead_count": other_dead_count,
        "health_summary": health_summary,
        "total_runtime_seconds": round(runtime_seconds, 3),
        "total_runtime": analyzer._format_eta(runtime_seconds),
        "cache_path": cache_path,
        "analysis_health_report_path": health_report_path,
    }
    if canceled:
        raise AnalysisCancelled(
            "Stream analysis was canceled after saving completed probe results",
            result=result,
        )
    return result


def install() -> None:
    analyzer.analyze_assigned_streams = analyze_assigned_streams
