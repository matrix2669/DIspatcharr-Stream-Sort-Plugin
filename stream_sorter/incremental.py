from __future__ import annotations

import collections
import hashlib
import json
import os
import time
import tempfile
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from . import analyzer
from .capacity import build_capacity_manager
from .scoring import estimate_nominal_throughput_kbps, parse_fps, parse_resolution
from .reliability import RELIABILITY_PATH, load_reliability_cache
from .throughput import (
    DEFAULT_USER_AGENT,
    LEGACY_CACHE_PATH,
    load_cache as load_throughput_cache,
    probe_stream,
)


ANALYSIS_HEALTH_REPORT_PATH = "/data/dispatcharr_stream_sort_health_report.json"
MEDIA_CHECK_HISTORY_RETENTION_DAYS = 90
MEDIA_CHECK_HISTORY_MAX_ROWS = 2000
MEDIA_BITRATE_RELATIVE_TOLERANCE = 0.30
MEDIA_BITRATE_ABSOLUTE_TOLERANCE_KBPS = 500.0


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
    if ttl_hours <= 0 or jitter_percent <= 0:
        return ttl_hours
    jitter_ratio = jitter_percent / 100.0
    digest = int(hashlib.md5(str(url_hash or "").encode("utf-8")).hexdigest()[:8], 16)
    variance = (digest / float(0xFFFFFFFF)) * 2.0 - 1.0
    adjusted = ttl_hours * (1.0 + (variance * jitter_ratio))
    return max(0.0, adjusted)


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
            checked_at = (
                entry.get("health_checked_at")
                or entry.get("playback_health_checked_at")
                or entry.get("media_checked_at")
                or entry.get("tested_at")
            )
            age = _age_hours(checked_at, now)
            if age is None:
                return "missing_timestamp"
            if dead_ttl_hours > 0 and age < dead_ttl_hours:
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


def throughput_check_reason(
    entry: Mapping[str, Any] | None,
    *,
    url_hash: str,
    ttl_hours: float,
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
    status = str(throughput.get("status") or "unknown").strip().lower()
    if status != "healthy":
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
    absolute_tolerance_kbps: float | None = None,
) -> bool:
    if new_bitrate is None and previous_bitrate is None:
        return False
    if previous_bitrate is None or new_bitrate is None:
        return False
    relative = MEDIA_BITRATE_RELATIVE_TOLERANCE if relative_tolerance is None else max(0.0, relative_tolerance)
    absolute = MEDIA_BITRATE_ABSOLUTE_TOLERANCE_KBPS if absolute_tolerance_kbps is None else max(0.0, absolute_tolerance_kbps)
    tolerance = max(absolute, previous_bitrate * relative)
    return abs(new_bitrate - previous_bitrate) > tolerance


def _media_stats_changed_for_throughput(
    previous_stats: Mapping[str, Any] | None,
    new_stats: Mapping[str, Any] | None,
    *,
    media_bitrate_relative_tolerance: float = MEDIA_BITRATE_RELATIVE_TOLERANCE,
    media_bitrate_absolute_tolerance_kbps: float = MEDIA_BITRATE_ABSOLUTE_TOLERANCE_KBPS,
) -> bool:
    if not previous_stats:
        return bool(new_stats)
    if not new_stats:
        return False
    if _stats_signature(previous_stats) != _stats_signature(new_stats):
        return True
    return _is_significant_bitrate_change(
        _extract_video_bitrate(previous_stats),
        _extract_video_bitrate(new_stats),
        relative_tolerance=media_bitrate_relative_tolerance,
        absolute_tolerance_kbps=media_bitrate_absolute_tolerance_kbps,
    )


def _status_counts(items, cache) -> collections.Counter[str]:
    counts: collections.Counter[str] = collections.Counter()
    for item in items:
        entry = cache.get(str(item["id"])) or {}
        counts[str(entry.get("status") or "unknown").lower()] += 1
    return counts


def _throughput_counts(items, cache) -> collections.Counter[str]:
    counts: collections.Counter[str] = collections.Counter()
    for item in items:
        entry = cache.get(str(item["id"])) or {}
        if str(entry.get("status") or "unknown").lower() != "alive":
            continue
        throughput = entry.get("throughput") if isinstance(entry, Mapping) else None
        counts[str((throughput or {}).get("status") or "unknown").lower()] += 1
    return counts


def _overall_health_text(counts) -> str:
    return f"alive={counts.get('alive', 0)} dead={counts.get('dead', 0)} skipped={counts.get('skipped', 0)} unknown={counts.get('unknown', 0)}"


def _overall_throughput_text(counts) -> str:
    return f"healthy={counts.get('healthy', 0)} marginal={counts.get('marginal', 0)} insufficient={counts.get('insufficient', 0)} unknown={counts.get('unknown', 0)}"


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
    history.append(
        {
            "checked_at": tested_at,
            "previous_status": previous_status,
            "status": new_status,
            "reason": reason or "unknown",
            "error_type": str(result.get("error_type") or ""),
            "error": str(result.get("error") or ""),
            "source": str(result.get("health_source") or entry.get("health_source") or "stream_sort_analyzer"),
        }
    )
    observed_at = _parse_datetime(tested_at) or datetime.now(timezone.utc)
    cutoff = observed_at - timedelta(days=MEDIA_CHECK_HISTORY_RETENTION_DAYS)
    history = [
        row for row in history
        if (_parse_datetime(row.get("checked_at")) or observed_at) >= cutoff
    ]
    if len(history) > MEDIA_CHECK_HISTORY_MAX_ROWS:
        history = history[-MEDIA_CHECK_HISTORY_MAX_ROWS:]
    entry["health_check_history"] = history


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
    total_changes = 0
    total_dead_checks = 0
    stream_reports = []

    for item in items:
        entry = cache.get(str(item["id"])) or {}
        status = str(entry.get("status") or "unknown").lower()
        summary[status] += 1
        history = entry.get("health_check_history") or []
        timeline: list[tuple[datetime, str, str]] = []
        dead = 0
        for row in history:
            row_status = str(row.get("status") or "").lower()
            checked_at = _parse_datetime(row.get("checked_at"))
            if not row_status or checked_at is None:
                continue
            recorded_previous = str(row.get("previous_status") or "").lower()
            timeline.append((checked_at, row_status, recorded_previous))
            all_checked_at.append(checked_at)
            hourly_total_checks[checked_at.hour] += 1
            minute_check_counts[checked_at.strftime("%Y%m%d%H%M")] += 1
            if row_status == "dead":
                dead += 1
                total_dead_checks += 1
                hourly_dead_checks[checked_at.hour] += 1

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
            previous_status = recorded_previous or prior_status
            if previous_status and previous_status != row_status:
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
            elif not previous_status:
                if row_status == "dead":
                    dead_started_at = checked_at
                elif row_status == "alive":
                    alive_started_at = checked_at
            prior_status = row_status

        if prior_status == "dead" and dead_started_at is not None:
            current_dead_episode_hours.append(
                max(0.0, (now - dead_started_at).total_seconds() / 3600.0)
            )

        last_record = history[-1] if history else {}
        history_len = len(history)
        dead_ratio = dead / history_len if history_len else 0.0
        stream_id = int(item["id"])
        status_changes[stream_id] = changes
        dead_counts[stream_id] = dead
        stream_reports.append(
            {
                "stream_id": stream_id,
                "name": str(item.get("name") or ""),
                "last_status": str(last_record.get("status") or status).lower(),
                "last_reason": str(last_record.get("reason") or "none"),
                "history_len": history_len,
                "status_changes": changes,
                "dead_checks": dead,
                "dead_check_ratio": round(dead_ratio, 4),
                "last_checked_at": last_record.get("checked_at"),
                "age_hours": _age_hours(last_record.get("checked_at"), now),
            }
        )

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
    total_history_rows = sum(len((cache.get(str(item["id"])) or {}).get("health_check_history") or []) for item in items)

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
        if row["history_len"] >= 4 and row["dead_check_ratio"] > 0.75
    ]
    problematic_streams.sort(
        key=lambda row: (row["dead_check_ratio"], row["dead_checks"], row["history_len"]),
        reverse=True,
    )
    busiest_minute = max(minute_check_counts.values(), default=0)
    concentration_ratio = busiest_minute / total_history_rows if total_history_rows else 0.0

    return {
        "generated_at": now.isoformat(),
        "selected_streams": len(items),
        "channels_selected": channels_selected,
        "status_counts": {status: count for status, count in summary.items()},
        "observations": {
            "history_rows": total_history_rows,
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
        "ttl_tuning_guidance": {
            "suggested_health_ttl_hours": _round_if_present(alive_episode_p25, 2),
            "suggested_dead_ttl_hours": _round_if_present(dead_recovery_p50, 2),
        },
        "status_patterns": {
            "unstable_streams": unstable_top,
            "problematic_streams": problematic_streams,
            "hourly_dead_ratio": hourly,
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


def _merge_dispatcharr_metadata(
    item: Mapping[str, Any],
    previous: Mapping[str, Any],
    *,
    media_bitrate_relative_tolerance: float = MEDIA_BITRATE_RELATIVE_TOLERANCE,
    media_bitrate_absolute_tolerance_kbps: float = MEDIA_BITRATE_ABSOLUTE_TOLERANCE_KBPS,
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
        media_bitrate_absolute_tolerance_kbps=media_bitrate_absolute_tolerance_kbps,
    )
    return merged, True, changed


def _sync_dispatcharr_metadata(
    items,
    cache,
    *,
    media_bitrate_relative_tolerance: float = MEDIA_BITRATE_RELATIVE_TOLERANCE,
    media_bitrate_absolute_tolerance_kbps: float = MEDIA_BITRATE_ABSOLUTE_TOLERANCE_KBPS,
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
            media_bitrate_absolute_tolerance_kbps=media_bitrate_absolute_tolerance_kbps,
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


def _merge_media_result(item, previous, result, *, analysis_reason: str | None = None) -> dict[str, Any]:
    merged = dict(previous or {})
    previous_throughput = merged.get("throughput")
    previous_stats = merged.get("stats")
    previous_status = str(merged.get("status") or "unknown").lower()
    merged.update(dict(result))
    checked = result.get("tested_at") or analyzer._utc_now_iso()
    merged["health_checked_at"] = checked
    merged["media_checked_at"] = checked
    merged["content_checked_at"] = checked
    merged["health_source"] = "stream_sort_analyzer"
    merged["stream_id"] = item.get("id")
    merged["stream_name"] = item.get("name")
    merged["m3u_account_id"] = item.get("account_id")
    merged["m3u_account_name"] = item.get("account_name")
    merged["url_hash"] = analyzer._stream_url_hash(str(item.get("url") or ""))
    status = str(result.get("status") or "unknown").lower()
    result_stats = result.get("stats")
    if isinstance(result_stats, Mapping) and result_stats:
        merged["stats"] = dict(result_stats)
        merged["metadata_updated_at"] = checked
        merged["metadata_source"] = "stream_sort_analyzer"
    elif status == "skipped" and isinstance(previous_stats, Mapping) and previous_stats:
        merged["stats"] = dict(previous_stats)
    if status == "dead":
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
    _append_health_history(
        merged,
        reason=analysis_reason,
        previous_status=previous_status,
        new_status=str(result.get("status") or "unknown").lower(),
        tested_at=checked,
        result=result,
    )
    return merged


def _merge_throughput_result(item, entry, result, *, ttl_hours: float) -> dict[str, Any]:
    merged = dict(entry)
    throughput = dict(result)
    checked_at = throughput.get("tested_at") or analyzer._utc_now_iso()
    throughput["checked_at"] = checked_at
    throughput["url_hash"] = analyzer._stream_url_hash(str(item.get("url") or ""))
    throughput["m3u_account_id"] = item.get("account_id")
    throughput["m3u_account_name"] = item.get("account_name")
    checked_dt = _parse_datetime(checked_at)
    if checked_dt is not None and ttl_hours > 0:
        throughput["expires_at"] = (checked_dt + timedelta(hours=ttl_hours)).isoformat()
    else:
        throughput.pop("expires_at", None)
    merged["throughput"] = throughput
    return merged


def _migrate_legacy_throughput(items, cache, *, ttl_hours: float) -> int:
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
        cache[key] = _merge_throughput_result(item, entry, result, ttl_hours=ttl_hours)
        migrated += 1
    return migrated


def _analysis_reason(
    entry: Mapping[str, Any] | None,
    *,
    url_hash: str,
    health_ttl_hours: float,
    content_ttl_hours: float,
    metadata_ttl_hours: float,
    dead_ttl_hours: float | None = None,
    ttl_jitter_percent: float = 0.0,
    now: datetime,
) -> str | None:
    reason = health_check_reason(
        entry,
        url_hash=url_hash,
        ttl_hours=health_ttl_hours,
        content_ttl_hours=content_ttl_hours,
        dead_ttl_hours=dead_ttl_hours,
        ttl_jitter_percent=ttl_jitter_percent,
        now=now,
    )
    if reason == "status_dead_ttl":
        return None
    if reason:
        return f"health_{reason}"
    reason = metadata_check_reason(
        entry,
        url_hash=url_hash,
        ttl_hours=metadata_ttl_hours,
        ttl_jitter_percent=ttl_jitter_percent,
        now=now,
    )
    if reason:
        return f"metadata_{reason}"
    return None


def analyze_assigned_streams(
    settings: Mapping[str, Any],
    *,
    logger,
    cache_path: str = analyzer.ANALYSIS_CACHE_PATH,
    health_report_path: str | None = None,
) -> dict[str, Any]:
    from apps.channels.models import ChannelStream
    from .sorter import resolve_channel_scope

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
    max_streams = max(0, analyzer._as_int(settings.get("analysis_max_streams"), 0))
    metadata_ttl_hours = max(0.0, analyzer._as_float(settings.get("stream_data_ttl_hours"), 12.0))
    health_ttl_hours = max(0.0, analyzer._as_float(settings.get("health_content_ttl_hours"), 24.0))
    dead_ttl_hours = max(0.0, analyzer._as_float(settings.get("dead_content_ttl_hours"), 1.0))
    content_ttl_hours = max(0.0, analyzer._as_float(settings.get("content_validation_ttl_hours"), 168.0))
    media_bitrate_relative_tolerance = min(
        1.0,
        max(
            0.0,
            analyzer._as_float(settings.get("media_bitrate_relative_tolerance_percent"), 30.0) / 100.0,
        ),
    )
    media_bitrate_absolute_tolerance_kbps = max(
        0.0,
        analyzer._as_float(settings.get("media_bitrate_absolute_tolerance_kbps"), 500.0),
    )
    playback_health_reuse = analyzer._as_bool(settings.get("playback_health_reuse"), True)
    playback_health_min_seconds = max(60.0, analyzer._as_float(settings.get("playback_health_min_seconds"), 300.0))
    playback_health_clean_min_seconds = max(30.0, analyzer._as_float(settings.get("playback_health_clean_min_seconds"), 60.0))
    playback_health_ttl_hours = max(0.0, analyzer._as_float(settings.get("playback_health_ttl_hours"), 6.0))
    throughput_ttl_hours = max(0.0, analyzer._as_float(settings.get("healthy_throughput_ttl_hours"), 6.0))
    analysis_ttl_jitter_percent = max(0.0, min(100.0, analyzer._as_float(settings.get("analysis_ttl_jitter_percent"), 0.0)))
    throughput_duration = max(1.0, analyzer._as_float(settings.get("probe_duration_seconds"), 8.0))
    throughput_timeout = max(throughput_duration + 2.0, analyzer._as_float(settings.get("probe_timeout_seconds"), 10.0))
    throughput_account_delay = max(0.0, analyzer._as_float(settings.get("probe_per_account_delay_seconds"), 1.0))

    queryset = ChannelStream.objects.select_related("stream", "stream__m3u_account").order_by("channel_id", "order", "id")
    if channel_ids is not None:
        queryset = queryset.filter(channel_id__in=channel_ids)
    rows = list(queryset)

    items = []
    seen = set()
    for row in rows:
        stream = row.stream
        if stream.id in seen:
            continue
        seen.add(stream.id)
        items.append(_item_from_stream(stream))
        if max_streams and len(items) >= max_streams:
            break

    total = len(items)
    cache = analyzer.load_analysis_cache(cache_path)

    playback_health_refreshed = 0
    if playback_health_reuse:
        playback_health_refreshed = _sync_runtime_playback_health(
            items,
            cache,
            load_reliability_cache(RELIABILITY_PATH),
            min_playback_seconds=playback_health_min_seconds,
            min_clean_playback_seconds=playback_health_clean_min_seconds,
            ttl_hours=playback_health_ttl_hours,
            now=datetime.now(timezone.utc),
        )
        if playback_health_refreshed:
            analyzer.save_analysis_cache(cache, cache_path)
            logger.info(
                "[Analyze] Reused recent successful playback as reachability evidence for %d streams",
                playback_health_refreshed,
            )

    migrated = _migrate_legacy_throughput(items, cache, ttl_hours=throughput_ttl_hours)
    if migrated:
        analyzer.save_analysis_cache(cache, cache_path)
        logger.info("[Analyze] Migrated %d matching legacy throughput measurements into the unified cache", migrated)

    dispatcharr_metadata_refreshed, dispatcharr_metadata_changed_ids = _sync_dispatcharr_metadata(
        items,
        cache,
        media_bitrate_relative_tolerance=media_bitrate_relative_tolerance,
        media_bitrate_absolute_tolerance_kbps=media_bitrate_absolute_tolerance_kbps,
    )
    if dispatcharr_metadata_refreshed:
        analyzer.save_analysis_cache(cache, cache_path)
        logger.info("[Analyze] Refreshed basic metadata for %d streams from newer Dispatcharr stream_stats", dispatcharr_metadata_refreshed)

    now = datetime.now(timezone.utc)
    media_due = []
    for item in items:
        reason = _analysis_reason(
            cache.get(str(item["id"])),
            url_hash=analyzer._stream_url_hash(str(item.get("url") or "")),
            health_ttl_hours=health_ttl_hours,
            content_ttl_hours=content_ttl_hours,
            metadata_ttl_hours=metadata_ttl_hours,
            dead_ttl_hours=dead_ttl_hours,
            ttl_jitter_percent=analysis_ttl_jitter_percent,
            now=now,
        )
        if reason:
            media_due.append((item, reason))

    media_reason_counts = collections.Counter(reason for _, reason in media_due)

    media_due_ids = {int(item["id"]) for item, _ in media_due}
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
                ttl_jitter_percent=analysis_ttl_jitter_percent,
                now=now,
            )
            if reason or int(item["id"]) in dispatcharr_metadata_changed_ids:
                initial_throughput_due += 1
            elif int(item["id"]) not in media_due_ids:
                initial_fully_cached += 1
        elif int(item["id"]) not in media_due_ids:
            initial_fully_cached += 1

    logger.info(
        "[Analyze] Starting: streams=%d media_due=%d throughput_due=%d fully_cached=%d playback_health_refreshed=%d dispatcharr_metadata_refreshed=%d metadata_ttl=%.1fh health_ttl=%.1fh dead_ttl=%.1fh content_ttl=%.1fh healthy_throughput_ttl=%.1fh ttl_jitter=%.1f%% workers=%d",
        total, len(media_due), initial_throughput_due, initial_fully_cached, playback_health_refreshed, dispatcharr_metadata_refreshed,
        metadata_ttl_hours, health_ttl_hours, dead_ttl_hours, content_ttl_hours, throughput_ttl_hours, analysis_ttl_jitter_percent, workers,
    )
    if not items:
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
            "throughput_checked": 0,
            "capacity_deferred": 0,
            "fully_cached": 0,
            "dispatcharr_metadata_refreshed": 0,
            "playback_health_refreshed": 0,
            "channels_selected": len({row.channel_id for row in rows}),
            "filters": filter_summary,
            "status_counts": {},
            "throughput_status_counts": {},
            "cache_path": cache_path,
            "analysis_health_report_path": health_report_path,
        }

    capacity_manager = build_capacity_manager(items, logger=logger)

    media_results = {}
    media_capacity_deferred_ids = set()
    previous_media_stats = {
        int(item["id"]): (cache.get(str(item["id"])) or {}).get("stats")
        for item, _ in media_due
    }
    reason_by_id = {int(item["id"]): reason for item, reason in media_due}
    limiter = analyzer._PerAccountStartLimiter(account_delay)
    media_started = time.monotonic()

    def run_media(item):
        analyzer._RATE_LIMIT_GUARD.wait_if_throttled()
        limiter.wait(item.get("account_id"))
        result = analyzer.analyze_stream(
            str(item.get("url") or ""),
            stream_id=item.get("id"),
            stream_name=str(item.get("name") or ""),
            settings=settings,
            user_agent=str(item.get("user_agent") or DEFAULT_USER_AGENT),
            logger=logger,
        )
        if result.get("error_type") == "rate_limited":
            analyzer._RATE_LIMIT_GUARD.record_hit(logger)
        return result

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
            media_results[int(item["id"])] = dict(result)
            elapsed = max(time.monotonic() - media_started, 0.001)
            eta = elapsed / completed * (len(media_due) - completed) if completed < len(media_due) else 0.0
            counts = _status_counts(items, cache)
            old_status = str((cache.get(str(item["id"])) or {}).get("status") or "unknown").lower()
            new_status = str(result.get("status") or "unknown").lower()
            counts[old_status] -= 1
            counts[new_status] += 1
            stats = result.get("stats") or {}
            logger.info(
                "[Analyze Media] %d%% (%d/%d) stream=%s reason=%s health=%s resolution=%s fps=%s bitrate=%skbps | overall %s cached_media=%d pending_media=%d | ETA=%s",
                int(round(completed / len(media_due) * 100)), completed, len(media_due), item["id"],
                reason_by_id[int(item["id"])], new_status, stats.get("resolution") or "n/a",
                f"{float(stats['source_fps']):.1f}" if stats.get("source_fps") is not None else "n/a",
                f"{float(stats['video_bitrate']):.0f}" if stats.get("video_bitrate") is not None else "n/a",
                _overall_health_text(counts), total - len(media_due), len(media_due) - completed, analyzer._format_eta(eta),
            )

        by_id = {int(item["id"]): item for item, _ in media_due}
        for retry_pass in range(1, retries + 1):
            retry_ids = [
                sid for sid, result in media_results.items()
                if str(result.get("error_type") or "") in analyzer.RETRYABLE_ERROR_TYPES
            ]
            if not retry_ids:
                break
            backoff = max(1.0, account_delay * 3.0)
            logger.info("[Analyze Retry %d/%d] waiting %.1fs before retrying %d media checks", retry_pass, retries, backoff, len(retry_ids))
            time.sleep(backoff)
            retry_items = [by_id[sid] for sid in retry_ids]
            for item, future in _fair_account_futures(
                retry_items,
                run_media,
                max_workers=workers,
                thread_name_prefix="stream-sort-media-retry",
                capacity_manager=capacity_manager,
                max_per_account=1,
            ):
                if future is None:
                    logger.info(
                        "[Analyze Retry %d/%d] stream=%s deferred because its M3U connection limit is occupied",
                        retry_pass,
                        retries,
                        item["id"],
                    )
                    continue
                try:
                    media_results[int(item["id"])] = dict(future.result())
                except Exception as exc:
                    media_results[int(item["id"])] = {
                        "tested_at": analyzer._utc_now_iso(),
                        "status": "dead",
                        "error_type": "other",
                        "error": str(exc),
                        "stats": {},
                        "details": {},
                    }

        for item, _ in media_due:
            sid = int(item["id"])
            if sid not in media_results:
                continue
            result = media_results[sid]
            cache[str(sid)] = _merge_media_result(
                item,
                cache.get(str(sid)),
                result,
                analysis_reason=reason_by_id.get(sid),
            )
            analyzer._persist_dispatcharr_result(sid, result, logger)
        analyzer.save_analysis_cache(cache, cache_path)

    media_changed_ids = {
        sid for sid, result in media_results.items()
        if _media_stats_changed_for_throughput(
            previous_media_stats.get(sid),
            result.get("stats"),
            media_bitrate_relative_tolerance=media_bitrate_relative_tolerance,
            media_bitrate_absolute_tolerance_kbps=media_bitrate_absolute_tolerance_kbps,
        )
    }
    media_changed_ids.update(dispatcharr_metadata_changed_ids)

    now = datetime.now(timezone.utc)
    throughput_due = []
    for item in items:
        sid = int(item["id"])
        entry = cache.get(str(sid)) or {}
        if str(entry.get("status") or "unknown").lower() != "alive":
            continue
        reason = throughput_check_reason(
            entry,
            url_hash=analyzer._stream_url_hash(str(item.get("url") or "")),
            ttl_hours=throughput_ttl_hours,
            ttl_jitter_percent=analysis_ttl_jitter_percent,
            now=now,
        )
        if sid in media_changed_ids:
            reason = "media_changed"
        if reason:
            throughput_due.append((item, reason))
    throughput_reason_counts = collections.Counter(reason for _, reason in throughput_due)

    throughput_checked_ids = set()
    throughput_capacity_deferred_ids = set()
    if throughput_due:
        throughput_started = time.monotonic()
        account_probe_limiter = analyzer._PerAccountStartLimiter(throughput_account_delay)
        throughput_reason_by_id = {int(item["id"]): reason for item, reason in throughput_due}

        def run_throughput(item):
            account_probe_limiter.wait(item.get("account_id"))
            entry = cache.get(str(item["id"])) or {}
            stats = entry.get("stats") or {}
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

        throughput_items = [item for item, _reason in throughput_due]
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
                eta = elapsed / completed * (len(throughput_due) - completed) if completed < len(throughput_due) else 0.0
                logger.info(
                    "[Analyze Throughput] %d%% (%d/%d) stream=%s reason=%s throughput=deferred_capacity measured=n/aMbps nominal=n/akbps | M3U connection limit is occupied; cached result preserved | ETA=%s",
                    int(round(completed / len(throughput_due) * 100)),
                    completed,
                    len(throughput_due),
                    sid,
                    reason,
                    analyzer._format_eta(eta),
                )
                continue
            try:
                result, nominal = future.result()
            except Exception as exc:
                result = {
                    "status": "unknown",
                    "tested_at": analyzer._utc_now_iso(),
                    "error": f"{type(exc).__name__}: {exc}",
                }
                nominal = None
            entry = cache.get(str(item["id"])) or {}
            cache[str(item["id"])] = _merge_throughput_result(item, entry, result, ttl_hours=throughput_ttl_hours)
            throughput_checked_ids.add(int(item["id"]))
            elapsed = max(time.monotonic() - throughput_started, 0.001)
            eta = elapsed / completed * (len(throughput_due) - completed) if completed < len(throughput_due) else 0.0
            counts = _throughput_counts(items, cache)
            alive_count = sum(
                1 for candidate in items
                if str((cache.get(str(candidate["id"])) or {}).get("status") or "unknown").lower() == "alive"
            )
            logger.info(
                "[Analyze Throughput] %d%% (%d/%d) stream=%s reason=%s throughput=%s measured=%sMbps nominal=%skbps | overall %s cached_throughput=%d pending_throughput=%d | ETA=%s",
                int(round(completed / len(throughput_due) * 100)), completed, len(throughput_due), item["id"], reason,
                result.get("status") or "unknown", result.get("measured_mbps", "n/a"), nominal,
                _overall_throughput_text(counts), max(0, alive_count - len(throughput_due)),
                len(throughput_due) - completed, analyzer._format_eta(eta),
            )

    if throughput_checked_ids:
        analyzer.save_analysis_cache(cache, cache_path)

    media_checked_ids = set(media_results)
    capacity_deferred_ids = media_capacity_deferred_ids | throughput_capacity_deferred_ids
    fully_cached = sum(
        1 for item in items
        if int(item["id"]) not in media_checked_ids
        and int(item["id"]) not in throughput_checked_ids
        and int(item["id"]) not in capacity_deferred_ids
    )
    health_counts = _status_counts(items, cache)
    throughput_counts = _throughput_counts(items, cache)
    logger.info(
        "[Analyze] Complete: streams=%d media_checked=%d throughput_checked=%d capacity_deferred=%d fully_cached=%d playback_health_refreshed=%d dispatcharr_metadata_refreshed=%d | health %s | throughput %s",
        total, len(media_checked_ids), len(throughput_checked_ids), len(capacity_deferred_ids), fully_cached, playback_health_refreshed, dispatcharr_metadata_refreshed,
        _overall_health_text(health_counts), _overall_throughput_text(throughput_counts),
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
    return {
        "streams_analyzed": total,
        "streams_selected": total,
        "media_checked": len(media_checked_ids),
        "throughput_checked": len(throughput_checked_ids),
        "capacity_deferred": len(capacity_deferred_ids),
        "fully_cached": fully_cached,
        "dispatcharr_metadata_refreshed": dispatcharr_metadata_refreshed,
        "playback_health_refreshed": playback_health_refreshed,
        "channels_selected": len({row.channel_id for row in rows}),
        "filters": filter_summary,
        "status_counts": {key: value for key, value in health_counts.items() if value > 0},
        "throughput_status_counts": {key: value for key, value in throughput_counts.items() if value > 0},
        "cache_path": cache_path,
        "analysis_health_report_path": health_report_path,
    }


def install() -> None:
    analyzer.analyze_assigned_streams = analyze_assigned_streams
