from __future__ import annotations

import collections
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from . import analyzer
from .scoring import estimate_nominal_throughput_kbps, parse_fps, parse_resolution
from .throughput import DEFAULT_USER_AGENT, LEGACY_CACHE_PATH, load_cache as load_throughput_cache, probe_stream


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


def _age_hours(value: Any, now: datetime) -> float | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return max(0.0, (now - parsed).total_seconds() / 3600.0)


def media_check_reason(entry: Mapping[str, Any] | None, *, url_hash: str, ttl_hours: float, now: datetime) -> str | None:
    if not entry:
        return "missing"
    if str(entry.get("url_hash") or "") != url_hash:
        return "url_changed"
    status = str(entry.get("status") or "unknown").strip().lower()
    if status != "alive":
        return f"status_{status or 'unknown'}"
    if ttl_hours <= 0:
        return "ttl_forced"
    checked_at = entry.get("media_checked_at") or entry.get("tested_at")
    age = _age_hours(checked_at, now)
    if age is None:
        return "missing_timestamp"
    if age >= ttl_hours:
        return "ttl_expired"
    return None


def throughput_check_reason(entry: Mapping[str, Any] | None, *, url_hash: str, ttl_hours: float, now: datetime) -> str | None:
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
    if age >= ttl_hours:
        return "ttl_expired"
    return None


def _stats_signature(stats: Mapping[str, Any] | None) -> tuple[Any, ...]:
    stats = stats or {}
    width, height = parse_resolution(stats)
    fps = parse_fps(stats)
    bitrate = stats.get("video_bitrate") or stats.get("video_bitrate_kbps")
    return width, height, round(float(fps), 3) if fps is not None else None, bitrate


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


def _item_from_stream(stream) -> dict[str, Any]:
    account = stream.m3u_account
    try:
        user_agent = account.get_user_agent_string() if account else DEFAULT_USER_AGENT
    except Exception:
        user_agent = DEFAULT_USER_AGENT
    return {"id": stream.id, "name": stream.name or "", "url": stream.url or "", "account_id": getattr(stream, "m3u_account_id", None), "account_name": getattr(account, "name", "") if account else "", "user_agent": user_agent or DEFAULT_USER_AGENT}


def _merge_media_result(item, previous, result) -> dict[str, Any]:
    merged = dict(previous or {})
    previous_throughput = merged.get("throughput")
    merged.update(dict(result))
    merged["media_checked_at"] = result.get("tested_at") or analyzer._utc_now_iso()
    merged["stream_id"] = item.get("id")
    merged["stream_name"] = item.get("name")
    merged["m3u_account_id"] = item.get("account_id")
    merged["m3u_account_name"] = item.get("account_name")
    merged["url_hash"] = analyzer._stream_url_hash(str(item.get("url") or ""))
    status = str(result.get("status") or "unknown").lower()
    if status == "dead":
        checked = result.get("tested_at") or analyzer._utc_now_iso()
        merged["throughput"] = {"status": "unknown", "tested_at": checked, "checked_at": checked, "url_hash": merged["url_hash"], "error": "throughput invalidated because media analysis marked the stream dead"}
    elif isinstance(previous_throughput, Mapping):
        merged["throughput"] = dict(previous_throughput)
    else:
        merged.pop("throughput", None)
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
    """Adopt safe legacy throughput rows into the unified cache once.

    A legacy result is reused only when the analysis cache already proves that
    the current URL hash is the same. This avoids trusting a throughput result
    after the stream URL changed.
    """
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


def analyze_assigned_streams(settings: Mapping[str, Any], *, logger, cache_path: str = analyzer.ANALYSIS_CACHE_PATH) -> dict[str, Any]:
    from apps.channels.models import ChannelStream
    from .sorter import resolve_channel_scope

    channel_ids, filter_summary = resolve_channel_scope(settings)
    workers = max(1, min(16, analyzer._as_int(settings.get("analysis_workers"), 2)))
    retries = max(0, min(5, analyzer._as_int(settings.get("analysis_retries"), 3)))
    account_delay = max(0.0, analyzer._as_float(settings.get("analysis_per_account_delay_seconds"), 1.0))
    max_streams = max(0, analyzer._as_int(settings.get("analysis_max_streams"), 0))
    media_ttl_hours = max(0.0, analyzer._as_float(settings.get("stream_data_ttl_hours"), 12.0))
    throughput_ttl_hours = max(0.0, analyzer._as_float(settings.get("healthy_throughput_ttl_hours"), 6.0))
    throughput_duration = max(1.0, analyzer._as_float(settings.get("probe_duration_seconds"), 8.0))
    throughput_timeout = max(throughput_duration + 2.0, analyzer._as_float(settings.get("probe_timeout_seconds"), 10.0))
    throughput_rate_per_minute = max(1, analyzer._as_int(settings.get("probe_rate_per_minute"), 6))
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
    migrated = _migrate_legacy_throughput(items, cache, ttl_hours=throughput_ttl_hours)
    if migrated:
        analyzer.save_analysis_cache(cache, cache_path)
        logger.info("[Analyze] Migrated %d matching legacy throughput measurements into the unified cache", migrated)

    now = datetime.now(timezone.utc)
    media_due = []
    for item in items:
        reason = media_check_reason(cache.get(str(item["id"])), url_hash=analyzer._stream_url_hash(str(item.get("url") or "")), ttl_hours=media_ttl_hours, now=now)
        if reason:
            media_due.append((item, reason))

    media_due_ids = {int(item["id"]) for item, _ in media_due}
    initial_throughput_due = 0
    initial_fully_cached = 0
    for item in items:
        entry = cache.get(str(item["id"])) or {}
        status = str(entry.get("status") or "unknown").lower()
        if status == "alive":
            reason = throughput_check_reason(entry, url_hash=analyzer._stream_url_hash(str(item.get("url") or "")), ttl_hours=throughput_ttl_hours, now=now)
            if reason:
                initial_throughput_due += 1
            elif int(item["id"]) not in media_due_ids:
                initial_fully_cached += 1
        elif int(item["id"]) not in media_due_ids:
            initial_fully_cached += 1

    logger.info("[Analyze] Starting: streams=%d media_due=%d throughput_due=%d fully_cached=%d media_ttl=%.1fh healthy_throughput_ttl=%.1fh workers=%d", total, len(media_due), initial_throughput_due, initial_fully_cached, media_ttl_hours, throughput_ttl_hours, workers)
    if not items:
        return {"streams_analyzed": 0, "streams_selected": 0, "media_checked": 0, "throughput_checked": 0, "fully_cached": 0, "channels_selected": len({row.channel_id for row in rows}), "filters": filter_summary, "status_counts": {}, "throughput_status_counts": {}, "cache_path": cache_path}

    media_results = {}
    old_signatures = {int(item["id"]): _stats_signature((cache.get(str(item["id"])) or {}).get("stats")) for item, _ in media_due}
    reason_by_id = {int(item["id"]): reason for item, reason in media_due}
    limiter = analyzer._PerAccountStartLimiter(account_delay)
    media_started = time.monotonic()

    def run_media(item):
        analyzer._RATE_LIMIT_GUARD.wait_if_throttled()
        limiter.wait(item.get("account_id"))
        result = analyzer.analyze_stream(str(item.get("url") or ""), stream_id=item.get("id"), stream_name=str(item.get("name") or ""), settings=settings, user_agent=str(item.get("user_agent") or DEFAULT_USER_AGENT), logger=logger)
        if result.get("error_type") == "rate_limited":
            analyzer._RATE_LIMIT_GUARD.record_hit(logger)
        return result

    if media_due:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="stream-sort-media") as executor:
            futures = {executor.submit(run_media, item): item for item, _ in media_due}
            for completed, future in enumerate(as_completed(futures), start=1):
                item = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {"tested_at": analyzer._utc_now_iso(), "status": "dead", "error_type": "other", "error": str(exc), "stats": {}, "details": {}}
                media_results[int(item["id"])] = dict(result)
                elapsed = max(time.monotonic() - media_started, 0.001)
                eta = elapsed / completed * (len(media_due) - completed) if completed < len(media_due) else 0.0
                counts = _status_counts(items, cache)
                old_status = str((cache.get(str(item["id"])) or {}).get("status") or "unknown").lower()
                new_status = str(result.get("status") or "unknown").lower()
                counts[old_status] -= 1
                counts[new_status] += 1
                stats = result.get("stats") or {}
                logger.info("[Analyze Media] %d%% (%d/%d) stream=%s reason=%s health=%s resolution=%s fps=%s bitrate=%skbps | overall %s cached_media=%d pending_media=%d | ETA=%s", int(round(completed / len(media_due) * 100)), completed, len(media_due), item["id"], reason_by_id[int(item["id"])], new_status, stats.get("resolution") or "n/a", f"{float(stats['source_fps']):.1f}" if stats.get("source_fps") is not None else "n/a", f"{float(stats['video_bitrate']):.0f}" if stats.get("video_bitrate") is not None else "n/a", _overall_health_text(counts), total - len(media_due), len(media_due) - completed, analyzer._format_eta(eta))

        by_id = {int(item["id"]): item for item, _ in media_due}
        for retry_pass in range(1, retries + 1):
            retry_ids = [sid for sid, result in media_results.items() if str(result.get("error_type") or "") in analyzer.RETRYABLE_ERROR_TYPES]
            if not retry_ids:
                break
            backoff = max(1.0, account_delay * 3.0)
            logger.info("[Analyze Retry %d/%d] waiting %.1fs before retrying %d media checks", retry_pass, retries, backoff, len(retry_ids))
            time.sleep(backoff)
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="stream-sort-media-retry") as executor:
                futures = {executor.submit(run_media, by_id[sid]): by_id[sid] for sid in retry_ids}
                for future in as_completed(futures):
                    item = futures[future]
                    try:
                        media_results[int(item["id"])] = dict(future.result())
                    except Exception as exc:
                        media_results[int(item["id"])] = {"tested_at": analyzer._utc_now_iso(), "status": "dead", "error_type": "other", "error": str(exc), "stats": {}, "details": {}}

        for item, _ in media_due:
            sid = int(item["id"])
            result = media_results[sid]
            cache[str(sid)] = _merge_media_result(item, cache.get(str(sid)), result)
            analyzer._persist_dispatcharr_result(sid, result, logger)
        analyzer.save_analysis_cache(cache, cache_path)

    media_changed_ids = {sid for sid, result in media_results.items() if _stats_signature(result.get("stats")) != old_signatures.get(sid)}
    now = datetime.now(timezone.utc)
    throughput_due = []
    for item in items:
        sid = int(item["id"])
        entry = cache.get(str(sid)) or {}
        if str(entry.get("status") or "unknown").lower() != "alive":
            continue
        reason = throughput_check_reason(entry, url_hash=analyzer._stream_url_hash(str(item.get("url") or "")), ttl_hours=throughput_ttl_hours, now=now)
        if sid in media_changed_ids:
            reason = "media_changed"
        if reason:
            throughput_due.append((item, reason))

    throughput_checked_ids = set()
    if throughput_due:
        throughput_started = time.monotonic()
        min_start_interval = 60.0 / float(throughput_rate_per_minute)
        last_probe_started = 0.0
        account_next_eligible = {}
        for completed, (item, reason) in enumerate(throughput_due, start=1):
            account_id = item.get("account_id")
            wait_until = max(last_probe_started + min_start_interval if last_probe_started else 0.0, account_next_eligible.get(account_id, 0.0))
            while time.monotonic() < wait_until:
                time.sleep(min(0.25, wait_until - time.monotonic()))
            last_probe_started = time.monotonic()
            entry = cache.get(str(item["id"])) or {}
            stats = entry.get("stats") or {}
            _width, height = parse_resolution(stats)
            fps = parse_fps(stats)
            nominal = estimate_nominal_throughput_kbps(height, fps)
            result = probe_stream(str(item.get("url") or ""), nominal_video_kbps=nominal, duration_seconds=throughput_duration, timeout_seconds=throughput_timeout, user_agent=str(item.get("user_agent") or DEFAULT_USER_AGENT))
            account_next_eligible[account_id] = time.monotonic() + throughput_account_delay
            cache[str(item["id"])] = _merge_throughput_result(item, entry, result, ttl_hours=throughput_ttl_hours)
            analyzer.save_analysis_cache(cache, cache_path)
            throughput_checked_ids.add(int(item["id"]))
            elapsed = max(time.monotonic() - throughput_started, 0.001)
            eta = elapsed / completed * (len(throughput_due) - completed) if completed < len(throughput_due) else 0.0
            counts = _throughput_counts(items, cache)
            alive_count = sum(1 for candidate in items if str((cache.get(str(candidate["id"])) or {}).get("status") or "unknown").lower() == "alive")
            logger.info("[Analyze Throughput] %d%% (%d/%d) stream=%s reason=%s throughput=%s measured=%sMbps nominal=%skbps | overall %s cached_throughput=%d pending_throughput=%d | ETA=%s", int(round(completed / len(throughput_due) * 100)), completed, len(throughput_due), item["id"], reason, result.get("status") or "unknown", result.get("measured_mbps", "n/a"), nominal, _overall_throughput_text(counts), max(0, alive_count - len(throughput_due)), len(throughput_due) - completed, analyzer._format_eta(eta))

    media_checked_ids = set(media_results)
    fully_cached = sum(1 for item in items if int(item["id"]) not in media_checked_ids and int(item["id"]) not in throughput_checked_ids)
    health_counts = _status_counts(items, cache)
    throughput_counts = _throughput_counts(items, cache)
    logger.info("[Analyze] Complete: streams=%d media_checked=%d throughput_checked=%d fully_cached=%d | health %s | throughput %s", total, len(media_checked_ids), len(throughput_checked_ids), fully_cached, _overall_health_text(health_counts), _overall_throughput_text(throughput_counts))
    return {"streams_analyzed": total, "streams_selected": total, "media_checked": len(media_checked_ids), "throughput_checked": len(throughput_checked_ids), "fully_cached": fully_cached, "channels_selected": len({row.channel_id for row in rows}), "filters": filter_summary, "status_counts": {key: value for key, value in health_counts.items() if value > 0}, "throughput_status_counts": {key: value for key, value in throughput_counts.items() if value > 0}, "cache_path": cache_path}


def install() -> None:
    """Make the cache-aware analyzer the implementation used by plugin.py."""
    analyzer.analyze_assigned_streams = analyze_assigned_streams
