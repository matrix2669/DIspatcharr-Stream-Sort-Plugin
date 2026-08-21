from __future__ import annotations

import collections
import time
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


def health_check_reason(
    entry: Mapping[str, Any] | None,
    *,
    url_hash: str,
    ttl_hours: float,
    content_ttl_hours: float | None = None,
    now: datetime,
) -> str | None:
    if not entry:
        return "missing"
    if str(entry.get("url_hash") or "") != url_hash:
        return "url_changed"
    status = str(entry.get("status") or "unknown").strip().lower()
    if status != "alive":
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
    if age >= ttl_hours:
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
            if playback_age is None or playback_age >= content_ttl_hours:
                return "content_missing"
        elif content_ttl_hours <= 0 or content_age >= content_ttl_hours:
            return "content_ttl_expired"
    return None


def metadata_check_reason(entry: Mapping[str, Any] | None, *, url_hash: str, ttl_hours: float, now: datetime) -> str | None:
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
    if age >= ttl_hours:
        return "ttl_expired"
    return None


media_check_reason = health_check_reason


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


def _merge_dispatcharr_metadata(item: Mapping[str, Any], previous: Mapping[str, Any]) -> tuple[dict[str, Any], bool, bool]:
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
    old_signature = _stats_signature(merged.get("stats"))
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
    return merged, True, _stats_signature(stats) != old_signature


def _sync_dispatcharr_metadata(items, cache) -> tuple[int, set[int]]:
    refreshed = 0
    changed_ids: set[int] = set()
    for item in items:
        key = str(item["id"])
        previous = cache.get(key)
        if not isinstance(previous, Mapping):
            continue
        merged, did_refresh, signature_changed = _merge_dispatcharr_metadata(item, previous)
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
        cache[key] = entry
        refreshed += 1
    return refreshed


def _merge_media_result(item, previous, result) -> dict[str, Any]:
    merged = dict(previous or {})
    previous_throughput = merged.get("throughput")
    previous_stats = merged.get("stats")
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


def _analysis_reason(entry: Mapping[str, Any] | None, *, url_hash: str, health_ttl_hours: float, content_ttl_hours: float, metadata_ttl_hours: float, now: datetime) -> str | None:
    reason = health_check_reason(
        entry,
        url_hash=url_hash,
        ttl_hours=health_ttl_hours,
        content_ttl_hours=content_ttl_hours,
        now=now,
    )
    if reason:
        return f"health_{reason}"
    reason = metadata_check_reason(entry, url_hash=url_hash, ttl_hours=metadata_ttl_hours, now=now)
    if reason:
        return f"metadata_{reason}"
    return None


def analyze_assigned_streams(settings: Mapping[str, Any], *, logger, cache_path: str = analyzer.ANALYSIS_CACHE_PATH) -> dict[str, Any]:
    from apps.channels.models import ChannelStream
    from .sorter import resolve_channel_scope

    channel_ids, filter_summary = resolve_channel_scope(settings)
    workers = max(1, min(16, analyzer._as_int(settings.get("analysis_workers"), 2)))
    retries = max(0, min(5, analyzer._as_int(settings.get("analysis_retries"), 3)))
    account_delay = max(0.0, analyzer._as_float(settings.get("analysis_per_account_delay_seconds"), 1.0))
    max_streams = max(0, analyzer._as_int(settings.get("analysis_max_streams"), 0))
    metadata_ttl_hours = max(0.0, analyzer._as_float(settings.get("stream_data_ttl_hours"), 12.0))
    health_ttl_hours = max(0.0, analyzer._as_float(settings.get("health_content_ttl_hours"), 24.0))
    content_ttl_hours = max(0.0, analyzer._as_float(settings.get("content_validation_ttl_hours"), 168.0))
    playback_health_reuse = analyzer._as_bool(settings.get("playback_health_reuse"), True)
    playback_health_min_seconds = max(60.0, analyzer._as_float(settings.get("playback_health_min_seconds"), 300.0))
    playback_health_clean_min_seconds = max(30.0, analyzer._as_float(settings.get("playback_health_clean_min_seconds"), 60.0))
    playback_health_ttl_hours = max(0.0, analyzer._as_float(settings.get("playback_health_ttl_hours"), 6.0))
    throughput_ttl_hours = max(0.0, analyzer._as_float(settings.get("healthy_throughput_ttl_hours"), 6.0))
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

    dispatcharr_metadata_refreshed, dispatcharr_metadata_changed_ids = _sync_dispatcharr_metadata(items, cache)
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
            now=now,
        )
        if reason:
            media_due.append((item, reason))

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
                now=now,
            )
            if reason or int(item["id"]) in dispatcharr_metadata_changed_ids:
                initial_throughput_due += 1
            elif int(item["id"]) not in media_due_ids:
                initial_fully_cached += 1
        elif int(item["id"]) not in media_due_ids:
            initial_fully_cached += 1

    logger.info(
        "[Analyze] Starting: streams=%d media_due=%d throughput_due=%d fully_cached=%d playback_health_refreshed=%d dispatcharr_metadata_refreshed=%d metadata_ttl=%.1fh health_ttl=%.1fh content_ttl=%.1fh healthy_throughput_ttl=%.1fh workers=%d",
        total, len(media_due), initial_throughput_due, initial_fully_cached, playback_health_refreshed, dispatcharr_metadata_refreshed,
        metadata_ttl_hours, health_ttl_hours, content_ttl_hours, throughput_ttl_hours, workers,
    )
    if not items:
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
        }

    capacity_manager = build_capacity_manager(items, logger=logger)

    media_results = {}
    media_capacity_deferred_ids = set()
    old_signatures = {
        int(item["id"]): _stats_signature((cache.get(str(item["id"])) or {}).get("stats"))
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
            cache[str(sid)] = _merge_media_result(item, cache.get(str(sid)), result)
            analyzer._persist_dispatcharr_result(sid, result, logger)
        analyzer.save_analysis_cache(cache, cache_path)

    media_changed_ids = {
        sid for sid, result in media_results.items()
        if _stats_signature(result.get("stats")) != old_signatures.get(sid)
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
            now=now,
        )
        if sid in media_changed_ids:
            reason = "media_changed"
        if reason:
            throughput_due.append((item, reason))

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
            analyzer.save_analysis_cache(cache, cache_path)
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
    }


def install() -> None:
    analyzer.analyze_assigned_streams = analyze_assigned_streams
