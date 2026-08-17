from __future__ import annotations

import json
import os
import tempfile
import time
from datetime import datetime, timezone
from typing import Any

from .scoring import (
    StreamCandidate,
    estimate_nominal_throughput_kbps,
    parse_fps,
    parse_name_rules,
    parse_resolution,
    parse_source_rules,
    rank_candidates,
)
from .throughput import DEFAULT_CACHE_PATH, DEFAULT_USER_AGENT, load_cache, probe_stream, save_cache


REPORT_PATH = "/data/dispatcharr_stream_sort_report.json"


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _settings(settings: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_rules": parse_source_rules(settings.get("source_scores", "")),
        "name_rules": parse_name_rules(settings.get("name_score_rules", "")),
        "throughput_cache_ttl_minutes": max(
            0.0, _as_float(settings.get("throughput_cache_ttl_minutes"), 30.0)
        ),
        "include_single_stream_channels": _as_bool(
            settings.get("include_single_stream_channels"), False
        ),
    }


def _load_channel_candidates(throughput_cache: dict[str, dict[str, Any]]):
    from apps.channels.models import ChannelStream

    rows = list(
        ChannelStream.objects.select_related("channel", "stream", "stream__m3u_account")
        .order_by("channel_id", "order", "id")
    )
    grouped: dict[int, list[Any]] = {}
    for row in rows:
        grouped.setdefault(row.channel_id, []).append(row)

    results: list[tuple[Any, list[Any], list[StreamCandidate]]] = []
    for channel_rows in grouped.values():
        channel = channel_rows[0].channel
        candidates: list[StreamCandidate] = []
        for row in channel_rows:
            stream = row.stream
            account = stream.m3u_account
            candidates.append(
                StreamCandidate(
                    stream_id=stream.id,
                    name=stream.name or "",
                    original_order=row.order,
                    stats=stream.stream_stats,
                    stats_updated_at=stream.stream_stats_updated_at,
                    m3u_account_id=getattr(stream, "m3u_account_id", None),
                    m3u_account_name=getattr(account, "name", "") if account else "",
                    m3u_account_active=bool(getattr(account, "is_active", True)) if account else True,
                    is_stale=bool(getattr(stream, "is_stale", False)),
                    url=stream.url,
                    throughput=throughput_cache.get(str(stream.id)),
                )
            )
        results.append((channel, channel_rows, candidates))
    return results


def _evaluation_json(evaluation) -> dict[str, Any]:
    return {
        "stream_id": evaluation.stream_id,
        "name": evaluation.name,
        "original_order": evaluation.original_order,
        "viability": evaluation.viability,
        "viability_rank": evaluation.viability_rank,
        "resolution_tier": evaluation.resolution_tier,
        "resolution": (
            f"{evaluation.width}x{evaluation.height}"
            if evaluation.width and evaluation.height
            else None
        ),
        "fps": round(evaluation.fps, 3) if evaluation.fps is not None else None,
        "video_bitrate_kbps": (
            round(evaluation.video_bitrate_kbps, 1)
            if evaluation.video_bitrate_kbps is not None
            else None
        ),
        "throughput_status": evaluation.throughput_status,
        "score": evaluation.total_score,
        "score_breakdown": evaluation.breakdown,
        "matched_name_rules": evaluation.matched_name_rules,
        "matched_source_rule": evaluation.source_rule,
        "notes": evaluation.notes,
    }


def _write_json_atomic(payload: dict[str, Any], path: str) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".stream-sort-report-", suffix=".json", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=False, default=str)
            handle.write("\n")
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def sort_channels(
    settings: dict[str, Any],
    *,
    apply: bool,
    logger,
    report_path: str = REPORT_PATH,
    cache_path: str = DEFAULT_CACHE_PATH,
) -> dict[str, Any]:
    from django.db import transaction
    from apps.channels.models import ChannelStream

    cfg = _settings(settings)
    cache = load_cache(cache_path)
    channels = _load_channel_candidates(cache)

    changed_channels = 0
    changed_rows = 0
    skipped_channels = 0
    channel_reports: list[dict[str, Any]] = []
    rows_to_update: list[Any] = []

    now = datetime.now(timezone.utc)
    for channel, channel_rows, candidates in channels:
        if len(candidates) < 2 and not cfg["include_single_stream_channels"]:
            skipped_channels += 1
            continue

        ranked = rank_candidates(
            candidates,
            source_rules=cfg["source_rules"],
            name_rules=cfg["name_rules"],
            throughput_cache_ttl_minutes=cfg["throughput_cache_ttl_minutes"],
            now=now,
        )
        old_ids = [row.stream_id for row in sorted(channel_rows, key=lambda r: (r.order, r.id))]
        new_ids = [evaluation.stream_id for evaluation in ranked]
        changed = old_ids != new_ids
        if changed:
            changed_channels += 1

        by_stream_id = {row.stream_id: row for row in channel_rows}
        for new_order, evaluation in enumerate(ranked):
            row = by_stream_id[evaluation.stream_id]
            if row.order != new_order:
                changed_rows += 1
                row.order = new_order
                rows_to_update.append(row)

        channel_reports.append(
            {
                "channel_id": channel.id,
                "channel_number": channel.channel_number,
                "channel_name": channel.name,
                "changed": changed,
                "current_stream_ids": old_ids,
                "proposed_stream_ids": new_ids,
                "streams": [_evaluation_json(item) for item in ranked],
            }
        )

    if apply and rows_to_update:
        with transaction.atomic():
            ChannelStream.objects.bulk_update(rows_to_update, ["order"])

    payload = {
        "generated_at": now.isoformat(),
        "mode": "apply" if apply else "dry_run",
        "channels_evaluated": len(channel_reports),
        "channels_changed": changed_channels,
        "rows_changed": changed_rows,
        "channels_skipped": skipped_channels,
        "channels": channel_reports,
    }
    _write_json_atomic(payload, report_path)

    logger.info(
        "Stream Sort %s: evaluated=%s changed_channels=%s changed_rows=%s report=%s",
        "apply" if apply else "dry-run",
        len(channel_reports),
        changed_channels,
        changed_rows,
        report_path,
    )
    return payload


def probe_assigned_streams(
    settings: dict[str, Any],
    *,
    logger,
    cache_path: str = DEFAULT_CACHE_PATH,
) -> dict[str, Any]:
    from apps.channels.models import ChannelStream

    duration = max(1.0, _as_float(settings.get("probe_duration_seconds"), 8.0))
    timeout = max(duration + 2.0, _as_float(settings.get("probe_timeout_seconds"), 10.0))
    rate_per_minute = max(1, _as_int(settings.get("probe_rate_per_minute"), 6))
    per_account_delay = max(0.0, _as_float(settings.get("probe_per_account_delay_seconds"), 1.0))
    max_streams = max(0, _as_int(settings.get("probe_max_streams"), 0))

    rows = list(
        ChannelStream.objects.select_related("stream", "stream__m3u_account", "stream__m3u_account__user_agent")
        .order_by("channel_id", "order", "id")
    )
    seen: set[int] = set()
    streams: list[dict[str, Any]] = []
    for row in rows:
        stream = row.stream
        if stream.id in seen:
            continue
        seen.add(stream.id)
        width, height = parse_resolution(stream.stream_stats)
        fps = parse_fps(stream.stream_stats)
        account = stream.m3u_account
        try:
            user_agent = account.get_user_agent_string() if account else DEFAULT_USER_AGENT
        except Exception:
            user_agent = DEFAULT_USER_AGENT
        streams.append(
            {
                "id": stream.id,
                "name": stream.name or "",
                "url": stream.url or "",
                "account_id": getattr(stream, "m3u_account_id", None),
                "user_agent": user_agent or DEFAULT_USER_AGENT,
                "nominal_video_kbps": estimate_nominal_throughput_kbps(height, fps),
            }
        )
        if max_streams and len(streams) >= max_streams:
            break

    cache = load_cache(cache_path)
    results: dict[int, dict[str, Any]] = {}
    min_start_interval = 60.0 / float(rate_per_minute)
    last_probe_started = 0.0
    account_next_eligible: dict[int | None, float] = {}

    for item in streams:
        account_id = item["account_id"]
        next_global = last_probe_started + min_start_interval if last_probe_started else 0.0
        next_account = account_next_eligible.get(account_id, 0.0)
        wait_until = max(next_global, next_account)
        while time.monotonic() < wait_until:
            time.sleep(min(0.25, wait_until - time.monotonic()))

        last_probe_started = time.monotonic()
        result = probe_stream(
            item["url"],
            nominal_video_kbps=item["nominal_video_kbps"],
            duration_seconds=duration,
            timeout_seconds=timeout,
            user_agent=item["user_agent"],
        )
        account_next_eligible[account_id] = time.monotonic() + per_account_delay
        cache[str(item["id"])] = result
        results[item["id"]] = result
        logger.info(
            "Stream Sort probe: stream=%s status=%s throughput=%sMbps nominal=%skbps",
            item["id"],
            result.get("status"),
            result.get("measured_mbps", "n/a"),
            item["nominal_video_kbps"],
        )
        # Persist incrementally so partial work survives an interrupted run.
        save_cache(cache, cache_path)

    counts: dict[str, int] = {}
    for result in results.values():
        status = str(result.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1

    return {
        "streams_probed": len(results),
        "status_counts": counts,
        "cache_path": cache_path,
    }
