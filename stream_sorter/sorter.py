from __future__ import annotations

import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from .scoring import StreamCandidate, parse_name_rules, parse_source_rules, parse_video_bitrate_kbps, rank_candidates
from .throughput import DEFAULT_CACHE_PATH, load_cache, probe_stream, save_cache

REPORT_PATH = "/data/dispatcharr_stream_sort_report.json"


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None: return default
    if isinstance(value, bool): return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}

def _as_float(value: Any, default: float) -> float:
    try: return float(value)
    except (TypeError, ValueError): return default

def _as_int(value: Any, default: int) -> int:
    try: return int(value)
    except (TypeError, ValueError): return default

def _settings(settings):
    return {"source_rules": parse_source_rules(settings.get("source_scores", "")), "name_rules": parse_name_rules(settings.get("name_score_rules", "")), "throughput_cache_ttl_hours": max(0.0, _as_float(settings.get("throughput_cache_ttl_hours"), 24.0)), "include_single_stream_channels": _as_bool(settings.get("include_single_stream_channels"), False)}


def _load_channel_candidates(throughput_cache):
    from apps.channels.models import ChannelStream
    rows = list(ChannelStream.objects.select_related("channel", "stream", "stream__m3u_account").order_by("channel_id", "order", "id")); grouped = {}
    for row in rows: grouped.setdefault(row.channel_id, []).append(row)
    results = []
    for channel_rows in grouped.values():
        channel = channel_rows[0].channel; candidates = []
        for row in channel_rows:
            stream = row.stream; account = stream.m3u_account
            candidates.append(StreamCandidate(stream_id=stream.id, name=stream.name or "", original_order=row.order, stats=stream.stream_stats, stats_updated_at=stream.stream_stats_updated_at, m3u_account_id=getattr(stream, "m3u_account_id", None), m3u_account_name=getattr(account, "name", "") if account else "", m3u_account_active=bool(getattr(account, "is_active", True)) if account else True, is_stale=bool(getattr(stream, "is_stale", False)), url=stream.url, throughput=throughput_cache.get(str(stream.id))))
        results.append((channel, channel_rows, candidates))
    return results


def _evaluation_json(e):
    return {"stream_id": e.stream_id, "name": e.name, "original_order": e.original_order, "viability": e.viability, "viability_rank": e.viability_rank, "resolution_tier": e.resolution_tier, "resolution": f"{e.width}x{e.height}" if e.width and e.height else None, "fps": round(e.fps, 3) if e.fps is not None else None, "video_bitrate_kbps": round(e.video_bitrate_kbps, 1) if e.video_bitrate_kbps is not None else None, "throughput_status": e.throughput_status, "score": e.total_score, "score_breakdown": e.breakdown, "matched_name_rules": e.matched_name_rules, "matched_source_rule": e.source_rule, "notes": e.notes}


def _write_json_atomic(payload, path):
    directory = os.path.dirname(path) or "."; os.makedirs(directory, exist_ok=True); fd, tmp_path = tempfile.mkstemp(prefix=".stream-sort-report-", suffix=".json", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle: json.dump(payload, handle, indent=2, sort_keys=False, default=str); handle.write("\n")
        os.replace(tmp_path, path)
    except Exception:
        try: os.unlink(tmp_path)
        except OSError: pass
        raise


def sort_channels(settings, *, apply, logger, report_path=REPORT_PATH, cache_path=DEFAULT_CACHE_PATH):
    from django.db import transaction
    from apps.channels.models import ChannelStream
    cfg = _settings(settings); cache = load_cache(cache_path); channels = _load_channel_candidates(cache); changed_channels = changed_rows = skipped_channels = 0; channel_reports = []; rows_to_update = []; now = datetime.now(timezone.utc)
    for channel, channel_rows, candidates in channels:
        if len(candidates) < 2 and not cfg["include_single_stream_channels"]: skipped_channels += 1; continue
        ranked = rank_candidates(candidates, source_rules=cfg["source_rules"], name_rules=cfg["name_rules"], throughput_cache_ttl_hours=cfg["throughput_cache_ttl_hours"], now=now)
        old_ids = [row.stream_id for row in sorted(channel_rows, key=lambda r: (r.order, r.id))]; new_ids = [e.stream_id for e in ranked]; changed = old_ids != new_ids
        if changed: changed_channels += 1
        by_stream_id = {row.stream_id: row for row in channel_rows}
        for new_order, e in enumerate(ranked):
            row = by_stream_id[e.stream_id]
            if row.order != new_order: changed_rows += 1; row.order = new_order; rows_to_update.append(row)
        channel_reports.append({"channel_id": channel.id, "channel_number": channel.channel_number, "channel_name": channel.name, "changed": changed, "current_stream_ids": old_ids, "proposed_stream_ids": new_ids, "streams": [_evaluation_json(e) for e in ranked]})
    if apply and rows_to_update:
        with transaction.atomic(): ChannelStream.objects.bulk_update(rows_to_update, ["order"])
    payload = {"generated_at": now.isoformat(), "mode": "apply" if apply else "dry_run", "channels_evaluated": len(channel_reports), "channels_changed": changed_channels, "rows_changed": changed_rows, "channels_skipped": skipped_channels, "channels": channel_reports}; _write_json_atomic(payload, report_path)
    logger.info("Stream Sort %s: evaluated=%s changed_channels=%s changed_rows=%s report=%s", "apply" if apply else "dry-run", len(channel_reports), changed_channels, changed_rows, report_path); return payload


def probe_assigned_streams(settings, *, logger, cache_path=DEFAULT_CACHE_PATH):
    from apps.channels.models import ChannelStream
    duration = max(0.5, _as_float(settings.get("probe_duration_seconds"), 2.5)); timeout = max(1.0, _as_float(settings.get("probe_timeout_seconds"), 6.0)); workers = max(1, min(8, _as_int(settings.get("probe_workers"), 2))); max_streams = max(0, _as_int(settings.get("probe_max_streams"), 0))
    rows = list(ChannelStream.objects.select_related("stream").order_by("channel_id", "order", "id")); seen = set(); streams = []
    for row in rows:
        stream = row.stream
        if stream.id in seen: continue
        seen.add(stream.id); streams.append({"id": stream.id, "name": stream.name or "", "url": stream.url or "", "nominal_video_kbps": parse_video_bitrate_kbps(stream.stream_stats)})
        if max_streams and len(streams) >= max_streams: break
    cache = load_cache(cache_path); results = {}
    def _one(item): return item, probe_stream(item["url"], nominal_video_kbps=item["nominal_video_kbps"], duration_seconds=duration, timeout_seconds=timeout)
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="stream-sort-probe") as executor:
        futures = [executor.submit(_one, item) for item in streams]
        for future in as_completed(futures):
            item, result = future.result(); cache[str(item["id"])] = result; results[item["id"]] = result; logger.info("Stream Sort probe: stream=%s status=%s throughput=%sMbps", item["id"], result.get("status"), result.get("measured_mbps", "n/a"))
    save_cache(cache, cache_path); counts = {}
    for result in results.values(): status = str(result.get("status") or "unknown"); counts[status] = counts.get(status, 0) + 1
    return {"streams_probed": len(results), "status_counts": counts, "cache_path": cache_path}
