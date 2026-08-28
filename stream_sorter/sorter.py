from __future__ import annotations

import json
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from fnmatch import fnmatchcase
from typing import Any, Iterable, Mapping

from .scoring import (
    StreamCandidate,
    estimate_nominal_throughput_kbps,
    parse_fps,
    parse_name_rules,
    parse_resolution,
    parse_source_rules,
    rank_candidates,
)
from .analyzer import _format_eta
from .throughput import DEFAULT_CACHE_PATH, DEFAULT_USER_AGENT, load_cache, probe_stream, save_cache
from .reliability import RELIABILITY_PATH, load_reliability_cache


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


def _split_filter_values(value: Any) -> list[str]:
    """Return normalized comma/newline/semicolon-separated filter tokens."""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        parts: list[str] = []
        for item in value:
            parts.extend(re.split(r"[,;\n]+", str(item)))
    else:
        parts = re.split(r"[,;\n]+", str(value))

    result: list[str] = []
    seen: set[str] = set()
    for part in parts:
        token = part.strip()
        if not token:
            continue
        dedupe_key = token.casefold()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        result.append(token)
    return result


def _resolve_filter_tokens(
    tokens: Iterable[str],
    records: Iterable[Mapping[str, Any]],
    *,
    label: str,
) -> tuple[set[int], list[dict[str, Any]]]:
    """Resolve filter tokens against rows containing ``id`` and ``name``.

    Supported syntax:
      - Local           -> case-insensitive exact name
      - Event* / name:Event* -> case-insensitive name wildcard
      - 7 / id:7       -> database ID
      - name:123       -> exact name even when the name is numeric
    """
    rows = [dict(record) for record in records]
    by_id = {int(row["id"]): row for row in rows}
    by_name = {str(row.get("name") or "").strip().casefold(): row for row in rows}

    resolved_ids: set[int] = set()
    resolved: list[dict[str, Any]] = []
    unresolved: list[str] = []
    seen_ids: set[int] = set()

    for raw_token in tokens:
        token = str(raw_token).strip()
        token_cf = token.casefold()
        matched_rows: list[dict[str, Any]] = []

        if token_cf.startswith("name:"):
            name_token = token[5:].strip()
            name_token_cf = name_token.casefold()
            if "*" in name_token or "?" in name_token:
                matched_rows = [
                    row
                    for row in rows
                    if fnmatchcase(str(row.get("name") or "").strip().casefold(), name_token_cf)
                ]
            else:
                row = by_name.get(name_token_cf)
                matched_rows = [row] if row is not None else []
        elif token_cf.startswith("id:"):
            id_text = token[3:].strip()
            row = by_id.get(int(id_text)) if id_text.isdigit() else None
            matched_rows = [row] if row is not None else []
        elif token.isdigit():
            row = by_id.get(int(token))
            matched_rows = [row] if row is not None else []
        elif "*" in token or "?" in token:
            matched_rows = [
                row
                for row in rows
                if fnmatchcase(str(row.get("name") or "").strip().casefold(), token_cf)
            ]
        else:
            row = by_name.get(token_cf)
            matched_rows = [row] if row is not None else []

        if not matched_rows:
            unresolved.append(token)
            continue

        for row in matched_rows:
            row_id = int(row["id"])
            resolved_ids.add(row_id)
            if row_id not in seen_ids:
                seen_ids.add(row_id)
                resolved.append({"id": row_id, "name": str(row.get("name") or "")})

    if unresolved:
        raise ValueError(
            f"Unknown {label} filter value(s): {', '.join(unresolved)}. "
            "Use an exact name, numeric ID, id:<ID>, name:<NAME>, or "
            "case-insensitive name wildcards (* and ?)."
        )

    return resolved_ids, resolved


def _partition_channel_scope(
    all_channel_ids: set[int],
    analyze_sort_ids: set[int],
    analyze_only_ids: set[int],
    *,
    analyze_sort_filtered: bool,
) -> tuple[set[int] | None, set[int] | None]:
    """Return analysis and sorting channel scopes.

    An empty Analyze & Sort filter means all channels remain in the ordinary
    scope. Analyze Only is always subtracted from sorting and therefore wins
    when the same channel appears in both lists.
    """
    analysis_ids = (
        analyze_sort_ids | analyze_only_ids
        if analyze_sort_filtered
        else None
    )
    if analyze_sort_filtered:
        sort_ids = analyze_sort_ids - analyze_only_ids
    elif analyze_only_ids:
        sort_ids = all_channel_ids - analyze_only_ids
    else:
        sort_ids = None
    return analysis_ids, sort_ids


def resolve_channel_scope(
    settings: Mapping[str, Any],
    *,
    purpose: str = "analysis",
) -> tuple[set[int] | None, dict[str, Any]]:
    """Resolve the configured analysis or sorting channel-ID scope.

    Settings select either channel groups or channel profiles and provide
    separate Analyze & Sort and Analyze Only filters. Removed legacy scope
    settings are intentionally ignored.
    """
    from django.db.models import Q
    from apps.channels.models import (
        Channel,
        ChannelGroup,
        ChannelProfile,
        ChannelProfileMembership,
    )

    if purpose not in {"analysis", "sort"}:
        raise ValueError(f"Unknown channel scope purpose: {purpose}")

    analyze_sort_tokens = _split_filter_values(settings.get("analyze_sort_filter", ""))
    analyze_only_tokens = _split_filter_values(settings.get("analyze_only_filter", ""))

    raw_filter_type = str(settings.get("channel_filter_type") or "channel_profile").strip().casefold()
    filter_type_aliases = {
        "group": "channel_group",
        "groups": "channel_group",
        "channel_group": "channel_group",
        "profile": "channel_profile",
        "profiles": "channel_profile",
        "channel_profile": "channel_profile",
    }
    filter_type = filter_type_aliases.get(raw_filter_type)
    if filter_type is None:
        raise ValueError("Filter type must be Channel groups or Channel profiles.")

    records = list(
        ChannelGroup.objects.values("id", "name")
        if filter_type == "channel_group"
        else ChannelProfile.objects.values("id", "name")
    )
    label = "channel group" if filter_type == "channel_group" else "channel profile"

    analyze_sort_ids, resolved_analyze_sort = _resolve_filter_tokens(
        analyze_sort_tokens,
        records,
        label=label,
    )
    analyze_only_ids, resolved_analyze_only = _resolve_filter_tokens(
        analyze_only_tokens,
        records,
        label=label,
    )

    def channel_ids_for(record_ids: set[int]) -> set[int]:
        if not record_ids:
            return set()
        if filter_type == "channel_group":
            return set(
                Channel.objects.filter(
                    Q(override__channel_group_id__in=record_ids)
                    | Q(
                        override__channel_group_id__isnull=True,
                        channel_group_id__in=record_ids,
                    )
                ).values_list("id", flat=True)
            )
        return set(
            ChannelProfileMembership.objects.filter(
                channel_profile_id__in=record_ids,
                enabled=True,
            ).values_list("channel_id", flat=True)
        )

    analyze_sort_channel_ids = channel_ids_for(analyze_sort_ids)
    analyze_only_channel_ids = channel_ids_for(analyze_only_ids)
    all_channel_ids = (
        set(Channel.objects.values_list("id", flat=True))
        if not analyze_sort_tokens and analyze_only_channel_ids
        else set()
    )
    analysis_ids, sort_ids = _partition_channel_scope(
        all_channel_ids,
        analyze_sort_channel_ids,
        analyze_only_channel_ids,
        analyze_sort_filtered=bool(analyze_sort_tokens),
    )
    allowed_channel_ids = analysis_ids if purpose == "analysis" else sort_ids

    combined_resolved = list(resolved_analyze_sort)
    combined_ids = {int(row["id"]) for row in combined_resolved}
    combined_resolved.extend(
        row for row in resolved_analyze_only if int(row["id"]) not in combined_ids
    )

    summary: dict[str, Any] = {
        "channel_groups": combined_resolved if filter_type == "channel_group" else [],
        "channel_profiles": combined_resolved if filter_type == "channel_profile" else [],
        "filter_type": filter_type,
        "scope_purpose": purpose,
        "analyze_sort_filters": resolved_analyze_sort,
        "analyze_only_filters": resolved_analyze_only,
        "match_mode": "all_channels" if allowed_channel_ids is None else filter_type,
        "selected_channel_count": (
            None if allowed_channel_ids is None else len(allowed_channel_ids)
        ),
    }
    return allowed_channel_ids, summary


def _settings(settings: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source_rules": parse_source_rules(settings.get("source_scores", "")),
        "name_rules": parse_name_rules(settings.get("name_score_rules", "")),
        "healthy_throughput_ttl_hours": max(
            0.0, _as_float(settings.get("healthy_throughput_ttl_hours"), 48.0)
        ),
        "degraded_throughput_ttl_hours": max(
            0.0, _as_float(settings.get("degraded_throughput_ttl_hours"), 24.0)
        ),
        "unknown_throughput_ttl_hours": max(
            0.0, _as_float(settings.get("unknown_throughput_ttl_hours"), 4.0)
        ),
        "throughput_ttl_jitter_percent": max(
            0.0, min(100.0, _as_float(settings.get("analysis_ttl_jitter_percent"), 30.0))
        ),
        "include_single_stream_channels": _as_bool(
            settings.get("include_single_stream_channels"), False
        ),
        "reliability_scoring_enabled": _as_bool(
            settings.get("reliability_scoring_enabled"), True
        ),
    }


def _load_channel_candidates(
    throughput_cache: dict[str, dict[str, Any]],
    reliability_cache: dict[str, Any],
    channel_ids: set[int] | None = None,
):
    from apps.channels.models import ChannelStream

    queryset = ChannelStream.objects.select_related(
        "channel", "stream", "stream__m3u_account"
    ).order_by("channel_id", "order", "id")
    if channel_ids is not None:
        queryset = queryset.filter(channel_id__in=channel_ids)
    rows = list(queryset)

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
                    reliability=(reliability_cache.get("streams") or {}).get(str(stream.id)),
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
        "reliability_status": evaluation.reliability_status,
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

    run_started = time.monotonic()
    cfg = _settings(settings)
    channel_ids, filter_summary = resolve_channel_scope(settings, purpose="sort")
    cache = load_cache(cache_path)
    reliability_cache = load_reliability_cache(RELIABILITY_PATH)
    channels = _load_channel_candidates(cache, reliability_cache, channel_ids)

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
            healthy_throughput_ttl_hours=cfg["healthy_throughput_ttl_hours"],
            degraded_throughput_ttl_hours=cfg["degraded_throughput_ttl_hours"],
            unknown_throughput_ttl_hours=cfg["unknown_throughput_ttl_hours"],
            throughput_ttl_jitter_percent=cfg["throughput_ttl_jitter_percent"],
            reliability_scoring_enabled=cfg["reliability_scoring_enabled"],
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

    runtime_seconds = max(0.0, time.monotonic() - run_started)
    payload = {
        "generated_at": now.isoformat(),
        "mode": "apply" if apply else "dry_run",
        "filters": filter_summary,
        "channels_evaluated": len(channel_reports),
        "channels_changed": changed_channels,
        "rows_changed": changed_rows,
        "channels_skipped": skipped_channels,
        "total_runtime_seconds": round(runtime_seconds, 3),
        "total_runtime": _format_eta(runtime_seconds),
        "channels": channel_reports,
    }
    _write_json_atomic(payload, report_path)

    logger.info(
        "Stream Sort %s: filter=%s selected=%s evaluated=%s changed_channels=%s changed_rows=%s report=%s runtime=%s",
        "apply" if apply else "dry-run",
        filter_summary["match_mode"],
        filter_summary["selected_channel_count"],
        len(channel_reports),
        changed_channels,
        changed_rows,
        report_path,
        payload["total_runtime"],
    )
    return payload


def probe_assigned_streams(
    settings: dict[str, Any],
    *,
    logger,
    cache_path: str = DEFAULT_CACHE_PATH,
) -> dict[str, Any]:
    from apps.channels.models import ChannelStream

    channel_ids, filter_summary = resolve_channel_scope(settings)
    duration = max(1.0, _as_float(settings.get("probe_duration_seconds"), 6.0))
    timeout = max(duration + 2.0, _as_float(settings.get("probe_timeout_seconds"), 10.0))
    rate_per_minute = max(1, _as_int(settings.get("probe_rate_per_minute"), 6))
    per_account_delay = max(0.0, _as_float(settings.get("probe_per_account_delay_seconds"), 1.0))
    max_streams = max(0, _as_int(settings.get("probe_max_streams"), 0))

    queryset = ChannelStream.objects.select_related(
        "stream", "stream__m3u_account", "stream__m3u_account__user_agent"
    ).order_by("channel_id", "order", "id")
    if channel_ids is not None:
        queryset = queryset.filter(channel_id__in=channel_ids)
    rows = list(queryset)

    selected_channel_count = len({row.channel_id for row in rows})
    seen: set[int] = set()
    streams: list[dict[str, Any]] = []
    for row in rows:
        stream = row.stream
        if stream.id in seen:
            continue
        seen.add(stream.id)
        _width, height = parse_resolution(stream.stream_stats)
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
        save_cache(cache, cache_path)

    counts: dict[str, int] = {}
    for result in results.values():
        status = str(result.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1

    return {
        "streams_probed": len(results),
        "channels_selected": selected_channel_count,
        "filters": filter_summary,
        "status_counts": counts,
        "cache_path": cache_path,
    }
