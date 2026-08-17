from __future__ import annotations

import logging
import os
import threading

try:
    import fcntl
except ImportError:  # pragma: no cover - Dispatcharr runs on Linux
    fcntl = None

from .scoring import DEFAULT_PREFIX_RULES
from .sorter import REPORT_PATH, probe_assigned_streams, resolve_channel_scope, sort_channels
from .throughput import DEFAULT_CACHE_PATH


PROBE_LOCK_PATH = "/data/dispatcharr_stream_sort_probe.lock"
_FALLBACK_JOB_LOCK = threading.Lock()


def _acquire_job_lock():
    """Acquire a cross-worker lock for the long-running probe job."""
    if fcntl is None:
        if not _FALLBACK_JOB_LOCK.acquire(blocking=False):
            return None
        return False

    os.makedirs(os.path.dirname(PROBE_LOCK_PATH), exist_ok=True)
    handle = open(PROBE_LOCK_PATH, "a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    return handle


def _release_job_lock(lock_handle) -> None:
    if lock_handle is False:
        _FALLBACK_JOB_LOCK.release()
        return
    if lock_handle is None:
        return
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
    finally:
        lock_handle.close()


def _notify(message: str) -> None:
    try:
        from core.utils import send_websocket_update

        send_websocket_update(
            "updates",
            "update",
            {"type": "plugin", "plugin": "Dispatcharr Stream Sort", "message": message},
        )
    except Exception:
        pass


def _background_probe_job(settings: dict, logger, lock_handle, *, sort_after: bool) -> None:
    from django.db import close_old_connections

    close_old_connections()
    try:
        probe_result = probe_assigned_streams(settings, logger=logger)
        if sort_after:
            sort_result = sort_channels(settings, apply=True, logger=logger)
            logger.info(
                "Stream Sort background probe+sort complete: probed=%s changed_channels=%s",
                probe_result["streams_probed"],
                sort_result["channels_changed"],
            )
            _notify(
                f"✅ Stream Sort: probed {probe_result['streams_probed']} streams; "
                f"sorted {sort_result['channels_changed']} changed channels."
            )
        else:
            logger.info(
                "Stream Sort background probe complete: probed=%s status_counts=%s",
                probe_result["streams_probed"],
                probe_result["status_counts"],
            )
            _notify(
                f"✅ Stream Sort: throughput probe complete for "
                f"{probe_result['streams_probed']} streams."
            )
    except Exception:
        logger.exception("Stream Sort background %s failed", "probe+sort" if sort_after else "probe")
        _notify("❌ Stream Sort: background throughput job failed. Check Dispatcharr logs.")
    finally:
        close_old_connections()
        _release_job_lock(lock_handle)


def _start_background_probe(settings: dict, logger, *, sort_after: bool) -> dict:
    # Resolve now so invalid group/profile names fail immediately in the action
    # response instead of several seconds later inside the background worker.
    _channel_ids, scope = resolve_channel_scope(settings)

    lock_handle = _acquire_job_lock()
    if lock_handle is None:
        return {
            "status": "error",
            "message": "A Stream Sort throughput job is already running.",
        }

    worker = threading.Thread(
        target=_background_probe_job,
        args=(dict(settings), logger, lock_handle),
        kwargs={"sort_after": sort_after},
        name="dispatcharr-stream-sort-probe",
        daemon=True,
    )
    try:
        worker.start()
    except Exception:
        _release_job_lock(lock_handle)
        raise

    selected = scope.get("selected_channel_count")
    scope_text = "all channels" if selected is None else f"{selected} selected channels"
    return {
        "status": "ok",
        "message": (
            f"Throughput probe + sort started in the background for {scope_text}."
            if sort_after
            else f"Throughput probe started in the background for {scope_text}."
        ),
        "background": True,
        "filters": scope,
    }


class Plugin:
    name = "Dispatcharr Stream Sort"
    version = "0.1.0"
    description = (
        "Ranks streams already assigned to Dispatcharr channels using hard resolution tiers "
        "and configurable quality, M3U source, prefix/regex, and throughput scoring."
    )
    author = "matrix2669"
    help_url = "https://github.com/matrix2669/DIspatcharr-Stream-Sort-Plugin"

    fields = [
        {
            "id": "scoring_info",
            "label": "Sorting model",
            "type": "info",
            "description": (
                "Dead/stale streams are demoted first. Resolution is a hard tier (2160p > 1440p > "
                "1080p > 720p > 576p > 480p). Inside a resolution tier, bitrate, frame rate, "
                "M3U source, name rules, and cached throughput are added into one score."
            ),
        },
        {
            "id": "filter_info",
            "label": "Channel scope",
            "type": "info",
            "description": (
                "Optional group/profile filters limit every action: Dry Run, Sort Streams, Probe Throughput, "
                "and Probe + Sort. Multiple values inside one filter are ORed; when both filters are set, "
                "a channel must match both."
            ),
        },
        {
            "id": "channel_group_filter",
            "label": "Channel group filter",
            "type": "text",
            "default": "",
            "placeholder": "Local\nSports",
            "help_text": (
                "Optional exact Dispatcharr channel group names or IDs, separated by commas or new lines. "
                "Examples: Local, id:4. Empty means all groups. Uses the channel's effective group, including overrides."
            ),
        },
        {
            "id": "channel_profile_filter",
            "label": "Channel profile filter",
            "type": "text",
            "default": "",
            "placeholder": "Stream Sort Test",
            "help_text": (
                "Optional exact Dispatcharr channel profile names or IDs, separated by commas or new lines. "
                "Only enabled profile memberships are included. Empty means all profiles/channels."
            ),
        },
        {
            "id": "source_scores",
            "label": "M3U source scores",
            "type": "text",
            "default": "",
            "placeholder": "Preferred Provider=20\nid:4=10\nBackup Provider=-10",
            "help_text": (
                "One source=score per line. Match by exact M3U account name or account ID (4=20 or id:4=20). "
                "Positive values promote; negative values demote. First matching rule wins."
            ),
        },
        {
            "id": "name_score_rules",
            "label": "Stream name scoring rules",
            "type": "text",
            "default": DEFAULT_PREFIX_RULES,
            "help_text": (
                "PREFIX=score creates a case-insensitive anchored prefix rule (for example US=20 or ROKU=-20). "
                "Advanced regex uses score::regex. All matching name rules are additive."
            ),
        },
        {
            "id": "throughput_cache_ttl_minutes",
            "label": "Throughput cache TTL (minutes)",
            "type": "number",
            "default": 30,
            "help_text": "Expired throughput probes become UNKNOWN and stop affecting score.",
        },
        {
            "id": "probe_duration_seconds",
            "label": "Throughput probe duration (seconds)",
            "type": "number",
            "default": 8,
        },
        {
            "id": "probe_timeout_seconds",
            "label": "Throughput probe timeout (seconds)",
            "type": "number",
            "default": 10,
        },
        {
            "id": "probe_rate_per_minute",
            "label": "Maximum throughput probes per minute",
            "type": "number",
            "default": 6,
            "help_text": "Global start-rate cap. Default mirrors Stream-Mapparr and limits provider load.",
        },
        {
            "id": "probe_per_account_delay_seconds",
            "label": "Per-source probe delay (seconds)",
            "type": "number",
            "default": 1,
            "help_text": "Minimum delay before starting another probe from the same M3U source.",
        },
        {
            "id": "probe_max_streams",
            "label": "Maximum streams per probe run",
            "type": "number",
            "default": 0,
            "help_text": "0 means all unique streams attached to channels inside the selected channel scope.",
        },
        {
            "id": "paths_info",
            "label": "Files",
            "type": "info",
            "description": f"Dry-run/apply report: {REPORT_PATH} | Throughput cache: {DEFAULT_CACHE_PATH}",
        },
    ]

    actions = [
        {
            "id": "dry_run",
            "label": "Dry Run",
            "description": "Calculate the proposed order for the selected channel scope without changing ChannelStream.order.",
            "button_label": "Dry Run",
            "button_variant": "outline",
            "button_color": "blue",
        },
        {
            "id": "sort_streams",
            "label": "Sort Streams",
            "description": "Apply the calculated order to streams already assigned to channels inside the selected scope.",
            "button_label": "Sort Streams",
            "button_variant": "filled",
            "button_color": "blue",
            "confirm": {
                "required": True,
                "title": "Apply stream ordering?",
                "message": "This changes ChannelStream.order only for the selected channel scope. It does not add or remove stream matches.",
            },
        },
        {
            "id": "probe_throughput",
            "label": "Probe Throughput",
            "description": "Start a background delivery-throughput probe for unique streams in the selected channel scope.",
            "button_label": "Probe Throughput",
            "button_variant": "outline",
            "button_color": "orange",
            "confirm": {
                "required": True,
                "title": "Probe assigned streams?",
                "message": "This opens provider stream connections for streams attached to channels inside the selected scope."
            },
        },
        {
            "id": "probe_and_sort",
            "label": "Probe + Sort",
            "description": "Probe streams in the selected channel scope, then apply ordering when probing completes.",
            "button_label": "Probe + Sort",
            "button_variant": "filled",
            "button_color": "orange",
            "confirm": {
                "required": True,
                "title": "Probe and sort streams?",
                "message": "This probes provider streams in the selected channel scope and then changes ChannelStream.order."
            },
        },
    ]

    def run(self, action: str, params: dict, context: dict):
        settings = context.get("settings") or {}
        logger = context.get("logger") or logging.getLogger("plugins.stream_sorter")

        try:
            if action == "dry_run":
                result = sort_channels(settings, apply=False, logger=logger)
                return {
                    "status": "ok",
                    "message": (
                        f"Dry run complete: {result['channels_evaluated']} channels evaluated; "
                        f"{result['channels_changed']} would change. Report: {REPORT_PATH}"
                    ),
                    **{k: result[k] for k in ("channels_evaluated", "channels_changed", "rows_changed", "filters")},
                }

            if action == "sort_streams":
                result = sort_channels(settings, apply=True, logger=logger)
                return {
                    "status": "ok",
                    "message": (
                        f"Sort complete: {result['channels_changed']} channels changed; "
                        f"{result['rows_changed']} order rows updated. Report: {REPORT_PATH}"
                    ),
                    **{k: result[k] for k in ("channels_evaluated", "channels_changed", "rows_changed", "filters")},
                }

            if action == "probe_throughput":
                return _start_background_probe(settings, logger, sort_after=False)

            if action == "probe_and_sort":
                return _start_background_probe(settings, logger, sort_after=True)

            return {"status": "error", "message": f"Unknown action: {action}"}
        except ValueError as exc:
            logger.warning("Stream Sort configuration error: %s", exc)
            return {"status": "error", "message": f"Configuration error: {exc}"}
        except Exception as exc:
            logger.exception("Stream Sort action %s failed", action)
            return {"status": "error", "message": f"{type(exc).__name__}: {exc}"}
