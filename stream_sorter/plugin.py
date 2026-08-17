from __future__ import annotations

from .scoring import DEFAULT_PREFIX_RULES
from .sorter import REPORT_PATH, probe_assigned_streams, sort_channels
from .throughput import DEFAULT_CACHE_PATH


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
            "help_text": "0 means all unique streams assigned to channels.",
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
            "description": "Calculate the proposed order without changing ChannelStream.order.",
            "button_label": "Dry Run",
            "button_variant": "outline",
            "button_color": "blue",
        },
        {
            "id": "sort_streams",
            "label": "Sort Streams",
            "description": "Apply the calculated order to streams already assigned to each channel.",
            "button_label": "Sort Streams",
            "button_variant": "filled",
            "button_color": "blue",
            "confirm": {
                "required": True,
                "title": "Apply stream ordering?",
                "message": "This changes ChannelStream.order only. It does not add or remove stream matches.",
            },
        },
        {
            "id": "probe_throughput",
            "label": "Probe Throughput",
            "description": "Measure delivery throughput for unique streams assigned to channels and cache the results.",
            "button_label": "Probe Throughput",
            "button_variant": "outline",
            "button_color": "orange",
            "confirm": {
                "required": True,
                "title": "Probe assigned streams?",
                "message": "This opens provider stream connections to measure delivery throughput.",
            },
        },
        {
            "id": "probe_and_sort",
            "label": "Probe + Sort",
            "description": "Refresh throughput measurements, then immediately apply stream ordering.",
            "button_label": "Probe + Sort",
            "button_variant": "filled",
            "button_color": "orange",
            "confirm": {
                "required": True,
                "title": "Probe and sort streams?",
                "message": "This opens provider streams for testing and then changes ChannelStream.order.",
            },
        },
    ]

    def run(self, action: str, params: dict, context: dict):
        settings = context.get("settings") or {}
        logger = context.get("logger")

        try:
            if action == "dry_run":
                result = sort_channels(settings, apply=False, logger=logger)
                return {
                    "status": "ok",
                    "message": (
                        f"Dry run complete: {result['channels_evaluated']} channels evaluated; "
                        f"{result['channels_changed']} would change. Report: {REPORT_PATH}"
                    ),
                    **{k: result[k] for k in ("channels_evaluated", "channels_changed", "rows_changed")},
                }

            if action == "sort_streams":
                result = sort_channels(settings, apply=True, logger=logger)
                return {
                    "status": "ok",
                    "message": (
                        f"Sort complete: {result['channels_changed']} channels changed; "
                        f"{result['rows_changed']} order rows updated. Report: {REPORT_PATH}"
                    ),
                    **{k: result[k] for k in ("channels_evaluated", "channels_changed", "rows_changed")},
                }

            if action == "probe_throughput":
                result = probe_assigned_streams(settings, logger=logger)
                return {
                    "status": "ok",
                    "message": (
                        f"Throughput probe complete: {result['streams_probed']} streams. "
                        f"Results: {result['status_counts']}"
                    ),
                    **result,
                }

            if action == "probe_and_sort":
                probe_result = probe_assigned_streams(settings, logger=logger)
                sort_result = sort_channels(settings, apply=True, logger=logger)
                return {
                    "status": "ok",
                    "message": (
                        f"Probed {probe_result['streams_probed']} streams; "
                        f"sorted {sort_result['channels_changed']} changed channels."
                    ),
                    "probe": probe_result,
                    "sort": {
                        k: sort_result[k]
                        for k in ("channels_evaluated", "channels_changed", "rows_changed")
                    },
                }

            return {"status": "error", "message": f"Unknown action: {action}"}
        except ValueError as exc:
            logger.warning("Stream Sort configuration error: %s", exc)
            return {"status": "error", "message": f"Configuration error: {exc}"}
        except Exception as exc:
            logger.exception("Stream Sort action %s failed", action)
            return {"status": "error", "message": f"{type(exc).__name__}: {exc}"}
