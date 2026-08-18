from __future__ import annotations

import json
import logging
import os
import threading

try:
    import fcntl
except ImportError:  # pragma: no cover - Dispatcharr runs on Linux
    fcntl = None

from .analyzer import ANALYSIS_CACHE_PATH, analyze_assigned_streams, probe_assigned_streams
from .reliability import RELIABILITY_PATH, record_runtime_event
from .sorter import REPORT_PATH, resolve_channel_scope, sort_channels
from .throughput import DEFAULT_CACHE_PATH


PROBE_LOCK_PATH = "/data/dispatcharr_stream_sort_probe.lock"
_FALLBACK_JOB_LOCK = threading.Lock()
M3U_SOURCE_SCORE_PREFIX = "m3u_source_score_"
LOG_PREFIX = "[Stream Sort]"


def _load_manifest() -> dict:
    path = os.path.join(os.path.dirname(__file__), "plugin.json")
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


_MANIFEST = _load_manifest()


class _StreamSortLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        text = str(msg)
        if not text.startswith(LOG_PREFIX):
            text = f"{LOG_PREFIX} {text}"
        return text, kwargs


LOGGER = _StreamSortLogger(logging.getLogger("plugins.stream_sorter"), {})


def _build_m3u_source_score_fields(accounts):
    """Return one neutral numeric score field per configured M3U account."""
    rows = sorted(
        (dict(account) for account in accounts),
        key=lambda row: (
            str(row.get("name") or "").casefold(),
            int(row.get("id") or 0),
        ),
    )
    fields = []
    for row in rows:
        account_id = int(row["id"])
        name = str(row.get("name") or f"M3U {account_id}")
        active = bool(row.get("is_active", True))
        fields.append(
            {
                "id": f"{M3U_SOURCE_SCORE_PREFIX}{account_id}",
                "label": name if active else f"{name} (inactive)",
                "type": "number",
                "default": 0,
                "help_text": (
                    f"M3U source ID {account_id}. Positive values promote this source, "
                    "negative values demote it, and 0 is neutral."
                ),
            }
        )
    return fields


def _settings_with_dynamic_source_scores(settings):
    """Translate per-account score fields into the existing source-rule format."""
    normalized = dict(settings or {})
    dynamic_scores = []
    for key, value in normalized.items():
        if not str(key).startswith(M3U_SOURCE_SCORE_PREFIX):
            continue
        account_id = str(key)[len(M3U_SOURCE_SCORE_PREFIX):]
        if not account_id.isdigit():
            continue
        try:
            score = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid score for M3U source ID {account_id}: {value!r}"
            ) from exc
        dynamic_scores.append((int(account_id), score))

    if dynamic_scores:
        dynamic_scores.sort(key=lambda item: item[0])
        normalized["source_scores"] = "\n".join(
            f"id:{account_id}={score:g}" for account_id, score in dynamic_scores
        )
    return normalized


def _acquire_job_lock():
    """Acquire one cross-worker lock for analyzer/throughput jobs."""
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


def _background_analyze_job(settings: dict, lock_handle, *, sort_after: bool) -> None:
    from django.db import close_old_connections

    close_old_connections()
    try:
        result = analyze_assigned_streams(settings, logger=LOGGER)
        if sort_after:
            sort_result = sort_channels(settings, apply=True, logger=LOGGER)
            LOGGER.info(
                "[Analyze + Sort] complete analyzed=%s changed_channels=%s",
                result["streams_analyzed"],
                sort_result["channels_changed"],
            )
            _notify(
                f"✅ Stream Sort: analyzed {result['streams_analyzed']} streams; "
                f"sorted {sort_result['channels_changed']} changed channels."
            )
        else:
            LOGGER.info(
                "[Analyze] background job complete analyzed=%s status_counts=%s",
                result["streams_analyzed"],
                result["status_counts"],
            )
            _notify(
                f"✅ Stream Sort: analysis complete for {result['streams_analyzed']} streams "
                f"({result['status_counts']})."
            )
    except Exception:
        LOGGER.exception("[Analyze] background job failed")
        _notify("❌ Stream Sort: stream analysis failed. Check Dispatcharr logs.")
    finally:
        close_old_connections()
        _release_job_lock(lock_handle)


def _background_probe_job(settings: dict, lock_handle, *, sort_after: bool) -> None:
    from django.db import close_old_connections

    close_old_connections()
    try:
        probe_result = probe_assigned_streams(settings, logger=LOGGER)
        if sort_after:
            sort_result = sort_channels(settings, apply=True, logger=LOGGER)
            LOGGER.info(
                "[Throughput + Sort] complete probed=%s changed_channels=%s",
                probe_result["streams_probed"],
                sort_result["channels_changed"],
            )
            _notify(
                f"✅ Stream Sort: probed {probe_result['streams_probed']} streams; "
                f"sorted {sort_result['channels_changed']} changed channels."
            )
        else:
            LOGGER.info(
                "[Throughput] background job complete probed=%s status_counts=%s",
                probe_result["streams_probed"],
                probe_result["status_counts"],
            )
            _notify(
                f"✅ Stream Sort: throughput probe complete for "
                f"{probe_result['streams_probed']} streams."
            )
    except Exception:
        LOGGER.exception("[Throughput] background job failed")
        _notify("❌ Stream Sort: background throughput job failed. Check Dispatcharr logs.")
    finally:
        close_old_connections()
        _release_job_lock(lock_handle)


def _start_background_job(settings: dict, *, kind: str, sort_after: bool) -> dict:
    _channel_ids, scope = resolve_channel_scope(settings)
    lock_handle = _acquire_job_lock()
    if lock_handle is None:
        return {
            "status": "error",
            "message": "A Stream Sort analysis/throughput job is already running.",
        }

    if kind == "analyze":
        target = _background_analyze_job
        thread_name = "dispatcharr-stream-sort-analyze"
        action_text = "Stream analysis"
    elif kind == "throughput":
        target = _background_probe_job
        thread_name = "dispatcharr-stream-sort-throughput"
        action_text = "Throughput probe"
    else:  # pragma: no cover - internal misuse only
        _release_job_lock(lock_handle)
        raise ValueError(f"Unknown background job kind: {kind}")

    worker = threading.Thread(
        target=target,
        args=(dict(settings), lock_handle),
        kwargs={"sort_after": sort_after},
        name=thread_name,
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
            f"{action_text} + sort started in the background for {scope_text}."
            if sort_after
            else f"{action_text} started in the background for {scope_text}."
        ),
        "background": True,
        "filters": scope,
    }


class Plugin:
    name = _MANIFEST["name"]
    version = _MANIFEST["version"]
    description = _MANIFEST["description"]
    author = _MANIFEST.get("author", "")
    help_url = _MANIFEST.get("help_url", "")
    fields = _MANIFEST.get("fields", [])
    actions = _MANIFEST.get("actions", [])

    def __init__(self):
        # Dispatcharr loads fields from the live Plugin instance for enabled
        # plugins. Replace the legacy free-form source_scores textarea with one
        # numeric input per current M3U account while retaining the old setting
        # internally for migration compatibility.
        instance_fields = [dict(field) for field in type(self).fields]
        source_index = next(
            (index for index, field in enumerate(instance_fields) if field.get("id") == "source_scores"),
            None,
        )
        if source_index is not None:
            try:
                from apps.m3u.models import M3UAccount

                accounts = list(M3UAccount.objects.all().values("id", "name", "is_active"))
                replacement = [
                    {
                        "id": "m3u_source_scores_info",
                        "label": "M3U source scores",
                        "type": "info",
                        "description": (
                            "All configured M3U sources are listed below. Every source starts at 0 (neutral). "
                            "Use positive values to promote a source and negative values to demote it."
                        ),
                    }
                ]
                source_fields = _build_m3u_source_score_fields(accounts)
                if source_fields:
                    replacement.extend(source_fields)
                else:
                    replacement.append(
                        {
                            "id": "m3u_source_scores_empty",
                            "label": "No M3U sources found",
                            "type": "info",
                            "description": "No Dispatcharr M3U accounts are currently configured.",
                        }
                    )
                self.fields = (
                    instance_fields[:source_index]
                    + replacement
                    + instance_fields[source_index + 1:]
                )
            except Exception as exc:
                LOGGER.warning("Unable to build dynamic M3U source score fields: %s", exc)
                self.fields = instance_fields
        else:
            self.fields = instance_fields

    def run(self, action: str, params: dict, context: dict):
        settings = _settings_with_dynamic_source_scores(context.get("settings") or {})
        # Deliberately do not use context["logger"]. Dispatcharr passes a shared
        # apps.plugins.loader logger, and IPTV Checker currently installs a
        # persistent [IPTV Checker] filter on that shared object. A dedicated
        # logger keeps Stream Sort identity correct even when both plugins run.
        logger = LOGGER

        try:
            if action == "record_runtime_event":
                return record_runtime_event(
                    (params or {}).get("event"),
                    (params or {}).get("payload") or {},
                    logger=logger,
                    path=RELIABILITY_PATH,
                )

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

            if action == "analyze_streams":
                return _start_background_job(settings, kind="analyze", sort_after=False)

            if action == "analyze_and_sort":
                return _start_background_job(settings, kind="analyze", sort_after=True)

            if action == "probe_throughput":
                return _start_background_job(settings, kind="throughput", sort_after=False)

            if action == "probe_and_sort":
                return _start_background_job(settings, kind="throughput", sort_after=True)

            return {"status": "error", "message": f"Unknown action: {action}"}
        except ValueError as exc:
            logger.warning("Configuration error: %s", exc)
            return {"status": "error", "message": f"Configuration error: {exc}"}
        except Exception as exc:
            logger.exception("Action %s failed", action)
            return {"status": "error", "message": f"{type(exc).__name__}: {exc}"}
