from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Mapping

try:
    import fcntl
except ImportError:  # pragma: no cover - Dispatcharr runs on Linux
    fcntl = None

from .analyzer import ANALYSIS_CACHE_PATH, _format_eta, probe_assigned_streams
from .execution_control import (
    AnalysisAlreadyRunning,
    AnalysisCancelled,
    analysis_maintenance_execution,
    request_analysis_cancel,
)
from .incremental import ANALYSIS_HEALTH_REPORT_PATH, _parse_datetime, analyze_assigned_streams
from .reliability import RELIABILITY_PATH, record_runtime_event, reset_reliability_cache
from .sorter import REPORT_PATH, resolve_channel_scope, sort_channels
from .throughput import LEGACY_CACHE_PATH


PROBE_LOCK_PATH = "/data/dispatcharr_stream_sort_probe.lock"
STATUS_PATH = "/data/dispatcharr_stream_sort_status.json"
TTL_RECOMMENDATION_PATH = "/data/dispatcharr_stream_sort_ttl_recommendations.json"
SCHEDULE_STATE_PATH = "/data/dispatcharr_stream_sort_schedule_state.json"
SCHEDULE_STATE_LOCK_PATH = "/data/dispatcharr_stream_sort_schedule_state.lock"
SCHEDULE_CLAIM_PREFIX = "dispatcharr-stream-sort-schedule"
_FALLBACK_JOB_LOCK = threading.Lock()
_FALLBACK_SCHEDULE_STATE_LOCK = threading.Lock()
M3U_SOURCE_SCORE_PREFIX = "m3u_source_score_"
LOG_PREFIX = "[Stream Sort]"
SCHEDULER_POLL_SECONDS = 10
SCHEDULER_THREAD_NAME = "dispatcharr-stream-sort-scheduler"
SCHEDULE_HISTORY_RETENTION_DAYS = 365
SCHEDULE_HISTORY_MAX_ROWS = 20000

_SCHEDULER_THREAD = None


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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return bool(value)


def _parse_cron_field(expression: str, *, minimum: int, maximum: int) -> set[int]:
    source = str(expression).strip()
    if source == "" or source == "*":
        return set(range(minimum, maximum + 1))
    if source == "?":
        return set(range(minimum, maximum + 1))

    results = set()
    for part in source.split(","):
        token = part.strip()
        if not token:
            continue
        if token == "*":
            results.update(range(minimum, maximum + 1))
            continue

        if token.startswith("*/"):
            try:
                step = int(token[2:])
            except ValueError as exc:
                raise ValueError(f"Invalid step value: {token}") from exc
            if step <= 0:
                raise ValueError(f"Cron step must be positive: {token}")
            results.update(range(minimum, maximum + 1, step))
            continue

        if "/" in token:
            base, step_text = token.split("/", 1)
            try:
                step = int(step_text)
            except ValueError as exc:
                raise ValueError(f"Invalid step value: {token}") from exc
            if step <= 0:
                raise ValueError(f"Cron step must be positive: {token}")
            range_start = minimum
            range_end = maximum
            if base != "*":
                if "-" not in base:
                    raise ValueError(f"Invalid stepped field: {token}")
                start_text, end_text = base.split("-", 1)
                try:
                    range_start = int(start_text.strip())
                    range_end = int(end_text.strip())
                except ValueError as exc:
                    raise ValueError(f"Invalid stepped range: {token}") from exc
            results.update(range(range_start, range_end + 1, step))
            continue

        if "-" in token:
            start_text, end_text = token.split("-", 1)
            try:
                start = int(start_text.strip())
                end = int(end_text.strip())
            except ValueError as exc:
                raise ValueError(f"Invalid range field: {token}") from exc
            if start > end:
                raise ValueError(f"Invalid range order: {token}")
            results.update(range(start, end + 1))
            continue

        try:
            value = int(token)
        except ValueError as exc:
            raise ValueError(f"Invalid cron field: {token}") from exc
        results.add(value)

    normalized = set()
    for value in results:
        if value < minimum or value > maximum:
            raise ValueError(f"Cron field value out of range: {value}")
        normalized.add(value)
    if not normalized:
        raise ValueError("Cron field is empty")
    return normalized


def _parse_cron_expression(expression: str) -> tuple[set[int], set[int], set[int], set[int], set[int]]:
    parts = str(expression or "").strip().split()
    if len(parts) != 5:
        raise ValueError("Cron expression must have 5 fields")
    minute_part, hour_part, dom_part, month_part, dow_part = parts
    minutes = _parse_cron_field(minute_part, minimum=0, maximum=59)
    hours = _parse_cron_field(hour_part, minimum=0, maximum=23)
    days_of_month = _parse_cron_field(dom_part, minimum=1, maximum=31)
    months = _parse_cron_field(month_part, minimum=1, maximum=12)
    days_of_week = _parse_cron_field(dow_part, minimum=0, maximum=7)
    normalized_dow = set()
    for value in days_of_week:
        normalized_dow.add(0 if value == 7 else value)
    return minutes, hours, days_of_month, months, normalized_dow


def _cron_matches(expression: str, when: datetime) -> bool:
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    when = when.astimezone(timezone.utc)
    parts = str(expression or "").strip().split()
    minutes, hours, days_of_month, months, days_of_week = _parse_cron_expression(expression)
    day_of_week = when.isoweekday() % 7
    dom_match = when.day in days_of_month
    dow_match = day_of_week in days_of_week
    dom_wildcard = parts[2] in {"*", "?"}
    dow_wildcard = parts[4] in {"*", "?"}
    day_matches = (
        dom_match and dow_match
        if dom_wildcard or dow_wildcard
        else dom_match or dow_match
    )
    return (
        when.minute in minutes
        and when.hour in hours
        and when.month in months
        and day_matches
    )


def _is_scheduler_process() -> bool:
    try:
        from dispatcharr.db.process_label import get_process_role

        return str(get_process_role()) == "uwsgi"
    except Exception:
        return True


def _load_schedule_state() -> dict:
    raw = _load_json(
        SCHEDULE_STATE_PATH,
        {
            "version": 2,
            "enabled": False,
            "cron": "",
            "apply_sort_after_analysis": True,
            "allow_parallel_checks": False,
            "generation": 0,
            "last_scheduled_minute": None,
            "last_run_at": None,
            "last_run_status": "idle",
            "last_run_message": "",
            "last_job_id": None,
            "history": [],
        },
    )
    return {
        "version": 2,
        "enabled": _safe_bool(raw.get("enabled"), False),
        "cron": str(raw.get("cron") or "").strip(),
        "apply_sort_after_analysis": _safe_bool(raw.get("apply_sort_after_analysis"), True),
        "allow_parallel_checks": _safe_bool(raw.get("allow_parallel_checks"), False),
        "generation": int(raw.get("generation") or 0),
        "last_scheduled_minute": str(raw.get("last_scheduled_minute") or "").strip() or None,
        "last_run_at": str(raw.get("last_run_at") or "").strip() or None,
        "last_run_status": str(raw.get("last_run_status") or "idle"),
        "last_run_message": str(raw.get("last_run_message") or ""),
        "last_job_id": raw.get("last_job_id"),
        "history": [dict(row) for row in (raw.get("history") or []) if isinstance(row, Mapping)],
    }


def _save_schedule_state(value: dict) -> None:
    payload = {
        "version": 2,
        "enabled": _safe_bool(value.get("enabled"), False),
        "cron": str(value.get("cron") or "").strip(),
        "apply_sort_after_analysis": _safe_bool(value.get("apply_sort_after_analysis"), True),
        "allow_parallel_checks": _safe_bool(value.get("allow_parallel_checks"), False),
        "generation": int(value.get("generation") or 0),
        "last_scheduled_minute": value.get("last_scheduled_minute"),
        "last_run_at": value.get("last_run_at"),
        "last_run_status": str(value.get("last_run_status") or "idle"),
        "last_run_message": str(value.get("last_run_message") or ""),
        "last_job_id": value.get("last_job_id"),
        "history": [dict(row) for row in (value.get("history") or []) if isinstance(row, Mapping)],
        "updated_at": _utc_now_iso(),
    }
    _save_json(SCHEDULE_STATE_PATH, payload)


def _append_schedule_history(state: dict, *, status: str, message: str, job_id=None) -> None:
    observed_at = datetime.now(timezone.utc)
    cutoff = observed_at - timedelta(days=SCHEDULE_HISTORY_RETENTION_DAYS)
    history = [
        dict(row)
        for row in (state.get("history") or [])
        if isinstance(row, Mapping)
        and (_parse_datetime(row.get("observed_at")) or observed_at) >= cutoff
    ]
    history.append(
        {
            "observed_at": observed_at.isoformat(),
            "status": str(status),
            "message": str(message),
            "job_id": job_id,
        }
    )
    state["history"] = history[-SCHEDULE_HISTORY_MAX_ROWS:]


def _mutate_schedule_state(mutator):
    os.makedirs(os.path.dirname(SCHEDULE_STATE_LOCK_PATH) or ".", exist_ok=True)
    if fcntl is None:
        with _FALLBACK_SCHEDULE_STATE_LOCK:
            state = _load_schedule_state()
            mutator(state)
            _save_schedule_state(state)
            return state
    with open(SCHEDULE_STATE_LOCK_PATH, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            state = _load_schedule_state()
            mutator(state)
            _save_schedule_state(state)
            return state
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _claim_schedule_minute(generation: int, current_minute: str) -> bool:
    try:
        from django.core.cache import cache

        return bool(
            cache.add(
                f"{SCHEDULE_CLAIM_PREFIX}:{generation}:{current_minute}",
                uuid.uuid4().hex,
                timeout=180,
            )
        )
    except Exception:
        claim_path = f"{SCHEDULE_STATE_LOCK_PATH}.{generation}.{current_minute}"
        try:
            descriptor = os.open(claim_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            return False
        os.close(descriptor)
        return True


def _load_live_plugin_settings() -> tuple[bool, dict]:
    from apps.plugins.models import PluginConfig

    config = PluginConfig.objects.filter(key="stream_sorter").values("enabled", "settings").first()
    if not config:
        return False, {}
    settings = config.get("settings") if isinstance(config.get("settings"), dict) else {}
    return bool(config.get("enabled")), _settings_with_dynamic_source_scores(settings)


def _configured_parallel_tests(settings: dict) -> int:
    try:
        return max(1, min(16, int(float(settings.get("analysis_workers", 2)))))
    except (TypeError, ValueError):
        return 2


def _load_status() -> dict:
    try:
        with open(STATUS_PATH, "r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return value if isinstance(value, dict) else {}


def _save_status(value: dict) -> None:
    directory = os.path.dirname(STATUS_PATH) or "."
    os.makedirs(directory, exist_ok=True)
    fd, temporary_path = tempfile.mkstemp(
        prefix=".dispatcharr-stream-sort-status-",
        suffix=".json",
        dir=directory,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temporary_path, STATUS_PATH)
    except Exception:
        try:
            os.unlink(temporary_path)
        except OSError:
            pass
        raise


def _update_status(job_id: str, **changes) -> None:
    value = _load_status()
    if value.get("job_id") != job_id:
        return
    value.update(changes)
    value["updated_at"] = _utc_now_iso()
    _save_status(value)


class _ProgressLogger:
    """Mirror analysis logs while persisting progress for the status action."""

    def __init__(self, job_id: str):
        self.job_id = job_id

    def __getattr__(self, name):
        return getattr(LOGGER, name)

    def info(self, msg, *args, **kwargs):
        LOGGER.info(msg, *args, **kwargs)
        try:
            text = str(msg) % args if args else str(msg)
            if text.startswith("[Analyze Media]"):
                phase = "media_analysis"
            elif text.startswith("[Analyze Combined Content]"):
                phase = "content_analysis"
            elif text.startswith("[Analyze Combined]"):
                phase = "combined_analysis"
            elif text.startswith("[Analyze Content Retry"):
                phase = "content_retry"
            elif text.startswith("[Analyze Content]"):
                phase = "content_analysis"
            elif text.startswith("[Analyze Throughput]"):
                phase = "throughput_analysis"
            elif text.startswith("[Analyze Retry"):
                phase = "media_retry"
            else:
                return
            match = re.search(r"\]\s+(\d+)%\s+\((\d+)/(\d+)\)", text)
            if match:
                _update_status(
                    self.job_id,
                    phase=phase,
                    progress_percent=int(match.group(1)),
                    progress_completed=int(match.group(2)),
                    progress_total=int(match.group(3)),
                )
        except Exception:
            # Status persistence must never interrupt the analyzer.
            pass


def _build_m3u_source_score_fields(accounts):
    """Return one bounded score selector per operator-managed M3U account."""
    rows = [dict(account) for account in accounts]
    rows = sorted(
        (row for row in rows if not bool(row.get("locked", False))),
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
                "type": "select",
                "default": 0,
                "options": [
                    {
                        "value": score,
                        "label": f"{score:+d}" if score else "0 (neutral)",
                    }
                for score in range(5, -6, -1)
                ],
                "help_text": f"M3U source ID {account_id}.",
            }
        )
    return fields


def _settings_with_dynamic_source_scores(settings, *, allowed_account_ids=None):
    """Translate per-account score fields into the existing source-rule format."""
    normalized = dict(settings or {})
    allowed_ids = (
        None
        if allowed_account_ids is None
        else {int(account_id) for account_id in allowed_account_ids}
    )
    dynamic_scores = []
    for key, value in normalized.items():
        if not str(key).startswith(M3U_SOURCE_SCORE_PREFIX):
            continue
        account_id = str(key)[len(M3U_SOURCE_SCORE_PREFIX):]
        if not account_id.isdigit():
            continue
        account_id_int = int(account_id)
        if allowed_ids is not None and account_id_int not in allowed_ids:
            continue
        try:
            raw_score = float(value)
            if raw_score != raw_score or raw_score in {float("inf"), float("-inf")}:
                raise ValueError
            score = max(-5, min(5, int(round(raw_score))))
            normalized[key] = score
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                f"Invalid score for M3U source ID {account_id}: {value!r}"
            ) from exc
        dynamic_scores.append((account_id_int, score))

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


def _job_is_running() -> bool:
    lock_handle = _acquire_job_lock()
    if lock_handle is None:
        return True
    _release_job_lock(lock_handle)
    return False


def _analysis_status() -> dict:
    value = _load_status()
    running = _job_is_running()
    if not value:
        return {
            "status": "ok",
            "message": "No Stream Sort analysis has been recorded yet.",
            "job_status": "running_unknown" if running else "idle",
            "running": running,
        }

    job_status = str(value.get("status") or "unknown")
    if job_status == "running" and not running:
        job_status = "interrupted"
        value.update(
            status=job_status,
            phase="interrupted",
            finished_at=value.get("finished_at") or _utc_now_iso(),
            updated_at=_utc_now_iso(),
        )
        _save_status(value)

    phase = str(value.get("phase") or "starting").replace("_", " ")
    progress = ""
    if value.get("progress_total"):
        progress = (
            f" {value.get('progress_percent', 0)}% "
            f"({value.get('progress_completed', 0)}/{value['progress_total']})."
        )
    if job_status == "running":
        worker_text = (
            f" Up to {value['parallel_tests']} tests run concurrently."
            if value.get("parallel_tests")
            else ""
        )
        message = f"Stream Sort analysis is running: {phase}.{progress}{worker_text}"
    elif job_status == "completed":
        result = value.get("result") or {}
        health_report_path = result.get("analysis_health_report_path")
        throughput_checked = result.get("throughput_checked", 0)
        throughput_attempted = result.get("throughput_attempted", throughput_checked)
        message = (
            f"Last Stream Sort analysis completed: {result.get('streams_analyzed', 0)} streams; "
            f"{result.get('media_checked', 0)} media checks; "
            f"{throughput_checked} completed throughput measurements from "
            f"{throughput_attempted} attempted streams; "
            f"{result.get('capacity_deferred', 0)} capacity-deferred checks; "
            f"{result.get('playback_health_refreshed', 0)} playback reachability reuses."
        )
        if health_report_path:
            message += f" Report: {health_report_path}."
    elif job_status == "failed":
        message = f"Last Stream Sort analysis failed: {value.get('error') or 'unknown error'}."
    else:
        message = f"Last Stream Sort analysis was {job_status}."

    details = dict(value)
    details.update(
        status="ok",
        message=message,
        job_status=job_status,
        running=running if job_status == "running" else False,
    )
    return details


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


def _safe_float(value, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_json(path: str, default: dict) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default
    return value if isinstance(value, dict) else default


def _save_json(path: str, value: dict) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, temporary_path = tempfile.mkstemp(
        prefix=".dispatcharr-stream-sort-recommendation-",
        suffix=".json",
        dir=directory,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temporary_path, path)
    except Exception:
        try:
            os.unlink(temporary_path)
        except OSError:
            pass
        raise


def _clamp_float(value: float | None, minimum: float, maximum: float) -> float | None:
    if value is None:
        return None
    return max(minimum, min(maximum, value))


def _format_hours(value: float | None) -> str:
    return "n/a" if value is None else str(value)


def _recommend_ttls(settings: dict, *, report: dict) -> dict:
    observations = report.get("observations") if isinstance(report, Mapping) else None
    if not isinstance(observations, Mapping):
        observations = {}
    alive_episodes = observations.get("alive_episode_duration_hours") if isinstance(observations.get("alive_episode_duration_hours"), Mapping) else {}
    dead_recoveries = observations.get("dead_recovery_duration_hours") if isinstance(observations.get("dead_recovery_duration_hours"), Mapping) else {}
    concentration = observations.get("check_concentration") if isinstance(observations.get("check_concentration"), Mapping) else {}
    status_patterns = report.get("status_patterns") if isinstance(report.get("status_patterns"), Mapping) else {}
    hourly = status_patterns.get("hourly_dead_ratio") if isinstance(status_patterns.get("hourly_dead_ratio"), list) else []
    reasons = report.get("reasons") if isinstance(report.get("reasons"), Mapping) else {}

    health_current = _safe_float(settings.get("stream_data_ttl_hours"), 12.0)
    dead_current = _safe_float(settings.get("dead_content_ttl_hours"), 1.0)
    healthy_throughput_current = _safe_float(settings.get("healthy_throughput_ttl_hours"), 24.0)
    degraded_throughput_current = _safe_float(settings.get("degraded_throughput_ttl_hours"), 12.0)
    unknown_throughput_current = _safe_float(settings.get("unknown_throughput_ttl_hours"), 4.0)
    jitter_current = _safe_float(settings.get("analysis_ttl_jitter_percent"), 30.0)

    selected_streams = int(report.get("selected_streams") or 0)
    history_rows = int(observations.get("history_rows") or 0)
    dead_ratio = float(observations.get("dead_check_ratio") or 0.0)
    status_change_ratio = float(observations.get("status_changes_per_check_ratio") or 0.0)
    history_span_hours = float(observations.get("history_span_hours") or 0.0)
    recovery_samples = int(dead_recoveries.get("samples") or 0)
    alive_episode_samples = int(alive_episodes.get("samples") or 0)
    minute_concentration = float(concentration.get("busiest_minute_ratio") or 0.0)

    max_dead_ratio_by_hour = max((
        float(row.get("dead_ratio")) for row in hourly
        if isinstance(row, Mapping)
        and isinstance(row.get("dead_ratio"), (int, float))
    ), default=0.0)

    health_base = alive_episodes.get("p25")
    if health_base is None:
        health_base = alive_episodes.get("p50")
    suggested_health = _clamp_float(_safe_float(health_base, None), 0.5, 240.0)

    dead_base = dead_recoveries.get("p50")
    if dead_base is None:
        dead_base = dead_recoveries.get("p90")
    suggested_dead = _clamp_float(_safe_float(dead_base, None), 0.25, 24.0)

    if minute_concentration >= 0.20:
        suggested_jitter = 25.0
    elif minute_concentration >= 0.10:
        suggested_jitter = 20.0
    elif selected_streams >= 75 or status_change_ratio >= 0.20:
        suggested_jitter = 15.0
    elif max_dead_ratio_by_hour >= 0.40:
        suggested_jitter = 20.0
    else:
        suggested_jitter = 0.0

    if max_dead_ratio_by_hour >= 0.25:
        suggested_jitter = max(suggested_jitter, 15.0)
    notes = []
    if history_rows < 20:
        notes.append("History is sparse; treat recommendations as provisional until at least 20 health rows are collected.")
    if selected_streams == 0:
        notes.append("No streams were included in the current report; recommendations are placeholders.")
    if dead_ratio >= 0.25:
        notes.append("Higher dead-check ratios are handled by consecutive-dead adaptive backoff; placeholder observations are excluded from general health tuning.")
    if status_change_ratio >= 0.20:
        notes.append("Frequent status transitions suggest moderate jitter to avoid synchronized rechecks.")
    if recovery_samples < 5:
        notes.append("Fewer than five dead-to-alive recoveries were observed; the dead TTL recommendation is provisional.")
    if alive_episode_samples < 5:
        notes.append("Fewer than five completed alive episodes were observed; the reachability TTL recommendation is provisional.")
    if history_span_hours < 72:
        notes.append("Collect at least 72 hours of history before treating TTL recommendations as stable.")
    notes.append("Throughput TTL recommendations are omitted until sufficient status-duration evidence has been collected; 24h/12h/4h remain provisional trial defaults.")

    confidence = "low"
    if history_span_hours >= 336 and recovery_samples >= 20 and alive_episode_samples >= 20:
        confidence = "high"
    elif history_span_hours >= 72 and recovery_samples >= 5 and alive_episode_samples >= 5:
        confidence = "medium"

    recommendations = {
        "stream_data_ttl_hours": suggested_health,
        "dead_content_ttl_hours": suggested_dead,
        "analysis_ttl_jitter_percent": suggested_jitter,
    }

    return {
        "generated_at": _utc_now_iso(),
        "health_report_path": ANALYSIS_HEALTH_REPORT_PATH,
        "selected_streams": selected_streams,
        "history_rows": history_rows,
        "confidence": confidence,
        "current_ttls": {
            "stream_data_ttl_hours": health_current,
            "dead_content_ttl_hours": dead_current,
            "healthy_throughput_ttl_hours": healthy_throughput_current,
            "degraded_throughput_ttl_hours": degraded_throughput_current,
            "unknown_throughput_ttl_hours": unknown_throughput_current,
            "analysis_ttl_jitter_percent": jitter_current,
        },
        "recommended_ttls": recommendations,
        "throughput_trial_defaults": {
            "healthy_throughput_ttl_hours": 24.0,
            "degraded_throughput_ttl_hours": 12.0,
            "unknown_throughput_ttl_hours": 4.0,
            "recommendation_status": "insufficient_evidence",
        },
        "observation_summary": {
            "history_span_hours": history_span_hours,
            "status_changes": observations.get("status_changes"),
            "dead_checks": observations.get("dead_checks"),
            "status_changes_per_check_ratio": status_change_ratio,
            "dead_check_ratio": dead_ratio,
            "dead_recovery_duration_hours": dead_recoveries,
            "alive_episode_duration_hours": alive_episodes,
            "check_concentration": concentration,
            "media_reasons": reasons.get("media_due") or {},
            "max_dead_ratio_by_hour": round(max_dead_ratio_by_hour, 4),
        },
        "recommendation_notes": notes,
        "recommendation_file": TTL_RECOMMENDATION_PATH,
    }


def _run_ttl_recommendation_action(settings: dict) -> dict:
    report = _load_json(ANALYSIS_HEALTH_REPORT_PATH, {})
    if not report:
        return {
            "status": "error",
            "message": (
                "No analysis health report found. Run Analyze Streams first so"
                f" it writes {ANALYSIS_HEALTH_REPORT_PATH}."
            ),
        }

    generated_at = _parse_datetime(report.get("generated_at"))
    if generated_at is None:
        return {
            "status": "error",
            "message": "The health report has no valid generation timestamp. Run Analyze Streams again.",
        }
    age_hours = (datetime.now(timezone.utc) - generated_at).total_seconds() / 3600.0
    if age_hours > 168:
        return {
            "status": "error",
            "message": "The health report is older than seven days. Run Analyze Streams again before recommending TTLs.",
        }
    if int(report.get("selected_streams") or 0) <= 0:
        return {
            "status": "error",
            "message": "The latest health report contains no selected streams. Run Analyze Streams for the intended scope.",
        }

    result = _recommend_ttls(settings, report=report)
    _save_json(TTL_RECOMMENDATION_PATH, result)

    current = result["current_ttls"]
    recommended = result["recommended_ttls"]
    message = (
        f"TTL recommendation complete. FFprobe TTL: {_format_hours(current['stream_data_ttl_hours'])}h"
        f" -> {_format_hours(recommended['stream_data_ttl_hours'])}h; Dead TTL: "
        f"{_format_hours(current['dead_content_ttl_hours'])}h -> {_format_hours(recommended['dead_content_ttl_hours'])}h;"
        f" suggested TTL jitter: {recommended['analysis_ttl_jitter_percent']}%."
    )

    return {
        "status": "ok",
        "message": message,
        "recommendation_path": TTL_RECOMMENDATION_PATH,
        "result": result,
    }


def _run_health_report_action() -> dict:
    report = _load_json(ANALYSIS_HEALTH_REPORT_PATH, {})
    if not report:
        return {
            "status": "error",
            "message": "No health report found. Run Analyze Streams first.",
        }
    generated_at = _parse_datetime(report.get("generated_at"))
    age_text = "unknown"
    if generated_at is not None:
        age_text = f"{max(0.0, (datetime.now(timezone.utc) - generated_at).total_seconds() / 3600.0):.1f}h"
    observations = report.get("observations") if isinstance(report.get("observations"), Mapping) else {}
    patterns = report.get("status_patterns") if isinstance(report.get("status_patterns"), Mapping) else {}
    problematic = patterns.get("problematic_streams") if isinstance(patterns.get("problematic_streams"), list) else []
    transitions = observations.get("transition_counts") if isinstance(observations.get("transition_counts"), Mapping) else {}
    message = (
        f"Health report age={age_text}; streams={int(report.get('selected_streams') or 0)}; "
        f"problematic (>75% dead, minimum 20 checks over 7 days)={len(problematic)}; "
        f"alive-to-dead={int(transitions.get('alive_to_dead') or 0)}; "
        f"dead-to-alive={int(transitions.get('dead_to_alive') or 0)}."
    )
    return {
        "status": "ok",
        "message": message,
        "report_path": ANALYSIS_HEALTH_REPORT_PATH,
        "result": report,
    }


def _scheduled_settings(settings: dict, *, allow_parallel_checks: bool) -> dict:
    scheduled = dict(settings or {})
    scheduled.pop("stream_sort_schedule_cron", None)
    scheduled.pop("stream_sort_apply_sort_after_scheduled_scan", None)
    scheduled.pop("stream_sort_allow_parallel_checks_on_scheduled_scan", None)
    if not allow_parallel_checks:
        scheduled["analysis_workers"] = 1
    return scheduled


def _set_schedule_job_running(generation: int, job_id: str) -> bool:
    accepted = {"value": False}

    def mutate(state):
        if not state.get("enabled") or int(state.get("generation") or 0) != generation:
            return
        state["last_scheduled_minute"] = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
        state["last_run_at"] = _utc_now_iso()
        state["last_run_status"] = "running"
        state["last_run_message"] = "Scheduled analysis is running."
        state["last_job_id"] = job_id
        accepted["value"] = True

    _mutate_schedule_state(mutate)
    return accepted["value"]


def _finish_schedule_job(generation: int | None, job_id: str, *, status: str, message: str) -> None:
    if generation is None:
        return

    def mutate(state):
        if int(state.get("generation") or 0) != generation:
            return
        if str(state.get("last_job_id") or "") != job_id:
            return
        state["last_run_status"] = status
        state["last_run_message"] = message
        _append_schedule_history(state, status=status, message=message, job_id=job_id)

    _mutate_schedule_state(mutate)


def _apply_schedule_action(settings: dict) -> dict:
    cron_expr = str(settings.get("stream_sort_schedule_cron") or "").strip()
    if cron_expr:
        _parse_cron_expression(cron_expr)
        message = (
            f"Saved stream schedule: cron='{cron_expr}'. "
            "Scheduled runs always load the current UI settings."
        )
    else:
        message = "Stream Sort schedule cleared; automatic scheduled analysis is disabled."

    def mutate(state):
        state["generation"] = int(state.get("generation") or 0) + 1
        state["cron"] = cron_expr
        state["apply_sort_after_analysis"] = _safe_bool(
            settings.get("stream_sort_apply_sort_after_scheduled_scan"), True
        )
        state["allow_parallel_checks"] = _safe_bool(
            settings.get("stream_sort_allow_parallel_checks_on_scheduled_scan"), False
        )
        state["enabled"] = bool(cron_expr)
        state["last_scheduled_minute"] = None
        state["last_run_status"] = "enabled" if cron_expr else "disabled"
        state["last_run_message"] = (
            f"Stream Sort schedule enabled with cron '{cron_expr}'."
            if cron_expr
            else "Stream Sort schedule disabled (empty cron)."
        )

    state = _mutate_schedule_state(mutate)
    return {
        "status": "ok",
        "message": message,
        "schedule_state": state,
    }


def _disable_schedule_action() -> dict:
    def mutate(state):
        state["generation"] = int(state.get("generation") or 0) + 1
        state["enabled"] = False
        state["last_run_status"] = "disabled"
        state["last_run_message"] = "Stream Sort schedule disabled by user."
        state["last_scheduled_minute"] = None

    state = _mutate_schedule_state(mutate)
    return {
        "status": "ok",
        "message": "Stream Sort scheduled analysis disabled.",
        "schedule_state": state,
    }


def _schedule_status_action() -> dict:
    state = _load_schedule_state()
    message = (
        f"Stream Sort schedule enabled={state['enabled']}; cron='{state['cron']}'; "
        f"apply_sort_after_analysis={state['apply_sort_after_analysis']}; "
        f"allow_parallel_checks={state['allow_parallel_checks']}; "
        f"last_run_status={state['last_run_status']}; last_run='{state['last_run_at'] or 'never'}'; "
        f"startup: checks run once per minute on UTC boundary to avoid duplicate immediate restarts."
    )
    return {
        "status": "ok",
        "message": message,
        "schedule_state": state,
    }


def _run_scheduled_scan(state: dict, now: datetime, settings: dict) -> dict:
    scheduled_settings = _scheduled_settings(
        settings,
        allow_parallel_checks=_safe_bool(state.get("allow_parallel_checks"), False),
    )
    return _start_background_job(
        scheduled_settings,
        kind="analyze",
        sort_after=bool(state.get("apply_sort_after_analysis", True)),
        schedule_generation=int(state.get("generation") or 0),
    )


def _check_schedule_tick(now: datetime | None = None, *, settings: dict | None = None) -> dict:
    if now is None:
        now = datetime.now(timezone.utc)
    now = now.replace(second=0, microsecond=0, tzinfo=timezone.utc) if now.tzinfo is None else now.replace(
        second=0,
        microsecond=0,
    )
    state = _load_schedule_state()
    if not state.get("enabled"):
        return {
            "status": "skipped",
            "message": "scheduler disabled",
            "state": state,
        }
    cron_expr = str(state.get("cron") or "").strip()
    if not cron_expr:
        state["enabled"] = False
        state["last_run_status"] = "disabled"
        state["last_run_message"] = "No cron configured; disabled."
        _save_schedule_state(state)
        return {
            "status": "disabled",
            "message": "No cron configured; scheduler disabled.",
            "state": state,
        }

    try:
        due = _cron_matches(cron_expr, now)
    except ValueError as exc:
        state["last_run_status"] = "error"
        state["last_run_message"] = f"Invalid cron: {exc}"
        _append_schedule_history(
            state,
            status="error",
            message=state["last_run_message"],
        )
        _save_schedule_state(state)
        return {
            "status": "error",
            "message": str(exc),
            "state": state,
        }

    current_minute = now.strftime("%Y%m%d%H%M")
    if not due:
        return {
            "status": "waiting",
            "message": "not due yet",
            "state": state,
        }
    generation = int(state.get("generation") or 0)
    if not _claim_schedule_minute(generation, current_minute):
        return {
            "status": "skipped",
            "message": "another worker already claimed this minute",
            "state": state,
        }
    latest = _load_schedule_state()
    if (
        not latest.get("enabled")
        or int(latest.get("generation") or 0) != generation
        or str(latest.get("cron") or "") != cron_expr
    ):
        return {
            "status": "skipped",
            "message": "schedule changed after this minute was claimed",
            "state": latest,
        }
    result = _run_scheduled_scan(latest, now=now, settings=dict(settings or {}))
    if not result.get("job_id"):
        def record_skip(current):
            if int(current.get("generation") or 0) != generation:
                return
            current["last_run_at"] = _utc_now_iso()
            current["last_run_status"] = "skipped_busy" if "already running" in str(result.get("message") or "") else "error"
            current["last_run_message"] = result.get("message") or "scheduled run skipped"
            _append_schedule_history(
                current,
                status=current["last_run_status"],
                message=current["last_run_message"],
            )

        state = _mutate_schedule_state(record_skip)
    else:
        state = _load_schedule_state()
    return {
        "status": result.get("status") or "error",
        "message": result.get("message") or "scheduled run skipped",
        "state": state,
        "result": result,
    }


class _StreamSortScheduler:
    def __init__(self):
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self):
        if self.thread is not None:
            return
        if not _is_scheduler_process():
            return
        self.thread = threading.Thread(
            target=self._loop,
            name=SCHEDULER_THREAD_NAME,
            daemon=True,
        )
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        thread = self.thread
        if thread and thread.is_alive():
            thread.join(timeout=1)

    def _loop(self):
        from django.db import close_old_connections

        while not self.stop_event.is_set():
            close_old_connections()
            try:
                enabled, settings = _load_live_plugin_settings()
                if enabled:
                    _check_schedule_tick(settings=settings)
            except Exception as exc:
                LOGGER.exception("Stream Sort scheduler failed: %s", exc)
                try:
                    def record_error(state):
                        state["last_run_status"] = "error"
                        state["last_run_message"] = f"{type(exc).__name__}: {exc}"
                        _append_schedule_history(
                            state,
                            status="error",
                            message=state["last_run_message"],
                        )

                    _mutate_schedule_state(record_error)
                except Exception:
                    pass
            finally:
                close_old_connections()
            self.stop_event.wait(SCHEDULER_POLL_SECONDS)
        close_old_connections()


def _start_scheduler() -> None:
    global _SCHEDULER_THREAD
    if _SCHEDULER_THREAD is None:
        _SCHEDULER_THREAD = _StreamSortScheduler()
    _SCHEDULER_THREAD.start()


def _stop_scheduler() -> None:
    if _SCHEDULER_THREAD is None:
        return
    _SCHEDULER_THREAD.stop()


def _background_analyze_job(
    settings: dict,
    lock_handle,
    *,
    sort_after: bool,
    job_id: str,
    schedule_generation: int | None = None,
) -> None:
    from django.db import close_old_connections

    job_started = time.monotonic()
    close_old_connections()
    try:
        _update_status(job_id, phase="preparing")
        result = analyze_assigned_streams(settings, logger=_ProgressLogger(job_id))
        if sort_after:
            _update_status(
                job_id,
                phase="sorting",
                progress_percent=None,
                progress_completed=None,
                progress_total=None,
            )
            sort_result = sort_channels(settings, apply=True, logger=LOGGER)
            result = {
                **result,
                "channels_changed": sort_result["channels_changed"],
                "rows_changed": sort_result["rows_changed"],
            }
            runtime_seconds = max(0.0, time.monotonic() - job_started)
            result["total_runtime_seconds"] = round(runtime_seconds, 3)
            result["total_runtime"] = _format_eta(runtime_seconds)
            LOGGER.info(
                "[Analyze + Sort] complete analyzed=%s changed_channels=%s health %s runtime=%s",
                result["streams_analyzed"],
                sort_result["channels_changed"],
                result["health_summary"],
                result["total_runtime"],
            )
            _notify(
                f"✅ Stream Sort: analyzed {result['streams_analyzed']} streams; "
                f"sorted {sort_result['channels_changed']} changed channels."
            )
        else:
            runtime_seconds = max(0.0, time.monotonic() - job_started)
            result["total_runtime_seconds"] = round(runtime_seconds, 3)
            result["total_runtime"] = _format_eta(runtime_seconds)
            LOGGER.info(
                "[Analyze] background job complete analyzed=%s health %s runtime=%s",
                result["streams_analyzed"],
                result["health_summary"],
                result["total_runtime"],
            )
            _notify(
                f"✅ Stream Sort: analysis complete for {result['streams_analyzed']} streams "
                f"({result['status_counts']})."
            )
        _update_status(
            job_id,
            status="completed",
            phase="complete",
            finished_at=_utc_now_iso(),
            result=result,
        )
        _finish_schedule_job(
            schedule_generation,
            job_id,
            status="completed",
            message=(
                f"Scheduled analysis completed for {result['streams_analyzed']} streams "
                f"in {result['total_runtime']}."
            ),
        )
    except AnalysisCancelled as exc:
        LOGGER.info("[Analyze] background job canceled")
        partial_result = exc.result if isinstance(exc.result, Mapping) else {}
        _update_status(
            job_id,
            status="canceled",
            phase="canceled",
            finished_at=_utc_now_iso(),
            error=str(exc),
            result=partial_result,
        )
        _notify(
            "Stream Sort: analysis canceled after saving "
            f"{partial_result.get('media_checked', 0)} completed media checks and "
            f"{partial_result.get('throughput_checked', 0)} completed throughput measurements from "
            f"{partial_result.get('throughput_attempted', partial_result.get('throughput_checked', 0))} attempted streams."
        )
        _finish_schedule_job(
            schedule_generation,
            job_id,
            status="canceled",
            message=str(exc),
        )
    except AnalysisAlreadyRunning as exc:
        LOGGER.warning("[Analyze] background job skipped: %s", exc)
        _update_status(
            job_id,
            status="skipped_busy",
            phase="skipped",
            finished_at=_utc_now_iso(),
            error=str(exc),
        )
        _finish_schedule_job(
            schedule_generation,
            job_id,
            status="skipped_busy",
            message=str(exc),
        )
    except Exception as exc:
        LOGGER.exception("[Analyze] background job failed")
        _update_status(
            job_id,
            status="failed",
            phase="failed",
            finished_at=_utc_now_iso(),
            error=f"{type(exc).__name__}: {exc}",
        )
        _notify("❌ Stream Sort: stream analysis failed. Check Dispatcharr logs.")
        _finish_schedule_job(
            schedule_generation,
            job_id,
            status="failed",
            message=f"{type(exc).__name__}: {exc}",
        )
    finally:
        close_old_connections()
        _release_job_lock(lock_handle)


def _background_probe_job(
    settings: dict,
    lock_handle,
    *,
    sort_after: bool,
    job_id: str,
    schedule_generation: int | None = None,
) -> None:
    from django.db import close_old_connections

    close_old_connections()
    try:
        _update_status(job_id, phase="throughput_analysis")
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
        _update_status(
            job_id,
            status="completed",
            phase="complete",
            finished_at=_utc_now_iso(),
            result={
                "streams_analyzed": probe_result.get("streams_probed", 0),
                "throughput_checked": probe_result.get("streams_probed", 0),
                **probe_result,
            },
        )
    except Exception as exc:
        LOGGER.exception("[Throughput] background job failed")
        _update_status(
            job_id,
            status="failed",
            phase="failed",
            finished_at=_utc_now_iso(),
            error=f"{type(exc).__name__}: {exc}",
        )
        _notify("❌ Stream Sort: background throughput job failed. Check Dispatcharr logs.")
    finally:
        close_old_connections()
        _release_job_lock(lock_handle)


def _start_background_job(
    settings: dict,
    *,
    kind: str,
    sort_after: bool,
    schedule_generation: int | None = None,
) -> dict:
    _channel_ids, scope = resolve_channel_scope(settings)
    lock_handle = _acquire_job_lock()
    if lock_handle is None:
        return {
            "status": "error",
            "message": "A Stream Sort analysis/throughput job is already running.",
        }
    job_id = uuid.uuid4().hex[:12]

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
        kwargs={
            "sort_after": sort_after,
            "job_id": job_id,
            "schedule_generation": schedule_generation,
        },
        name=thread_name,
        daemon=True,
    )
    try:
        _save_status(
            {
                "job_id": job_id,
                "status": "running",
                "kind": kind,
                "sort_after": sort_after,
                "parallel_tests": _configured_parallel_tests(settings),
                "phase": "starting",
                "started_at": _utc_now_iso(),
                "updated_at": _utc_now_iso(),
                "filters": scope,
            }
        )
        if schedule_generation is not None and not _set_schedule_job_running(schedule_generation, job_id):
            _update_status(
                job_id,
                status="cancelled",
                phase="cancelled",
                finished_at=_utc_now_iso(),
                error="Schedule was disabled or replaced before launch.",
            )
            _release_job_lock(lock_handle)
            return {
                "status": "skipped",
                "message": "Schedule was disabled or replaced before launch.",
            }
        worker.start()
    except Exception as exc:
        try:
            _update_status(
                job_id,
                status="failed",
                phase="failed",
                finished_at=_utc_now_iso(),
                error=f"{type(exc).__name__}: {exc}",
            )
        except Exception:
            pass
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
        "job_id": job_id,
        "filters": scope,
    }


def _run_reset_statistics_action(*, include_history: bool) -> dict:
    try:
        lease = analysis_maintenance_execution()
        lease.__enter__()
    except AnalysisAlreadyRunning:
        return {
            "status": "error",
            "message": "Statistics cannot be reset while an analysis scan is running. Stop the scan and wait for it to finish first.",
        }
    try:
        scan_paths = (
            ANALYSIS_CACHE_PATH,
            LEGACY_CACHE_PATH,
            ANALYSIS_HEALTH_REPORT_PATH,
            TTL_RECOMMENDATION_PATH,
            STATUS_PATH,
        )
        removed = []
        for path in scan_paths:
            try:
                os.unlink(path)
                removed.append(path)
            except FileNotFoundError:
                continue

        if include_history:
            reset_reliability_cache(RELIABILITY_PATH)

        scope = "scan data and all runtime history" if include_history else "scan data only"
        return {
            "status": "ok",
            "message": (
                f"Stream Sort statistics reset complete ({scope}). "
                "The schedule and plugin settings were preserved; the next analysis starts with no cached scan evidence."
            ),
            "reset_scope": "all_history" if include_history else "scan_only",
            "removed_paths": removed,
            "reliability_reset": include_history,
        }
    finally:
        lease.__exit__(None, None, None)


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
        # plugins. Insert one bounded selector per current operator-managed M3U
        # account immediately before the stream-name scoring controls.
        instance_fields = [dict(field) for field in type(self).fields]
        self._m3u_source_score_account_ids = None
        source_index = next(
            (index for index, field in enumerate(instance_fields) if field.get("id") == "name_score_rules"),
            None,
        )
        if source_index is not None:
            try:
                from apps.m3u.models import M3UAccount

                accounts = list(
                    M3UAccount.objects.filter(locked=False).values(
                        "id", "name", "is_active", "locked"
                    )
                )
                self._m3u_source_score_account_ids = {
                    int(account["id"]) for account in accounts
                }
                replacement = [
                    {
                        "id": "m3u_source_scores_info",
                        "label": "M3U source scores",
                        "type": "info",
                        "description": (
                            "Choose -5 to demote, 0 for neutral, or +5 to promote each source."
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
                    + instance_fields[source_index:]
                )
            except Exception as exc:
                raise RuntimeError("Unable to discover Dispatcharr M3U sources") from exc
        else:
            self.fields = instance_fields
        _start_scheduler()

    def run(self, action: str, params: dict, context: dict):
        settings = _settings_with_dynamic_source_scores(
            context.get("settings") or {},
            allowed_account_ids=self._m3u_source_score_account_ids,
        )
        # Deliberately do not use context["logger"]. Dispatcharr passes a shared
        # apps.plugins.loader logger, and IPTV Checker currently installs a
        # persistent [IPTV Checker] filter on that shared object. A dedicated
        # logger keeps Stream Sort identity correct even when both plugins run.
        logger = LOGGER

        try:
            if action == "record_runtime_event":
                event_name = (params or {}).get("event")
                if not event_name:
                    return {
                        "status": "ok",
                        "message": (
                            "Runtime reliability collection is automatic. "
                            "No manual event was recorded."
                        ),
                        "recorded": False,
                        "counted": False,
                    }
                return record_runtime_event(
                    event_name,
                    (params or {}).get("payload") or {},
                    logger=logger,
                    path=RELIABILITY_PATH,
                )
            if action == "apply_schedule":
                return _apply_schedule_action(settings)

            if action == "remove_schedule":
                return _disable_schedule_action()

            if action == "schedule_status":
                return _schedule_status_action()

            if action == "check_analysis_status":
                return _analysis_status()

            if action == "stop_analysis":
                return request_analysis_cancel()

            if action == "recommend_ttls":
                return _run_ttl_recommendation_action(settings)

            if action == "health_report":
                return _run_health_report_action()

            if action == "reset_scan_statistics":
                return _run_reset_statistics_action(include_history=False)

            if action == "reset_all_statistics":
                return _run_reset_statistics_action(include_history=True)

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

    def stop(self, context=None):
        _stop_scheduler()
