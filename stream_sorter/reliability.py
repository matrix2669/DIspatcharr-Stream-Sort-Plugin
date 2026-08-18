from __future__ import annotations

import json
import os
import tempfile
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

try:
    import fcntl
except ImportError:  # pragma: no cover - Dispatcharr runs on Linux
    fcntl = None


RELIABILITY_PATH = "/data/dispatcharr_stream_sort_reliability.json"
MAX_RECENT_EVENTS = 25
SWITCH_FAILOVER_WINDOW_SECONDS = 15.0
SWITCH_RECONNECT_SUPPRESSION_SECONDS = 2.0
TRACKED_EVENTS = {
    "channel_start",
    "channel_stop",
    "channel_buffering",
    "channel_reconnect",
    "channel_error",
    "channel_failover",
    "stream_switch",
}
_FALLBACK_LOCK = threading.Lock()


def _now_utc(now: datetime | None = None) -> datetime:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


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


def _as_int(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _empty_cache() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "updated_at": None,
        "scoring_enabled": False,
        "channels": {},
        "streams": {},
    }


def load_reliability_cache(path: str = RELIABILITY_PATH) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return _empty_cache()
    if not isinstance(loaded, dict):
        return _empty_cache()
    data = _empty_cache()
    data.update(loaded)
    if not isinstance(data.get("channels"), dict):
        data["channels"] = {}
    if not isinstance(data.get("streams"), dict):
        data["streams"] = {}
    data["scoring_enabled"] = False
    return data


def _save_reliability_cache(data: Mapping[str, Any], path: str) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".stream-sort-reliability-", suffix=".json", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(dict(data), handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


@contextmanager
def _locked_cache(path: str):
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    if fcntl is None:
        with _FALLBACK_LOCK:
            yield
        return
    lock_path = f"{path}.lock"
    with open(lock_path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _default_stream_resolver(stream_id: int | None, payload: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        from apps.channels.models import Stream
    except Exception:
        return None

    queryset = Stream.objects.select_related("m3u_account")
    stream = queryset.filter(id=stream_id).first() if stream_id is not None else None
    if stream is None:
        for key in ("stream_url", "channel_url", "new_url"):
            url = str(payload.get(key) or "").strip()
            if not url:
                continue
            stream = queryset.filter(url=url).order_by("id").first()
            if stream is not None:
                break
    if stream is None:
        name = str(payload.get("stream_name") or "").strip()
        if name:
            stream = queryset.filter(name=name).order_by("id").first()
    if stream is None:
        return None

    account = getattr(stream, "m3u_account", None)
    return {
        "stream_id": int(stream.id),
        "stream_name": str(getattr(stream, "name", "") or ""),
        "m3u_account_id": getattr(stream, "m3u_account_id", None),
        "m3u_account_name": str(getattr(account, "name", "") or "") if account else "",
    }


def _resolve_stream(
    stream_id: int | None,
    payload: Mapping[str, Any],
    resolver: Callable[[int | None, Mapping[str, Any]], dict[str, Any] | None],
) -> dict[str, Any] | None:
    try:
        resolved = resolver(stream_id, payload)
    except Exception:
        resolved = None
    if isinstance(resolved, Mapping) and _as_int(resolved.get("stream_id")) is not None:
        return dict(resolved)
    if stream_id is None:
        return None
    return {
        "stream_id": stream_id,
        "stream_name": str(payload.get("stream_name") or ""),
        "m3u_account_id": None,
        "m3u_account_name": str(payload.get("provider_name") or ""),
    }


def _channel_key(data: dict[str, Any], payload: Mapping[str, Any]) -> str | None:
    channels = data["channels"]
    channel_id = str(payload.get("channel_id") or "").strip()
    channel_name = str(payload.get("channel_name") or "").strip()
    name_key = f"name:{channel_name}" if channel_name else None
    if channel_id:
        id_key = f"id:{channel_id}"
        if id_key not in channels and name_key and name_key in channels:
            channels[id_key] = channels.pop(name_key)
        return id_key
    return name_key


def _new_stream_entry(stream_id: int) -> dict[str, Any]:
    return {
        "stream_id": stream_id,
        "stream_name": "",
        "m3u_account_id": None,
        "m3u_account_name": "",
        "playback_starts": 0,
        "playback_stops": 0,
        "playback_seconds": 0.0,
        "switches_to": 0,
        "switches_away": 0,
        "buffering_events": 0,
        "buffering_failovers": 0,
        "buffering_failover_seconds": 0.0,
        "failovers": 0,
        "reconnects": 0,
        "reconnects_suppressed": 0,
        "errors": 0,
        "last_event_at": None,
        "last_failure_at": None,
        "last_failure_reason": None,
        "recent_events": [],
    }


def _ensure_stream(data: dict[str, Any], info: Mapping[str, Any] | None, stream_id: int | None) -> dict[str, Any] | None:
    sid = _as_int((info or {}).get("stream_id")) or stream_id
    if sid is None:
        return None
    streams = data["streams"]
    entry = streams.setdefault(str(sid), _new_stream_entry(sid))
    if info:
        for key in ("stream_name", "m3u_account_id", "m3u_account_name"):
            value = info.get(key)
            if value not in (None, ""):
                entry[key] = value
    return entry


def _append_recent(
    entry: dict[str, Any],
    *,
    event: str,
    at: str,
    payload: Mapping[str, Any],
    role: str | None = None,
    classification: str | None = None,
    counted: bool | None = None,
) -> None:
    item: dict[str, Any] = {"event": event, "timestamp": at}
    if role:
        item["role"] = role
    if classification:
        item["classification"] = classification
    if counted is not None:
        item["counted"] = bool(counted)
    for key in ("channel_name", "reason", "duration", "speed"):
        value = payload.get(key)
        if value not in (None, ""):
            item[key] = value
    recent = entry.setdefault("recent_events", [])
    recent.append(item)
    if len(recent) > MAX_RECENT_EVENTS:
        del recent[:-MAX_RECENT_EVENTS]
    entry["last_event_at"] = at


def _add_playback_seconds(data: dict[str, Any], channel_state: Mapping[str, Any], stream_id: int | None, now: datetime) -> None:
    if stream_id is None:
        return
    started = _parse_datetime(channel_state.get("active_since"))
    if started is None:
        return
    seconds = max(0.0, (now - started).total_seconds())
    entry = _ensure_stream(data, None, stream_id)
    if entry is not None:
        entry["playback_seconds"] = round(float(entry.get("playback_seconds") or 0.0) + seconds, 3)


def _touch_failure(entry: dict[str, Any], at: str, reason: str) -> None:
    entry["last_failure_at"] = at
    entry["last_failure_reason"] = reason or "unknown"


def _switch_context(channel_state: Mapping[str, Any], now: datetime) -> tuple[int | None, int | None, float | None]:
    switch_at = _parse_datetime(channel_state.get("last_switch_at"))
    if switch_at is None:
        return None, None, None
    age = (now - switch_at).total_seconds()
    return (
        _as_int(channel_state.get("last_switch_previous_stream_id")),
        _as_int(channel_state.get("last_switch_stream_id")),
        age,
    )


def record_runtime_event(
    event_name: str | None,
    payload: Mapping[str, Any] | None,
    *,
    logger=None,
    path: str = RELIABILITY_PATH,
    resolver: Callable[[int | None, Mapping[str, Any]], dict[str, Any] | None] = _default_stream_resolver,
    now: datetime | None = None,
) -> dict[str, Any]:
    event = str(event_name or "").strip()
    if event not in TRACKED_EVENTS:
        return {
            "status": "ok",
            "message": "Runtime reliability collector is event-driven; no supported event was supplied.",
            "recorded": False,
            "counted": False,
            "classification": None,
            "scoring_applied": False,
            "path": path,
        }

    payload_dict = dict(payload or {})
    now_dt = _now_utc(now)
    now_iso = now_dt.isoformat()
    recorded_stream_id: int | None = None
    event_counted = True
    event_classification: str | None = None

    with _locked_cache(path):
        data = load_reliability_cache(path)
        key = _channel_key(data, payload_dict)
        if key is None:
            return {
                "status": "ok",
                "message": f"Ignored {event}: event payload did not identify a channel.",
                "recorded": False,
                "counted": False,
                "classification": None,
                "scoring_applied": False,
                "path": path,
            }

        channel_state = data["channels"].setdefault(
            key,
            {
                "channel_name": str(payload_dict.get("channel_name") or ""),
                "active_stream_id": None,
                "active_since": None,
                "last_switch_previous_stream_id": None,
                "last_switch_stream_id": None,
                "last_switch_at": None,
            },
        )
        if payload_dict.get("channel_name"):
            channel_state["channel_name"] = str(payload_dict["channel_name"])

        direct_stream_id = _as_int(payload_dict.get("stream_id"))
        current_stream_id = _as_int(channel_state.get("active_stream_id"))
        direct_info = _resolve_stream(direct_stream_id, payload_dict, resolver)
        resolved_stream_id = _as_int((direct_info or {}).get("stream_id"))

        if event == "channel_start":
            sid = resolved_stream_id or direct_stream_id or current_stream_id
            info = direct_info or _resolve_stream(sid, payload_dict, resolver)
            if current_stream_id is not None and current_stream_id != sid:
                _add_playback_seconds(data, channel_state, current_stream_id, now_dt)
            entry = _ensure_stream(data, info, sid)
            if entry is not None:
                entry["playback_starts"] = int(entry.get("playback_starts") or 0) + 1
                _append_recent(entry, event=event, at=now_iso, payload=payload_dict)
                sid = int(entry["stream_id"])
                channel_state["active_stream_id"] = sid
                channel_state["active_since"] = now_iso
                recorded_stream_id = sid

        elif event == "stream_switch":
            new_sid = resolved_stream_id or direct_stream_id
            previous_sid = _as_int(payload_dict.get("previous_stream_id")) or current_stream_id
            if previous_sid is not None and previous_sid != new_sid:
                _add_playback_seconds(data, channel_state, previous_sid, now_dt)
                previous_info = _resolve_stream(previous_sid, payload_dict, resolver)
                previous_entry = _ensure_stream(data, previous_info, previous_sid)
                if previous_entry is not None:
                    previous_entry["switches_away"] = int(previous_entry.get("switches_away") or 0) + 1
                    _append_recent(previous_entry, event=event, at=now_iso, payload=payload_dict, role="away")
            new_info = direct_info or _resolve_stream(new_sid, payload_dict, resolver)
            new_entry = _ensure_stream(data, new_info, new_sid)
            if new_entry is not None:
                new_sid = int(new_entry["stream_id"])
                new_entry["switches_to"] = int(new_entry.get("switches_to") or 0) + 1
                _append_recent(new_entry, event=event, at=now_iso, payload=payload_dict, role="to")
                channel_state["active_stream_id"] = new_sid
                channel_state["active_since"] = now_iso
                recorded_stream_id = new_sid
            channel_state["last_switch_previous_stream_id"] = previous_sid
            channel_state["last_switch_stream_id"] = new_sid
            channel_state["last_switch_at"] = now_iso

        elif event == "channel_failover":
            previous_sid = _as_int(payload_dict.get("previous_stream_id"))
            recent_previous = None
            switch_at = _parse_datetime(channel_state.get("last_switch_at"))
            if switch_at is not None and (now_dt - switch_at).total_seconds() <= SWITCH_FAILOVER_WINDOW_SECONDS:
                recent_previous = _as_int(channel_state.get("last_switch_previous_stream_id"))
            failing_sid = previous_sid or recent_previous or direct_stream_id or current_stream_id
            info = _resolve_stream(failing_sid, payload_dict, resolver)
            entry = _ensure_stream(data, info, failing_sid)
            if entry is not None:
                reason = str(payload_dict.get("reason") or "failover")
                entry["failovers"] = int(entry.get("failovers") or 0) + 1
                if reason == "buffering_timeout":
                    entry["buffering_failovers"] = int(entry.get("buffering_failovers") or 0) + 1
                    duration = _as_float(payload_dict.get("duration"))
                    if duration is not None:
                        entry["buffering_failover_seconds"] = round(
                            float(entry.get("buffering_failover_seconds") or 0.0) + max(0.0, duration), 3
                        )
                _touch_failure(entry, now_iso, reason)
                _append_recent(entry, event=event, at=now_iso, payload=payload_dict)
                recorded_stream_id = int(entry["stream_id"])
            channel_state["last_switch_previous_stream_id"] = None
            channel_state["last_switch_stream_id"] = None
            channel_state["last_switch_at"] = None

        elif event == "channel_reconnect":
            previous_sid, switch_sid, switch_age = _switch_context(channel_state, now_dt)
            reported_sid = resolved_stream_id or direct_stream_id
            switch_internal = (
                switch_age is not None
                and 0.0 <= switch_age <= SWITCH_RECONNECT_SUPPRESSION_SECONDS
                and previous_sid is not None
                and switch_sid is not None
                and current_stream_id == switch_sid
                and reported_sid == previous_sid
            )
            if switch_internal:
                info = direct_info or _resolve_stream(previous_sid, payload_dict, resolver)
                entry = _ensure_stream(data, info, previous_sid)
                if entry is not None:
                    entry["reconnects_suppressed"] = int(entry.get("reconnects_suppressed") or 0) + 1
                    _append_recent(
                        entry,
                        event=event,
                        at=now_iso,
                        payload=payload_dict,
                        classification="switch_internal",
                        counted=False,
                    )
                    recorded_stream_id = int(entry["stream_id"])
                event_counted = False
                event_classification = "switch_internal"
            else:
                # Once Stream Sort has observed a switch, its active stream state is
                # more current than a reconnect payload that may have been resolved
                # from stale Redis metadata.
                sid = current_stream_id or resolved_stream_id or direct_stream_id
                info = direct_info if sid == resolved_stream_id else _resolve_stream(sid, payload_dict, resolver)
                entry = _ensure_stream(data, info, sid)
                if entry is not None:
                    entry["reconnects"] = int(entry.get("reconnects") or 0) + 1
                    _append_recent(entry, event=event, at=now_iso, payload=payload_dict)
                    recorded_stream_id = int(entry["stream_id"])

        elif event in {"channel_buffering", "channel_error"}:
            sid = direct_stream_id or current_stream_id or resolved_stream_id
            info = direct_info or _resolve_stream(sid, payload_dict, resolver)
            entry = _ensure_stream(data, info, sid)
            if entry is not None:
                if event == "channel_buffering":
                    entry["buffering_events"] = int(entry.get("buffering_events") or 0) + 1
                else:
                    entry["errors"] = int(entry.get("errors") or 0) + 1
                    _touch_failure(entry, now_iso, str(payload_dict.get("reason") or "channel_error"))
                _append_recent(entry, event=event, at=now_iso, payload=payload_dict)
                recorded_stream_id = int(entry["stream_id"])

        elif event == "channel_stop":
            sid = current_stream_id or direct_stream_id or resolved_stream_id
            if sid is not None:
                _add_playback_seconds(data, channel_state, sid, now_dt)
            info = direct_info if sid == resolved_stream_id else _resolve_stream(sid, payload_dict, resolver)
            entry = _ensure_stream(data, info, sid)
            if entry is not None:
                entry["playback_stops"] = int(entry.get("playback_stops") or 0) + 1
                _append_recent(entry, event=event, at=now_iso, payload=payload_dict)
                recorded_stream_id = int(entry["stream_id"])
            channel_state["active_stream_id"] = None
            channel_state["active_since"] = None
            channel_state["last_switch_previous_stream_id"] = None
            channel_state["last_switch_stream_id"] = None
            channel_state["last_switch_at"] = None

        channel_state["updated_at"] = now_iso
        data["updated_at"] = now_iso
        data["scoring_enabled"] = False
        _save_reliability_cache(data, path)

    if recorded_stream_id is None:
        event_counted = False
    if logger is not None:
        logger.info(
            "[Reliability] event=%s stream=%s channel=%s recorded=%s counted=%s classification=%s scoring=disabled",
            event,
            recorded_stream_id or "unknown",
            payload_dict.get("channel_name") or payload_dict.get("channel_id") or "unknown",
            recorded_stream_id is not None,
            event_counted,
            event_classification or "normal",
        )
    return {
        "status": "ok",
        "message": f"Runtime reliability event recorded: {event}",
        "event": event,
        "stream_id": recorded_stream_id,
        "recorded": recorded_stream_id is not None,
        "counted": event_counted,
        "classification": event_classification,
        "scoring_applied": False,
        "path": path,
    }
