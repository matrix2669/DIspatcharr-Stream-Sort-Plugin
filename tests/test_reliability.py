import json
from datetime import datetime, timedelta, timezone

from stream_sorter.reliability import record_runtime_event


def _resolver(stream_id, payload):
    if stream_id is not None:
        return {
            "stream_id": int(stream_id),
            "stream_name": f"Stream {stream_id}",
            "m3u_account_id": 1,
            "m3u_account_name": "Provider",
        }
    if payload.get("stream_url") == "http://example.test/42":
        return {
            "stream_id": 42,
            "stream_name": "Resolved 42",
            "m3u_account_id": 2,
            "m3u_account_name": "Provider 2",
        }
    return None


def _load(path):
    return json.loads(path.read_text())


def test_switch_then_buffering_failover_is_attributed_to_previous_stream(tmp_path):
    path = tmp_path / "reliability.json"
    started = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
    record_runtime_event(
        "channel_start",
        {"channel_name": "NBC", "stream_id": 10},
        path=str(path), resolver=_resolver, now=started,
    )
    record_runtime_event(
        "stream_switch",
        {"channel_name": "NBC", "stream_id": 11},
        path=str(path), resolver=_resolver, now=started + timedelta(seconds=30),
    )
    record_runtime_event(
        "channel_failover",
        {"channel_name": "NBC", "reason": "buffering_timeout", "duration": 12.5},
        path=str(path), resolver=_resolver, now=started + timedelta(seconds=31),
    )

    data = _load(path)
    assert data["streams"]["10"]["failovers"] == 1
    assert data["streams"]["10"]["buffering_failovers"] == 1
    assert data["streams"]["10"]["buffering_failover_seconds"] == 12.5
    assert data["streams"]["11"]["failovers"] == 0
    assert data["channels"]["name:NBC"]["active_stream_id"] == 11
    assert data["scoring_enabled"] is False


def test_failover_before_switch_is_attributed_to_current_stream(tmp_path):
    path = tmp_path / "reliability.json"
    started = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
    record_runtime_event(
        "channel_start",
        {"channel_name": "CBS", "stream_id": 20},
        path=str(path), resolver=_resolver, now=started,
    )
    record_runtime_event(
        "channel_failover",
        {"channel_name": "CBS", "reason": "connection_failed"},
        path=str(path), resolver=_resolver, now=started + timedelta(seconds=5),
    )
    record_runtime_event(
        "stream_switch",
        {"channel_name": "CBS", "stream_id": 21},
        path=str(path), resolver=_resolver, now=started + timedelta(seconds=6),
    )

    data = _load(path)
    assert data["streams"]["20"]["failovers"] == 1
    assert data["streams"]["20"]["switches_away"] == 1
    assert data["streams"]["21"]["switches_to"] == 1


def test_channel_stop_accumulates_active_playback_time(tmp_path):
    path = tmp_path / "reliability.json"
    started = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
    record_runtime_event(
        "channel_start",
        {"channel_name": "FOX", "stream_id": 30},
        path=str(path), resolver=_resolver, now=started,
    )
    record_runtime_event(
        "channel_stop",
        {"channel_name": "FOX"},
        path=str(path), resolver=_resolver, now=started + timedelta(seconds=90),
    )

    data = _load(path)
    assert data["streams"]["30"]["playback_starts"] == 1
    assert data["streams"]["30"]["playback_stops"] == 1
    assert data["streams"]["30"]["playback_seconds"] == 90.0
    assert data["channels"]["name:FOX"]["active_stream_id"] is None


def test_stream_can_be_resolved_from_runtime_url_when_id_is_missing(tmp_path):
    path = tmp_path / "reliability.json"
    result = record_runtime_event(
        "channel_start",
        {"channel_name": "ABC", "stream_url": "http://example.test/42"},
        path=str(path), resolver=_resolver,
    )

    data = _load(path)
    assert result["stream_id"] == 42
    assert data["streams"]["42"]["stream_name"] == "Resolved 42"
    assert data["channels"]["name:ABC"]["active_stream_id"] == 42


def test_channel_buffering_is_collection_only(tmp_path):
    path = tmp_path / "reliability.json"
    started = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
    record_runtime_event(
        "channel_start",
        {"channel_name": "PBS", "stream_id": 50},
        path=str(path), resolver=_resolver, now=started,
    )
    result = record_runtime_event(
        "channel_buffering",
        {"channel_name": "PBS", "speed": 0.72},
        path=str(path), resolver=_resolver, now=started + timedelta(seconds=5),
    )

    data = _load(path)
    assert result["scoring_applied"] is False
    assert data["streams"]["50"]["buffering_events"] == 1
    assert data["scoring_enabled"] is False


def test_switch_internal_reconnect_is_retained_but_not_counted(tmp_path):
    path = tmp_path / "reliability.json"
    started = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
    record_runtime_event(
        "channel_start",
        {"channel_name": "NBC", "stream_id": 10},
        path=str(path), resolver=_resolver, now=started,
    )
    record_runtime_event(
        "stream_switch",
        {"channel_name": "NBC", "stream_id": 11},
        path=str(path), resolver=_resolver, now=started + timedelta(seconds=30),
    )
    result = record_runtime_event(
        "channel_reconnect",
        {"channel_name": "NBC", "stream_id": 10},
        path=str(path), resolver=_resolver, now=started + timedelta(seconds=30.35),
    )

    data = _load(path)
    old_stream = data["streams"]["10"]
    assert old_stream["reconnects"] == 0
    assert old_stream["reconnects_suppressed"] == 1
    assert data["streams"]["11"]["reconnects"] == 0
    assert old_stream["recent_events"][-1]["event"] == "channel_reconnect"
    assert old_stream["recent_events"][-1]["classification"] == "switch_internal"
    assert old_stream["recent_events"][-1]["counted"] is False
    assert result["stream_id"] == 10
    assert result["counted"] is False
    assert result["classification"] == "switch_internal"


def test_stale_reconnect_after_suppression_window_counts_against_active_stream(tmp_path):
    path = tmp_path / "reliability.json"
    started = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
    record_runtime_event(
        "channel_start",
        {"channel_name": "NBC", "stream_id": 10},
        path=str(path), resolver=_resolver, now=started,
    )
    record_runtime_event(
        "stream_switch",
        {"channel_name": "NBC", "stream_id": 11},
        path=str(path), resolver=_resolver, now=started + timedelta(seconds=30),
    )
    result = record_runtime_event(
        "channel_reconnect",
        {"channel_name": "NBC", "stream_id": 10},
        path=str(path), resolver=_resolver, now=started + timedelta(seconds=33),
    )

    data = _load(path)
    assert data["streams"]["10"]["reconnects"] == 0
    assert data["streams"]["11"]["reconnects"] == 1
    assert result["stream_id"] == 11
    assert result["counted"] is True
    assert result["classification"] is None


def test_immediate_reconnect_for_new_stream_is_not_suppressed(tmp_path):
    path = tmp_path / "reliability.json"
    started = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
    record_runtime_event(
        "channel_start",
        {"channel_name": "NBC", "stream_id": 10},
        path=str(path), resolver=_resolver, now=started,
    )
    record_runtime_event(
        "stream_switch",
        {"channel_name": "NBC", "stream_id": 11},
        path=str(path), resolver=_resolver, now=started + timedelta(seconds=30),
    )
    result = record_runtime_event(
        "channel_reconnect",
        {"channel_name": "NBC", "stream_id": 11},
        path=str(path), resolver=_resolver, now=started + timedelta(seconds=30.35),
    )

    data = _load(path)
    assert data["streams"]["10"]["reconnects"] == 0
    assert data["streams"]["11"]["reconnects"] == 1
    assert result["stream_id"] == 11
    assert result["counted"] is True
    assert result["classification"] is None
