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
    if payload.get("stream_url") == "http://example.test/42" or payload.get("url") == "http://example.test/42":
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
    assert data["scoring_enabled"] is True


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


def test_clean_long_playback_records_sustained_throughput_observation(tmp_path):
    path = tmp_path / "reliability.json"
    started = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
    record_runtime_event("channel_start", {"channel_name": "LONG", "stream_id": 30}, path=str(path), resolver=_resolver, now=started)
    record_runtime_event(
        "channel_stop",
        {"channel_name": "LONG", "runtime": 300, "total_bytes": 225_000_000},
        path=str(path), resolver=_resolver, now=started + timedelta(seconds=300),
    )

    row = _load(path)["streams"]["30"]["playback_throughput_history"][-1]
    assert row["measured_mbps"] == 6.0
    assert row["eligible_for_throughput"] is True


def test_buffering_failover_records_insufficient_delivery(tmp_path):
    path = tmp_path / "reliability.json"
    started = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
    record_runtime_event("channel_start", {"channel_name": "BAD", "stream_id": 30}, path=str(path), resolver=_resolver, now=started)
    record_runtime_event(
        "channel_failover",
        {"channel_name": "BAD", "reason": "buffering_timeout", "speed": 0.91},
        path=str(path), resolver=_resolver, now=started + timedelta(seconds=30),
    )

    row = _load(path)["streams"]["30"]["playback_throughput_history"][-1]
    assert row["status"] == "insufficient"
    assert row["speed"] == 0.91


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


def test_channel_buffering_updates_scoring_evidence(tmp_path):
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
    assert result["scoring_applied"] is True
    assert data["streams"]["50"]["buffering_events"] == 1
    assert data["scoring_enabled"] is True
    assert data["streams"]["50"]["reliability_evidence"]["buffering_events"] == 1


def test_channel_error_prefers_url_attribution_and_preserves_error_type(tmp_path):
    path = tmp_path / "reliability.json"
    started = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
    record_runtime_event(
        "channel_start", {"channel_name": "ABC", "stream_id": 10},
        path=str(path), resolver=_resolver, now=started,
    )
    result = record_runtime_event(
        "channel_error",
        {"channel_name": "ABC", "url": "http://example.test/42", "error_type": "connection_failed", "attempts": 3},
        path=str(path), resolver=_resolver, now=started + timedelta(seconds=10),
    )
    data = _load(path)
    assert result["stream_id"] == 42
    assert result["classification"] == "startup_failure"
    assert data["streams"]["42"]["last_failure_reason"] == "connection_failed"
    assert data["streams"]["42"]["recent_events"][-1]["attempts"] == 3


def test_clean_stop_records_reusable_playback_but_failed_session_does_not(tmp_path):
    path = tmp_path / "reliability.json"
    started = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
    record_runtime_event("channel_start", {"channel_name": "CLEAN", "stream_id": 60}, path=str(path), resolver=_resolver, now=started)
    record_runtime_event("channel_stop", {"channel_name": "CLEAN"}, path=str(path), resolver=_resolver, now=started + timedelta(seconds=61))
    record_runtime_event("channel_start", {"channel_name": "FAILED", "stream_id": 61}, path=str(path), resolver=_resolver, now=started)
    record_runtime_event("channel_error", {"channel_name": "FAILED", "stream_id": 61}, path=str(path), resolver=_resolver, now=started + timedelta(seconds=30))
    record_runtime_event("channel_stop", {"channel_name": "FAILED"}, path=str(path), resolver=_resolver, now=started + timedelta(seconds=70))
    data = _load(path)
    assert data["streams"]["60"]["last_clean_playback_seconds"] == 61
    assert "last_clean_playback_at" not in data["streams"]["61"]


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


def test_nested_events_are_queued_and_preserved_in_order(tmp_path):
    from stream_sorter import reliability as r
    path = str(tmp_path / "cache.json")
    nested = []
    def resolver(sid, payload):
        if not nested:
            nested.append(record_runtime_event("stream_switch", {"channel_name": "A", "stream_id": 2}, path=path, resolver=_resolver))
            nested.append(record_runtime_event("channel_failover", {"channel_name": "A"}, path=path, resolver=_resolver))
            nested.append(record_runtime_event("channel_stop", {"channel_name": "A"}, path=path, resolver=_resolver))
            nested.append(record_runtime_event("channel_start", {"channel_name": "B", "stream_id": 1}, path=path, resolver=_resolver))
        return _resolver(sid, payload)
    result = record_runtime_event("channel_start", {"channel_name": "A", "stream_id": 1}, path=path, resolver=resolver)
    data = r.load_reliability_cache(path)
    assert result["recorded"]
    assert all(x["status"] == "queued" and not x["recorded"] for x in nested)
    assert data["streams"]["1"]["playback_starts"] == 2
    assert data["streams"]["1"]["switches_away"] == 1
    assert data["streams"]["1"]["failovers"] == 1
    assert data["streams"]["2"]["playback_stops"] == 1
    assert data["channels"]["name:A"]["active_stream_id"] is None


def test_nested_reset_is_ordered_and_truthfully_queued(tmp_path):
    from stream_sorter import reliability as r
    path = str(tmp_path / "cache.json")
    results = []
    def resolver(sid, payload):
        if not results:
            results.append(r.reset_reliability_cache(path))
            results.append(record_runtime_event("channel_start", {"channel_name": "B", "stream_id": 2}, path=path, resolver=_resolver))
        return _resolver(sid, payload)
    record_runtime_event("channel_start", {"channel_name": "A", "stream_id": 1}, path=path, resolver=resolver)
    assert results[0] is False and results[1]["status"] == "queued"
    assert set(r.load_reliability_cache(path)["streams"]) == {"2"}


def test_queue_overflow_aborts_even_if_resolver_catches_it(tmp_path, monkeypatch):
    import pytest
    from stream_sorter import reliability as r
    path = str(tmp_path / "cache.json")
    monkeypatch.setattr(r, "MAX_QUEUED_OPERATIONS", 1)
    def resolver(sid, payload):
        for _ in range(2):
            try:
                record_runtime_event("channel_start", {"channel_name": "B", "stream_id": 2}, path=path, resolver=_resolver)
            except r.ReliabilityQueueFull:
                pass
        return _resolver(sid, payload)
    with pytest.raises(r.ReliabilityQueueFull):
        record_runtime_event("channel_start", {"channel_name": "A", "stream_id": 1}, path=path, resolver=resolver)
    assert not (tmp_path / "cache.json").exists()
    assert record_runtime_event("channel_start", {"channel_name": "A", "stream_id": 1}, path=path, resolver=_resolver)["recorded"]


def test_failed_save_aborts_and_releases_lock(tmp_path, monkeypatch):
    import pytest
    from stream_sorter import reliability as r
    path = str(tmp_path / "cache.json")
    original = r._save_reliability_cache
    monkeypatch.setattr(r, "_save_reliability_cache", lambda *a: (_ for _ in ()).throw(OSError("failed")))
    with pytest.raises(OSError):
        record_runtime_event("channel_start", {"channel_name": "A", "stream_id": 1}, path=path, resolver=_resolver)
    monkeypatch.setattr(r, "_save_reliability_cache", original)
    assert record_runtime_event("channel_start", {"channel_name": "A", "stream_id": 1}, path=path, resolver=_resolver)["recorded"]
    assert r.load_reliability_cache(path)["streams"]["1"]["playback_starts"] == 1


def test_thread_contention_aliases_and_independent_paths(tmp_path, monkeypatch):
    import threading
    import time
    from stream_sorter import reliability as r
    path = str(tmp_path / "cache.json")
    alias = str(tmp_path / "." / "cache.json")
    entered, release = threading.Event(), threading.Event()
    def resolver(sid, payload):
        entered.set()
        assert release.wait(2)
        return _resolver(sid, payload)
    owner = threading.Thread(target=record_runtime_event, args=("channel_start", {"channel_name": "A", "stream_id": 1}), kwargs={"path": path, "resolver": resolver})
    owner.start()
    assert entered.wait(2)
    monkeypatch.setattr(r, "LOCK_TIMEOUT_SECONDS", 0.08)
    started = time.monotonic()
    result = record_runtime_event("channel_start", {"channel_name": "B", "stream_id": 2}, path=alias, resolver=_resolver)
    elapsed = time.monotonic() - started
    assert result["status"] == "busy" and not result["recorded"] and 0.05 <= elapsed < 0.5
    assert record_runtime_event("channel_start", {"channel_name": "B", "stream_id": 2}, path=str(tmp_path / "other.json"), resolver=_resolver)["recorded"]
    release.set()
    owner.join(2)
    assert not owner.is_alive()
    assert record_runtime_event("channel_start", {"channel_name": "B", "stream_id": 2}, path=alias, resolver=_resolver)["recorded"]


def test_concurrent_writers_and_release_before_deadline(tmp_path):
    import threading
    import time
    from stream_sorter import reliability as r
    path = str(tmp_path / "cache.json")
    entered = threading.Event()
    def resolver(sid, payload):
        entered.set()
        time.sleep(0.04)
        return _resolver(sid, payload)
    owner = threading.Thread(target=record_runtime_event, args=("channel_start", {"channel_name": "A", "stream_id": 1}), kwargs={"path": path, "resolver": resolver})
    owner.start()
    assert entered.wait(1)
    assert record_runtime_event("channel_start", {"channel_name": "B", "stream_id": 1}, path=path, resolver=_resolver)["recorded"]
    owner.join(1)
    threads = [threading.Thread(target=record_runtime_event, args=("channel_start", {"channel_name": str(i), "stream_id": 1}), kwargs={"path": path, "resolver": _resolver}) for i in range(12)]
    for thread in threads: thread.start()
    for thread in threads: thread.join(2)
    assert r.load_reliability_cache(path)["streams"]["1"]["playback_starts"] == 14


def test_process_lock_deadline_and_fork_ownership(tmp_path, monkeypatch):
    import os
    import time
    import pytest
    from stream_sorter import reliability as r
    if not hasattr(os, "fork") or r.fcntl is None:
        pytest.skip("requires fork and flock")
    path = str(tmp_path / "cache.json")
    monkeypatch.setattr(r, "LOCK_TIMEOUT_SECONDS", 0.08)
    read_fd, write_fd = os.pipe()
    def resolver(sid, payload):
        child = os.fork()
        if child == 0:
            os.close(read_fd)
            start = time.monotonic()
            result = record_runtime_event("channel_start", {"channel_name": "child", "stream_id": 2}, path=path, resolver=_resolver)
            os.write(write_fd, f"{result['status']} {time.monotonic() - start}".encode())
            os._exit(0)
        os.close(write_fd)
        message = os.read(read_fd, 128).decode()
        os.close(read_fd)
        _, status = os.waitpid(child, 0)
        assert status == 0
        outcome, duration = message.split()
        assert outcome == "busy" and 0.05 <= float(duration) < 0.5
        return _resolver(sid, payload)
    assert record_runtime_event("channel_start", {"channel_name": "parent", "stream_id": 1}, path=path, resolver=resolver)["recorded"]
    assert set(r.load_reliability_cache(path)["streams"]) == {"1"}


def test_process_lock_release_before_deadline(tmp_path):
    import os
    import threading
    import time
    import pytest
    from stream_sorter import reliability as r
    if not hasattr(os, "fork") or r.fcntl is None:
        pytest.skip("requires fork and flock")
    path = str(tmp_path / "cache.json")
    ready_r, ready_w = os.pipe()
    child = os.fork()
    if child == 0:
        os.close(ready_r)
        with r._locked_cache(r._normalized_path(path)):
            os.write(ready_w, b"x")
            time.sleep(0.06)
        os._exit(0)
    os.close(ready_w)
    assert os.read(ready_r, 1) == b"x"
    os.close(ready_r)
    result = record_runtime_event("channel_start", {"channel_name": "A", "stream_id": 1}, path=path, resolver=_resolver)
    _, status = os.waitpid(child, 0)
    assert status == 0 and result["recorded"]


def test_reset_statistics_reports_reliability_failure(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from stream_sorter import plugin
    path = tmp_path / "reliability.json"
    path.write_text('{}')
    monkeypatch.setattr(plugin, "RELIABILITY_PATH", str(path))
    for name in ("ANALYSIS_CACHE_PATH", "LEGACY_CACHE_PATH", "ANALYSIS_HEALTH_REPORT_PATH", "TTL_RECOMMENDATION_PATH", "STATUS_PATH", "REPORT_PATH"):
        monkeypatch.setattr(plugin, name, str(tmp_path / name))
    @contextmanager
    def available():
        yield
    monkeypatch.setattr(plugin, "analysis_maintenance_execution", available)
    def fail(_path):
        raise RuntimeError("lock timed out")
    monkeypatch.setattr(plugin, "reset_reliability_cache", fail)
    result = plugin._run_reset_statistics_action(include_history=True)
    assert result["status"] == "error"
    assert result["reliability_reset"] is False
    assert "lock timed out" in result["message"]
    assert path.exists()


def test_process_writers_preserve_increments(tmp_path):
    import os
    import pytest
    from stream_sorter import reliability as r
    if not hasattr(os, "fork") or r.fcntl is None:
        pytest.skip("requires fork and flock")
    path = str(tmp_path / "cache.json")
    children = []
    for i in range(8):
        child = os.fork()
        if child == 0:
            result = record_runtime_event("channel_start", {"channel_name": str(i), "stream_id": 1}, path=path, resolver=_resolver)
            os._exit(0 if result["recorded"] else 1)
        children.append(child)
    assert all(os.waitpid(child, 0)[1] == 0 for child in children)
    assert r.load_reliability_cache(path)["streams"]["1"]["playback_starts"] == 8


def test_ignored_event_preserves_missing_and_malformed_cache(tmp_path):
    path = tmp_path / "cache.json"
    result = record_runtime_event("channel_start", {"stream_id": 1}, path=str(path), resolver=_resolver)
    assert not result["recorded"] and not path.exists()
    malformed = b'{ malformed cache\n'
    path.write_bytes(malformed)
    result = record_runtime_event("channel_start", {"stream_id": 1}, path=str(path), resolver=_resolver)
    assert not result["recorded"] and path.read_bytes() == malformed


def test_ignored_outer_event_still_commits_valid_queued_event(tmp_path, monkeypatch):
    from stream_sorter import reliability as r
    path = str(tmp_path / "cache.json")
    original = r._channel_key
    queued = []
    def channel_key(data, payload):
        if not payload.get("channel_name"):
            queued.append(record_runtime_event("channel_start", {"channel_name": "B", "stream_id": 2}, path=path, resolver=_resolver))
        return original(data, payload)
    monkeypatch.setattr(r, "_channel_key", channel_key)
    result = record_runtime_event("channel_start", {"stream_id": 1}, path=path, resolver=_resolver)
    assert not result["recorded"] and queued[0]["status"] == "queued"
    assert r.load_reliability_cache(path)["streams"]["2"]["playback_starts"] == 1


def test_fork_child_normal_unwind_cannot_commit_or_close_reused_fd(tmp_path, monkeypatch):
    import os
    import pytest
    from stream_sorter import reliability as r
    if not hasattr(os, "fork") or r.fcntl is None:
        pytest.skip("requires fork and flock")
    path = str(tmp_path / "cache.json")
    other_path = str(tmp_path / "child.txt")
    monkeypatch.setattr(r, "LOCK_TIMEOUT_SECONDS", 0.05)
    read_fd, write_fd = os.pipe()
    child_state = {}
    def resolver(sid, payload):
        lock_fd = next(iter(r._open_lock_handles)).fileno()
        child = os.fork()
        if child == 0:
            child_state["fd"] = os.open(other_path, os.O_CREAT | os.O_WRONLY, 0o600)
            child_state["reused"] = child_state["fd"] == lock_fd
            os.close(read_fd)
            return _resolver(sid, payload)
        os.close(write_fd)
        message = os.read(read_fd, 256).decode()
        os.close(read_fd)
        _, status = os.waitpid(child, 0)
        assert status == 0 and message == "denied:busy:absent:fd-open"
        return _resolver(sid, payload)
    try:
        result = record_runtime_event("channel_start", {"channel_name": "A", "stream_id": 1}, path=path, resolver=resolver)
    except RuntimeError as exc:
        if not child_state:
            raise
        denied = "denied" if "cannot commit after fork" in str(exc) and child_state["reused"] else "wrong-error"
        busy = record_runtime_event("channel_start", {"channel_name": "B", "stream_id": 2}, path=path, resolver=_resolver)["status"]
        absent = "absent" if not os.path.exists(path) else "written"
        try:
            os.write(child_state["fd"], b"ok")
            fd_state = "fd-open"
        except OSError:
            fd_state = "fd-closed"
        os.write(write_fd, f"{denied}:{busy}:{absent}:{fd_state}".encode())
        os.close(child_state["fd"])
        os.close(write_fd)
        os._exit(0)
    assert result["recorded"]
    assert (tmp_path / "child.txt").read_bytes() == b"ok"
    assert set(r.load_reliability_cache(path)["streams"]) == {"1"}
