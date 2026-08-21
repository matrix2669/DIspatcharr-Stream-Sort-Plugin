from collections import Counter
from types import SimpleNamespace

from stream_sorter.capacity import DispatcharrCapacityManager
from stream_sorter.incremental import _fair_account_futures


class RecordingLogger:
    def __init__(self):
        self.messages = []

    def warning(self, message, *args):
        self.messages.append(("warning", message % args))

    def error(self, message, *args):
        self.messages.append(("error", message % args))


def test_active_viewer_occupies_limited_m3u_capacity():
    account = SimpleNamespace(get_user_agent_string=lambda: "Dispatcharr-Test")
    profile = SimpleNamespace(id=30, max_streams=1, m3u_account=account)
    stream = SimpleNamespace(id=101)
    active = {30: 1}

    def reserve(candidate, _redis):
        if active[candidate.id] >= candidate.max_streams:
            return False, active[candidate.id], "profile_full"
        active[candidate.id] += 1
        return True, active[candidate.id], None

    def release(profile_id, _redis):
        active[profile_id] -= 1

    manager = DispatcharrCapacityManager(
        profiles={3: [profile]},
        streams={101: stream},
        redis_client=object(),
        reserve_profile_slot=reserve,
        release_profile_slot=release,
        resolve_live_stream_url=lambda *_args: "http://rewritten.test/stream",
        logger=RecordingLogger(),
    )

    item = {"id": 101, "account_id": 3, "url": "http://original.test/stream"}
    assert manager.try_acquire(item) == (False, None)
    active[30] = 0
    acquired, reservation = manager.try_acquire(item)
    assert acquired is True
    assert reservation.profile_id == 30
    assert reservation.reserved is True
    assert manager.prepare_item(item, reservation)["url"] == "http://rewritten.test/stream"
    assert active[30] == 1
    manager.release(reservation)
    assert active[30] == 0


def test_second_active_profile_adds_analyzer_capacity_and_rewrites_url():
    account = SimpleNamespace(get_user_agent_string=lambda: "Profile-UA")
    profiles = [
        SimpleNamespace(id=30, max_streams=1, m3u_account=account),
        SimpleNamespace(id=47, max_streams=1, m3u_account=account),
    ]
    stream = SimpleNamespace(id=101)
    active = {30: 1, 47: 0}

    def reserve(candidate, _redis):
        if active[candidate.id] >= candidate.max_streams:
            return False, active[candidate.id], "profile_full"
        active[candidate.id] += 1
        return True, active[candidate.id], None

    def release(profile_id, _redis):
        active[profile_id] -= 1

    manager = DispatcharrCapacityManager(
        profiles={3: profiles},
        streams={101: stream},
        redis_client=object(),
        reserve_profile_slot=reserve,
        release_profile_slot=release,
        resolve_live_stream_url=lambda _stream, _account, profile: (
            f"http://profile-{profile.id}.test/stream"
        ),
        logger=RecordingLogger(),
    )

    item = {"id": 101, "account_id": 3, "url": "http://original.test/stream"}
    acquired, reservation = manager.try_acquire(item)
    assert acquired is True
    assert reservation.profile_id == 47
    assert active == {30: 1, 47: 1}
    assert manager.prepare_item(item, reservation) == {
        **item,
        "url": "http://profile-47.test/stream",
        "user_agent": "Profile-UA",
        "m3u_profile_id": 47,
    }
    manager.release(reservation)
    assert active == {30: 1, 47: 0}


def test_unlimited_m3u_profile_does_not_require_redis():
    account = SimpleNamespace(get_user_agent_string=lambda: "Unlimited-UA")
    profile = SimpleNamespace(id=10, max_streams=0, m3u_account=account)
    stream = SimpleNamespace(id=501)
    manager = DispatcharrCapacityManager(
        profiles={10: [profile]},
        streams={501: stream},
        redis_client=None,
        reserve_profile_slot=lambda *_args: (_ for _ in ()).throw(AssertionError("must not reserve")),
        release_profile_slot=lambda *_args: None,
        resolve_live_stream_url=lambda *_args: "http://unlimited.test/stream",
        logger=RecordingLogger(),
    )
    acquired, reservation = manager.try_acquire({"id": 501, "account_id": 10})
    assert acquired is True
    assert reservation.profile_id == 10
    assert reservation.reserved is False
    manager.release(reservation)


def test_scheduler_defers_full_source_and_preserves_other_capacity():
    released = []

    class Capacity:
        def try_acquire(self, item):
            if item["account_id"] == 10:
                return False, None
            return True, item["id"]

        def release(self, reservation):
            released.append(reservation)

    items = [
        {"id": 1, "account_id": 10},
        {"id": 2, "account_id": 20},
        {"id": 3, "account_id": 20},
    ]
    rows = list(
        _fair_account_futures(
            items,
            lambda item: item["id"],
            max_workers=3,
            thread_name_prefix="test-capacity",
            capacity_manager=Capacity(),
        )
    )

    deferred = [item["id"] for item, future in rows if future is None]
    completed = sorted(future.result() for _item, future in rows if future is not None)
    assert deferred == [1]
    assert completed == [2, 3]
    assert sorted(released) == [2, 3]


def test_scheduler_releases_reservation_when_worker_fails():
    released = []

    class Capacity:
        def try_acquire(self, item):
            return True, item["id"]

        def release(self, reservation):
            released.append(reservation)

    def fail(_item):
        raise RuntimeError("probe failed")

    rows = list(
        _fair_account_futures(
            [{"id": 42, "account_id": 3}],
            fail,
            max_workers=1,
            thread_name_prefix="test-capacity-failure",
            capacity_manager=Capacity(),
        )
    )
    assert released == [42]
    assert isinstance(rows[0][1].exception(), RuntimeError)


def test_retry_scheduler_limits_each_m3u_to_one_active_recheck():
    import threading
    import time

    lock = threading.Lock()
    active = Counter()
    peak = Counter()

    def worker(item):
        account_id = item["account_id"]
        with lock:
            active[account_id] += 1
            peak[account_id] = max(peak[account_id], active[account_id])
        time.sleep(0.01)
        with lock:
            active[account_id] -= 1
        return item["id"]

    items = [
        {"id": 1, "account_id": 10},
        {"id": 2, "account_id": 10},
        {"id": 3, "account_id": 20},
        {"id": 4, "account_id": 20},
    ]
    rows = list(
        _fair_account_futures(
            items,
            worker,
            max_workers=4,
            max_per_account=1,
            thread_name_prefix="test-serial-rechecks",
        )
    )

    assert sorted(future.result() for _item, future in rows) == [1, 2, 3, 4]
    assert peak == {10: 1, 20: 1}
