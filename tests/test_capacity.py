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
    profile = SimpleNamespace(id=30, max_streams=1)
    active = {30: 1}

    def reserve(candidate, _redis):
        if active[candidate.id] >= candidate.max_streams:
            return False, active[candidate.id], "profile_full"
        active[candidate.id] += 1
        return True, active[candidate.id], None

    def release(profile_id, _redis):
        active[profile_id] -= 1

    manager = DispatcharrCapacityManager(
        limits={3: 1},
        profiles={3: profile},
        redis_client=object(),
        reserve_profile_slot=reserve,
        release_profile_slot=release,
        logger=RecordingLogger(),
    )

    assert manager.try_acquire({"account_id": 3}) == (False, None)
    active[30] = 0
    assert manager.try_acquire({"account_id": 3}) == (True, 30)
    assert active[30] == 1
    manager.release(30)
    assert active[30] == 0


def test_unlimited_m3u_does_not_require_redis_or_profile():
    manager = DispatcharrCapacityManager(
        limits={10: 0},
        profiles={},
        redis_client=None,
        reserve_profile_slot=lambda *_args: (_ for _ in ()).throw(AssertionError("must not reserve")),
        release_profile_slot=lambda *_args: None,
        logger=RecordingLogger(),
    )
    assert manager.try_acquire({"account_id": 10}) == (True, None)


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
