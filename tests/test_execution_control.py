import threading

import pytest

from stream_sorter import execution_control, incremental


@pytest.fixture
def control_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(
        execution_control,
        "ANALYSIS_EXECUTION_LOCK_PATH",
        str(tmp_path / "execution.lock"),
    )
    monkeypatch.setattr(
        execution_control,
        "ANALYSIS_EXECUTION_STATE_PATH",
        str(tmp_path / "execution.json"),
    )
    monkeypatch.setattr(
        execution_control,
        "ANALYSIS_CANCEL_PATH",
        str(tmp_path / "cancel.json"),
    )
    monkeypatch.setattr(
        execution_control,
        "ANALYSIS_CONTROL_LOCK_PATH",
        str(tmp_path / "control.lock"),
    )
    monkeypatch.setattr(execution_control, "_active_token", None)


def test_execution_lease_rejects_overlapping_direct_call(control_paths):
    entered = threading.Event()
    release = threading.Event()
    results = []

    @execution_control.exclusive_analysis_execution
    def operation():
        entered.set()
        release.wait(timeout=2)
        return "complete"

    thread = threading.Thread(target=lambda: results.append(operation()))
    thread.start()
    assert entered.wait(timeout=2)
    with pytest.raises(execution_control.AnalysisAlreadyRunning):
        operation()
    release.set()
    thread.join(timeout=2)

    assert results == ["complete"]
    assert not thread.is_alive()


def test_cancel_before_commit_raises_cancelled(control_paths):
    @execution_control.exclusive_analysis_execution
    def operation():
        response = execution_control.request_analysis_cancel()
        assert response["status"] == "ok"
        assert execution_control.close_analysis_cancel_window() is True

    with pytest.raises(execution_control.AnalysisCancelled):
        operation()


def test_cancel_check_falls_back_to_persisted_execution_token(control_paths):
    execution_control._write_json(
        execution_control.ANALYSIS_EXECUTION_STATE_PATH,
        {"token": "persisted-token"},
    )
    execution_control._write_json(
        execution_control.ANALYSIS_CANCEL_PATH,
        {"token": "persisted-token"},
    )

    assert execution_control.analysis_cancel_requested() is True


def test_stop_is_rejected_after_commit_window_closes(control_paths):
    @execution_control.exclusive_analysis_execution
    def operation():
        execution_control.close_analysis_cancel_window()
        return execution_control.request_analysis_cancel()

    response = operation()
    assert response["status"] == "skipped"
    assert "already committing" in response["message"]


def test_cancelled_scheduler_checkpoints_completed_work_and_releases_exact_reservation(monkeypatch):
    canceled = {"value": False}
    started = []
    released = []

    class Capacity:
        def try_acquire(self, item):
            return True, item["id"]

        def release(self, reservation):
            released.append(reservation)

    def worker(item):
        started.append(item["id"])
        canceled["value"] = True
        return item["id"]

    monkeypatch.setattr(
        incremental,
        "analysis_cancel_requested",
        lambda: canceled["value"],
    )
    rows = list(
        incremental._fair_account_futures(
            [
                {"id": 1, "account_id": 10},
                {"id": 2, "account_id": 10},
            ],
            worker,
            max_workers=1,
            thread_name_prefix="test-cancel-drain",
            capacity_manager=Capacity(),
        )
    )

    assert started == [1]
    assert released == [1]
    assert [future.result() for _item, future in rows] == [1]
