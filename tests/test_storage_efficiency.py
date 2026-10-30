import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from stream_sorter import analyzer, execution_control, incremental, plugin


def test_progress_throttles_unchanged_phase_and_preserves_every_log(monkeypatch):
    system_logger = Mock()
    persist = Mock()
    monkeypatch.setattr(plugin, "LOGGER", system_logger)
    monkeypatch.setattr(plugin, "_update_status", persist)
    ticks = iter([0.0, 1.0, 9.999, 10.0, 19.999, 20.0])
    monkeypatch.setattr(plugin, "time", SimpleNamespace(monotonic=lambda: next(ticks)))
    progress = plugin._ProgressLogger("job-1")

    for completed in range(1, 7):
        progress.info(
            "[Analyze Media] %d%% (%d/%d) stream=%s",
            completed * 10, completed, 10, completed,
            extra={"check": completed},
        )

    assert system_logger.info.call_count == 6
    assert [call.args[2] for call in system_logger.info.call_args_list] == list(range(1, 7))
    assert [call.kwargs["extra"] for call in system_logger.info.call_args_list] == [
        {"check": completed} for completed in range(1, 7)
    ]
    assert [call.kwargs["progress_completed"] for call in persist.call_args_list] == [1, 4, 6]
    assert all(call.args == ("job-1",) for call in persist.call_args_list)
    progress.warning("warning %s", "retained")
    progress.error("error retained")
    system_logger.warning.assert_called_once_with("warning %s", "retained")
    system_logger.error.assert_called_once_with("error retained")


def test_progress_persists_phase_changes_immediately_and_resets_interval(monkeypatch):
    monkeypatch.setattr(plugin, "LOGGER", Mock())
    persist = Mock()
    monkeypatch.setattr(plugin, "_update_status", persist)
    ticks = iter([100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 115.999, 116.0])
    monkeypatch.setattr(plugin, "time", SimpleNamespace(monotonic=lambda: next(ticks)))
    progress = plugin._ProgressLogger("job-1")
    prefixes = [
        "Analyze Media", "Analyze Retry 1", "Analyze Combined",
        "Analyze Combined Content", "Analyze Content Retry 1",
        "Analyze Content", "Analyze Throughput", "Analyze Throughput", "Analyze Throughput",
    ]
    for completed, prefix in enumerate(prefixes, 1):
        progress.info("[%s] %d%% (%d/10)", prefix, completed * 10, completed)

    assert [call.kwargs["phase"] for call in persist.call_args_list] == [
        "media_analysis", "media_retry", "combined_analysis",
        "content_retry", "content_analysis", "throughput_analysis", "throughput_analysis",
    ]
    assert persist.call_args.kwargs["progress_completed"] == 9


def test_interleaved_combined_capture_and_content_share_one_throttle(monkeypatch):
    system_logger = Mock()
    persist = Mock()
    monkeypatch.setattr(plugin, "LOGGER", system_logger)
    monkeypatch.setattr(plugin, "_update_status", persist)
    ticks = iter([100.0, 101.0, 102.0, 109.999, 110.0])
    monkeypatch.setattr(plugin, "time", SimpleNamespace(monotonic=lambda: next(ticks)))
    progress = plugin._ProgressLogger("job-1")
    # The pipeline logs remote capture and local decoding results interleaved.
    messages = [
        ("[Analyze Combined] %d%% (%d/%d) stream=%s content=pending throughput=healthy", 10, 1, 10, 42),
        ("[Analyze Combined Content] %d%% (%d/%d) stream=%s health=alive", 10, 1, 10, 42),
        ("[Analyze Combined] %d%% (%d/%d) stream=%s content=pending throughput=healthy", 20, 2, 10, 43),
        ("[Analyze Combined Content] %d%% (%d/%d) stream=%s health=alive", 20, 2, 10, 43),
    ]
    for args in messages:
        progress.info(*args)

    persist.assert_called_once_with(
        "job-1", phase="combined_analysis", progress_percent=10,
        progress_completed=1, progress_total=10,
    )
    assert [call.args for call in system_logger.info.call_args_list] == messages

    progress.info("[Analyze Combined] 30% (3/10) stream=44")
    assert persist.call_count == 2
    assert persist.call_args.kwargs == {
        "phase": "combined_analysis", "progress_percent": 30,
        "progress_completed": 3, "progress_total": 10,
    }


@pytest.mark.parametrize(
    "analysis_prefix,retry_prefixes,phase",
    [
        (
            "Analyze Combined",
            ["Analyze Combined Retry 1/3", "Analyze Combined Retry 1/3 Content",
             "Analyze Combined Retry 2/3", "Analyze Combined Retry 2/3 Content"],
            "combined_retry",
        ),
        (
            "Analyze Throughput",
            ["Analyze Throughput Retry 1/3", "Analyze Throughput Retry 2/3",
             "Analyze Throughput Retry 3/3", "Analyze Throughput Retry 3/3"],
            "throughput_retry",
        ),
    ],
)
def test_retry_prefixes_share_stable_phase_and_normal_throttle(
    monkeypatch, analysis_prefix, retry_prefixes, phase,
):
    system_logger = Mock()
    persist = Mock()
    monkeypatch.setattr(plugin, "LOGGER", system_logger)
    monkeypatch.setattr(plugin, "_update_status", persist)
    ticks = iter([100.0, 101.0, 102.0, 110.999, 111.0])
    monkeypatch.setattr(plugin, "time", SimpleNamespace(monotonic=lambda: next(ticks)))
    progress = plugin._ProgressLogger("job-1")
    progress.info("[%s] 100%% (10/10)", analysis_prefix)
    for completed, prefix in enumerate(retry_prefixes, 1):
        progress.info("[%s] %d%% (%d/4)", prefix, completed * 25, completed)
        assert persist.call_count == (3 if completed == 4 else 2)

    assert [call.kwargs["phase"] for call in persist.call_args_list] == [
        "combined_analysis" if phase == "combined_retry" else "throughput_analysis",
        phase, phase,
    ]
    assert [call.kwargs["progress_completed"] for call in persist.call_args_list] == [10, 1, 4]
    assert system_logger.info.call_count == 5


def test_progress_failure_is_throttled_without_interrupting_logs(monkeypatch):
    system_logger = Mock()
    persist = Mock(side_effect=OSError("disk unavailable"))
    monkeypatch.setattr(plugin, "LOGGER", system_logger)
    monkeypatch.setattr(plugin, "_update_status", persist)
    monkeypatch.setattr(plugin, "time", SimpleNamespace(monotonic=lambda: 0.0))
    progress = plugin._ProgressLogger("job-1")
    progress.info("[Analyze] Starting")
    progress.info("[Analyze Media] no progress counters")
    progress.info("[Analyze Media] 10% (1/10)")
    progress.info("[Analyze Media] 20% (2/10)")
    progress.info("[Analyze Throughput] 10% (1/10)")
    assert persist.call_count == 2
    assert system_logger.info.call_count == 5


@pytest.mark.parametrize("canceled", [False, True])
def test_background_terminal_status_bypasses_progress_throttle(tmp_path, monkeypatch, canceled):
    django = types.ModuleType("django")
    django_db = types.ModuleType("django.db")
    django_db.close_old_connections = Mock()
    django.db = django_db
    monkeypatch.setitem(sys.modules, "django", django)
    monkeypatch.setitem(sys.modules, "django.db", django_db)
    monkeypatch.setattr(plugin, "STATUS_PATH", str(tmp_path / "status.json"))
    monkeypatch.setattr(plugin, "time", SimpleNamespace(monotonic=lambda: 0.0))
    monkeypatch.setattr(plugin, "LOGGER", Mock())
    for name in ("_notify", "_release_job_lock", "_finish_schedule_job"):
        monkeypatch.setattr(plugin, name, Mock())
    result = {"streams_analyzed": 10, "status_counts": {"alive": 10}, "health_summary": "alive=10"}

    def analyze(_settings, *, logger):
        logger.info("[Analyze Media] 10% (1/10)")
        logger.info("[Analyze Media] 20% (2/10)")
        assert plugin._load_status()["progress_completed"] == 1
        if canceled:
            raise execution_control.AnalysisCancelled("stopped", result=result)
        return dict(result)

    monkeypatch.setattr(plugin, "analyze_assigned_streams", analyze)
    plugin._save_status({"job_id": "job-1", "status": "running"})
    sort = Mock()
    monkeypatch.setattr(plugin, "sort_channels", sort)
    plugin._background_analyze_job({}, None, sort_after=canceled, job_id="job-1")
    status = plugin._load_status()
    assert status["status"] == ("canceled" if canceled else "completed")
    assert status["phase"] == ("canceled" if canceled else "complete")
    assert status["finished_at"]
    assert status["result"]["streams_analyzed"] == 10
    assert django_db.close_old_connections.call_count == 2
    plugin._release_job_lock.assert_called_once_with(None)
    sort.assert_not_called()


@pytest.mark.parametrize("canceled", [False, True])
def test_startup_imports_and_completed_probe_flush_once_at_scan_end(tmp_path, monkeypatch, canceled):
    for name in (
        "ANALYSIS_EXECUTION_LOCK_PATH", "ANALYSIS_EXECUTION_STATE_PATH",
        "ANALYSIS_CANCEL_PATH", "ANALYSIS_CONTROL_LOCK_PATH",
    ):
        monkeypatch.setattr(execution_control, name, str(tmp_path / name))
    monkeypatch.setattr(execution_control, "_active_token", None)

    class QuerySet(list):
        def select_related(self, *_args):
            return self

        def order_by(self, *_args):
            return self

    stream = SimpleNamespace(
        id=42, name="Stream 42", url="http://example.test/42",
        m3u_account=None, m3u_account_id=None,
    )
    models = types.ModuleType("apps.channels.models")
    models.ChannelStream = SimpleNamespace(objects=QuerySet([SimpleNamespace(channel_id=1, stream=stream)]))
    for name in ("apps", "apps.channels"):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    monkeypatch.setitem(sys.modules, "apps.channels.models", models)
    monkeypatch.setattr("stream_sorter.sorter.resolve_channel_scope", lambda _settings: (None, {}))
    monkeypatch.setattr(incremental, "load_reliability_cache", lambda _path: {})

    # Exercise all three startup import branches while isolating their evidence sources.
    def import_evidence(marker):
        def apply(_items, cache, *_args, **_kwargs):
            cache.setdefault("42", {})[marker] = True
            return (1, set()) if marker == "metadata_import" else 1
        return apply

    monkeypatch.setattr(incremental, "_sync_runtime_playback_evidence", import_evidence("playback_import"))
    monkeypatch.setattr(incremental, "_migrate_legacy_throughput", import_evidence("legacy_import"))
    monkeypatch.setattr(incremental, "_sync_dispatcharr_metadata", import_evidence("metadata_import"))
    monkeypatch.setattr(incremental, "content_check_reason", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(incremental, "throughput_check_reason", lambda *_args, **_kwargs: None)
    capacity = SimpleNamespace(try_acquire=Mock(return_value=(True, "reservation-42")), release=Mock())
    monkeypatch.setattr(incremental, "build_capacity_manager", lambda *_args, **_kwargs: capacity)
    save = Mock(wraps=analyzer.save_analysis_cache)
    monkeypatch.setattr(analyzer, "save_analysis_cache", save)
    persist_result = Mock()
    monkeypatch.setattr(analyzer, "_persist_dispatcharr_result", persist_result)
    tested_at = datetime.now(timezone.utc).isoformat()

    def probe(*_args, **_kwargs):
        # Imports stay in memory until completed work is ready to flush.
        save.assert_not_called()
        if canceled:
            assert execution_control.request_analysis_cancel()["status"] == "ok"
        return {
            "status": "alive", "tested_at": tested_at, "error_type": None,
            "stats": {"resolution": "1920x1080", "video_bitrate": 4000}, "details": {},
        }

    monkeypatch.setattr(analyzer, "analyze_stream", probe)
    logger = Mock()
    cache_path = tmp_path / "analysis.json"

    def run():
        return incremental.analyze_assigned_streams(
            {"analysis_workers": 1, "analysis_per_account_delay_seconds": 0},
            logger=logger, cache_path=str(cache_path),
        )

    if canceled:
        with pytest.raises(execution_control.AnalysisCancelled) as exc:
            run()
        result = exc.value.result
    else:
        result = run()

    save.assert_called_once()
    persisted = json.loads(cache_path.read_text())
    entry = persisted["42"]
    assert all(entry[marker] for marker in ("playback_import", "legacy_import", "metadata_import"))
    assert entry["status"] == "alive"
    assert entry["ffprobe_checked_at"] == tested_at
    assert result["media_checked"] == 1
    assert result["playback_health_refreshed"] == result["dispatcharr_metadata_refreshed"] == 1
    capacity.release.assert_called_once_with("reservation-42")
    persist_result.assert_called_once()
    messages = [call.args[0] for call in logger.info.call_args_list]
    assert any("Imported Dispatcharr playback" in message for message in messages)
    assert any("Migrated %d matching legacy" in message for message in messages)
    assert any("Refreshed basic metadata" in message for message in messages)
    assert not Path(execution_control.ANALYSIS_EXECUTION_STATE_PATH).exists()
