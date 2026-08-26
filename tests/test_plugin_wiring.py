from pathlib import Path
import sys
import types
from types import SimpleNamespace

import pytest

import stream_sorter.incremental as incremental
import stream_sorter.plugin as plugin


def test_plugin_uses_incremental_analyzer_directly():
    assert plugin.analyze_assigned_streams is incremental.analyze_assigned_streams

    source = (Path(__file__).parents[1] / "stream_sorter" / "plugin.py").read_text()
    assert "from .incremental import" in source
    assert "analyze_assigned_streams" in source
    assert "from .analyzer import ANALYSIS_CACHE_PATH, analyze_assigned_streams" not in source


def test_manual_runtime_collector_action_is_an_explicit_noop(monkeypatch):
    monkeypatch.setattr(plugin, "_start_scheduler", lambda: None)
    instance = plugin.Plugin()
    result = instance.run("record_runtime_event", {}, {"settings": {}})
    assert result["recorded"] is False
    assert result["counted"] is False
    assert "automatic" in result["message"].lower()


@pytest.mark.parametrize("sort_after", [False, True])
def test_background_analysis_completion_logs_total_runtime_for_manual_and_scheduled_path(monkeypatch, sort_after):
    django_module = types.ModuleType("django")
    django_db_module = types.ModuleType("django.db")
    django_db_module.close_old_connections = lambda: None
    django_module.db = django_db_module
    monkeypatch.setitem(sys.modules, "django", django_module)
    monkeypatch.setitem(sys.modules, "django.db", django_db_module)

    result = {
        "streams_analyzed": 10,
        "status_counts": {"alive": 8, "dead": 2},
        "health_summary": "alive=8 dead=2 (placeholder=1 other_dead=1) skipped=0 unknown=0",
    }
    monkeypatch.setattr(plugin, "analyze_assigned_streams", lambda *_args, **_kwargs: dict(result))
    monkeypatch.setattr(
        plugin,
        "sort_channels",
        lambda *_args, **_kwargs: {"channels_changed": 2, "rows_changed": 3},
    )
    monkeypatch.setattr(plugin, "_update_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(plugin, "_finish_schedule_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(plugin, "_notify", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(plugin, "_release_job_lock", lambda *_args, **_kwargs: None)
    ticks = iter([100.0, 112.0])
    monkeypatch.setattr(plugin.time, "monotonic", lambda: next(ticks))
    messages = []
    monkeypatch.setattr(
        plugin,
        "LOGGER",
        SimpleNamespace(
            info=lambda *args, **_kwargs: messages.append(args),
            warning=lambda *_args, **_kwargs: None,
            exception=lambda *_args, **_kwargs: None,
        ),
    )

    plugin._background_analyze_job(
        {},
        None,
        sort_after=sort_after,
        job_id="job-1",
        schedule_generation=7,
    )

    completion = next(args for args in messages if "complete analyzed=%s" in args[0])
    assert "health %s runtime=%s" in completion[0]
    assert completion[-1] == "12s"
