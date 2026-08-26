import sys
import types
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from stream_sorter import analyzer, sorter


def test_human_runtime_format():
    assert analyzer._format_eta(5.9) == "5s"
    assert analyzer._format_eta(65) == "1m 5s"
    assert analyzer._format_eta(3665) == "1h 1m"


@pytest.mark.parametrize("apply,mode", [(False, "dry-run"), (True, "apply")])
def test_sort_completion_logs_total_runtime(tmp_path, monkeypatch, apply, mode):
    django_module = types.ModuleType("django")
    django_db_module = types.ModuleType("django.db")
    django_db_module.transaction = SimpleNamespace(atomic=lambda: nullcontext())
    models_module = types.ModuleType("apps.channels.models")
    models_module.ChannelStream = SimpleNamespace(objects=SimpleNamespace(bulk_update=lambda *_args, **_kwargs: None))
    monkeypatch.setitem(sys.modules, "django", django_module)
    monkeypatch.setitem(sys.modules, "django.db", django_db_module)
    monkeypatch.setitem(sys.modules, "apps", types.ModuleType("apps"))
    monkeypatch.setitem(sys.modules, "apps.channels", types.ModuleType("apps.channels"))
    monkeypatch.setitem(sys.modules, "apps.channels.models", models_module)

    monkeypatch.setattr(sorter, "resolve_channel_scope", lambda _settings: (None, {"match_mode": "all", "selected_channel_count": 0}))
    monkeypatch.setattr(sorter, "load_cache", lambda _path: {})
    monkeypatch.setattr(sorter, "load_reliability_cache", lambda _path: {})
    monkeypatch.setattr(sorter, "_load_channel_candidates", lambda *_args: {})
    monkeypatch.setattr(sorter, "_write_json_atomic", lambda *_args: None)
    ticks = iter([10.0, 15.0])
    monkeypatch.setattr(sorter.time, "monotonic", lambda: next(ticks))
    messages = []
    logger = SimpleNamespace(info=lambda *args, **_kwargs: messages.append(args))

    result = sorter.sort_channels(
        {},
        apply=apply,
        logger=logger,
        report_path=str(tmp_path / "report.json"),
        cache_path=str(tmp_path / "cache.json"),
    )

    assert result["total_runtime_seconds"] == 5.0
    assert result["total_runtime"] == "5s"
    assert messages[-1][1] == mode
    assert messages[-1][-1] == "5s"
    assert "runtime=%s" in messages[-1][0]
