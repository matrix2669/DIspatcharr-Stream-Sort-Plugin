import json
from contextlib import contextmanager

from stream_sorter import plugin
from stream_sorter.execution_control import AnalysisAlreadyRunning


def _configure_paths(tmp_path, monkeypatch):
    paths = {
        "ANALYSIS_CACHE_PATH": tmp_path / "analysis.json",
        "LEGACY_CACHE_PATH": tmp_path / "legacy-throughput.json",
        "ANALYSIS_HEALTH_REPORT_PATH": tmp_path / "health-report.json",
        "TTL_RECOMMENDATION_PATH": tmp_path / "ttl.json",
        "STATUS_PATH": tmp_path / "status.json",
        "RELIABILITY_PATH": tmp_path / "reliability.json",
    }
    for name, path in paths.items():
        monkeypatch.setattr(plugin, name, str(path))
        path.write_text('{"data": true}\n', encoding="utf-8")

    @contextmanager
    def available_lease():
        yield

    monkeypatch.setattr(plugin, "analysis_maintenance_execution", available_lease)
    return paths


def test_reset_statistics_scan_only_preserves_runtime_history(tmp_path, monkeypatch):
    paths = _configure_paths(tmp_path, monkeypatch)

    result = plugin._run_reset_statistics_action({"reset_statistics_include_history": False})

    assert result["status"] == "ok"
    assert result["reset_scope"] == "scan_only"
    assert paths["RELIABILITY_PATH"].exists()
    for name, path in paths.items():
        if name != "RELIABILITY_PATH":
            assert not path.exists()


def test_reset_statistics_all_history_clears_runtime_history(tmp_path, monkeypatch):
    paths = _configure_paths(tmp_path, monkeypatch)

    result = plugin._run_reset_statistics_action({"reset_statistics_include_history": True})

    assert result["status"] == "ok"
    assert result["reset_scope"] == "all_history"
    reliability = json.loads(paths["RELIABILITY_PATH"].read_text(encoding="utf-8"))
    assert reliability["streams"] == {}
    assert reliability["channels"] == {}


def test_reset_statistics_refuses_active_scan(tmp_path, monkeypatch):
    paths = _configure_paths(tmp_path, monkeypatch)

    @contextmanager
    def busy_lease():
        raise AnalysisAlreadyRunning("busy")
        yield

    monkeypatch.setattr(plugin, "analysis_maintenance_execution", busy_lease)

    result = plugin._run_reset_statistics_action({"reset_statistics_include_history": True})

    assert result["status"] == "error"
    assert all(path.exists() for path in paths.values())
