from stream_sorter import plugin


class _RecordingLogger:
    def info(self, *_args, **_kwargs):
        pass


def test_progress_logger_persists_media_and_throughput_progress(tmp_path, monkeypatch):
    status_path = tmp_path / "status.json"
    monkeypatch.setattr(plugin, "STATUS_PATH", str(status_path))
    monkeypatch.setattr(plugin, "LOGGER", _RecordingLogger())
    plugin._save_status({"job_id": "job-1", "status": "running"})

    logger = plugin._ProgressLogger("job-1")
    logger.info("[Analyze Media] %d%% (%d/%d) stream=%s", 25, 1, 4, 101)
    media = plugin._load_status()
    assert media["phase"] == "media_analysis"
    assert (media["progress_percent"], media["progress_completed"], media["progress_total"]) == (25, 1, 4)

    logger.info("[Analyze Throughput] %d%% (%d/%d) stream=%s", 50, 2, 4, 102)
    throughput = plugin._load_status()
    assert throughput["phase"] == "throughput_analysis"
    assert (throughput["progress_percent"], throughput["progress_completed"], throughput["progress_total"]) == (50, 2, 4)


def test_analysis_status_reports_running_progress_without_overwriting_api_status(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin, "STATUS_PATH", str(tmp_path / "status.json"))
    monkeypatch.setattr(plugin, "_job_is_running", lambda: True)
    plugin._save_status(
        {
            "job_id": "job-1",
            "status": "running",
            "phase": "media_analysis",
            "parallel_tests": 5,
            "progress_percent": 40,
            "progress_completed": 4,
            "progress_total": 10,
        }
    )

    result = plugin._analysis_status()
    assert result["status"] == "ok"
    assert result["job_status"] == "running"
    assert result["running"] is True
    assert "40% (4/10)" in result["message"]
    assert "Up to 5 tests run concurrently" in result["message"]


def test_analysis_status_marks_stale_running_job_interrupted(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin, "STATUS_PATH", str(tmp_path / "status.json"))
    monkeypatch.setattr(plugin, "_job_is_running", lambda: False)
    plugin._save_status({"job_id": "job-1", "status": "running", "phase": "media_analysis"})

    result = plugin._analysis_status()
    assert result["status"] == "ok"
    assert result["job_status"] == "interrupted"
    assert plugin._load_status()["status"] == "interrupted"


def test_analysis_status_summarizes_completed_result(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin, "STATUS_PATH", str(tmp_path / "status.json"))
    monkeypatch.setattr(plugin, "_job_is_running", lambda: False)
    plugin._save_status(
        {
            "job_id": "job-1",
            "status": "completed",
            "phase": "complete",
            "result": {
                "streams_analyzed": 20,
                "media_checked": 8,
                "throughput_checked": 6,
                "playback_health_refreshed": 4,
            },
        }
    )

    result = plugin._analysis_status()
    assert result["job_status"] == "completed"
    assert "20 streams" in result["message"]
    assert "8 media checks" in result["message"]
