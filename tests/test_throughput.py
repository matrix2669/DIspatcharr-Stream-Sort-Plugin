import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from stream_sorter.throughput import _split_url_headers, capture_stream_sample, load_cache, probe_stream, save_cache


def test_split_m3u_url_headers():
    url, headers = _split_url_headers(
        "http://example.test/live|User-Agent=VLC%2F3.0&Referer=http%3A%2F%2Ffoo.test%2F"
    )
    assert url == "http://example.test/live"
    assert headers["User-Agent"] == "VLC/3.0"
    assert headers["Referer"] == "http://foo.test/"


def test_cache_round_trip(tmp_path: Path):
    path = tmp_path / "cache.json"
    payload = {"42": {"status": "healthy", "measured_mbps": 12.3}}
    save_cache(payload, str(path))
    assert load_cache(str(path)) == payload
    assert json.loads(path.read_text()) == payload


def test_unified_cache_extracts_nested_throughput_with_freshness_timestamp(tmp_path: Path):
    path = tmp_path / "analysis.json"
    checked_at = datetime.now(timezone.utc).isoformat()
    path.write_text(
        json.dumps(
            {
                "42": {
                    "status": "alive",
                    "stats": {"height": 1080},
                    "throughput": {
                        "status": "healthy",
                        "measured_mbps": 12.3,
                        "checked_at": checked_at,
                    },
                }
            }
        )
    )
    loaded = load_cache(str(path))
    assert loaded["42"]["status"] == "healthy"
    assert loaded["42"]["measured_mbps"] == 12.3
    assert loaded["42"]["checked_at"] == checked_at


def test_unified_cache_does_not_apply_stored_expiration(tmp_path: Path):
    path = tmp_path / "analysis.json"
    expired = datetime.now(timezone.utc) - timedelta(seconds=1)
    path.write_text(
        json.dumps(
            {
                "42": {
                    "status": "alive",
                    "throughput": {
                        "status": "healthy",
                        "measured_mbps": 12.3,
                        "expires_at": expired.isoformat(),
                    },
                }
            }
        )
    )
    loaded = load_cache(str(path))["42"]
    assert loaded["status"] == "healthy"
    assert "expires_at" not in loaded


def test_corrupt_cache_returns_empty(tmp_path: Path):
    path = tmp_path / "cache.json"
    path.write_text("not-json")
    assert load_cache(str(path)) == {}


def test_missing_url_probe_is_unknown_not_dead():
    result = probe_stream("", nominal_video_kbps=6000)
    assert result["status"] == "unknown"
    assert "error" in result


def test_combined_capture_uses_wall_clock_not_media_duration(monkeypatch, tmp_path):
    commands = []

    class FakeProcess:
        returncode = 255

        def __init__(self, command):
            commands.append(command)
            Path(command[-1]).write_bytes(b"x" * 2_000_000)
            self.calls = 0

        def communicate(self, timeout=None):
            self.calls += 1
            if self.calls == 1:
                raise __import__("subprocess").TimeoutExpired(commands[0], timeout)
            return "", ""

        def send_signal(self, _signal):
            return None

    monkeypatch.setattr("stream_sorter.throughput.subprocess.Popen", lambda command, **_kwargs: FakeProcess(command))
    moments = iter((100.0, 108.0))
    monkeypatch.setattr("stream_sorter.throughput.time.monotonic", lambda: next(moments))
    result, sample_path = capture_stream_sample(
        "http://example.test/live.ts",
        nominal_video_kbps=1000,
        duration_seconds=8.0,
        temp_directory=str(tmp_path),
    )

    assert sample_path is not None
    assert result["measurement_source"] == "ffmpeg_stream_copy"
    assert result["elapsed_seconds"] == 8.0
    assert result["measured_mbps"] == 2.0
    assert "-t" not in commands[0]
    assert Path(sample_path).parent == tmp_path
    Path(sample_path).unlink()


def test_combined_capture_rejects_early_successful_exit(monkeypatch):
    class ShortProcess:
        returncode = 0

        def __init__(self, command):
            Path(command[-1]).write_bytes(b"x" * 1000)

        def communicate(self, timeout=None):
            return "", ""

    monkeypatch.setattr("stream_sorter.throughput.subprocess.Popen", lambda command, **_kwargs: ShortProcess(command))
    result, sample_path = capture_stream_sample(
        "http://example.test/live.ts",
        nominal_video_kbps=1000,
        duration_seconds=8.0,
    )

    assert sample_path is None
    assert result["status"] == "unknown"
    assert "expected at least" in result["error"]


def test_combined_capture_reports_temporary_directory_permission_error(monkeypatch):
    def denied(*_args, **_kwargs):
        raise PermissionError(13, "Permission denied", "/dev/shm/stream-sorter")

    monkeypatch.setattr("stream_sorter.throughput.tempfile.mkstemp", denied)
    result, sample_path = capture_stream_sample(
        "http://example.test/live.ts",
        nominal_video_kbps=1000,
        temp_directory="/dev/shm/stream-sorter",
    )
    assert sample_path is None
    assert result["status"] == "unknown"
    assert result["error_type"] == "stream_unreachable"
    assert "PermissionError" in result["error"]
