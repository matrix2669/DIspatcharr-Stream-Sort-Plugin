import json
import subprocess

from stream_sorter import analyzer


def _probe_payload(*, duration=None, include_video=True, include_audio=True, packet_count=30):
    streams = []
    if include_video:
        streams.append(
            {
                "index": 0,
                "codec_type": "video",
                "codec_name": "h264",
                "width": 1920,
                "height": 1080,
                "r_frame_rate": "60000/1001",
                "pix_fmt": "yuv420p",
            }
        )
    if include_audio:
        streams.append(
            {
                "index": 1,
                "codec_type": "audio",
                "codec_name": "aac",
                "sample_rate": "48000",
                "channels": 2,
                "bit_rate": "192000",
            }
        )
    packets = [
        {"stream_index": 0, "size": "12500", "duration_time": "0.0333667"}
        for _ in range(packet_count)
    ]
    fmt = {"format_name": "mpegts"}
    if duration is not None:
        fmt["duration"] = str(duration)
    return {"streams": streams, "packets": packets, "format": fmt}


def _settings(**overrides):
    base = {
        "black_screen_detection": False,
        "frozen_video_detection": False,
        "silent_audio_detection": False,
        "placeholder_file_detection": True,
        "analysis_duration_seconds": 5,
        "analysis_connection_timeout_seconds": 10,
        "analysis_probe_timeout_seconds": 20,
    }
    base.update(overrides)
    return base


def test_analyze_stream_alive_calculates_packet_bitrate(monkeypatch):
    payload = _probe_payload(packet_count=30)

    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args[0], 0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(analyzer.subprocess, "run", fake_run)
    result = analyzer.analyze_stream(
        "http://example.test/live.ts",
        stream_id=7,
        stream_name="Example",
        settings=_settings(),
    )

    assert result["status"] == "alive"
    assert result["stats"]["resolution"] == "1920x1080"
    assert result["stats"]["source_fps"] == 59.94
    assert result["stats"]["video_bitrate"] > 0
    assert result["details"]["packet_count"] == 30


def test_metadata_only_analysis_does_not_run_content_ffmpeg(monkeypatch):
    payload = _probe_payload(packet_count=30)
    calls = []

    def fake_run(*args, **kwargs):
        calls.append(args[0])
        return subprocess.CompletedProcess(args[0], 0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(analyzer.subprocess, "run", fake_run)
    result = analyzer.analyze_stream(
        "http://example.test/live.ts",
        stream_id=7,
        stream_name="Example",
        settings=_settings(),
        include_content=False,
    )

    assert result["status"] == "alive"
    assert len(calls) == 1
    assert "content" not in result["details"]


def test_zero_dimensions_are_dead_and_retryable(monkeypatch):
    payload = _probe_payload()
    payload["streams"][0]["width"] = 0
    payload["streams"][0]["height"] = 0

    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args[0], 0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(analyzer.subprocess, "run", fake_run)
    result = analyzer.analyze_stream(
        "http://example.test/zero-dimensions.ts",
        stream_id=12,
        stream_name="Zero dimensions",
        settings=_settings(),
    )

    assert result["status"] == "dead"
    assert result["error_type"] == "invalid_video_dimensions"
    assert result["error_type"] in analyzer.RETRYABLE_ERROR_TYPES
    assert result["stats"]["resolution"] == "0x0"


def test_fixed_duration_placeholder_is_dead(monkeypatch):
    payload = _probe_payload(duration=600)

    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args[0], 0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(analyzer.subprocess, "run", fake_run)
    result = analyzer.analyze_stream(
        "http://example.test/placeholder.ts",
        stream_id=8,
        stream_name="Placeholder",
        settings=_settings(),
    )

    assert result["status"] == "dead"
    assert result["error_type"] == "placeholder_file"
    assert result["error_type"] in analyzer.RETRYABLE_ERROR_TYPES
    assert result["stats"]["resolution"] == "1920x1080"


def test_http_429_is_skipped_not_dead(monkeypatch):
    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args[0], 1, stdout="", stderr="HTTP error 429 Too Many Requests")

    monkeypatch.setattr(analyzer.subprocess, "run", fake_run)
    result = analyzer.analyze_stream(
        "http://example.test/live.ts",
        stream_id=9,
        stream_name="Rate limited",
        settings=_settings(),
    )

    assert result["status"] == "skipped"
    assert result["error_type"] == "rate_limited"


def test_audio_only_stream_is_skipped(monkeypatch):
    payload = _probe_payload(include_video=False, include_audio=True, packet_count=0)

    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args[0], 0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(analyzer.subprocess, "run", fake_run)
    result = analyzer.analyze_stream(
        "http://example.test/radio.ts",
        stream_id=10,
        stream_name="Radio",
        settings=_settings(),
    )

    assert result["status"] == "skipped"
    assert result["error_type"] == "no_video_stream"


def test_black_frozen_and_silent_parsers():
    stderr = """
    black_start:0 black_end:4.2 black_duration:4.2
    lavfi.freezedetect.freeze_start: 0
    mean_volume: -91.0 dB
    """
    assert analyzer._parse_blackdetect_output(stderr) == [(0.0, 4.2, 4.2)]
    assert analyzer._parse_freezedetect_output(stderr) == [0.0]
    assert analyzer._parse_mean_volume_db(stderr) == -91.0


def test_content_health_failures_are_retryable():
    assert {
        "placeholder_file",
        "black_screen",
        "frozen_video",
        "silent_audio",
    } <= analyzer.RETRYABLE_ERROR_TYPES


def test_streamlink_host_is_skipped_without_running_ffprobe(monkeypatch):
    def should_not_run(*args, **kwargs):
        raise AssertionError("ffprobe should not run")

    monkeypatch.setattr(analyzer.subprocess, "run", should_not_run)
    result = analyzer.analyze_stream(
        "https://www.youtube.com/watch?v=abc",
        stream_id=11,
        stream_name="YouTube",
        settings=_settings(),
    )
    assert result["status"] == "skipped"
    assert result["error_type"] == "streamlink_only"
def test_content_timeout_is_retryable_dead_result(monkeypatch):
    import logging
    import subprocess

    from stream_sorter import analyzer

    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], 20)

    monkeypatch.setattr(analyzer.subprocess, "run", timeout)
    result = analyzer.apply_content_analysis(
        {"status": "alive", "stats": {}, "details": {}},
        "http://provider.test/live",
        settings={
            "black_screen_detection": True,
            "frozen_video_detection": False,
            "silent_audio_detection": False,
            "content_ffmpeg_timeout_seconds": 20,
        },
        logger=logging.getLogger("test"),
    )

    assert result["status"] == "dead"
    assert result["error_type"] == "timeout"
    assert result["details"]["content"]["measured"] is False


def test_no_applicable_content_detectors_are_completed_without_ffmpeg(monkeypatch):
    from stream_sorter import analyzer

    def should_not_run(*args, **kwargs):
        raise AssertionError("ffmpeg should not run")

    monkeypatch.setattr(analyzer.subprocess, "run", should_not_run)
    result = analyzer.apply_content_analysis(
        {"status": "alive", "stats": {}, "details": {"has_audio": False}},
        "http://provider.test/live",
        settings={
            "black_screen_detection": False,
            "frozen_video_detection": False,
            "silent_audio_detection": True,
        },
    )

    assert result["status"] == "alive"
    assert result["details"]["content"]["measured"] is True
    assert result["details"]["content"]["skip_reason"] == "no_applicable_detectors"
