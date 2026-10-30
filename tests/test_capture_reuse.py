import io
import json
import logging
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from stream_sorter import analyzer, incremental, throughput


class Clock:
    def __init__(self): self.now = 0.0
    def __call__(self): return self.now


class Response:
    def __init__(self, clock, *, chunk=b'x' * 65536, eof=False):
        self.clock, self.chunk, self.eof = clock, chunk, eof
        self.closed = False
        self.headers = {}
        self.reads = 0
    def read1(self, size):
        self.clock.now += 0.25
        self.reads += 1
        return b'' if self.eof else self.chunk[:size]
    def close(self): self.closed = True


@pytest.mark.parametrize('failure', ['limit', 'eof', 'cancel', 'read_error'])
def test_bounded_capture_never_retains_incomplete_file(tmp_path, monkeypatch, failure):
    clock = Clock()
    response = Response(clock, eof=failure == 'eof')
    if failure == 'read_error':
        response.read1 = Mock(side_effect=TimeoutError())
    monkeypatch.setattr(throughput, 'time', SimpleNamespace(monotonic=clock))
    opened = Mock(return_value=response)
    monkeypatch.setattr(throughput.urllib.request, 'urlopen', opened)
    measured, path = throughput.capture_bounded_sample('http://fake.test/live',
        duration_seconds=6, timeout_seconds=8, temp_directory=str(tmp_path),
        max_bytes=65536, stop_requested=lambda: failure == 'cancel')
    assert path is None
    assert measured['complete'] is False
    assert 'measured_mbps' not in measured
    assert measured['bytes'] <= 65536
    if failure == 'cancel':
        opened.assert_not_called()
    else:
        assert response.closed
    assert list(tmp_path.iterdir()) == []


def test_bounded_capture_counts_live_bytes_wall_time_and_profile_headers(tmp_path, monkeypatch):
    clock = Clock()
    response = Response(clock)
    requests = []
    def opened(request, **kwargs):
        requests.append(request)
        assert kwargs['timeout'] <= 2
        return response
    monkeypatch.setattr(throughput, 'time', SimpleNamespace(monotonic=clock))
    monkeypatch.setattr(throughput.urllib.request, 'urlopen', opened)
    measured, path = throughput.capture_bounded_sample(
        'http://fake.test/native-rewrite|User-Agent=embedded-agent&Referer=approved',
        duration_seconds=2, timeout_seconds=8, temp_directory=str(tmp_path),
        user_agent='profile-agent', max_bytes=1024 * 1024)
    try:
        assert measured['complete'] is True
        assert measured['bytes'] == Path(path).stat().st_size == 8 * 65536
        assert measured['elapsed_seconds'] == 2
        assert measured['measured_mbps'] == 8 * 65536 * 8 / 2 / 1_000_000
        assert requests[0].get_header('User-agent') == 'embedded-agent'
        assert requests[0].get_header('Referer') == 'approved'
        assert requests[0].full_url == 'http://fake.test/native-rewrite'
    finally:
        Path(path).unlink()


@pytest.fixture
def media_fixture(tmp_path):
    ffmpeg = shutil.which('ffmpeg')
    ffprobe = shutil.which('ffprobe')
    if not ffmpeg or not ffprobe:
        if os.environ.get('REQUIRE_MEDIA_TESTS') == '1':
            pytest.fail('CI requires FFmpeg and FFprobe media equivalence tests')
        pytest.skip('Controlled media equivalence requires FFmpeg/FFprobe')
    sample = tmp_path / 'controlled.ts'
    subprocess.run([ffmpeg, '-hide_banner', '-loglevel', 'error', '-f', 'lavfi',
        '-i', 'testsrc2=size=640x360:rate=25', '-t', '10', '-c:v', 'mpeg2video',
        '-b:v', '2000k', '-minrate', '2000k', '-maxrate', '2000k', '-bufsize', '2000k',
        '-f', 'mpegts', str(sample)], check=True, timeout=30)
    return sample, {'analysis_ffmpeg_path': ffmpeg, 'analysis_ffprobe_path': ffprobe,
        'analysis_duration_seconds': 2, 'content_sample_seconds': 2,
        'silent_audio_detection': False, 'placeholder_file_detection': False}


def test_controlled_local_metadata_bitrate_fps_content_and_placeholder(media_fixture):
    sample, settings = media_fixture
    logger = logging.getLogger(__name__)
    direct = analyzer.analyze_stream(str(sample), stream_id=42, stream_name='controlled',
                                    settings=settings, logger=logger, include_content=False)
    local = analyzer.analyze_stream(str(sample), stream_id=42, stream_name='controlled',
                                   settings=settings, logger=logger, include_content=False, local_source=True)
    assert direct['status'] == local['status'] == 'alive'
    assert local['details']['sample_duration_seconds'] >= 6
    for key in ('video_codec', 'height', 'width', 'resolution', 'source_fps', 'video_bitrate'):
        assert direct['stats'][key] == local['stats'][key]
    assert local['stats']['video_bitrate'] >= 500
    assert 'stream_type' not in local['stats']
    checked = analyzer.apply_content_analysis(local, str(sample), settings=settings,
                    logger=logger, local_source=True)
    assert checked['status'] == 'alive'
    assert checked['details']['content']['measured'] is True
    placeholder = analyzer.analyze_stream(str(sample), stream_id=42, stream_name='finite',
        settings={**settings, 'placeholder_file_detection': True}, logger=logger, include_content=False)
    assert placeholder['error_type'] == 'placeholder_file'
    floored = analyzer.analyze_stream(str(sample), stream_id=42, stream_name='floor',
        settings={**settings, 'minimum_video_bitrate_kbps': 100000}, logger=logger, include_content=False, local_source=True)
    assert floored['error_type'] == 'low_bitrate'


def test_finite_http_object_keeps_direct_placeholder_gate(tmp_path, monkeypatch):
    response = Response(Clock())
    response.headers = {'Content-Length': '12345678'}
    monkeypatch.setattr(throughput.urllib.request, 'urlopen', Mock(return_value=response))
    result, path = throughput.capture_bounded_sample('http://fake.test/fixed.ts',
        duration_seconds=6, timeout_seconds=8, temp_directory=str(tmp_path))
    assert path is None and result['complete'] is False
    assert response.reads == 0 and response.closed
    assert list(tmp_path.iterdir()) == []


def test_short_clean_decode_cannot_refresh_content_ttl(media_fixture):
    sample, settings = media_fixture
    ffmpeg = settings['analysis_ffmpeg_path']
    short = sample.with_name('short.ts')
    subprocess.run([ffmpeg, '-hide_banner', '-loglevel', 'error', '-i', str(sample),
        '-t', '0.4', '-c', 'copy', '-f', 'mpegts', str(short)], check=True, timeout=10)
    result = analyzer.apply_content_analysis({'status': 'alive', 'stats': {}, 'details': {}},
        str(short), settings=settings, local_source=True, logger=logging.getLogger(__name__))
    assert result['details']['content']['measured'] is False
    assert result['details']['content']['error_type'] == 'content_sample_incomplete'
