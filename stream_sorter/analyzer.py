from __future__ import annotations
import collections
import hashlib
import json
import logging
import os
import re
import subprocess
import tempfile
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Mapping
from .scoring import estimate_nominal_throughput_kbps, parse_fps, parse_resolution
from .throughput import DEFAULT_USER_AGENT, load_cache as load_throughput_cache, probe_stream, save_cache as save_throughput_cache
ANALYSIS_CACHE_PATH = '/data/dispatcharr_stream_sort_analysis.json'
MIN_PACKETS_FOR_BITRATE_CALC = 30
DEFAULT_STREAMLINK_HOSTS = 'youtube.com, youtu.be, twitch.tv, kick.com'
RETRYABLE_ERROR_TYPES = {
    'timeout',
    'connection_refused',
    'network_unreachable',
    'stream_unreachable',
    'server_error',
    'invalid_video_dimensions',
    'placeholder_file',
    'black_screen',
    'frozen_video',
    'silent_audio',
}

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def _as_bool(value: Any, default: bool=False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}

def _format_eta(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f'{seconds}s'
    if seconds < 3600:
        return f'{seconds // 60}m {seconds % 60}s'
    return f'{seconds // 3600}h {seconds % 3600 // 60}m'

def _stream_url_hash(raw_url: str) -> str:
    return hashlib.sha256((raw_url or '').encode('utf-8', 'replace')).hexdigest()[:24]

def load_analysis_cache(path: str=ANALYSIS_CACHE_PATH) -> dict[str, dict[str, Any]]:
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return {str(k): v for k, v in data.items() if isinstance(v, dict)}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    return {}

def save_analysis_cache(cache: Mapping[str, Mapping[str, Any]], path: str=ANALYSIS_CACHE_PATH) -> None:
    directory = os.path.dirname(path) or '.'
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix='.stream-sort-analysis-', suffix='.json', dir=directory)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(cache, handle, indent=2, sort_keys=True, default=str)
            handle.write('\n')
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

def _split_url_headers(raw_url: str) -> tuple[str, dict[str, str]]:
    if '|' not in (raw_url or ''):
        return (raw_url or '', {})
    url, raw_headers = raw_url.split('|', 1)
    headers: dict[str, str] = {}
    for pair in raw_headers.split('&'):
        if '=' not in pair:
            continue
        key, value = pair.split('=', 1)
        key = urllib.parse.unquote_plus(key).strip()
        value = urllib.parse.unquote_plus(value).strip()
        if key:
            headers[key] = value
    return (url, headers)

def _input_http_args(extra_headers: Mapping[str, str], fallback_user_agent: str) -> list[str]:
    headers = dict(extra_headers or {})
    ua = headers.pop('User-Agent', None) or headers.pop('user-agent', None) or fallback_user_agent
    args = ['-user_agent', ua or DEFAULT_USER_AGENT]
    if headers:
        header_blob = ''.join((f'{key}: {value}\r\n' for key, value in headers.items()))
        args.extend(['-headers', header_blob])
    return args

def _mask_url(error_message: str, raw_url: str, stream_id: Any) -> str:
    message = str(error_message or '')
    clean_url, _headers = _split_url_headers(raw_url or '')
    for value in (raw_url, clean_url):
        if value:
            message = message.replace(value, f'[Stream ID: {stream_id}]')
    return message

def _parse_rate(value: Any) -> float | None:
    if value in (None, '', 'N/A'):
        return None
    try:
        if isinstance(value, str) and '/' in value:
            num, den = value.split('/', 1)
            den_f = float(den)
            if den_f == 0:
                return None
            return float(num) / den_f
        return float(value)
    except (TypeError, ValueError, ZeroDivisionError):
        return None

def _parse_container_duration(probe_data: Mapping[str, Any]) -> float | None:
    try:
        raw = (probe_data.get('format') or {}).get('duration')
        seconds = float(raw)
    except (AttributeError, TypeError, ValueError):
        return None
    if seconds != seconds or seconds <= 0:
        return None
    return seconds

def _parse_container_bitrate_kbps(probe_data: Mapping[str, Any]) -> int | None:
    try:
        raw = (probe_data.get('format') or {}).get('bit_rate')
        return int(round(float(raw) / 1000.0))
    except (AttributeError, TypeError, ValueError):
        return None

def _parse_blackdetect_output(stderr: str) -> list[tuple[float, float, float]]:
    pattern = re.compile('black_start:(?P<start>[\\d.]+)\\s+black_end:(?P<end>[\\d.]+)\\s+black_duration:(?P<dur>[\\d.]+)')
    segments: list[tuple[float, float, float]] = []
    for match in pattern.finditer(stderr or ''):
        try:
            segments.append((float(match.group('start')), float(match.group('end')), float(match.group('dur'))))
        except (TypeError, ValueError):
            pass
    return segments

def _parse_freezedetect_output(stderr: str) -> list[float]:
    starts: list[float] = []
    for match in re.finditer('lavfi\\.freezedetect\\.freeze_start:\\s*(?P<start>[\\d.]+)', stderr or ''):
        try:
            starts.append(float(match.group('start')))
        except (TypeError, ValueError):
            pass
    return starts

def _parse_mean_volume_db(stderr: str) -> float | None:
    match = re.search('mean_volume:\\s*(-?(?:inf|[\\d.]+))\\s*dB', stderr or '')
    if not match:
        return None
    try:
        return float(match.group(1))
    except (TypeError, ValueError):
        return None

def _effective_freeze_seconds(min_seconds: Any, sample_seconds: Any) -> int:
    wanted = _as_int(min_seconds, 4)
    sample = max(2, _as_int(sample_seconds, 6))
    return max(1, min(wanted, sample - 1))

def _streamlink_only_url(raw_url: str, settings: Mapping[str, Any]) -> bool:
    clean_url, _headers = _split_url_headers(raw_url or '')
    try:
        host = (urllib.parse.urlparse(clean_url).hostname or '').lower()
    except Exception:
        return False
    raw_hosts = str(settings.get('analysis_streamlink_hosts') or DEFAULT_STREAMLINK_HOSTS)
    suffixes = [item.strip().lower().lstrip('.') for item in raw_hosts.split(',') if item.strip()]
    return any((host == suffix or host.endswith('.' + suffix) for suffix in suffixes))

def _classify_error(stderr: str, returncode: int | None=None) -> str:
    text = str(stderr or '')
    lower = text.lower()
    if 'timed out' in lower or 'timeout' in lower or 'connection timeout' in lower:
        return 'timeout'
    if 'option not found' in lower or 'unrecognized option' in lower:
        return 'ffprobe_option_error'
    if '404' in text or ('not found' in lower and 'http' in lower):
        return '404_not_found'
    if '403' in text or 'forbidden' in lower:
        return '403_forbidden'
    if 'too many requests' in lower or 'rate limit' in lower or re.search('\\b429\\b', text):
        return 'rate_limited'
    if '500' in text or 'internal server error' in lower:
        return 'server_error'
    if 'connection refused' in lower:
        return 'connection_refused'
    if 'network unreachable' in lower or 'no route to host' in lower:
        return 'network_unreachable'
    if 'invalid data found' in lower or 'invalid argument' in lower:
        return 'invalid_stream'
    if 'protocol not supported' in lower:
        return 'unsupported_protocol'
    if returncode == 1:
        return 'stream_unreachable'
    return 'other'

def _analyze_content(raw_url: str, *, settings: Mapping[str, Any], user_agent: str, has_audio: bool, logger) -> dict[str, Any]:
    want_black = _as_bool(settings.get('black_screen_detection'), True)
    want_freeze = _as_bool(settings.get('frozen_video_detection'), True)
    want_audio = _as_bool(settings.get('silent_audio_detection'), True) and has_audio
    if not (want_black or want_freeze or want_audio):
        return {'black': None, 'frozen': None, 'mean_volume_db': None, 'measured': False}
    ffmpeg_path = str(settings.get('analysis_ffmpeg_path') or '/usr/local/bin/ffmpeg')
    sample_seconds = max(2, _as_int(settings.get('content_sample_seconds'), 6))
    min_black = max(1, _as_int(settings.get('black_screen_min_seconds'), 3))
    freeze_seconds = _effective_freeze_seconds(settings.get('frozen_video_min_seconds'), sample_seconds)
    ffmpeg_timeout = max(sample_seconds + 5, _as_int(settings.get('content_ffmpeg_timeout_seconds'), 20))
    connection_timeout = max(1, _as_int(settings.get('analysis_connection_timeout_seconds'), 10))
    clean_url, extra_headers = _split_url_headers(raw_url)
    video_filters = [f'blackdetect=d={min_black}:pic_th=0.98']
    if want_freeze:
        video_filters.append(f'freezedetect=n=-60dB:d={freeze_seconds}')
    cmd = [ffmpeg_path, '-hide_banner', '-nostats', '-loglevel', 'info']
    cmd.extend(_input_http_args(extra_headers, user_agent))
    cmd.extend(['-rw_timeout', str(connection_timeout * 1000000), '-i', clean_url, '-t', str(sample_seconds)])
    if want_audio:
        cmd.extend(['-af', 'volumedetect'])
    else:
        cmd.append('-an')
    cmd.extend(['-vf', ','.join(video_filters), '-f', 'null', '-'])
    try:
        completed = subprocess.run(cmd, capture_output=True, text=True, timeout=ffmpeg_timeout)
    except FileNotFoundError:
        logger.warning('[Analyze] ffmpeg not found at %s; content checks fail open', ffmpeg_path)
        return {'black': None, 'frozen': None, 'mean_volume_db': None, 'measured': False}
    except subprocess.TimeoutExpired:
        logger.warning('[Analyze] ffmpeg content check timed out after %ss; content checks fail open', ffmpeg_timeout)
        return {'black': None, 'frozen': None, 'mean_volume_db': None, 'measured': False}
    except Exception as exc:
        logger.warning('[Analyze] ffmpeg content check failed (%s); content checks fail open', exc)
        return {'black': None, 'frozen': None, 'mean_volume_db': None, 'measured': False}
    stderr = completed.stderr or ''
    clean_exit = completed.returncode == 0
    return {'black': True if _parse_blackdetect_output(stderr) else False if clean_exit and want_black else None, 'frozen': True if want_freeze and _parse_freezedetect_output(stderr) else False if clean_exit and want_freeze else None, 'mean_volume_db': _parse_mean_volume_db(stderr) if want_audio else None, 'measured': clean_exit}

def analyze_stream(raw_url: str, *, stream_id: Any, stream_name: str, settings: Mapping[str, Any], user_agent: str=DEFAULT_USER_AGENT, logger=None) -> dict[str, Any]:
    logger = logger or logging.getLogger('plugins.stream_sorter')
    tested_at = _utc_now_iso()
    base = {'tested_at': tested_at, 'status': 'dead', 'error_type': 'other', 'error': '', 'stats': {}, 'details': {}}
    if not raw_url:
        return {**base, 'error_type': 'missing_url', 'error': 'Stream has no URL'}
    if _streamlink_only_url(raw_url, settings):
        return {**base, 'status': 'skipped', 'error_type': 'streamlink_only', 'error': 'Streamlink-only host cannot be validated by ffprobe'}
    ffprobe_path = str(settings.get('analysis_ffprobe_path') or '/usr/local/bin/ffprobe')
    connection_timeout = max(1, _as_int(settings.get('analysis_connection_timeout_seconds'), 10))
    probe_timeout = max(1, _as_int(settings.get('analysis_probe_timeout_seconds'), 20))
    analysis_duration = max(1, _as_int(settings.get('analysis_duration_seconds'), 5))
    total_timeout = probe_timeout + analysis_duration + 5
    clean_url, extra_headers = _split_url_headers(raw_url)
    cmd = [ffprobe_path, '-print_format', 'json']
    cmd.extend(_input_http_args(extra_headers, user_agent))
    cmd.extend(['-timeout', str(connection_timeout * 1000000), '-analyzeduration', str(probe_timeout * 1000000), '-probesize', '10000000', '-show_streams', '-show_packets', '-show_format', '-read_intervals', f'%+{analysis_duration}', clean_url])
    started = time.monotonic()
    try:
        completed = subprocess.run(cmd, capture_output=True, text=True, timeout=total_timeout)
    except subprocess.TimeoutExpired:
        return {**base, 'error_type': 'timeout', 'error': f'Connection timeout after {total_timeout} seconds', 'details': {'probe_elapsed_seconds': round(time.monotonic() - started, 3)}}
    except FileNotFoundError:
        return {**base, 'error_type': 'ffprobe_missing', 'error': f'ffprobe not found at {ffprobe_path}'}
    except Exception as exc:
        return {**base, 'error_type': 'other', 'error': str(exc)}
    elapsed = max(time.monotonic() - started, 0.001)
    if completed.returncode != 0:
        error = (completed.stderr or '').strip() or 'Stream not accessible'
        error_type = _classify_error(error, completed.returncode)
        status = 'skipped' if error_type == 'rate_limited' else 'dead'
        return {**base, 'status': status, 'error_type': error_type, 'error': _mask_url(error, raw_url, stream_id), 'details': {'probe_elapsed_seconds': round(elapsed, 3)}}
    try:
        probe_data = json.loads(completed.stdout or '{}')
    except json.JSONDecodeError as exc:
        return {**base, 'error_type': 'invalid_probe_json', 'error': f'ffprobe returned invalid JSON: {exc}', 'details': {'probe_elapsed_seconds': round(elapsed, 3)}}
    streams = probe_data.get('streams') or []
    video_stream = next((row for row in streams if row.get('codec_type') == 'video'), None)
    audio_stream = next((row for row in streams if row.get('codec_type') == 'audio'), None)
    if video_stream is None:
        return {**base, 'status': 'skipped', 'error_type': 'no_video_stream', 'error': 'No video stream found', 'details': {'probe_elapsed_seconds': round(elapsed, 3)}}
    width = _as_int(video_stream.get('width'), 0)
    height = _as_int(video_stream.get('height'), 0)
    fps = _parse_rate(video_stream.get('r_frame_rate') or video_stream.get('avg_frame_rate'))
    video_bitrate: float | None = None
    for raw_bitrate in (video_stream.get('bit_rate'), (probe_data.get('format') or {}).get('bit_rate')):
        try:
            if raw_bitrate not in (None, '', 'N/A'):
                video_bitrate = float(raw_bitrate) / 1000.0
                break
        except (TypeError, ValueError):
            pass
    packets = probe_data.get('packets') or []
    video_index = video_stream.get('index')
    video_packets = [packet for packet in packets if packet.get('stream_index') == video_index]
    if not video_packets:
        video_packets = packets
    calculated_bitrate = None
    if video_bitrate is None and len(video_packets) >= MIN_PACKETS_FOR_BITRATE_CALC:
        total_size = 0
        total_duration = 0.0
        for packet in video_packets:
            try:
                total_size += int(packet.get('size') or 0)
                total_duration += float(packet.get('duration_time') or 0)
            except (TypeError, ValueError):
                continue
        if total_duration > 0:
            calculated_bitrate = total_size * 8.0 / (total_duration * 1000.0)
            video_bitrate = calculated_bitrate
    if video_bitrate is not None:
        video_bitrate = float(int(round(video_bitrate)))
    audio_codec = None
    sample_rate = None
    audio_channels: str | int | None = None
    audio_bitrate = None
    if audio_stream:
        audio_codec = audio_stream.get('codec_name')
        try:
            sample_rate = int(audio_stream.get('sample_rate')) if audio_stream.get('sample_rate') else None
        except (TypeError, ValueError):
            sample_rate = None
        audio_channels = audio_stream.get('channel_layout') or audio_stream.get('channels')
        if isinstance(audio_channels, int):
            audio_channels = {1: 'mono', 2: 'stereo', 6: '5.1', 8: '7.1'}.get(audio_channels, f'{audio_channels}ch')
        try:
            if audio_stream.get('bit_rate'):
                audio_bitrate = float(audio_stream['bit_rate']) / 1000.0
        except (TypeError, ValueError):
            audio_bitrate = None
    format_name = str((probe_data.get('format') or {}).get('format_name') or '')
    if 'mpegts' in format_name:
        stream_type = 'mpegts'
    elif 'hls' in format_name or 'm3u8' in format_name:
        stream_type = 'hls'
    elif 'flv' in format_name:
        stream_type = 'flv'
    else:
        stream_type = format_name.split(',', 1)[0] if format_name else 'unknown'
    stats = {'video_codec': video_stream.get('codec_name'), 'resolution': f'{width}x{height}' if width and height else '0x0', 'width': width, 'height': height, 'source_fps': round(fps, 3) if fps else None, 'pixel_format': video_stream.get('pix_fmt'), 'video_bitrate': video_bitrate, 'audio_codec': audio_codec, 'sample_rate': sample_rate, 'audio_channels': audio_channels, 'audio_bitrate': audio_bitrate, 'stream_type': stream_type}
    stats = {key: value for key, value in stats.items() if value is not None}
    details: dict[str, Any] = {'probe_elapsed_seconds': round(elapsed, 3), 'packet_count': len(video_packets), 'analysis_duration_seconds': analysis_duration}
    if calculated_bitrate is not None:
        details['calculated_bitrate_kbps'] = round(calculated_bitrate, 1)
    if width <= 0 or height <= 0:
        return {
            **base,
            'status': 'dead',
            'error_type': 'invalid_video_dimensions',
            'error': f'Video stream reported invalid dimensions ({width}x{height})',
            'stats': stats,
            'details': details,
        }
    container_duration = _parse_container_duration(probe_data)
    if container_duration is not None:
        details['container_duration_seconds'] = round(container_duration, 3)
        container_bitrate = _parse_container_bitrate_kbps(probe_data)
        if container_bitrate is not None:
            details['container_bitrate_kbps'] = container_bitrate
        if _as_bool(settings.get('placeholder_file_detection'), True):
            return {**base, 'status': 'dead', 'error_type': 'placeholder_file', 'error': f'Fixed-duration file ({container_duration:.1f}s) instead of a continuous live stream', 'stats': stats, 'details': details}
    content = _analyze_content(raw_url, settings=settings, user_agent=user_agent, has_audio=audio_stream is not None, logger=logger)
    details['content'] = content
    if _as_bool(settings.get('black_screen_detection'), True) and content.get('black') is True:
        return {**base, 'status': 'dead', 'error_type': 'black_screen', 'error': 'Stream decodes to a black screen', 'stats': stats, 'details': details}
    if _as_bool(settings.get('frozen_video_detection'), True) and content.get('frozen') is True:
        return {**base, 'status': 'dead', 'error_type': 'frozen_video', 'error': 'Stream decodes to a frozen (unchanging) picture', 'stats': stats, 'details': details}
    if _as_bool(settings.get('silent_audio_detection'), True) and audio_stream is not None:
        threshold = _as_float(settings.get('silent_audio_max_db'), -70.0)
        mean_db = content.get('mean_volume_db')
        if mean_db is not None and float(mean_db) <= threshold:
            return {**base, 'status': 'dead', 'error_type': 'silent_audio', 'error': f'Audio track is silent (mean {mean_db} dBFS; threshold {threshold} dBFS)', 'stats': stats, 'details': details}
    return {**base, 'status': 'alive', 'error_type': None, 'error': '', 'stats': stats, 'details': details}

class RateLimitGuard:
    WINDOW_SECONDS = 60
    TRIP_THRESHOLD = 5
    BASE_COOLDOWN_SECONDS = 60
    MAX_COOLDOWN_SECONDS = 600
    DECAY_AFTER_SECONDS = 300

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._hit_times: collections.deque[float] = collections.deque()
        self._cooldown_until = 0.0
        self._next_cooldown = self.BASE_COOLDOWN_SECONDS
        self._last_hit_time = 0.0

    def record_hit(self, logger=None) -> None:
        now = time.monotonic()
        with self._lock:
            self._hit_times.append(now)
            self._last_hit_time = now
            cutoff = now - self.WINDOW_SECONDS
            while self._hit_times and self._hit_times[0] < cutoff:
                self._hit_times.popleft()
            if len(self._hit_times) >= self.TRIP_THRESHOLD and now >= self._cooldown_until:
                cooldown = self._next_cooldown
                self._cooldown_until = now + cooldown
                self._next_cooldown = min(self._next_cooldown * 2, self.MAX_COOLDOWN_SECONDS)
                self._hit_times.clear()
                if logger:
                    logger.warning('[Analyze] provider rate-limit guard tripped; pausing new checks for %ss', cooldown)

    def wait_if_throttled(self, stop_event=None) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                if self._last_hit_time and now - self._last_hit_time > self.DECAY_AFTER_SECONDS:
                    self._next_cooldown = self.BASE_COOLDOWN_SECONDS
                remaining = self._cooldown_until - now
            if remaining <= 0:
                return
            if stop_event is not None and stop_event.is_set():
                return
            time.sleep(min(remaining, 1.0))
_RATE_LIMIT_GUARD = RateLimitGuard()

class _PerAccountStartLimiter:

    def __init__(self, delay_seconds: float) -> None:
        self.delay_seconds = max(0.0, delay_seconds)
        self._lock = threading.Lock()
        self._next: dict[int | None, float] = {}

    def wait(self, account_id: int | None) -> None:
        if self.delay_seconds <= 0:
            return
        while True:
            with self._lock:
                now = time.monotonic()
                eligible = self._next.get(account_id, 0.0)
                if now >= eligible:
                    self._next[account_id] = now + self.delay_seconds
                    return
                wait_for = eligible - now
            time.sleep(min(wait_for, 0.25))

def _persist_dispatcharr_result(stream_id: int, result: Mapping[str, Any], logger) -> bool:
    status = str(result.get('status') or '').lower()
    if status == 'skipped':
        return False
    try:
        from apps.channels.models import Stream
        from django.utils import timezone as django_timezone
        stream = Stream.objects.filter(id=stream_id).first()
        if stream is None:
            logger.warning('[Analyze] stream=%s disappeared before metadata update', stream_id)
            return False
        stream.stream_stats = dict(result.get('stats') or {}) if status == 'alive' else {}
        # Dispatcharr owns is_stale as provider-refresh lifecycle state. It is
        # not a playback exclusion flag, so analyzer health remains in the
        # plugin cache/report until Dispatcharr exposes a supported health API.
        fields = ['stream_stats']
        if hasattr(stream, 'stream_stats_updated_at'):
            stream.stream_stats_updated_at = django_timezone.now()
            fields.append('stream_stats_updated_at')
        stream.save(update_fields=fields)
        return True
    except Exception as exc:
        logger.warning('[Analyze] stream=%s metadata update failed: %s', stream_id, exc)
        return False

def _result_cache_entry(item: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
    return {**dict(result), 'stream_id': item.get('id'), 'stream_name': item.get('name'), 'm3u_account_id': item.get('account_id'), 'm3u_account_name': item.get('account_name'), 'url_hash': _stream_url_hash(str(item.get('url') or ''))}

def _overall_text(counts: Mapping[str, int], total: int, completed: int) -> str:
    return f"alive={counts.get('alive', 0)} dead={counts.get('dead', 0)} skipped={counts.get('skipped', 0)} pending={max(0, total - completed)}"

def analyze_assigned_streams(settings: Mapping[str, Any], *, logger, cache_path: str=ANALYSIS_CACHE_PATH) -> dict[str, Any]:
    from apps.channels.models import ChannelStream
    from .sorter import resolve_channel_scope
    channel_ids, filter_summary = resolve_channel_scope(settings)
    workers = max(1, min(16, _as_int(settings.get('analysis_workers'), 2)))
    retries = max(0, min(5, _as_int(settings.get('analysis_retries'), 3)))
    account_delay = max(0.0, _as_float(settings.get('analysis_per_account_delay_seconds'), 1.0))
    max_streams = max(0, _as_int(settings.get('analysis_max_streams'), 0))
    queryset = ChannelStream.objects.select_related('stream', 'stream__m3u_account').order_by('channel_id', 'order', 'id')
    if channel_ids is not None:
        queryset = queryset.filter(channel_id__in=channel_ids)
    rows = list(queryset)
    seen: set[int] = set()
    items: list[dict[str, Any]] = []
    for row in rows:
        stream = row.stream
        if stream.id in seen:
            continue
        seen.add(stream.id)
        account = stream.m3u_account
        try:
            user_agent = account.get_user_agent_string() if account else DEFAULT_USER_AGENT
        except Exception:
            user_agent = DEFAULT_USER_AGENT
        items.append({'id': stream.id, 'name': stream.name or '', 'url': stream.url or '', 'account_id': getattr(stream, 'm3u_account_id', None), 'account_name': getattr(account, 'name', '') if account else '', 'user_agent': user_agent or DEFAULT_USER_AGENT})
        if max_streams and len(items) >= max_streams:
            break
    total = len(items)
    cache = load_analysis_cache(cache_path)
    results: dict[int, dict[str, Any]] = {}
    counts: collections.Counter[str] = collections.Counter()
    limiter = _PerAccountStartLimiter(account_delay)
    started = time.monotonic()

    def run_one(item: Mapping[str, Any]) -> dict[str, Any]:
        _RATE_LIMIT_GUARD.wait_if_throttled()
        limiter.wait(item.get('account_id'))
        result = analyze_stream(str(item.get('url') or ''), stream_id=item.get('id'), stream_name=str(item.get('name') or ''), settings=settings, user_agent=str(item.get('user_agent') or DEFAULT_USER_AGENT), logger=logger)
        if result.get('error_type') == 'rate_limited':
            _RATE_LIMIT_GUARD.record_hit(logger)
        return result

    def accept(item: Mapping[str, Any], result: Mapping[str, Any], completed: int, *, retry_label: str | None=None) -> None:
        stream_id = int(item['id'])
        previous = results.get(stream_id)
        if previous:
            counts[str(previous.get('status') or 'unknown')] -= 1
        results[stream_id] = dict(result)
        status = str(result.get('status') or 'unknown')
        counts[status] += 1
        cache[str(stream_id)] = _result_cache_entry(item, result)
        save_analysis_cache(cache, cache_path)
        _persist_dispatcharr_result(stream_id, result, logger)
        elapsed = max(time.monotonic() - started, 0.001)
        eta = elapsed / completed * (total - completed) if completed and completed < total else 0.0
        stats = result.get('stats') or {}
        resolution = stats.get('resolution') or 'n/a'
        fps = stats.get('source_fps')
        bitrate = stats.get('video_bitrate')
        reason = result.get('error_type') or 'ok'
        prefix = f'[Analyze {retry_label}]' if retry_label else '[Analyze]'
        logger.info('%s %d%% (%d/%d) stream=%s health=%s reason=%s resolution=%s fps=%s bitrate=%skbps | overall %s | ETA=%s', prefix, int(round(completed / total * 100)) if total else 100, completed, total, stream_id, status, reason, resolution, f'{float(fps):.1f}' if fps is not None else 'n/a', f'{float(bitrate):.0f}' if bitrate is not None else 'n/a', _overall_text(counts, total, completed), _format_eta(eta))
    logger.info('[Analyze] Starting: %d unique streams with %d workers', total, workers)
    if not items:
        return {'streams_analyzed': 0, 'channels_selected': len({row.channel_id for row in rows}), 'filters': filter_summary, 'status_counts': {}, 'cache_path': cache_path}
    completed_count = 0
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='stream-sort-analyze') as executor:
        future_to_item = {executor.submit(run_one, item): item for item in items}
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {'tested_at': _utc_now_iso(), 'status': 'dead', 'error_type': 'other', 'error': str(exc), 'stats': {}, 'details': {}}
            completed_count += 1
            accept(item, result, completed_count)
    by_id = {int(item['id']): item for item in items}
    for retry_pass in range(1, retries + 1):
        retry_ids = [stream_id for stream_id, result in results.items() if str(result.get('error_type') or '') in RETRYABLE_ERROR_TYPES]
        if not retry_ids:
            break
        backoff = max(1.0, account_delay * 3.0)
        logger.info('[Analyze Retry %d/%d] waiting %.1fs before retrying %d streams', retry_pass, retries, backoff, len(retry_ids))
        time.sleep(backoff)
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='stream-sort-retry') as executor:
            future_to_item = {executor.submit(run_one, by_id[stream_id]): by_id[stream_id] for stream_id in retry_ids}
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {'tested_at': _utc_now_iso(), 'status': 'dead', 'error_type': 'other', 'error': str(exc), 'stats': {}, 'details': {}}
                accept(item, result, total, retry_label=f'Retry {retry_pass}/{retries}')
    elapsed = time.monotonic() - started
    logger.info('[Analyze] Complete: %d/%d in %s | overall %s', len(results), total, _format_eta(elapsed), _overall_text(counts, total, total))
    return {'streams_analyzed': len(results), 'channels_selected': len({row.channel_id for row in rows}), 'filters': filter_summary, 'status_counts': {key: value for key, value in counts.items() if value > 0}, 'cache_path': cache_path}

def probe_assigned_streams(settings: Mapping[str, Any], *, logger, cache_path: str | None=None) -> dict[str, Any]:
    from apps.channels.models import ChannelStream
    from .sorter import resolve_channel_scope
    from .throughput import DEFAULT_CACHE_PATH
    cache_path = cache_path or DEFAULT_CACHE_PATH
    channel_ids, filter_summary = resolve_channel_scope(settings)
    duration = max(1.0, _as_float(settings.get('probe_duration_seconds'), 8.0))
    timeout = max(duration + 2.0, _as_float(settings.get('probe_timeout_seconds'), 10.0))
    rate_per_minute = max(1, _as_int(settings.get('probe_rate_per_minute'), 6))
    per_account_delay = max(0.0, _as_float(settings.get('probe_per_account_delay_seconds'), 1.0))
    max_streams = max(0, _as_int(settings.get('probe_max_streams'), 0))
    queryset = ChannelStream.objects.select_related('stream', 'stream__m3u_account').order_by('channel_id', 'order', 'id')
    if channel_ids is not None:
        queryset = queryset.filter(channel_id__in=channel_ids)
    rows = list(queryset)
    selected_channel_count = len({row.channel_id for row in rows})
    seen: set[int] = set()
    streams: list[dict[str, Any]] = []
    skipped_dead = 0
    for row in rows:
        stream = row.stream
        if stream.id in seen:
            continue
        seen.add(stream.id)
        if stream.stream_stats is not None and len(stream.stream_stats) == 0 and (stream.stream_stats_updated_at is not None):
            skipped_dead += 1
            continue
        _width, height = parse_resolution(stream.stream_stats)
        fps = parse_fps(stream.stream_stats)
        account = stream.m3u_account
        try:
            user_agent = account.get_user_agent_string() if account else DEFAULT_USER_AGENT
        except Exception:
            user_agent = DEFAULT_USER_AGENT
        streams.append({'id': stream.id, 'url': stream.url or '', 'account_id': getattr(stream, 'm3u_account_id', None), 'nominal_video_kbps': estimate_nominal_throughput_kbps(height, fps), 'user_agent': user_agent or DEFAULT_USER_AGENT})
        if max_streams and len(streams) >= max_streams:
            break
    cache = load_throughput_cache(cache_path)
    results: dict[int, dict[str, Any]] = {}
    counts: collections.Counter[str] = collections.Counter()
    min_start_interval = 60.0 / float(rate_per_minute)
    last_probe_started = 0.0
    account_next_eligible: dict[int | None, float] = {}
    started = time.monotonic()
    total = len(streams)
    logger.info('[Throughput] Starting: %d streams (%d known-dead skipped)', total, skipped_dead)
    for completed, item in enumerate(streams, start=1):
        account_id = item['account_id']
        next_global = last_probe_started + min_start_interval if last_probe_started else 0.0
        next_account = account_next_eligible.get(account_id, 0.0)
        wait_until = max(next_global, next_account)
        while time.monotonic() < wait_until:
            time.sleep(min(0.25, wait_until - time.monotonic()))
        last_probe_started = time.monotonic()
        result = probe_stream(item['url'], nominal_video_kbps=item['nominal_video_kbps'], duration_seconds=duration, timeout_seconds=timeout, user_agent=item['user_agent'])
        account_next_eligible[account_id] = time.monotonic() + per_account_delay
        cache[str(item['id'])] = result
        results[item['id']] = result
        status = str(result.get('status') or 'unknown')
        counts[status] += 1
        save_throughput_cache(cache, cache_path)
        elapsed = max(time.monotonic() - started, 0.001)
        eta = elapsed / completed * (total - completed) if completed < total else 0.0
        logger.info('[Throughput] %d%% (%d/%d) stream=%s health=%s throughput=%sMbps nominal=%skbps | overall healthy=%d marginal=%d insufficient=%d unknown=%d pending=%d | ETA=%s', int(round(completed / total * 100)) if total else 100, completed, total, item['id'], status, result.get('measured_mbps', 'n/a'), item['nominal_video_kbps'], counts.get('healthy', 0), counts.get('marginal', 0), counts.get('insufficient', 0), counts.get('unknown', 0), max(0, total - completed), _format_eta(eta))
    logger.info('[Throughput] Complete: %d/%d in %s | overall healthy=%d marginal=%d insufficient=%d unknown=%d; known-dead skipped=%d', len(results), total, _format_eta(time.monotonic() - started), counts.get('healthy', 0), counts.get('marginal', 0), counts.get('insufficient', 0), counts.get('unknown', 0), skipped_dead)
    return {'streams_probed': len(results), 'streams_skipped_dead': skipped_dead, 'channels_selected': selected_channel_count, 'filters': filter_summary, 'status_counts': dict(counts), 'cache_path': cache_path}
