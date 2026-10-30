"""ADR-025 evaluation archive. No I/O at import; workers retain only bounded RAM.

Only the scan coordinator drains/commits the queue. Contexts describe actual
calls, not terminal history, inferred health or extra provider measurements.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from functools import wraps
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
import threading
import time
from urllib.parse import urlsplit
from collections import deque

PAYLOAD_BYTES = 32 * 1024
PROVENANCE_BYTES = 256 * 1024
TRACK_LIMIT = 64
MARKER_LIMIT = 128
FLUSH_ROWS = 100
FLUSH_BYTES = 2 * 1024 * 1024
_worker = ContextVar('stream_sort_observation_worker', default=None)
_attempt = ContextVar('stream_sort_observation_attempt', default=None)
_mode = ContextVar('stream_sort_observation_mode', default=None)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) and abs(result) <= 1e18 else None
    except (TypeError, ValueError, OverflowError):
        return None


def identity(value):
    result = number(value)
    return int(result) if result is not None and result >= 0 and result.is_integer() else None


def choice(value, allowed):
    return value if isinstance(value, str) and value in allowed else None


ERRORS = frozenset(('timeout', 'connection_refused', 'network_unreachable',
    'stream_unreachable', 'server_error', 'invalid_stream', 'invalid_video_dimensions',
    'low_bitrate', 'placeholder_file', 'black_screen', 'frozen_video', 'silent_audio',
    'content_sample_incomplete', 'content_tool_unavailable', 'ffprobe_missing',
    'missing_url', 'streamlink_only', 'rate_limited', 'no_video_stream',
    'invalid_probe_json', 'other', 'capture_ok', 'capture_worker_failed',
    'capture_deadline', 'capture_canceled', 'capture_byte_limit', 'capture_empty',
    'capture_http_error', 'capture_network_error', 'capture_invalid_response',
    'capture_protocol_unsupported', 'capture_storage_failed', 'capture_incomplete',
    'capture_setup_failed', 'capture_open_failed', 'capture_finite_object', 'capture_eof',
    'capture_too_little_data', 'capture_read_failed', 'capture_write_failed', 'capture_close_failed',
    'capture_invalid_config', 'capture_start_failed', 'capture_unsupported_protocol', 'capture_invalid_result'))
STATUSES = frozenset(('alive', 'dead', 'skipped', 'unknown', 'healthy', 'marginal', 'insufficient'))
SOURCES = frozenset(('raw_http_capture', 'ffmpeg_stream_copy', 'direct_http_read'))
_DUE_REASONS = frozenset(('missing', 'url_changed', 'provider_changed', 'provider_missing',
    'dead_retry_pending', 'dead_ttl_expired', 'ttl_forced', 'missing_timestamp', 'ttl_expired',
    'health_revalidation', 'content_recovery_required', 'content_missing', 'content_ttl_expired',
    'status_dead_retry_pending', 'status_dead_ttl')) | frozenset('status_' + status for status in STATUSES)
REASONS = (_DUE_REASONS | frozenset('ffprobe_' + reason for reason in _DUE_REASONS)
    | frozenset(('unknown', 'ffprobe_missing', 'throughput_missing', 'media_changed',
        'placeholder_recovered', 'retry', 'local_sample_validation', 'shared_media_capture')))


# Value allowlists: arbitrary tool strings/tags never cross the boundary.
ENUMS = {
    'codec_type': {'video', 'audio', 'subtitle', 'data', 'attachment'},
    'codec_name': {'h264', 'hevc', 'mpeg2video', 'mpeg1video', 'av1', 'vp9', 'vp8',
        'aac', 'ac3', 'eac3', 'mp2', 'mp3', 'opus', 'vorbis', 'flac', 'pcm_s16le',
        'dvb_subtitle', 'dvb_teletext', 'timed_id3'},
    'profile': {'Main', 'High', 'Baseline', 'Constrained Baseline', 'Main 10',
        'High 10', 'LC', 'HE-AAC', 'HE-AACv2', 'Simple', 'Advanced Simple'},
    'pix_fmt': {'yuv420p', 'yuv420p10le', 'yuv422p', 'yuv422p10le', 'yuv444p',
        'yuv444p10le', 'nv12', 'p010le', 'yuvj420p'},
    'field_order': {'progressive', 'tt', 'bb', 'tb', 'bt', 'unknown'},
    'color_range': {'tv', 'pc', 'unknown'},
    'color_space': {'bt709', 'bt470bg', 'smpte170m', 'smpte240m', 'bt2020nc', 'bt2020c', 'rgb', 'unknown'},
    'color_transfer': {'bt709', 'smpte170m', 'smpte240m', 'bt2020-10', 'bt2020-12', 'smpte2084', 'arib-std-b67', 'unknown'},
    'color_primaries': {'bt709', 'bt470m', 'bt470bg', 'smpte170m', 'smpte240m', 'bt2020', 'unknown'},
    'channel_layout': {'mono', 'stereo', '2.1', '3.0', '4.0', '5.0', '5.1', '5.1(side)', '7.1', '7.1(wide)'},
}
TRACK_NUMBERS = ('index', 'width', 'height', 'coded_width', 'coded_height', 'level',
    'sample_rate', 'channels', 'bits_per_raw_sample', 'bits_per_sample',
    'bit_rate', 'start_time', 'duration', 'nb_frames', 'refs', 'has_b_frames')
RATES = ('r_frame_rate', 'avg_frame_rate', 'time_base', 'sample_aspect_ratio', 'display_aspect_ratio')


def edge_fingerprint(value):
    # Legacy throughput results use netloc, which may contain userinfo/port.
    # Normalize to hostname before hashing so neither credentials nor profile
    # rotation become an edge identity in the evaluation archive.
    try:
        hostname = urlsplit('//' + value).hostname if isinstance(value, str) else None
        return hashlib.sha256(hostname.lower().encode()).hexdigest() if hostname else None
    except (TypeError, ValueError):
        return None


def rational(value):
    if isinstance(value, str) and len(value) <= 48 and re.fullmatch(r'-?\d{1,18}[/ :]-?\d{1,18}', value):
        return value
    return None


def bounded(payload):
    """Numeric/identity fields win; every removal is explicit."""
    payload.setdefault('truncations', {})
    for key in ('tracks', 'black_intervals', 'freeze_markers', 'channels'):
        limit = TRACK_LIMIT if key == 'tracks' else MARKER_LIMIT
        values = payload.get(key)
        if isinstance(values, list) and len(values) > limit:
            payload['truncations'][key] = len(values) - limit
            payload[key] = values[:limit]
    black = payload.get('black_intervals') or []
    freeze = payload.get('freeze_markers') or []
    if len(black) + len(freeze) > MARKER_LIMIT:
        black_limit = min(len(black), max(MARKER_LIMIT - len(freeze), MARKER_LIMIT // 2))
        for key, limit in (('black_intervals', black_limit), ('freeze_markers', MARKER_LIMIT - black_limit)):
            values = payload.get(key) or []
            payload['truncations'][key] = payload['truncations'].get(key, 0) + max(0, len(values) - limit)
            payload[key] = values[:limit]
    for key in ('tracks', 'black_intervals', 'freeze_markers', 'channels'):
        values = payload.get(key)
        while values and len(encoded(payload)) > PAYLOAD_BYTES:
            values.pop()
            payload['truncations'][key] = payload['truncations'].get(key, 0) + 1
    if len(encoded(payload)) > PAYLOAD_BYTES:
        raise ValueError('observation scalar bound exceeded')
    return payload


def _metadata(probe):
    """Normalize in place while raw packets are already available to the probe."""
    target = _attempt.get()
    if target is None:
        return
    tracks = probe.get('streams') or []
    values = []
    for raw in tracks[:TRACK_LIMIT]:
        if not isinstance(raw, dict):
            continue
        row = {key: number(raw.get(key)) for key in TRACK_NUMBERS}
        row.update({key: choice(raw.get(key), allowed) for key, allowed in ENUMS.items()})
        row.update({key: rational(raw.get(key)) for key in RATES})
        values.append(row)
    target['tracks'] = values
    if len(tracks) > TRACK_LIMIT:
        target.setdefault('truncations', {})['tracks'] = len(tracks) - TRACK_LIMIT
    packets = probe.get('packets') or []
    video = next((raw.get('index') for raw in tracks if raw.get('codec_type') == 'video'), None)
    count, size, duration, first_pts, last_pts = 0, 0, 0.0, None, None
    for packet in packets:
        if packet.get('stream_index') != video:
            continue
        count += 1
        size += max(0, number(packet.get('size')) or 0)
        duration += max(0, number(packet.get('duration_time')) or 0)
        pts = number(packet.get('pts_time'))
        if pts is not None:
            first_pts = pts if first_pts is None else min(first_pts, pts)
            last_pts = pts if last_pts is None else max(last_pts, pts)
    target['packets'] = {'total_count': len(packets), 'video_count': count,
        'video_bytes': size, 'video_duration_seconds': duration,
        'video_first_pts_seconds': first_pts, 'video_last_pts_seconds': last_pts,
        'calculated_video_bitrate_kbps': size * 8 / duration / 1000 if duration > 0 and count >= 30 else None,
        'bitrate_source': 'video_packet_durations' if duration > 0 and count >= 30 else None}
    fmt = probe.get('format') or {}
    target['primary_video'] = {'stream_index': identity(video),
        'declared_bitrate_bps': next((number(raw.get('bit_rate')) for raw in tracks if raw.get('index') == video), None),
        'calculated_bitrate_kbps': target['packets']['calculated_video_bitrate_kbps'],
        'selection': 'video_packet_durations' if target['packets']['calculated_video_bitrate_kbps'] is not None else 'declared_or_unknown'}
    target['container'] = {'duration_seconds': number(fmt.get('duration')),
        'declared_bitrate_bps': number(fmt.get('bit_rate')), 'size_bytes': number(fmt.get('size'))}


def metadata(probe):
    try:
        _metadata(probe)
    except Exception:
        context = _worker.get()
        if context:
            context[0].collection_error()


def normalized_stats(stats):
    normalized = {key: number(stats.get(key)) for key in ('width', 'height', 'source_fps',
        'video_bitrate', 'sample_rate', 'audio_bitrate')}
    for key, enum in (('video_codec', 'codec_name'), ('audio_codec', 'codec_name'),
                      ('pixel_format', 'pix_fmt'), ('audio_channels', 'channel_layout')):
        normalized[key] = choice(stats.get(key), ENUMS[enum])
    normalized['resolution'] = (f"{int(normalized['width'])}x{int(normalized['height'])}"
        if normalized['width'] is not None and normalized['height'] is not None else None)
    normalized['stream_type'] = choice(stats.get('stream_type'), {'mpegts', 'hls', 'flv', 'unknown'})
    return normalized


def selected_metadata(*, calculated, declared, selected, packet_fallback):
    """Archive the analyzer's exact selection, never a second estimator."""
    target = _attempt.get()
    if target is None:
        return
    try:
        primary = target.setdefault('primary_video', {})
        primary.update(calculated_bitrate_kbps=number(calculated),
            declared_bitrate_kbps=number(declared), selected_bitrate_kbps=number(selected),
            packet_selection='all_packets_fallback' if packet_fallback else 'video_stream_index',
            selection=('all_packets_fallback' if packet_fallback else 'video_packet_durations')
                if calculated is not None else 'declared_video_bitrate' if declared is not None else 'unknown')
    except Exception:
        context = _worker.get()
        if context:
            context[0].collection_error()


def output_text(value):
    if isinstance(value, bytes):
        return value.decode('utf-8', errors='replace')
    return value if isinstance(value, str) else ''


def decoder_duration(stdout):
    decoded = None
    for match in re.finditer(r'^out_time_us=([^\r\n]+)', output_text(stdout), re.MULTILINE):
        value = number(match.group(1))
        if value is not None and value >= 0:
            value /= 1_000_000
            decoded = value if decoded is None else max(decoded, value)
    return decoded


def content_output(stdout, stderr, *, partial=False):
    decoded = decoder_duration(stdout)
    details(decoded_duration_seconds=decoded,
        decoder_duration_source='ffmpeg_out_time_us' if decoded is not None else None,
        partial_output=partial)
    text = output_text(stderr)
    content(text)
    if partial:
        match = re.search(r'mean_volume:\s*(-?(?:inf|[\d.]+))\s*dB', text)
        value = match.group(1) if match else None
        details(partial_mean_volume_db=number(value),
            partial_mean_volume_negative_infinity=value == '-inf')
    return decoded


def content(stderr):
    target = _attempt.get()
    if target is None:
        return
    try:
        patterns = (
            ('black_intervals', r'black_start:([\d.]+)\s+black_end:([\d.]+)\s+black_duration:([\d.]+)'),
            ('freeze_markers', r'lavfi\.freezedetect\.(freeze_start|freeze_end|freeze_duration):\s*([\d.]+)'),
        )
        for key, pattern in patterns:
            values, count = [], 0
            for match in re.finditer(pattern, stderr or ''):
                count += 1
                if len(values) < MARKER_LIMIT:
                    values.append([number(v) for v in match.groups()] if key == 'black_intervals' else
                                  {'kind': match.group(1), 'seconds': number(match.group(2))})
            target[key] = values
            target.setdefault('truncations', {})[key] = max(0, count - MARKER_LIMIT)
    except Exception:
        context = _worker.get()
        if context:
            context[0].collection_error()


def details(**values):
    target = _attempt.get()
    if target is not None:
        target.update(values)


def probe(component, mode):
    """Wrap actual calls only. Telemetry cannot alter their return/exception."""
    def decorate(function):
        @wraps(function)
        def wrapped(*args, **kwargs):
            context = _worker.get()
            if context is None:
                return function(*args, **kwargs)
            archive, item, phase, wave = context
            start = time.monotonic()
            payload = {'started_at': utc(), 'component': component,
                'mode': mode, 'phase': phase, 'wave': wave,
                'stream_id': identity(item.get('id')), 'provider_id': identity(item.get('account_id')),
                'profile_id': identity(item.get('m3u_profile_id')),
                'channels': [{'channel_id': identity(row.get('channel_id')),
                    'channel_number': number(row.get('channel_number'))} for row in (item.get('channels') or [])[:MARKER_LIMIT]],
                'truncations': {}, 'reason': choice((item.get('_observation_reasons') or {}).get(component), REASONS) or 'unknown'}
            channel_count = len(item.get('channels') or [])
            if channel_count > MARKER_LIMIT:
                payload['truncations']['channels'] = channel_count - MARKER_LIMIT
            if component == 'throughput' and mode == 'bounded_http_capture' and payload['reason'] == 'unknown':
                payload['reason'] = 'shared_media_capture'
            if kwargs.get('local_source'):
                payload['mode'] = 'local_sample_validation'
            elif _mode.get() in {'placeholder_confirm_1s'}:
                payload['mode'] = _mode.get()
            token = _attempt.set(payload)
            result = None
            try:
                result = function(*args, **kwargs)
                return result
            finally:
                _attempt.reset(token)
                try:
                    raw = result[0] if isinstance(result, tuple) else result
                    raw = raw if isinstance(raw, dict) else {}
                    payload.update(finished_at=utc(), elapsed_seconds=max(0, time.monotonic() - start),
                        status=choice(raw.get('status'), STATUSES),
                        error_type=choice(raw.get('error_type') or raw.get('capture_code'), ERRORS),
                        outcome='returned' if result is not None else 'exception')
                    if component == 'metadata':
                        payload['selected_stats'] = normalized_stats(raw.get('stats') or {})
                    if component == 'content':
                        content = (raw.get('details') or {}).get('content') or raw
                        for key in ('black', 'frozen', 'measured', 'skipped'):
                            payload[key] = content.get(key) if isinstance(content.get(key), bool) else None
                        payload['mean_volume_db'] = number(content.get('mean_volume_db'))
                        payload['mean_volume_negative_infinity'] = content.get('mean_volume_db') == -math.inf
                        payload['error_type'] = payload['error_type'] or choice(content.get('error_type'), ERRORS)
                    if component == 'throughput':
                        payload['requested_duration_seconds'] = number(kwargs.get('duration_seconds'))
                        payload['requested_timeout_seconds'] = number(kwargs.get('timeout_seconds'))
                        for key in ('bytes', 'elapsed_seconds', 'measured_mbps', 'nominal_video_kbps'):
                            payload['measurement_' + key] = number(raw.get(key))
                        payload['measurement_source'] = choice(raw.get('measurement_source'), SOURCES)
                        payload['elapsed_source'] = choice(raw.get('elapsed_source'), {'acquisition', 'parent_wall', 'provider_read_wall', 'provider_capture_wall'})
                        payload['capture_code'] = choice(raw.get('capture_code'), ERRORS)
                        edge = raw.get('edge_host')
                        payload['edge_host_sha256'] = edge_fingerprint(edge)
                        payload['edge_provenance'] = 'sha256_hostname' if payload['edge_host_sha256'] else None
                    archive.collect(bounded(payload))
                except Exception:
                    # Only telemetry errors are caught here. Storage runs on the
                    # coordinator and propagates as StorageError, never media dead.
                    archive.collection_error()
                    # Retain the attempt identity even if an optional normalized
                    # payload could not be encoded. Never forward offending text.
                    try:
                        archive.collect({'started_at': payload['started_at'], 'finished_at': utc(),
                            'component': component, 'mode': payload['mode'], 'phase': phase, 'wave': wave,
                            'stream_id': identity(item.get('id')), 'provider_id': identity(item.get('account_id')),
                            'profile_id': identity(item.get('m3u_profile_id')), 'outcome': 'telemetry_unavailable',
                            'truncations': {'optional_payload': True}})
                    except Exception:
                        archive.collection_error()
        return wrapped
    return decorate


@contextmanager
def mode(value):
    token = _mode.set(value)
    try:
        yield
    finally:
        _mode.reset(token)


def selected_profile(item):
    context = _worker.get()
    if context:
        context[1]['m3u_profile_id'] = item.get('m3u_profile_id')


def local_work(function):
    @wraps(function)
    def wrapped(item, *args, **kwargs):
        from .storage import active_run
        archive = getattr(active_run(), 'observations', None)
        prepared = {**item, 'm3u_profile_id': item.get('_observation_profile_id')}
        phase = (item.get('_observation_phase') or 'stream-sort-combined-capture') + '-local'
        with worker_context(archive, prepared, phase, item.get('_observation_wave', 1)):
            return function(item, *args, **kwargs)
    return wrapped


def utc():
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def worker_context(archive, item, phase, wave):
    token = _worker.set((archive, item, phase, wave) if archive else None)
    try:
        yield
    finally:
        _worker.reset(token)


# Numeric/boolean defaults only; no executable paths, filters, regexes or tokens.
def configuration(settings):
    from .analyzer import _as_bool, _as_float, _as_int, _effective_freeze_seconds
    # Keep transformations aligned with incremental/analyzer/throughput.
    specs = {
        'analysis_workers': (2, 1, 16, True), 'analysis_retries': (3, 0, 5, True),
        'analysis_per_account_delay_seconds': (1, 0, None, False),
        'stream_data_ttl_hours': (18, 0, None, False), 'dead_content_ttl_hours': (1, 0, None, False),
        'content_validation_ttl_hours': (168, 0, None, False),
        'healthy_throughput_ttl_hours': (48, 0, None, False), 'degraded_throughput_ttl_hours': (24, 0, None, False),
        'unknown_throughput_ttl_hours': (4, 0, None, False), 'analysis_ttl_jitter_percent': (30, 0, 100, False),
        'media_bitrate_relative_tolerance_percent': (30, 0, 100, False),
        'playback_health_min_seconds': (300, 60, None, False), 'playback_health_clean_min_seconds': (60, 30, None, False),
        'analysis_duration_seconds': (5, 1, None, True), 'analysis_probe_timeout_seconds': (20, 1, None, True),
        'analysis_connection_timeout_seconds': (10, 1, None, True), 'minimum_video_bitrate_kbps': (500, 0, None, False),
        'content_sample_seconds': (6, 2, None, True), 'black_screen_min_seconds': (3, 1, None, True),
        'silent_audio_max_db': (-70, None, None, False),
        'probe_duration_seconds': (6, 1, None, False), 'probe_per_account_delay_seconds': (1, 0, None, False),
        'capture_max_mib': (32, 1, 64, True),
    }
    values = {}
    for key, (default, low, high, integral) in specs.items():
        value = (_as_int if integral else _as_float)(settings.get(key), default)
        value = value if number(value) is not None else default
        values[key] = min(high, value) if high is not None else value
        if low is not None:
            values[key] = max(low, values[key])
    for key, default in (('placeholder_file_detection', True), ('black_screen_detection', True),
            ('frozen_video_detection', True), ('silent_audio_detection', True),
            ('playback_health_reuse', True), ('bounded_capture_reuse', False), ('retain_probe_observations', True)):
        values[key] = _as_bool(settings.get(key), default)
    values['frozen_video_min_seconds'] = _effective_freeze_seconds(settings.get('frozen_video_min_seconds'), values['content_sample_seconds'])
    values['content_ffmpeg_timeout_seconds'] = max(values['content_sample_seconds'] + 5, _as_int(settings.get('content_ffmpeg_timeout_seconds'), 20))
    values['probe_timeout_seconds'] = max(values['probe_duration_seconds'] + 2, _as_float(settings.get('probe_timeout_seconds'), 10))
    return values


def provenance(settings, scope):
    config = configuration(settings)
    source = hashlib.sha256()
    package = Path(__file__).parent
    for path in sorted([*package.glob('*.py'), package / 'plugin.json']):
        source.update(path.name.encode())
        source.update(path.read_bytes())
    manifest_version = json.loads((package / 'plugin.json').read_text()).get('version')
    manifest_version = manifest_version if isinstance(manifest_version, str) and re.fullmatch(r'\d+\.\d+\.\d+(?:-(?:beta|alpha|rc)\.\d+)?', manifest_version) else None
    versions = {}
    for tool in ('ffprobe', 'ffmpeg'):
        try:
            output = subprocess.run([str(settings.get('analysis_' + tool + '_path') or '/usr/local/bin/' + tool), '-version'],
                capture_output=True, text=True, timeout=3, check=False)
            match = re.match(rf'{tool} version (?:n)?(\d+(?:\.\d+){{0,3}})(?=[\s-]|$)', output.stdout or '')
            versions[tool] = match.group(1) if output.returncode == 0 and match else None
        except Exception:
            versions[tool] = None
    value = {'archive_schema': 3, 'payload_version': 1, 'started_at': utc(),
        'configuration': config, 'configuration_sha256': hashlib.sha256(encoded(config)).hexdigest(),
        'source': {'git_sha': None, 'package_sha256': source.hexdigest(), 'method': 'package_python_and_manifest_bytes', 'plugin_version': manifest_version},
        'tools': versions, 'scope': scope, 'truncations': {},
        'limits': {'payload_bytes': PAYLOAD_BYTES, 'provenance_bytes': PROVENANCE_BYTES,
            'tracks': TRACK_LIMIT, 'content_markers_total': MARKER_LIMIT,
            'flush_rows': FLUSH_ROWS, 'flush_bytes': FLUSH_BYTES},
        'metric_definitions': {'bytes': 'encoded_utf8_payload_bytes_excluding_sql_and_indexes',
            'committed_rows': 'acknowledged_unique_attempts_including_idempotent_retry',
            'flush_elapsed_seconds': 'coordinator_work_excluding_successful_metrics_update'}}
    if len(encoded(value)) > PROVENANCE_BYTES:
        value['scope'] = {'selected_streams': scope['selected_streams'], 'selected_channels': scope['selected_channels']}
        value['truncations']['scope'] = True
    return value


class Archive:
    def __init__(self, store, owner, settings, items):
        self.store, self.owner = store, owner
        self.coordinator = threading.get_ident()
        self.lock = threading.Lock()
        self.queue = deque()
        self.pending_bytes = 0
        self.sequence = 0
        self.waves = {}
        self.metrics = {'collected_rows': 0, 'encoded_payload_bytes': 0, 'committed_rows': 0,
            'committed_payload_bytes': 0, 'batches': 0, 'flush_elapsed_seconds': 0.0,
            'flush_calls': 0, 'failed_flush_calls': 0, 'collection_errors': 0, 'max_buffer_rows': 0, 'max_buffer_bytes': 0}
        channels = sorted({identity(row.get('channel_id')) for item in items for row in item.get('channels') or []} - {None})
        # Bound run context independently of provider count; full per-item context
        # lives on attempts. No names or URL-derived identities.
        scope = {'selected_streams': len(items), 'selected_channels': len(channels),
            'channel_ids': channels[:10000], 'stream_ids': [identity(item.get('id')) for item in items[:10000]]}
        scope['ids_truncated'] = len(channels) > 10000 or len(items) > 10000
        self.provenance = provenance(settings, scope)
        self.run_saved = False
        self.state = None

    def wave(self, phase):
        self.waves[phase] = self.waves.get(phase, 0) + 1
        return self.waves[phase]

    def collection_error(self):
        with self.lock:
            self.metrics['collection_errors'] += 1

    def collect(self, payload):
        with self.lock:
            self.sequence += 1
            payload['attempt_sequence'] = self.sequence
            payload['run_id'] = self.owner
            payload['observation_id'] = hashlib.sha256(f'{self.owner}:{self.sequence}'.encode()).hexdigest()
            data = encoded(bounded(payload)).decode()
            size = len(data.encode())
            self.queue.append((payload['observation_id'], payload['stream_id'], payload['provider_id'], payload['started_at'], data, size))
            self.pending_bytes += size
            self.metrics['collected_rows'] += 1
            self.metrics['encoded_payload_bytes'] += size
            self.metrics['max_buffer_rows'] = max(self.metrics['max_buffer_rows'], len(self.queue))
            self.metrics['max_buffer_bytes'] = max(self.metrics['max_buffer_bytes'], self.pending_bytes)

    def flush(self, *, force=False, state='running'):
        if threading.get_ident() != self.coordinator:
            raise RuntimeError('only coordinator may flush observations')
        if force and self.run_saved and not self.queue and state == self.state:
            return
        if not force and len(self.queue) < FLUSH_ROWS and self.pending_bytes < FLUSH_BYTES:
            return
        from .storage import StorageError, active_run
        run = active_run()
        if run is not None and hasattr(run, 'ensure'):
            run.ensure()
        started = time.monotonic()
        self.metrics['flush_calls'] += 1
        recorded_elapsed = 0
        try:
            if not self.run_saved:
                self.store.observation_run(self.owner, self.provenance, self.metrics, state)
                self.run_saved = True
            while self.queue:
                with self.lock:
                    batch = list(self.queue)[:FLUSH_ROWS]
                self.store.observation_batch(self.owner, batch)
                # Acknowledge ONLY after successful transaction exit. An unknown
                # commit is retried with exactly the same IDs and payload bytes.
                with self.lock:
                    for row in batch:
                        assert self.queue[0][0] == row[0]
                        self.queue.popleft()
                        self.pending_bytes -= row[5]
                    self.metrics['committed_rows'] += len(batch)
                    self.metrics['committed_payload_bytes'] += sum(row[5] for row in batch)
                    self.metrics['batches'] += 1
            recorded_elapsed = max(0, time.monotonic() - started)
            self.metrics['flush_elapsed_seconds'] += recorded_elapsed
            self.store.observation_run(self.owner, self.provenance, self.metrics, state)
            self.state = state
        except Exception as exc:
            self.metrics['failed_flush_calls'] += 1
            self.metrics['flush_elapsed_seconds'] += max(0, time.monotonic() - started - recorded_elapsed)
            if isinstance(exc, StorageError):
                raise
            raise StorageError('Observation archive flush failed; unacknowledged batch retained') from exc

    def snapshot(self):
        return {**self.metrics, 'pending_rows': len(self.queue), 'pending_payload_bytes': self.pending_bytes}
