"""Credential-free capture diagnostics; fixtures never contact a provider."""
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock

import pytest

from stream_sorter import analyzer, capture_worker, incremental, throughput
from test_capture_reuse import Clock, Response
from test_storage_backend import pg_scan  # reuse the bounded in-memory scan harness


SECRET = 'credential-marker'
PRIVATE_URL = f'http://user:{SECRET}@provider.invalid/private/{SECRET}|Authorization={SECRET}'


def measured(**overrides):
    return {'complete': True, 'capture_code': 'capture_ok', 'bytes': 1048576,
            'elapsed_seconds': 8, 'elapsed_source': 'acquisition',
            'measurement_source': 'raw_http_capture', 'measured_mbps': 1.048576,
            **overrides}


@pytest.mark.parametrize('branch,code,received', [
    ('open', 'capture_open_failed', 0),
    ('finite', 'capture_finite_object', 0),
    ('limit', 'capture_byte_limit', 65536),
    ('eof', 'capture_eof', 65536),
    ('read', 'capture_read_failed', 65536),
    ('little', 'capture_too_little_data', 16),
    ('write', 'capture_write_failed', 0),
    ('close', 'capture_close_failed', 8 * 65536),
    ('success', 'capture_ok', 8 * 65536),
])
def test_child_reasons_and_partial_measurements_are_sanitized(tmp_path, branch, code, received):
    clock = Clock()
    response = Response(clock, chunk=b'body' if branch == 'little' else b'x' * 65536)
    if branch == 'finite':
        response.headers = {'Content-Range': 'bytes 0-65535/65536'}
    if branch == 'close':
        response.close = Mock(side_effect=OSError(PRIVATE_URL))
    reads = response.read1
    def read(size):
        if response.reads == 1 and branch == 'read':
            raise OSError(PRIVATE_URL)
        if response.reads == 1 and branch == 'eof':
            return b''
        return reads(size)
    response.read1 = read
    opener = Mock(return_value=response, side_effect=OSError(PRIVATE_URL) if branch == 'open' else None)
    config = throughput._capture_config(PRIVATE_URL, duration_seconds=1 if branch == 'little' else 2,
        timeout_seconds=8, user_agent=SECRET, max_bytes=65536 if branch == 'limit' else 1048576,
        path=str(tmp_path if branch == 'write' else tmp_path / 'private.sample'), started=0)
    result = capture_worker.capture(config, now=clock, opener=opener)
    assert result['capture_code'] == code
    assert result['complete'] is (branch == 'success')
    assert result['bytes'] == received
    assert result['elapsed_seconds'] == clock.now
    assert SECRET not in json.dumps(result)
    if branch == 'close':
        response.close.assert_called_once()
    elif branch != 'open':
        assert response.closed


@pytest.mark.parametrize('case,code', [
    ('cancel', 'capture_canceled'), ('protocol', 'capture_unsupported_protocol'),
    ('config', 'capture_invalid_config'), ('storage', 'capture_storage_failed'),
    ('start', 'capture_start_failed'),
])
def test_parent_pre_acquisition_codes_and_cleanup(tmp_path, monkeypatch, case, code):
    start = Mock(side_effect=OSError(PRIVATE_URL))
    monkeypatch.setattr(throughput.subprocess, 'Popen', start)
    result, path = throughput.capture_bounded_sample(
        'rtsp://private.invalid/' + SECRET if case == 'protocol' else PRIVATE_URL,
        duration_seconds=40 if case == 'config' else 1, timeout_seconds=60,
        temp_directory=str(tmp_path / 'missing') if case == 'storage' else str(tmp_path),
        stop_requested=lambda: case == 'cancel')
    assert path is None and not list(tmp_path.iterdir())
    assert result['capture_code'] == code
    assert result['error_type'] == 'capture_incomplete'
    assert result['elapsed_source'] == 'parent_wall'
    assert result['bytes'] == 0
    assert SECRET not in json.dumps(result)
    if case == 'start':
        start.assert_called_once()
    else:
        start.assert_not_called()


@pytest.mark.parametrize('payload,code', [
    ({'complete': False, 'bytes': 12, 'elapsed_seconds': .5,
      'capture_code': 'capture_read_failed'}, 'capture_read_failed'),
    ({'complete': False, 'bytes': 12, 'capture_code': PRIVATE_URL,
      'error': PRIVATE_URL}, 'capture_worker_failed'),
    ({'complete': False, 'bytes': -1, 'elapsed_seconds': float('nan'),
      'capture_code': ['secret']}, 'capture_worker_failed'),
    ([], 'capture_invalid_result'),
    ({'complete': True, 'bytes': 12, 'elapsed_seconds': 2}, 'capture_invalid_result'),
])
def test_parent_rejects_untrusted_child_data_without_leaking_it(tmp_path, monkeypatch, payload, code):
    child = Mock(returncode=0)
    def communicate(**kwargs):
        config = json.loads(kwargs['input'])
        Path(config['path']).write_bytes(b'partial data')
        return json.dumps(payload), None
    child.communicate.side_effect = communicate
    child.poll.return_value = 0
    monkeypatch.setattr(throughput.subprocess, 'Popen', Mock(return_value=child))
    result, path = throughput.capture_bounded_sample(PRIVATE_URL, duration_seconds=1,
        timeout_seconds=2, temp_directory=str(tmp_path))
    assert path is None and not list(tmp_path.iterdir())
    assert result['capture_code'] == code
    assert result['bytes'] == 12
    assert SECRET not in json.dumps(result, allow_nan=False)
    child.wait.assert_called_once()
    if isinstance(payload, dict) and payload.get('elapsed_seconds') == .5:
        assert result['elapsed_seconds'] == .5
        assert result['elapsed_source'] == 'acquisition'
    assert 'measured_mbps' not in result  # failures cannot refresh throughput TTL


@pytest.mark.parametrize('branch,reason,actual', [
    ('metadata', 'local_metadata_rejected', 8),
    ('short', 'local_sample_too_short', .2),
    ('unknown', 'local_sample_too_short', None),
    ('content', 'local_content_unmeasured', 8),
    ('exception', 'local_validation_failed', 8),
    ('success', 'reuse_ok', 8),
    ('black', 'reuse_ok', 8),  # measured failed health is still reusable evidence
])
def test_local_validation_reasons_cleanup_release_and_secret_redaction(tmp_path, monkeypatch, caplog,
                                                                      branch, reason, actual):
    path = tmp_path / (SECRET + '.sample')
    path.write_bytes(b'fixture')
    release = Mock()
    item = {'id': 42, 'name': SECRET, 'url': PRIVATE_URL, '_release_provider': release}
    details = {} if actual is None else {'sample_duration_seconds': actual}
    metadata = {'status': 'dead' if branch == 'metadata' else 'alive',
                'error_type': PRIVATE_URL, 'error': PRIVATE_URL,
                'details': details, 'stats': {}}
    def probe(*args, **kwargs):
        release.assert_called_once()
        return metadata
    def content(base, *args, **kwargs):
        if branch == 'exception':
            raise RuntimeError(PRIVATE_URL)
        return {**base, 'status': 'dead' if branch == 'black' else 'alive',
                'details': {'content': {'measured': branch != 'content',
                    'error': PRIVATE_URL, 'error_type': PRIVATE_URL}}}
    monkeypatch.setattr(analyzer, 'analyze_stream', probe)
    monkeypatch.setattr(analyzer, 'apply_content_analysis', content)
    logger = logging.getLogger('safe-capture')
    caplog.set_level(logging.INFO, logger='safe-capture')
    result, diagnostic = incremental._validate_shared_capture(item, measured(), str(path),
        settings={}, logger=logger, sample_seconds=6, needs_content=True)
    assert diagnostic['reason'] == reason
    assert diagnostic['actual_playable_seconds'] == actual
    assert diagnostic['required_playable_seconds'] == 6
    assert diagnostic['capture_code'] == 'capture_ok'
    assert diagnostic['capture_mbps'] == 1.048576
    assert diagnostic['bytes'] == 1048576
    assert diagnostic['elapsed_seconds'] == 8
    assert not path.exists()
    collector = incremental._CaptureTelemetry(True)
    collector.record(42, diagnostic, logger)
    summary = collector.snapshot()
    assert summary['attempts'] == 1
    assert summary['reason_counts'] == {reason: 1}
    assert SECRET not in caplog.text + json.dumps(diagnostic) + json.dumps(summary)
    if branch in ('success', 'black'):
        assert diagnostic['outcome'] == 'reused'
        incremental._attach_capture_diagnostic(result, diagnostic)
        assert result['_shared_capture']['content']['details']['capture_reuse'] == diagnostic
        assert result['_shared_capture']['content']['status'] == ('dead' if branch == 'black' else 'alive')
    else:
        assert result is None and diagnostic['outcome'] == 'validation_rejected'


def test_diagnostic_unknowns_and_nonfinite_data_stay_unknown():
    diagnostic = incremental._capture_diagnostic({'capture_code': SECRET, 'bytes': -1,
        'measured_mbps': float('nan'), 'elapsed_seconds': float('inf'),
        'measurement_source': PRIVATE_URL, 'elapsed_source': PRIVATE_URL})
    assert diagnostic['reason'] == 'capture_worker_failed'
    for key in ('bytes', 'capture_mbps', 'elapsed_seconds', 'actual_playable_seconds',
                'required_playable_seconds', 'measurement_source', 'elapsed_source'):
        assert diagnostic[key] is None
    assert SECRET not in json.dumps(diagnostic, allow_nan=False)


def test_counters_are_thread_safe_bounded_and_no_attempts_are_explicit():
    collector = incremental._CaptureTelemetry(False)
    assert collector.snapshot()['enabled'] is False
    assert collector.snapshot()['no_attempts'] is True
    def record(sid):
        collector.record(sid, incremental._capture_skip('reuse_disabled'), Mock())
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(record, range(1000)))
    summary = collector.snapshot()
    assert summary['attempts'] == 0 and summary['no_attempts'] is True
    assert summary['reason_counts'] == {'reuse_disabled': 1000}
    assert summary['outcome_counts'] == {'skipped': 1000}
    assert not any(isinstance(value, list) for value in vars(collector).values())


@pytest.mark.parametrize('enabled', [False, True])
def test_empty_scan_has_disabled_or_enabled_zero_attempt_summary(pg_scan, enabled):
    pg_scan.rows.clear()
    result = pg_scan.run(bounded_capture_reuse=enabled)
    assert result['capture_reuse'] == {
        'enabled': enabled, 'attempts': 0, 'no_attempts': True,
        'reason_counts': {}, 'outcome_counts': {}, 'validation_error_counts': {}}


@pytest.mark.parametrize('mode,reason', [
    ('disabled', 'reuse_disabled'), ('ram', 'reuse_ram_unavailable'),
    ('metadata', 'reuse_metadata_only'), ('window', 'reuse_window_exceeds_bound'),
    ('placeholder', 'reuse_placeholder'),
])
def test_scan_direct_paths_record_safe_no_attempt_reasons(pg_scan, monkeypatch, mode, reason):
    context = pg_scan
    if mode == 'placeholder':
        context.store.current['42']['error_type'] = 'placeholder_file'
    monkeypatch.setattr(incremental, '_select_capture_temp_directory',
        lambda *a, **kw: None if mode == 'ram' else str(context.tmp_path))
    monkeypatch.setattr(incremental, 'content_check_reason',
        lambda *a, **kw: 'missing' if mode == 'window' else None)
    monkeypatch.setattr(incremental, 'throughput_check_reason',
        lambda *a, **kw: None if mode == 'metadata' else 'missing')
    monkeypatch.setattr(analyzer, 'analyze_stream', Mock(return_value={
        'status': 'dead' if mode == 'placeholder' else 'alive',
        'error_type': 'placeholder_file' if mode == 'placeholder' else None,
        'tested_at': context.now.isoformat(), 'details': {}, 'stats': {}}))
    capture = Mock(side_effect=AssertionError('no acquisition permitted'))
    monkeypatch.setattr(incremental, 'capture_bounded_sample', capture)
    overrides = {} if mode == 'disabled' else {'bounded_capture_reuse': True}
    if mode == 'window':
        overrides['probe_duration_seconds'] = 31
    result = context.run(**overrides)
    assert result['capture_reuse']['no_attempts'] is True
    assert result['capture_reuse']['attempts'] == 0
    assert result['capture_reuse']['reason_counts'] == {reason: 1}
    capture.assert_not_called()


@pytest.mark.parametrize('branch,reason,outcome', [
    ('acquisition', 'capture_eof', 'acquisition_failed'),
    ('short', 'local_sample_too_short', 'validation_rejected'),
    ('success', 'reuse_ok', 'reused'),
])
def test_scan_summaries_and_fallback_keep_reservations_and_ttls(pg_scan, monkeypatch,
                                                             branch, reason, outcome):
    context = pg_scan
    context.store.current['42'].update(status='alive', error_type=None)
    previous_content_checked = context.store.current['42']['content_checked_at']
    monkeypatch.setattr(incremental, '_select_capture_temp_directory', lambda *a, **kw: str(context.tmp_path))
    monkeypatch.setattr(incremental, 'content_check_reason', lambda *a, **kw: None)
    monkeypatch.setattr(incremental, 'throughput_check_reason', lambda *a, **kw: 'missing')
    def capture(*args, **kwargs):
        if branch == 'acquisition':
            return measured(complete=False, capture_code='capture_eof', bytes=65536), None
        path = context.tmp_path / 'fixture.ram'
        path.write_bytes(b'fixture')
        return measured(), str(path)
    probes = []
    def probe(source, **kwargs):
        probes.append(bool(kwargs.get('local_source')))
        return {'status': 'alive', 'tested_at': context.now.isoformat(), 'stats': {},
                'details': {'sample_duration_seconds': .2 if branch == 'short' else 8}}
    monkeypatch.setattr(incremental, 'capture_bounded_sample', capture)
    monkeypatch.setattr(analyzer, 'analyze_stream', probe)
    result = context.run(bounded_capture_reuse=True)
    telemetry = result['capture_reuse']
    assert telemetry['reason_counts'] == {reason: 1}
    assert telemetry['outcome_counts'] == {outcome: 1}
    assert telemetry['attempts'] == 1 and telemetry['no_attempts'] is False
    assert probes == ([False] if branch == 'acquisition' else [True, False] if branch == 'short' else [True])
    assert context.capacity.release.call_count == {'success': 1, 'acquisition': 2, 'short': 3}[branch]
    assert result['throughput_attempted'] == result['throughput_checked'] == 1
    entry = context.store.current['42']
    assert entry['status'] == 'alive'
    assert entry['content_checked_at'] == previous_content_checked  # metadata cannot refresh content
    assert not list(context.tmp_path.glob('*.ram'))
    assert context.store.record_scan.call_args.args[-1]['capture_reuse'] == telemetry


@pytest.mark.parametrize('phase,error', [('metadata', 'low_bitrate'), ('content', 'content_sample_incomplete')])
def test_known_validation_errors_have_bounded_reason_counts(tmp_path, monkeypatch, phase, error):
    path = tmp_path / 'sample'
    path.write_bytes(b'fixture')
    metadata = {'status': 'dead' if phase == 'metadata' else 'alive', 'error_type': error,
                'stats': {}, 'details': {'sample_duration_seconds': 8}}
    monkeypatch.setattr(analyzer, 'analyze_stream', Mock(return_value=metadata))
    monkeypatch.setattr(analyzer, 'apply_content_analysis', Mock(return_value={
        'details': {'content': {'measured': False, 'error_type': error, 'error': PRIVATE_URL}}}))
    result, diagnostic = incremental._validate_shared_capture({'id': 42, '_release_provider': Mock()},
        measured(), str(path), settings={}, logger=Mock(), sample_seconds=6, needs_content=True)
    assert result is None and diagnostic['validation_error'] == error
    collector = incremental._CaptureTelemetry(True)
    collector.record(42, diagnostic, Mock())
    assert collector.snapshot()['validation_error_counts'] == {error: 1}
    assert not path.exists()


def test_watchdog_deletion_keeps_missing_partial_bytes_unknown(tmp_path, monkeypatch):
    child = Mock(returncode=124)
    def communicate(**kwargs):
        config = json.loads(kwargs['input'])
        Path(config['path']).unlink()  # child's independent deadline cleanup
        return '', None
    child.communicate.side_effect = communicate
    child.poll.return_value = 124
    monkeypatch.setattr(throughput.subprocess, 'Popen', Mock(return_value=child))
    result, path = throughput.capture_bounded_sample(PRIVATE_URL, duration_seconds=1,
        timeout_seconds=2, temp_directory=str(tmp_path))
    assert path is None and result['capture_code'] == 'capture_deadline'
    assert result['bytes'] is None
    diagnostic = incremental._capture_diagnostic(result)
    assert diagnostic['capture_mbps'] is None
    assert diagnostic['elapsed_source'] == 'parent_wall'
    assert not list(tmp_path.iterdir())


def test_new_diagnostics_only_use_system_log_and_do_not_persist_ui_progress(monkeypatch):
    from stream_sorter import plugin
    system_log, write_status = Mock(), Mock()
    monkeypatch.setattr(plugin, 'LOGGER', system_log)
    monkeypatch.setattr(plugin, '_update_status', write_status)
    logger = plugin._ProgressLogger('fixture-job')
    collector = incremental._CaptureTelemetry(False)
    for sid in range(100):
        collector.record(sid, incremental._capture_skip('reuse_disabled'), logger)
    logger.info('[Analyze Capture] summary=%s', json.dumps(collector.snapshot()))
    assert system_log.info.call_count == 101
    write_status.assert_not_called()


def test_release_error_propagates_and_still_cleans_sample(tmp_path, monkeypatch):
    path = tmp_path / 'sample'
    path.write_bytes(b'fixture')
    probe = Mock()
    monkeypatch.setattr(analyzer, 'analyze_stream', probe)
    with pytest.raises(RuntimeError, match='reservation release failed'):
        incremental._validate_shared_capture({'id': 42,
            '_release_provider': Mock(side_effect=RuntimeError('reservation release failed'))},
            measured(), str(path), settings={}, logger=Mock(), sample_seconds=6, needs_content=True)
    probe.assert_not_called()
    assert not path.exists()


def test_retry_is_forced_direct_and_reports_skip_without_extra_capture(pg_scan, monkeypatch):
    context = pg_scan
    monkeypatch.setattr(incremental, '_select_capture_temp_directory', lambda *a, **kw: str(context.tmp_path))
    monkeypatch.setattr(incremental, 'content_check_reason', lambda *a, **kw: None)
    monkeypatch.setattr(incremental, 'throughput_check_reason', lambda *a, **kw: None)
    probe = Mock(side_effect=[
        {'status': 'dead', 'error_type': 'timeout', 'stats': {}, 'details': {}},
        {'status': 'alive', 'stats': {}, 'details': {}}])
    monkeypatch.setattr(analyzer, 'analyze_stream', probe)
    capture = Mock(side_effect=AssertionError('metadata/retry must stay direct'))
    monkeypatch.setattr(incremental, 'capture_bounded_sample', capture)
    result = context.run(bounded_capture_reuse=True, analysis_retries=1)
    assert result['capture_reuse']['reason_counts'] == {
        'reuse_metadata_only': 1, 'reuse_forced_direct': 1}
    assert result['capture_reuse']['attempts'] == 0
    assert probe.call_count == context.capacity.release.call_count == 2
    capture.assert_not_called()
