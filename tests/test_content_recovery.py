"""Cross-scan recovery obligations, with real JSON files and a PG contract double."""
import copy
import itertools
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from stream_sorter import analyzer, incremental, storage
from test_storage_backend import pg_scan

JSON_LOAD = analyzer.load_analysis_cache
JSON_SAVE = analyzer.save_analysis_cache


def legacy_content_dead(item, checked):
    return {'stream_id': item['id'], 'm3u_account_id': item['account_id'],
        'url_hash': analyzer._stream_url_hash(item['url']), 'status': 'dead',
        'error_type': 'black_screen', 'error': 'Confirmed black picture',
        'health_source': 'stream_sort_content', 'health_checked_at': checked,
        'dead_checked_at': checked, 'content_checked_at': checked,
        'content_m3u_account_id': item['account_id'], 'consecutive_dead_results': 1,
        'stats': {'width': 1920, 'height': 1080, 'source_fps': 25},
        'details': {'content': {'measured': True, 'black': True, 'tested_at': checked}}}


@pytest.mark.parametrize('backend', ['json', 'postgresql'])
def test_content_failure_survives_exhausted_media_failure_and_later_incomplete_scans(pg_scan, monkeypatch, backend):
    context = pg_scan
    item = {'id': 42, 'account_id': 7, 'url': 'http://example.test/source'}
    checked = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    initial = legacy_content_dead(item, checked)
    cache_path = context.tmp_path / 'unused.json'
    monkeypatch.setattr(analyzer, 'load_analysis_cache', JSON_LOAD)
    monkeypatch.setattr(analyzer, 'save_analysis_cache', JSON_SAVE)
    if backend == 'json':
        JSON_SAVE({'42': initial}, str(cache_path))
    else:
        context.store.current = {'42': copy.deepcopy(initial)}
    monkeypatch.setattr(incremental, 'throughput_check_reason', lambda *a, **kw: None)
    if backend == 'json':
        monkeypatch.setattr(incremental, 'load_throughput_cache', lambda path: {})
    # Exercise real retry orchestration without spending seconds in backoff.
    ticks = itertools.count()
    monkeypatch.setattr(incremental, 'time', SimpleNamespace(monotonic=lambda: float(next(ticks)), sleep=lambda _: None))
    mode = {'media': 'timeout', 'content': 'unmeasured'}
    probes, contents = [], []
    def probe(*args, **kwargs):
        probes.append(mode['media'])
        return {'status': 'dead' if mode['media'] == 'timeout' else 'alive',
            'error_type': 'timeout' if mode['media'] == 'timeout' else None,
            'error': 'FFprobe timed out' if mode['media'] == 'timeout' else '',
            'tested_at': datetime.now(timezone.utc).isoformat(),
            'stats': {} if mode['media'] == 'timeout' else initial['stats'], 'details': {}}
    def content(base, *args, **kwargs):
        contents.append(mode['content'])
        measured = mode['content'] == 'success'
        return {**base, 'status': 'alive', 'error_type': None, 'error': '',
            'details': {'content': {'measured': measured, 'black': False if measured else None,
                'tested_at': datetime.now(timezone.utc).isoformat()}}}
    monkeypatch.setattr(analyzer, 'analyze_stream', probe)
    monkeypatch.setattr(analyzer, 'apply_content_analysis', content)
    def read():
        return JSON_LOAD(str(cache_path))['42'] if backend == 'json' else copy.deepcopy(context.store.current['42'])
    def write(entry):
        if backend == 'json':
            JSON_SAVE({'42': entry}, str(cache_path))
        else:
            # The PG hot path loads compact current state, not historical rows.
            context.store.current = {'42': storage._current(entry)}
            context.store.staged.clear()
    def run(*, deferred=False):
        context.capacity.try_acquire.reset_mock(side_effect=True)
        context.capacity.try_acquire.return_value = (True, 'slot')
        if deferred:
            context.capacity.try_acquire.side_effect = [(True, 'metadata-slot'), (False, None)]
        return context.run(storage_backend=backend, analysis_retries=3, playback_health_reuse=False)

    # Scan 1: latest source/error changes completely; no pending retry remains.
    run()
    first = read()
    assert probes == ['timeout'] * 4 and not contents
    assert first['error_type'] == 'timeout' and first['health_source'] == 'stream_sort_analyzer'
    assert first['retry_pending'] is False and first['retry_telemetry']['retries_exhausted'] is True
    assert first['content_recovery_required'] is True
    assert first['content_checked_at'] == checked

    # Expire only the health gate. The failed content timestamp is still well
    # inside 168 hours and cannot stand in for completed recovery.
    first['health_checked_at'] = first['dead_checked_at'] = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    write(first)
    mode['media'] = 'alive'
    run(deferred=True)
    deferred = read()
    assert deferred['status'] == 'dead' and deferred['content_recovery_required'] is True
    assert deferred['retry_pending'] is True and not contents
    assert deferred['content_checked_at'] == checked
    assert deferred['health_check_history'][-1]['previous_status'] == 'dead'

    # Scan 3: a tool-unavailable/fail-open result still is not recovery proof.
    write(deferred)
    run()
    unmeasured = read()
    assert contents == ['unmeasured']
    assert unmeasured['status'] == 'dead' and unmeasured['content_recovery_required'] is True
    assert unmeasured['retry_pending'] is True and unmeasured['content_checked_at'] == checked
    assert unmeasured['health_check_history'][-1]['terminal'] is False

    # Only a completed applicable direct content result discharges the flag.
    write(unmeasured)
    mode['content'] = 'success'
    run()
    recovered = read()
    assert contents == ['unmeasured', 'success']
    assert recovered['status'] == 'alive' and recovered['content_recovery_required'] is False
    assert recovered['content_checked_at'] != checked
    assert recovered['health_check_history'][-1]['previous_status'] == 'dead'
    assert recovered['health_check_history'][-1]['terminal'] is True


@pytest.mark.parametrize('skipped', [False, True])
def test_content_merger_sets_debt_on_failure_and_clears_only_applicable_success(skipped):
    item = {'id': 42, 'account_id': 7, 'url': 'http://example.test/source'}
    now = datetime.now(timezone.utc).isoformat()
    failed = incremental._merge_content_result(item, {'status': 'alive'}, {
        'status': 'dead', 'error_type': 'black_screen', 'tested_at': now,
        'details': {'content': {'measured': True, 'black': True, 'tested_at': now}}})
    assert failed['content_recovery_required'] is True
    media_failure = incremental._merge_media_result(item, failed, {
        'status': 'dead', 'error_type': 'timeout', 'tested_at': now,
        'content_recovery_required': False, 'stats': {}, 'details': {}})
    assert media_failure['content_recovery_required'] is True
    metadata = incremental._merge_media_result(item, media_failure, {
        'status': 'alive', 'tested_at': now, 'stats': {}, 'details': {}}, record_history=False)
    assert metadata['content_recovery_required'] is True
    result = incremental._merge_content_result(item, metadata, {
        'status': 'alive', 'tested_at': now,
        'details': {'content': {'measured': True, 'skipped': skipped, 'tested_at': now}}})
    assert result['content_recovery_required'] is skipped
    assert result['status'] == ('dead' if skipped else 'alive')


def test_playback_and_core_metadata_imports_do_not_discharge_content_recovery():
    item = {'id': 42, 'account_id': 7, 'url': 'http://example.test/source',
        'dispatcharr_stats': {'width': 1920, 'height': 1080},
        'dispatcharr_stats_updated_at': datetime.now(timezone.utc).isoformat()}
    checked = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    entry = legacy_content_dead(item, checked)
    entry['content_recovery_required'] = True
    imported, _, _ = incremental._merge_dispatcharr_metadata(item, entry)
    cache = {'42': imported}
    incremental._sync_runtime_playback_evidence([item], cache, {'streams': {'42': {
        'last_clean_playback_at': datetime.now(timezone.utc).isoformat(),
        'last_clean_playback_seconds': 300, 'last_clean_playback_m3u_account_id': 7}}},
        min_clean_playback_seconds=60, min_throughput_playback_seconds=300)
    assert cache['42']['content_recovery_required'] is True
    assert cache['42']['status'] == 'dead'
    metadata = incremental._merge_media_result(item, cache['42'], {
        'status': 'alive', 'tested_at': datetime.now(timezone.utc).isoformat(),
        'stats': {}, 'details': {}}, record_history=False)
    assert incremental.content_check_reason(metadata, url_hash=metadata['url_hash'],
        ttl_hours=168, dead_ttl_hours=1, provider_id=7,
        now=datetime.now(timezone.utc)) == 'content_recovery_required'
