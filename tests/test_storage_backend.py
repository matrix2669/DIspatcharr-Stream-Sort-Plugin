import copy
import json
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from stream_sorter import analyzer, execution_control, incremental, storage
from stream_sorter.storage_import import iter_entries


class MemoryStore:
    """Behavior double; SQL/concurrency claims are reserved for real PG CI."""
    alias = 'default'
    def __init__(self, initial):
        self.current = copy.deepcopy(initial)
        self.staged = {}
        self.calls = []
        self.playback_calls = 0
        self.new_playback = None
        self.record_scan = Mock()
        self.persist_core = Mock()
        self.close_worker_connection = Mock()
    def acquire(self, owner):
        self.owner = owner
    def release(self, owner):
        assert owner == self.owner
    def heartbeat(self, owner):
        assert owner == self.owner
    def load(self, items, *, owner=None, history=False):
        self.calls.append(('load', [item['id'] for item in items], owner, history))
        result = {str(item['id']): copy.deepcopy(self.current[str(item['id'])])
                  for item in items if str(item['id']) in self.current}
        if owner:
            result.update({str(item['id']): copy.deepcopy(self.staged[str(item['id'])])
                           for item in items if str(item['id']) in self.staged})
        return result
    def flush(self, entries, owner, *, terminal=False):
        self.calls.append(('flush', list(entries), terminal))
        destination = self.current if terminal else self.staged
        destination.update(copy.deepcopy(entries))
    def playback(self, ids):
        self.playback_calls += 1
        self.calls.append(('playback', list(ids)))
        return self.new_playback if self.new_playback and self.playback_calls > 1 else {}


class QuietLease:
    def __init__(self, store):
        self.store = store
        self.owner = 'own-run'
        self.lost = __import__('threading').Event()
    def ensure(self):
        if self.lost.is_set():
            raise storage.LeaseLost('heartbeat lost')
    def __enter__(self):
        self.store.acquire(self.owner)
        return self
    def __exit__(self, *exc):
        self.store.release(self.owner)


@pytest.fixture
def pg_scan(tmp_path, monkeypatch):
    for key in ('ANALYSIS_EXECUTION_LOCK_PATH', 'ANALYSIS_EXECUTION_STATE_PATH',
                'ANALYSIS_CANCEL_PATH', 'ANALYSIS_CONTROL_LOCK_PATH'):
        monkeypatch.setattr(execution_control, key, str(tmp_path / key))
    monkeypatch.setattr(execution_control, '_active_token', None)
    monkeypatch.setattr(storage, '_active_lease', None)
    monkeypatch.setattr(storage, 'RunLease', QuietLease)
    now = datetime.now(timezone.utc)
    old = (now - timedelta(days=10)).isoformat()
    initial = {'42': {'stream_id': 42, 'm3u_account_id': 7,
        'status': 'dead', 'error_type': 'black_screen', 'error': 'confirmed black',
        'url_hash': analyzer._stream_url_hash('http://example.test/source'),
        'health_checked_at': old, 'ffprobe_checked_at': old, 'content_checked_at': old,
        'consecutive_dead_results': 3}, '99': {'status': 'dead'}}
    store = MemoryStore(initial)
    monkeypatch.setattr(storage, 'PostgresStore', lambda *args: store)
    class Rows(list):
        def select_related(self, *_args): return self
        def order_by(self, *_args): return self
    account = SimpleNamespace(id=7, name='Provider', get_user_agent_string=lambda: 'profile-agent')
    stream = SimpleNamespace(id=42, name='Forty Two', m3u_account=account,
        m3u_account_id=7, url='http://example.test/source', stream_stats={}, stream_stats_updated_at=None)
    rows = Rows([SimpleNamespace(channel_id=1, stream=stream)])
    for name in ('apps', 'apps.channels', 'apps.channels.models'):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    sys.modules['apps.channels.models'].ChannelStream = SimpleNamespace(objects=rows)
    monkeypatch.setattr('stream_sorter.sorter.resolve_channel_scope', lambda _: (None, {}))
    save_json = Mock(side_effect=AssertionError('PG must not write JSON cache'))
    monkeypatch.setattr(analyzer, 'save_analysis_cache', save_json)
    monkeypatch.setattr(analyzer, 'load_analysis_cache', Mock(side_effect=AssertionError('no PG JSON fallback')))
    monkeypatch.setattr(incremental, '_save_json', Mock())
    monkeypatch.setattr(incremental, 'load_throughput_cache', Mock(side_effect=AssertionError('no legacy read during PG scan')))
    capacity = SimpleNamespace(try_acquire=Mock(return_value=(True, 'reservation')),
                              release=Mock(), prepare_item=lambda item, _: dict(item))
    monkeypatch.setattr(incremental, 'build_capacity_manager', lambda *a, **kw: capacity)
    monkeypatch.setattr(incremental, 'probe_stream', lambda *a, **kw: {'tested_at': now.isoformat(), 'status': 'healthy', 'measured_mbps': 10})
    monkeypatch.setattr(incremental, 'capture_stream_sample', Mock(side_effect=AssertionError('combined path not expected')))
    settings = {'storage_backend': 'postgresql', 'analysis_workers': 1,
                'analysis_retries': 0, 'analysis_per_account_delay_seconds': 0,
                'probe_per_account_delay_seconds': 0, 'analysis_ttl_jitter_percent': 0}
    def run(**overrides):
        return incremental.analyze_assigned_streams({**settings, **overrides},
            logger=Mock(), cache_path=str(tmp_path / 'unused.json'))
    return SimpleNamespace(store=store, run=run, now=now, rows=rows,
                           capacity=capacity, tmp_path=tmp_path)


def test_phase_flush_requeries_only_affected_stream_and_keeps_terminal_dead(pg_scan, monkeypatch):
    context = pg_scan
    def probe(*args, **kwargs):
        return {'tested_at': context.now.isoformat(), 'status': 'alive',
                'stats': {'height': 1080, 'video_bitrate': 4000}, 'details': {}}
    def content(base, source, **kwargs):
        # This is AFTER FFprobe phase commit and targeted own-run requery.
        assert context.store.current['42']['status'] == 'dead'
        assert context.store.staged['42']['status'] == 'alive'
        calls = context.store.calls
        first_flush = next(i for i, row in enumerate(calls) if row[0] == 'flush')
        assert calls[first_flush + 1] == ('load', [42], 'own-run', False)
        assert calls[first_flush + 2] == ('playback', [42])
        return {**base, 'status': 'alive', 'details': {'content': {'measured': True, 'tested_at': context.now.isoformat()}}}
    monkeypatch.setattr(analyzer, 'analyze_stream', probe)
    monkeypatch.setattr(analyzer, 'apply_content_analysis', content)
    # Force separate content to exercise the phase boundary before throughput.
    monkeypatch.setattr(incremental, 'throughput_check_reason', lambda *a, **kw: None)
    context.run()
    entry = context.store.current['42']
    assert entry['status'] == 'alive'
    assert len(entry['health_check_history']) == 1
    assert entry['health_check_history'][0]['previous_status'] == 'dead'
    assert entry['health_check_history'][0]['terminal'] is True
    assert all(call[1] == [42] for call in context.store.calls if call[0] in {'load', 'playback'})
    assert context.store.current['99'] == {'status': 'dead'}
    assert context.capacity.release.call_count == 2


def test_fresh_playback_after_phase_avoids_due_content_for_previously_alive_stream(pg_scan, monkeypatch):
    context = pg_scan
    context.store.current['42'].update({'status': 'alive', 'error_type': None})
    fresh = (context.now + timedelta(seconds=1)).isoformat()
    context.store.new_playback = {'streams': {'42': {
        'last_clean_playback_at': fresh, 'last_clean_playback_seconds': 300,
        'last_clean_playback_m3u_account_id': 7,
    }}}
    monkeypatch.setattr(analyzer, 'analyze_stream', lambda *a, **kw: {
        'status': 'alive', 'tested_at': context.now.isoformat(), 'stats': {}, 'details': {}})
    monkeypatch.setattr(analyzer, 'apply_content_analysis', Mock(side_effect=AssertionError('fresh playback satisfies content only')))
    monkeypatch.setattr(incremental, 'throughput_check_reason', lambda *a, **kw: None)
    result = context.run()
    assert result['content_checked'] == 0
    assert context.store.current['42']['content_checked_at'] == fresh
    assert context.store.current['42']['health_check_history'][0]['previous_status'] == 'alive'


def test_cancel_flushes_completed_metadata_without_recovering_dead_pending_content(pg_scan, monkeypatch):
    context = pg_scan
    def probe(*args, **kwargs):
        execution_control.request_analysis_cancel()
        return {'status': 'alive', 'tested_at': context.now.isoformat(), 'stats': {'height': 720}, 'details': {}}
    monkeypatch.setattr(analyzer, 'analyze_stream', probe)
    with pytest.raises(execution_control.AnalysisCancelled):
        context.run()
    entry = context.store.current['42']
    assert entry['status'] == 'dead'
    assert entry['stats']['height'] == 720
    assert entry['retry_pending'] is True
    assert entry['health_check_history'][0]['terminal'] is False
    assert len(entry['health_check_history']) == 1
    assert context.capacity.release.call_count == 1


def test_empty_pg_scan_does_not_load_full_store_or_write_json(pg_scan):
    pg_scan.rows.clear()
    result = pg_scan.run()
    assert result['streams_selected'] == 0
    assert not any(row[0] == 'flush' for row in pg_scan.store.calls)
    assert pg_scan.store.calls[0][1] == []


def test_heartbeat_loss_stops_new_provider_operations_and_flush(pg_scan, monkeypatch):
    def probe(*args, **kwargs):
        storage.active_run().lost.set()
        return {'status': 'alive', 'tested_at': pg_scan.now.isoformat(), 'stats': {}, 'details': {}}
    monkeypatch.setattr(analyzer, 'analyze_stream', probe)
    with pytest.raises(storage.LeaseLost):
        pg_scan.run()
    assert pg_scan.capacity.try_acquire.call_count == 1
    assert pg_scan.capacity.release.call_count == 1
    assert not any(row[0] == 'flush' for row in pg_scan.store.calls)


def test_selector_never_falls_back_to_json_after_pg_error(monkeypatch):
    monkeypatch.setattr(storage, 'PostgresStore', Mock(side_effect=storage.StorageError('db unavailable')))
    assert storage.selected_store({}) is None
    assert storage.selected_store({'storage_backend': 'json'}) is None
    with pytest.raises(storage.StorageError, match='db unavailable'):
        storage.selected_store({'storage_backend': 'postgresql'})
    with pytest.raises(storage.StorageError):
        storage.selected_store({'storage_backend': 'typo'})


def test_legacy_streaming_import_bounded_record_and_invalid_json(tmp_path, monkeypatch):
    from stream_sorter import storage_import
    path = tmp_path / 'analysis.json'
    path.write_text(json.dumps({'42': {'status': 'alive'}, '43': {'status': 'dead'}}))
    assert [key for key, _ in iter_entries(path)] == ['42', '43']
    path.write_text('{"42": {')
    with pytest.raises(storage.StorageError, match='Truncated'):
        list(iter_entries(path))
    path.write_text(json.dumps({'42': {'data': 'x' * 1000}}))
    monkeypatch.setattr(storage_import, 'MAX_ENTRY_BYTES', 100)
    with pytest.raises(storage.StorageError, match='exceeds'):
        list(iter_entries(path))


@pytest.mark.parametrize('playable', [8, 0.2])
def test_shared_capture_wired_cleanup_reservation_and_newly_due_throughput(pg_scan, monkeypatch, playable):
    context = pg_scan
    current = context.store.current['42']
    current.update({'status': 'alive', 'stats': {'height': 720, 'width': 1280, 'source_fps': 25},
        'throughput': {'status': 'healthy', 'checked_at': context.now.isoformat(),
            'url_hash': current['url_hash'], 'm3u_account_id': 7, 'measured_mbps': 10}})
    # Initially fresh throughput becomes due only after resolution changes.
    monkeypatch.setattr(incremental, 'throughput_check_reason', lambda *a, **kw: None)
    monkeypatch.setattr(incremental, '_select_capture_temp_directory', lambda *a, **kw: str(context.tmp_path))
    paths = []
    captures = []
    def capture(url, **kwargs):
        captures.append((url, kwargs))
        path = context.tmp_path / 'bounded.sample'
        assert not path.exists()
        path.write_bytes(b'controlled')
        paths.append(path)
        return {'tested_at': context.now.isoformat(), 'complete': True, 'bytes': 1024,
                'measured_mbps': 12, 'elapsed_seconds': 8}, str(path)
    def metadata(source, **kwargs):
        local = kwargs.get('local_source', False)
        if local:
            assert Path(source).exists()
        else:
            assert playable < 1  # only the incomplete case uses live fallback
            assert source == 'http://native.test/profile'
        return {'status': 'alive', 'tested_at': context.now.isoformat(),
                'stats': {'height': 1080, 'width': 1920, 'source_fps': 25},
                'details': {'sample_duration_seconds': playable}}
    contents = []
    def content(base, source, **kwargs):
        contents.append((source, kwargs.get('local_source', False)))
        return {**base, 'status': 'alive', 'details': {
            'content': {'measured': True, 'tested_at': context.now.isoformat()}}}
    monkeypatch.setattr(incremental, 'capture_bounded_sample', capture)
    monkeypatch.setattr(analyzer, 'analyze_stream', metadata)
    monkeypatch.setattr(analyzer, 'apply_content_analysis', content)
    context.capacity.prepare_item = lambda item, _: {**item, 'url': 'http://native.test/profile', 'user_agent': 'profile-agent'}
    if playable < 1:
        # Legacy combined fallback is a separate reserved provider operation.
        def combined(url, **kwargs):
            path = context.tmp_path / 'fallback.ts'
            path.write_bytes(b'valid')
            paths.append(path)
            return {'status': 'healthy', 'tested_at': context.now.isoformat(), 'measured_mbps': 9}, str(path)
        monkeypatch.setattr(incremental, 'capture_stream_sample', combined)
        monkeypatch.setattr(incremental, '_analyze_local_capture', lambda item, base, path, **kw: (
            Path(path).unlink() or content(base, path, local_source=True)))
    result = context.run(bounded_capture_reuse=True)
    assert len(captures) == 1
    assert captures[0][0] == 'http://native.test/profile'
    assert captures[0][1]['user_agent'] == 'profile-agent'
    assert captures[0][1]['max_bytes'] == 32 * 1024 * 1024
    assert not any(path.exists() for path in paths)
    assert result['throughput_checked'] == result['throughput_attempted'] == 1
    assert result['content_checked'] == 1
    assert context.capacity.release.call_count == (1 if playable == 8 else 4)
    entry = context.store.current['42']
    assert entry['url_hash'] == analyzer._stream_url_hash('http://example.test/source')
    assert entry['throughput']['reason'] == 'media_changed'
    assert '_shared_capture' not in entry


def test_metadata_only_keeps_lightweight_direct_probe(pg_scan, monkeypatch):
    context = pg_scan
    monkeypatch.setattr(incremental, 'content_check_reason', lambda *a, **kw: None)
    monkeypatch.setattr(incremental, 'throughput_check_reason', lambda *a, **kw: None)
    monkeypatch.setattr(incremental, '_select_capture_temp_directory', lambda *a, **kw: str(context.tmp_path))
    monkeypatch.setattr(incremental, 'capture_bounded_sample', Mock(side_effect=AssertionError('metadata only must stay direct')))
    probe = Mock(return_value={'status': 'alive', 'tested_at': context.now.isoformat(), 'stats': {}, 'details': {}})
    monkeypatch.setattr(analyzer, 'analyze_stream', probe)
    context.run(bounded_capture_reuse=True)
    probe.assert_called_once()
    assert not probe.call_args.kwargs.get('local_source')


def test_shared_capture_file_count_never_exceeds_workers(pg_scan, monkeypatch):
    import threading
    context = pg_scan
    template = context.rows[0].stream
    for sid in (43, 44):
        stream = SimpleNamespace(**{**vars(template), 'id': sid})
        context.rows.append(SimpleNamespace(channel_id=sid, stream=stream))
        context.store.current[str(sid)] = copy.deepcopy(context.store.current['42'])
    monkeypatch.setattr(incremental, 'throughput_check_reason', lambda *a, **kw: None)
    monkeypatch.setattr(incremental, '_select_capture_temp_directory', lambda *a, **kw: str(context.tmp_path))
    barrier = threading.Barrier(2)
    lock = threading.Lock()
    seen = 0
    peak = 0
    def capture(url, **kwargs):
        nonlocal seen, peak
        with lock:
            seen += 1
            index = seen
            path = context.tmp_path / f'sample-{index}.ram'
            path.write_bytes(b'fixture')
            peak = max(peak, len(list(context.tmp_path.glob('*.ram'))))
        if index < 3:
            barrier.wait(timeout=3)
        return {'tested_at': context.now.isoformat(), 'complete': True,
                'measured_mbps': 12, 'elapsed_seconds': 8}, str(path)
    monkeypatch.setattr(incremental, 'capture_bounded_sample', capture)
    monkeypatch.setattr(analyzer, 'analyze_stream', lambda *a, **kw: {
        'status': 'alive', 'tested_at': context.now.isoformat(), 'stats': {},
        'details': {'sample_duration_seconds': 8}})
    monkeypatch.setattr(analyzer, 'apply_content_analysis', lambda base, *a, **kw: {
        **base, 'details': {'content': {'measured': True, 'tested_at': context.now.isoformat()}}})
    result = context.run(bounded_capture_reuse=True, analysis_workers=2)
    assert result['content_checked'] == 3
    assert peak == 2
    assert not list(context.tmp_path.glob('*.ram'))
    assert context.capacity.release.call_count == 3


def test_heartbeat_thread_closes_its_own_connection_and_fails_closed(monkeypatch):
    close_old = Mock()
    connection = Mock()
    django_db = types.ModuleType('django.db')
    django_db.close_old_connections = close_old
    django_db.connections = {'default': connection}
    monkeypatch.setitem(sys.modules, 'django.db', django_db)
    store = Mock(alias='default')
    run = storage.RunLease(store)
    run.stop.wait = Mock(return_value=False)
    heartbeat_store = Mock()
    heartbeat_store.heartbeat.side_effect = OSError('database unavailable')
    monkeypatch.setattr(storage, 'PostgresStore', Mock(return_value=heartbeat_store))
    run._loop()
    assert run.lost.is_set()
    assert close_old.call_count == 2
    connection.close.assert_called_once()
    heartbeat_store.heartbeat.assert_called_once_with(run.owner)
    with pytest.raises(storage.LeaseLost):
        run.ensure()


def test_unchanged_fully_cached_pg_scan_has_no_state_or_history_writes(pg_scan, monkeypatch):
    context = pg_scan
    entry = context.store.current['42']
    entry.update({'status': 'alive', 'error_type': None,
                  'ffprobe_checked_at': context.now.isoformat(),
                  'health_checked_at': context.now.isoformat(),
                  'content_checked_at': context.now.isoformat(),
                  'content_m3u_account_id': 7,
                  'metadata_updated_at': context.now.isoformat(),
                  'stats': {'width': 1920, 'height': 1080}})
    monkeypatch.setattr(incremental, 'throughput_check_reason', lambda *a, **kw: None)
    monkeypatch.setattr(analyzer, 'analyze_stream', Mock(side_effect=AssertionError('cached stream must not probe')))
    result = context.run()
    assert result['media_checked'] == result['content_checked'] == 0
    assert result['cache_path'] is None
    assert result['analysis_health_report_path'] is None
    assert not any(row[0] == 'flush' for row in context.store.calls)
    assert not any(row[0] == 'load' and row[3] for row in context.store.calls)
    context.capacity.try_acquire.assert_not_called()


def test_pg_placeholder_recovery_keeps_forced_content_after_phase_requery(pg_scan, monkeypatch):
    context = pg_scan
    context.store.current['42'].update({'error_type': 'placeholder_file',
        'content_checked_at': context.now.isoformat(), 'content_m3u_account_id': 7,
        'throughput': {'status': 'healthy', 'checked_at': context.now.isoformat(),
                       'm3u_account_id': 7, 'measured_mbps': 12}})
    calls = []
    def probe(*args, **kwargs):
        calls.append(kwargs['settings'].get('analysis_duration_seconds', 5))
        return {'status': 'alive', 'tested_at': context.now.isoformat(), 'stats': {}, 'details': {}}
    monkeypatch.setattr(analyzer, 'analyze_stream', probe)
    content = Mock(side_effect=lambda base, *a, **kw: {**base, 'details': {
        'content': {'measured': True, 'tested_at': context.now.isoformat()}}})
    monkeypatch.setattr(analyzer, 'apply_content_analysis', content)
    result = context.run(bounded_capture_reuse=True)
    assert calls == [1, 5]
    assert result['content_checked'] == result['throughput_checked'] == 1
    content.assert_called_once()
    assert context.store.current['42']['health_check_history'][0]['reason'] == 'content_placeholder_recovered'


@pytest.mark.parametrize('fallback,cancel', [(False, False), (True, False), (True, True)])
def test_local_processors_observe_exact_reservation_release(pg_scan, monkeypatch, fallback, cancel):
    context = pg_scan
    held, released = set(), []
    next_slot = iter(range(1, 20))
    def acquire(item):
        slot = next(next_slot)
        held.add(slot)
        return True, slot
    def release(slot):
        assert slot in held
        held.remove(slot)
        released.append(slot)
    context.capacity.try_acquire.side_effect = acquire
    context.capacity.release.side_effect = release
    monkeypatch.setattr(incremental, '_select_capture_temp_directory', lambda *a, **kw: str(context.tmp_path))
    monkeypatch.setattr(incremental, 'throughput_check_reason', lambda *a, **kw: None)
    def capture(*args, **kwargs):
        assert held == {1}
        path = context.tmp_path / 'sample.ram'
        path.write_bytes(b'sample')
        return {'tested_at': context.now.isoformat(), 'measured_mbps': 12, 'complete': True}, str(path)
    def metadata(source, **kwargs):
        local = kwargs.get('local_source', False)
        if local:
            assert released == [1] and not held
            if cancel:
                execution_control.request_analysis_cancel()
        else:
            assert fallback and not cancel
            assert held == {2}
        return {'status': 'alive', 'tested_at': context.now.isoformat(), 'stats': {},
                'details': {'sample_duration_seconds': .2 if fallback and local else 8}}
    def content(base, source, **kwargs):
        if kwargs.get('local_source'):
            assert released == [1] and not held
        return {**base, 'details': {'content': {'measured': True, 'tested_at': context.now.isoformat()}}}
    monkeypatch.setattr(incremental, 'capture_bounded_sample', capture)
    monkeypatch.setattr(analyzer, 'analyze_stream', metadata)
    monkeypatch.setattr(analyzer, 'apply_content_analysis', content)
    if cancel:
        with pytest.raises(execution_control.AnalysisCancelled):
            context.run(bounded_capture_reuse=True)
        assert released == [1]
    else:
        context.run(bounded_capture_reuse=True)
        assert len(released) == (3 if fallback else 1)
    assert len(released) == len(set(released))
    assert not held and not (context.tmp_path / 'sample.ram').exists()


def test_auxiliary_import_parser_streams_nested_maps_and_history(tmp_path):
    from stream_sorter.storage_import import iter_legacy_records
    path = tmp_path / 'aux.json'
    path.write_text(json.dumps({'streams': {'42': {'playback_starts': 5}}, 'channels': {'id:1': {'active_stream_id': 42}}}))
    with path.open() as handle:
        rows = list(iter_legacy_records(handle, 'reliability'))
    assert rows == [('streams', '42', {'playback_starts': 5}), ('channels', 'id:1', {'active_stream_id': 42})]


@pytest.mark.parametrize('error_type', ['black_screen', 'frozen_video', 'silent_audio', 'content_sample_incomplete', 'timeout'])
@pytest.mark.parametrize('outcome', ['negative', 'deferred', 'unmeasured'])
def test_confirmed_content_recovery_survives_pg_requery_and_requires_completion(
    pg_scan, monkeypatch, error_type, outcome,
):
    context = pg_scan
    old = (context.now - timedelta(hours=2)).isoformat()
    context.store.current['42'].update({'status': 'dead', 'error_type': error_type,
        'health_source': 'stream_sort_content', 'health_checked_at': old, 'dead_checked_at': old, 'content_checked_at': old,
        'content_m3u_account_id': 7, 'consecutive_dead_results': 1})
    monkeypatch.setattr(analyzer, 'analyze_stream', lambda *a, **kw: {
        'status': 'alive', 'tested_at': context.now.isoformat(), 'stats': {}, 'details': {}})
    monkeypatch.setattr(incremental, 'throughput_check_reason', lambda *a, **kw: None)
    def content(base, *args, **kwargs):
        assert context.store.current['42']['status'] == 'dead'
        assert context.store.staged['42']['status'] == 'alive'
        return {**base, 'status': 'dead' if outcome == 'negative' else 'alive',
            'error_type': error_type if outcome == 'negative' else None,
            'details': {'content': {'measured': outcome == 'negative', 'tested_at': context.now.isoformat()}}}
    check = Mock(side_effect=content)
    monkeypatch.setattr(analyzer, 'apply_content_analysis', check)
    if outcome == 'deferred':
        context.capacity.try_acquire.side_effect = [(True, 'metadata-slot'), (False, None)]
    result = context.run()
    entry = context.store.current['42']
    assert entry['status'] == 'dead'
    assert len(entry['health_check_history']) == 1
    assert entry['health_check_history'][0]['previous_status'] == 'dead'
    if outcome != 'deferred':
        assert entry['health_check_history'][0]['reason'] == 'content_dead_ttl_expired'
    assert check.call_count == (0 if outcome == 'deferred' else 1)
    if outcome != 'negative':
        assert entry['retry_pending'] is True
        assert entry['content_recovery_required'] is True
        assert entry['content_checked_at'] == old
        assert entry['health_check_history'][0]['terminal'] is False



def test_bounded_ram_admission_uses_configured_cap_not_legacy_remux_budget(tmp_path, monkeypatch):
    root = tmp_path / 'ram'
    available = incremental._CAPTURE_HEADROOM_BYTES + 2 * 32 * 1024 * 1024
    monkeypatch.setattr(incremental.os, 'statvfs', lambda path: SimpleNamespace(
        f_frsize=1, f_bsize=1, f_bavail=available))
    assert incremental._select_capture_temp_directory(2, shared_memory_root=str(root),
        bytes_per_worker=32 * 1024 * 1024) == str(root)
    assert incremental._select_capture_temp_directory(2, shared_memory_root=str(root)) is None
