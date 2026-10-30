"""Real disposable PostgreSQL tests. CI sets REQUIRE_POSTGRES_TESTS=1.

Never point these at an installed Dispatcharr database. Each test drops only
plugin tables in the explicitly configured disposable test database.
"""
import json
import os
import threading
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

if os.environ.get('REQUIRE_POSTGRES_TESTS') != '1':
    pytest.skip('Disposable PostgreSQL not configured; integration is pending CI', allow_module_level=True)

import django
from django.conf import settings
from django.db import connections

if not settings.configured:
    settings.configure(DATABASES={'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.environ['STREAM_SORT_TEST_DB'],
        'USER': os.environ['STREAM_SORT_TEST_USER'],
        'PASSWORD': os.environ['STREAM_SORT_TEST_PASSWORD'],
        'HOST': os.environ.get('STREAM_SORT_TEST_HOST', '127.0.0.1'),
        'PORT': os.environ.get('STREAM_SORT_TEST_PORT', '5432'),
        'CONN_MAX_AGE': 0,
    }}, USE_TZ=True, SECRET_KEY='disposable-test-only')
    django.setup()

from stream_sorter import storage
from stream_sorter.reliability import record_runtime_event
from stream_sorter.storage_import import import_analysis

TABLES = ('scan', 'sort_channel', 'sort_run', 'sort_snapshot', 'sort_rollup', 'sort_legacy_daily', 'component', 'current', 'history', 'rollup', 'playback', 'import', 'lease', 'schema')
ITEM = {'id': 42, 'account_id': 7}
NOW = datetime.now(timezone.utc).isoformat()


@pytest.fixture
def store():
    value = storage.PostgresStore()
    with value.connection.cursor() as cursor:
        # Connection failure is a test failure, never an integration skip.
        for name in TABLES:
            cursor.execute(f'DROP TABLE IF EXISTS stream_sort_{name}')
    value.initialize()
    yield value
    connections.close_all()


def count(store, table):
    with store.connection.cursor() as cursor:
        cursor.execute(f'SELECT count(*) FROM stream_sort_{table}')
        return cursor.fetchone()[0]


def seed(store, status='dead'):
    store.acquire('seed')
    store.flush({'42': {'status': status, 'm3u_account_id': 7}}, 'seed', terminal=True)
    store.release('seed')


def expire(store):
    with store.connection.cursor() as cursor:
        cursor.execute("UPDATE stream_sort_lease SET expires_at = clock_timestamp() - interval '1 second'")


def test_missing_newer_schema_and_idempotent_explicit_initialization(store):
    assert store.initialize()['initialized'] is False
    with store.connection.cursor() as cursor:
        cursor.execute('UPDATE stream_sort_schema SET version = 99')
    with pytest.raises(storage.StorageError, match='Unsupported'):
        store.check_schema()
    with pytest.raises(storage.StorageError, match='Unsupported'):
        store.initialize()
    with store.connection.cursor() as cursor:
        cursor.execute('DROP TABLE stream_sort_schema')
    with pytest.raises(storage.StorageError, match='explicitly initialize'):
        store.check_schema()


def test_phase_overlay_cannot_recover_confirmed_dead_for_other_readers(store):
    seed(store)
    store.acquire('scan')
    component = {'status': 'alive', 'stats': {'height': 1080}, 'ffprobe_checked_at': NOW, 'm3u_account_id': 7}
    store.flush({'42': component}, 'scan')
    assert store.load([ITEM])['42']['status'] == 'dead'
    assert store.load([ITEM], owner='scan')['42']['status'] == 'alive'
    assert store.load([{'id': 42, 'account_id': 999}], owner='scan') == {}
    component['health_check_history'] = [{'checked_at': NOW, 'previous_status': 'dead', 'status': 'alive', 'terminal': True}]
    store.flush({'42': component}, 'scan', terminal=True)
    store.flush({'42': component}, 'scan', terminal=True)
    loaded = store.load([ITEM], history=True)['42']
    assert loaded['status'] == 'alive'
    assert len(loaded['health_check_history']) == 1
    assert loaded['health_check_history'][0]['previous_status'] == 'dead'


def test_expired_owner_cannot_renew_write_or_release_takeover(store):
    seed(store)
    store.acquire('old')
    expire(store)
    with pytest.raises(storage.LeaseLost):
        store.heartbeat('old')
    store.acquire('new')
    with pytest.raises(storage.LeaseLost):
        store.flush({'42': {'status': 'alive', 'm3u_account_id': 7}}, 'old', terminal=True)
    store.release('old')
    store.heartbeat('new')
    assert store.load([ITEM])['42']['status'] == 'dead'


def test_batch_rollback_on_end_of_batch_fence_loss(store, monkeypatch):
    seed(store)
    store.acquire('scan')
    fence = store.fence
    calls = 0
    def fail_second(cursor, owner):
        nonlocal calls
        calls += 1
        fence(cursor, owner)
        if calls == 2:
            raise storage.LeaseLost('delayed worker expired during batch')
    monkeypatch.setattr(store, 'fence', fail_second)
    with pytest.raises(storage.LeaseLost):
        store.flush({'42': {'status': 'alive', 'm3u_account_id': 7}}, 'scan', terminal=True)
    monkeypatch.setattr(store, 'fence', fence)
    assert store.load([ITEM])['42']['status'] == 'dead'


def test_simultaneous_takeover_has_one_winner_and_old_owner_stays_fenced(store):
    store.acquire('old')
    expire(store)
    barrier = threading.Barrier(2)
    outcomes = []
    def contend(owner):
        contender = storage.PostgresStore()
        try:
            barrier.wait(timeout=3)
            contender.acquire(owner)
            outcomes.append(('won', owner))
        except storage.StorageError:
            outcomes.append(('busy', owner))
        finally:
            connections['default'].close()
    threads = [threading.Thread(target=contend, args=(owner,)) for owner in ('a', 'b')]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)
        assert not thread.is_alive()
    assert sorted(result for result, _ in outcomes) == ['busy', 'won']
    with pytest.raises(storage.LeaseLost):
        store.flush({'42': {'m3u_account_id': 7}}, 'old')


def test_playback_event_concurrent_with_scan_flush_survives(store):
    seed(store)
    store.acquire('scan')
    snapshot = store.load([ITEM])
    barrier = threading.Barrier(2)
    failures = []
    def playback():
        try:
            barrier.wait(timeout=3)
            record_runtime_event('channel_start', {'channel_id': 1, 'stream_id': 42},
                settings={'storage_backend': 'postgresql'},
                resolver=lambda sid, _: {'stream_id': sid, 'm3u_account_id': 7})
        except Exception as exc:
            failures.append(exc)
        finally:
            connections['default'].close()
    worker = threading.Thread(target=playback)
    worker.start()
    barrier.wait(timeout=3)
    store.flush(snapshot, 'scan', terminal=True)
    worker.join(timeout=15)
    assert not worker.is_alive() and not failures
    assert store.playback([42])['streams']['42']['playback_starts'] == 1
    assert store.load([ITEM])['42']['status'] == 'dead'
    assert count(store, 'history') == 1


def test_resumable_legacy_import_and_idempotent_history(store, tmp_path, monkeypatch):
    path = tmp_path / 'analysis.json'
    entry = {'m3u_account_id': 7, 'status': 'dead', 'health_check_history': [
        {'checked_at': NOW, 'previous_status': 'alive', 'status': 'dead'},
        {'checked_at': NOW, 'previous_status': 'dead', 'status': 'alive'},
    ]}
    original = json.dumps({'42': entry, '43': {'m3u_account_id': 7}})
    path.write_text(original)
    import_analysis(store, path)
    import_analysis(store, path)
    assert count(store, 'history') == 2
    assert count(store, 'current') == 2
    assert path.read_text() == original
    path.write_text('{}')
    with pytest.raises(storage.StorageError, match='changed'):
        import_analysis(store, path)


def test_reset_scan_preserves_playback_and_reset_all_clears_it(store):
    record_runtime_event('channel_start', {'channel_id': 1, 'stream_id': 42},
        settings={'storage_backend': 'postgresql'}, resolver=lambda sid, _: {'stream_id': sid})
    seed(store)
    store.acquire('reset')
    store.reset('reset', include_history=False)
    assert count(store, 'current') == 0
    assert count(store, 'playback') == 2
    assert count(store, 'history') == 1
    store.reset('reset', include_history=True)
    assert count(store, 'playback') == count(store, 'history') == 0


def test_bounded_lock_timeout_prevents_indefinite_writer_wait(store):
    store.acquire('scan')
    locked = threading.Event()
    release = threading.Event()
    def hold():
        blocker = storage.PostgresStore()
        try:
            with blocker.transaction() as cursor:
                cursor.execute('SELECT * FROM stream_sort_lease FOR UPDATE')
                locked.set()
                assert release.wait(5)
        finally:
            connections['default'].close()
    thread = threading.Thread(target=hold)
    thread.start()
    assert locked.wait(3)
    try:
        from django.db import OperationalError
        with pytest.raises(OperationalError):
            store.heartbeat('scan')
    finally:
        release.set()
        thread.join(5)


def test_import_resumes_after_committed_history_batch_without_duplicates(store, tmp_path, monkeypatch):
    path = tmp_path / 'analysis.json'
    rows = [{'checked_at': (datetime.now(timezone.utc) + timedelta(seconds=n)).isoformat(),
             'status': 'dead', 'previous_status': 'dead'} for n in range(104)]
    path.write_text(json.dumps({'42': {'m3u_account_id': 7, 'health_check_history': rows}}))
    original = store.fence
    calls = 0
    def interrupted(cursor, owner):
        nonlocal calls
        calls += 1
        if calls == 4:
            raise storage.LeaseLost('simulated interrupted migration')
        return original(cursor, owner)
    monkeypatch.setattr(store, 'fence', interrupted)
    with pytest.raises(storage.LeaseLost):
        import_analysis(store, path)
    assert count(store, 'history') == 100
    assert count(store, 'current') == 0
    monkeypatch.setattr(store, 'fence', original)
    import_analysis(store, path)
    assert count(store, 'history') == 104
    assert count(store, 'current') == 1


def test_default_lease_expiry_uses_database_time_and_finite_heartbeat(store):
    assert storage.HEARTBEAT_SECONDS == 20
    assert storage.LEASE_SECONDS == 120
    store.acquire('clock')
    with store.connection.cursor() as cursor:
        cursor.execute('SELECT extract(epoch FROM expires_at - clock_timestamp()) FROM stream_sort_lease')
        seconds = float(cursor.fetchone()[0])
    assert 118 < seconds <= 120
    store.heartbeat('clock')


def test_playback_older_event_cannot_overwrite_newer_current_state(store):
    resolver = lambda sid, _: {'stream_id': sid, 'm3u_account_id': 7}
    now = datetime.now(timezone.utc)
    for when in (now, now - timedelta(hours=1)):
        record_runtime_event('channel_start', {'channel_id': 1, 'stream_id': 42},
            now=when, settings={'storage_backend': 'postgresql'}, resolver=resolver)
    current = store.playback([42])['streams']['42']
    assert current['playback_starts'] == 1
    assert current['last_event_at'] == now.isoformat()


def test_expired_history_pruning_is_bounded_and_explicit(store):
    store.acquire('maintenance')
    old = (datetime.now(timezone.utc) - timedelta(days=100)).isoformat()
    with store.connection.cursor() as cursor:
        cursor.executemany('INSERT INTO stream_sort_history VALUES (%s, %s, 42, 7, %s, %s, %s::jsonb)',
            [(str(n), str(n), 'health_check_history', old, '{}') for n in range(1005)])
    assert store.retain('maintenance') == 1000
    assert count(store, 'history') == 5
    assert store.retain('maintenance') == 5


@pytest.fixture
def sort_case(store, tmp_path, monkeypatch):
    from types import SimpleNamespace
    import sys
    import types
    import uuid
    from django.db import models, connection
    from stream_sorter import analyzer, execution_control, sorter
    for key in ('ANALYSIS_EXECUTION_LOCK_PATH', 'ANALYSIS_EXECUTION_STATE_PATH', 'ANALYSIS_CONTROL_LOCK_PATH', 'ANALYSIS_CANCEL_PATH'):
        monkeypatch.setattr(execution_control, key, str(tmp_path / key))
    Meta = type('Meta', (), {'app_label': 'stream_sort_test', 'db_table': 'stream_sort_test_order'})
    row_model = type('SortRow' + uuid.uuid4().hex, (models.Model,), {
        '__module__': __name__, 'Meta': Meta,
        'order': models.IntegerField(), 'stream_id': models.IntegerField(), 'channel_id': models.IntegerField()})
    with connection.cursor() as cursor:
        cursor.execute('DROP TABLE IF EXISTS stream_sort_test_order')
    with connection.schema_editor() as editor:
        editor.create_model(row_model)
    row_model.objects.bulk_create([row_model(order=0, stream_id=42, channel_id=10), row_model(order=1, stream_id=43, channel_id=10)])
    account = SimpleNamespace(id=7, name='Provider', is_active=True)
    channel = SimpleNamespace(id=10, channel_number=100, name='Selected')
    rows = list(row_model.objects.order_by('order'))
    for row in rows:
        row.channel = channel
        row.stream = SimpleNamespace(id=row.stream_id, name=str(row.stream_id),
            url=f'http://example.test/{row.stream_id}', m3u_account_id=7, m3u_account=account,
            stream_stats={'width': 3840, 'height': 2160}, stream_stats_updated_at=None,
            is_stale=False)
    store.acquire('seed')
    now = datetime.now(timezone.utc).isoformat()
    store.flush({str(row.stream_id): {'status': 'dead' if row.stream_id == 42 else 'alive',
        'm3u_account_id': 7, 'url_hash': analyzer._stream_url_hash(row.stream.url),
        'stats': {'width': 1280, 'height': 720}, 'metadata_updated_at': now,
        'health_checked_at': now} for row in rows}, 'seed', terminal=True)
    store.release('seed')
    module = types.ModuleType('apps.channels.models')
    module.ChannelStream = row_model
    monkeypatch.setitem(sys.modules, 'apps', types.ModuleType('apps'))
    monkeypatch.setitem(sys.modules, 'apps.channels', types.ModuleType('apps.channels'))
    monkeypatch.setitem(sys.modules, 'apps.channels.models', module)
    monkeypatch.setattr(sorter, 'scoped_rows', lambda *a, **kw: (rows, {'match_mode': 'all_channels', 'selected_channel_count': 1}))
    monkeypatch.setattr(sorter, 'resolve_channel_scope', lambda *a, **kw: ({10}, {'match_mode': 'all_channels', 'selected_channel_count': 1}))
    yield store, row_model, rows
    with connection.schema_editor() as editor:
        editor.delete_model(row_model)


def test_pg_sort_commits_orders_and_history_together_and_dry_run_preserves_it(sort_case):
    from stream_sorter import sorter
    store, model, rows = sort_case
    result = sorter.sort_channels({'storage_backend': 'postgresql'}, apply=True, logger=Mock())
    assert result['channels'][0]['proposed_stream_ids'] == [43, 42]
    assert list(model.objects.order_by('order').values_list('stream_id', flat=True)) == [43, 42]
    report = store.sort_report([10])
    assert len(report['sort_history']) == 1
    assert report['sort_history'][0]['channels_changed'] == 1
    assert len(report['sort_history'][0]['changed_channels'][0]['movements']) == 2
    sorter.sort_channels({'storage_backend': 'postgresql'}, apply=False, logger=Mock())
    assert count(store, 'sort_run') == 1
    assert store.sort_report([999])['sort_history'] == []


def test_pg_sort_rollback_reverts_core_orders_and_history(sort_case, monkeypatch):
    from stream_sorter import sorter
    store, model, rows = sort_case
    original = storage.PostgresStore.save_sort
    def fail_after_history(self, *args, **kwargs):
        original(self, *args, **kwargs)
        raise RuntimeError('simulated failed sort commit')
    monkeypatch.setattr(storage.PostgresStore, 'save_sort', fail_after_history)
    with pytest.raises(RuntimeError, match='failed sort'):
        sorter.sort_channels({'storage_backend': 'postgresql'}, apply=True, logger=Mock())
    assert list(model.objects.order_by('order').values_list('stream_id', flat=True)) == [42, 43]
    assert count(store, 'sort_run') == count(store, 'sort_channel') == count(store, 'sort_snapshot') == 0


def test_pg_health_and_ttl_actions_read_scoped_current_and_retained_history(sort_case, monkeypatch):
    from stream_sorter import plugin
    store, model, rows = sort_case
    store.acquire('scan')
    store.record_scan('scan', [42, 43], {'streams_selected': 2})
    store.release('scan')
    monkeypatch.setattr(plugin, '_load_json', Mock(side_effect=AssertionError('no stale JSON')))
    settings = {'storage_backend': 'postgresql'}
    health = plugin._run_health_report_action(settings)
    assert health['status'] == 'ok' and health['result']['selected_streams'] == 2
    assert health['result']['status_counts']['dead'] == 1
    assert plugin._run_ttl_recommendation_action(settings)['status'] == 'ok'


def test_explicit_schema_v1_upgrade_is_required_and_transactional(store):
    with store.connection.cursor() as cursor:
        for name in TABLES[:6]:
            cursor.execute(f'DROP TABLE stream_sort_{name}')
        cursor.execute('UPDATE stream_sort_schema SET version = 1')
    with pytest.raises(storage.StorageError, match='Unsupported'):
        store.check_schema()
    assert store.initialize()['upgraded_from'] == 1
    store.check_schema()


def test_auxiliary_legacy_import_resumes_and_preserves_live_playback(store, tmp_path, monkeypatch):
    from stream_sorter import storage_import
    reliability = tmp_path / 'reliability.json'
    original = json.dumps({'channels': {'id:1': {'active_stream_id': 42, 'updated_at': NOW}},
        'streams': {'42': {'stream_id': 42, 'm3u_account_id': 7, 'playback_starts': 5,
            'last_event_at': NOW, 'recent_events': [{'event': 'channel_start', 'timestamp': NOW}],
            'playback_throughput_history': [{'observed_at': NOW, 'kind': 'clean_playback',
                'eligible_for_throughput': True, 'runtime_seconds': 300, 'measured_mbps': 12,
                'm3u_account_id': 7}]}}})
    reliability.write_text(original)
    advance = storage_import._advance
    def interrupted(cursor, source, position):
        if position == 1:
            raise RuntimeError('simulated interrupted playback import')
        advance(cursor, source, position)
    monkeypatch.setattr(storage_import, '_advance', interrupted)
    with pytest.raises(RuntimeError):
        storage_import.import_auxiliary(store, reliability, kind='reliability')
    assert count(store, 'history') == 2
    monkeypatch.setattr(storage_import, '_advance', advance)
    storage_import.import_auxiliary(store, reliability, kind='reliability')
    storage_import.import_auxiliary(store, reliability, kind='reliability')
    assert count(store, 'history') == 2
    assert store.playback([42])['streams']['42']['playback_starts'] == 5
    assert reliability.read_text() == original


def test_legacy_sort_import_deduplicates_and_keeps_unattributed_rollups(store, tmp_path):
    from stream_sorter.storage_import import import_auxiliary
    path = tmp_path / 'sort.json'
    now = datetime.now(timezone.utc)
    payload = {'mode': 'apply', 'sort_history': [
        {'generated_at': now.isoformat(), 'channels_evaluated': 10, 'channels_changed': 1,
         'changed_channels': [{'channel_id': 10, 'movements': [{'stream_id': 42, 'old_position': 1, 'new_position': 2}]}]},
        {'generated_at': (now + timedelta(seconds=1)).isoformat(), 'channels_evaluated': 10, 'channels_changed': 0, 'changed_channels': []}],
        'last_applied_snapshot': [{'channel_id': 10, 'stream_ids': [43, 42], 'streams': []}],
        'sort_daily_rollups': {now.date().isoformat(): {'applied_runs': 2, 'channels_evaluated': 20}}}
    original = json.dumps(payload)
    path.write_text(original)
    import_auxiliary(store, path, kind='sort')
    import_auxiliary(store, path, kind='sort')
    assert count(store, 'sort_run') == 2 and count(store, 'sort_snapshot') == 1
    report = store.sort_report([10])
    assert report['sort_daily_rollups'] == {}
    assert report['legacy_unscoped_daily_rollups'][now.date().isoformat()]['applied_runs'] == 2
    assert len(report['legacy_unscoped_runs']) == 2
    assert path.read_text() == original


def test_reset_all_waits_for_inflight_playback_before_deleting_prior_counters(store):
    import time
    record_runtime_event('channel_start', {'channel_id': 1, 'stream_id': 42},
        settings={'storage_backend': 'postgresql'}, resolver=lambda sid, _: {'stream_id': sid})
    store.acquire('reset-owner')
    loaded, allow_commit, resetting, reset_done = (threading.Event() for _ in range(4))
    failures, reset_pid = [], []
    def writer():
        worker_store = storage.PostgresStore()
        try:
            with worker_store.playback_event({'channel_id': 1, 'stream_id': 42},
                    lambda sid, _: {'stream_id': sid}) as data:
                entry = data['streams']['42']
                assert entry['playback_starts'] == 1
                entry['playback_starts'] = 2
                entry['last_event_at'] = datetime.now(timezone.utc).isoformat()
                loaded.set()
                assert allow_commit.wait(5)
        except Exception as exc:
            failures.append(exc)
        finally:
            connections['default'].close()
    def resetter():
        worker_store = storage.PostgresStore()
        try:
            with worker_store.connection.cursor() as cursor:
                cursor.execute('SELECT pg_backend_pid()')
                reset_pid.append(cursor.fetchone()[0])
            resetting.set()
            worker_store.reset('reset-owner', include_history=True)
        except Exception as exc:
            failures.append(exc)
        finally:
            reset_done.set()
            connections['default'].close()
    writer_thread = threading.Thread(target=writer)
    reset_thread = threading.Thread(target=resetter)
    writer_thread.start()
    try:
        assert loaded.wait(3)
        reset_thread.start()
        assert resetting.wait(3)
        deadline = time.monotonic() + 1.2
        waiting = False
        while time.monotonic() < deadline and not reset_done.is_set():
            with store.connection.cursor() as cursor:
                cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid = %s AND locktype = 'advisory' AND NOT granted)", [reset_pid[0]])
                waiting = cursor.fetchone()[0]
            if waiting:
                break
            time.sleep(.01)
        assert waiting, 'reset must serialize behind the already-loaded playback writer'
        assert not reset_done.is_set()
    finally:
        allow_commit.set()
        writer_thread.join(5)
        if reset_thread.ident is not None:
            reset_thread.join(5)
    assert not failures
    assert not writer_thread.is_alive() and not reset_thread.is_alive()
    assert count(store, 'playback') == count(store, 'history') == 0
    store.release('reset-owner')


def test_scope_freshness_requires_successful_superset_scan_not_overlap_or_cancel(sort_case):
    from stream_sorter import plugin
    store, model, rows = sort_case
    old = datetime.now(timezone.utc) - timedelta(days=10)
    for owner, ids, canceled in (('old-ab', [42, 43], False),
                                ('new-a', [42], False),
                                ('canceled-ab', [42, 43], True)):
        store.acquire(owner)
        store.record_scan(owner, ids, {'canceled': canceled, 'streams_selected': len(ids)})
        store.release(owner)
    with store.connection.cursor() as cursor:
        cursor.execute('UPDATE stream_sort_scan SET completed_at = %s WHERE run_id = %s', [old, 'old-ab'])
    assert store.latest_scan([42, 43])[0] == old
    assert store.latest_scan([43])[0] == old
    assert store.latest_scan([42])[0] > old
    assert store.latest_scan([999]) is None
    result = plugin._run_ttl_recommendation_action({'storage_backend': 'postgresql'})
    assert result['status'] == 'error'
    assert 'older than seven days' in result['message']


def test_content_recovery_obligation_survives_terminal_media_failure_and_phase_roundtrips(store):
    from stream_sorter import analyzer, incremental
    item = {'id': 42, 'account_id': 7, 'url': 'http://example.test/source'}
    checked = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    legacy = {'status': 'dead', 'error_type': 'black_screen', 'health_source': 'stream_sort_content',
              'content_checked_at': checked, 'content_m3u_account_id': 7,
              'url_hash': analyzer._stream_url_hash(item['url']), 'm3u_account_id': 7}
    timeout = {'status': 'dead', 'error_type': 'timeout', 'retry_pending': False,
               'tested_at': NOW, 'stats': {}, 'details': {}}
    failed = incremental._merge_media_result(item, legacy, timeout)
    store.acquire('media-failed')
    store.flush({'42': failed}, 'media-failed', terminal=True)
    store.release('media-failed')
    previous = store.load([ITEM])['42']
    assert previous['error_type'] == 'timeout' and previous['health_source'] == 'stream_sort_analyzer'
    assert previous['content_recovery_required'] is True and previous['retry_pending'] is False
    alive = {'status': 'alive', 'error_type': None, 'tested_at': NOW, 'stats': {}, 'details': {}}
    store.acquire('content-incomplete')
    component = incremental._merge_media_result(item, previous, alive, record_history=False)
    store.flush({'42': component}, 'content-incomplete')
    own = store.load([ITEM], owner='content-incomplete')['42']
    assert own['status'] == 'alive' and own['content_recovery_required'] is True
    assert store.load([ITEM])['42']['status'] == 'dead'
    unmeasured = {**alive, 'details': {'content': {'measured': False}}}
    incomplete = incremental._merge_content_result(item, own, unmeasured, history_previous_status='dead')
    store.flush({'42': incomplete}, 'content-incomplete', terminal=True)
    store.release('content-incomplete')
    persisted = store.load([ITEM])['42']
    assert persisted['status'] == 'dead' and persisted['content_recovery_required'] is True
    assert persisted['content_checked_at'] == checked
    passed = {**alive, 'details': {'content': {'measured': True, 'skipped': False, 'black': False}}}
    recovered = incremental._merge_content_result(item, persisted, passed, history_previous_status='dead')
    store.acquire('content-passed')
    store.flush({'42': recovered}, 'content-passed', terminal=True)
    store.release('content-passed')
    final = store.load([ITEM])['42']
    assert final['status'] == 'alive' and final['content_recovery_required'] is False
