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

TABLES = ('component', 'current', 'history', 'rollup', 'playback', 'import', 'lease', 'schema')
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
