"""Opt-in plugin-owned PostgreSQL storage. Importing this module performs no I/O.

All SQL identifiers are constants; values use Django parameters. No model/app
registration or private database credentials are needed.
"""
from __future__ import annotations

from contextlib import contextmanager
from functools import wraps
import hashlib
import json
import threading
import uuid

SCHEMA_VERSION = 1
BATCH_SIZE = 100
LEASE_SECONDS = 120
HEARTBEAT_SECONDS = 20
HISTORY_KEYS = ('health_check_history', 'throughput_check_history',
                'health_daily_rollups', 'recent_events', 'playback_throughput_history')
_active_lease = None


class StorageError(RuntimeError):
    pass


class LeaseLost(StorageError):
    pass


def backend_name(settings):
    value = str(settings.get('storage_backend') or 'json').strip().lower()
    if value not in {'json', 'postgresql'}:
        raise StorageError('storage_backend must be json or postgresql')
    return value


def reject_postgres(settings, action):
    if backend_name(settings) == 'postgresql':
        raise StorageError(f'{action} is not yet supported with PostgreSQL; no JSON state was read or written')


def selected_store(settings):
    return PostgresStore() if backend_name(settings) == 'postgresql' else None


def _dump(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str)


def _decode(value):
    return json.loads(value) if isinstance(value, str) else value


def _current(entry):
    return {key: value for key, value in entry.items() if key not in HISTORY_KEYS}


def _chunks(rows):
    rows = list(rows)
    for offset in range(0, len(rows), BATCH_SIZE):
        yield rows[offset:offset + BATCH_SIZE]


class PostgresStore:
    def __init__(self, alias='default'):
        from django.db import connections
        self.alias = alias
        self.connection = connections[alias]
        if self.connection.vendor != 'postgresql':
            raise StorageError('PostgreSQL storage requires the existing Django PostgreSQL connection')

    @contextmanager
    def transaction(self):
        from django.db import transaction
        with transaction.atomic(using=self.alias):
            with self.connection.cursor() as cursor:
                cursor.execute("SET LOCAL lock_timeout = '2s'")
                cursor.execute("SET LOCAL statement_timeout = '10s'")
                yield cursor

    def close_worker_connection(self):
        from django.db import close_old_connections, connections
        try:
            close_old_connections()
        finally:
            connections[self.alias].close()

    def check_schema(self):
        try:
            with self.transaction() as cursor:
                cursor.execute('SELECT version FROM stream_sort_schema WHERE singleton = 1')
                row = cursor.fetchone()
                if not row or row[0] != SCHEMA_VERSION:
                    raise StorageError(f'Unsupported Stream Sort schema {row[0] if row else None}; expected {SCHEMA_VERSION}')
        except StorageError:
            raise
        except Exception as exc:
            raise StorageError('Stream Sort schema unavailable; explicitly initialize/upgrade storage first') from exc

    def initialize(self):
        """The ONLY DDL entry point. Transactional and bounded across workers."""
        with self.transaction() as cursor:
            cursor.execute('SELECT pg_try_advisory_xact_lock(1936946021)')
            if not cursor.fetchone()[0]:
                raise StorageError('Another storage migration is running; try again after it completes')
            cursor.execute('CREATE TABLE IF NOT EXISTS stream_sort_schema (singleton integer PRIMARY KEY CHECK (singleton = 1), version integer NOT NULL)')
            cursor.execute('SELECT version FROM stream_sort_schema WHERE singleton = 1 FOR UPDATE')
            row = cursor.fetchone()
            if row:
                if row[0] != SCHEMA_VERSION:
                    raise StorageError(f'Unsupported Stream Sort schema {row[0]}; expected {SCHEMA_VERSION}')
                return {'schema_version': row[0], 'initialized': False}
            for ddl in (
                'CREATE TABLE stream_sort_lease (name text PRIMARY KEY, owner text NOT NULL, expires_at timestamptz NOT NULL)',
                'CREATE TABLE stream_sort_current (stream_id bigint PRIMARY KEY, provider_id bigint, data jsonb NOT NULL)',
                'CREATE TABLE stream_sort_component (owner text NOT NULL, stream_id bigint NOT NULL, provider_id bigint, data jsonb NOT NULL, PRIMARY KEY(owner, stream_id))',
                'CREATE TABLE stream_sort_history (event_id text PRIMARY KEY, run_id text NOT NULL, stream_id bigint NOT NULL, provider_id bigint, kind text NOT NULL, observed_at timestamptz NOT NULL, data jsonb NOT NULL)',
                'CREATE INDEX stream_sort_history_lookup ON stream_sort_history (stream_id, kind, observed_at)',
                'CREATE INDEX stream_sort_history_retention ON stream_sort_history (observed_at)',
                'CREATE UNIQUE INDEX stream_sort_terminal_once ON stream_sort_history(run_id, stream_id) WHERE kind = \'health_check_history\'',
                'CREATE TABLE stream_sort_rollup (stream_id bigint NOT NULL, day date NOT NULL, data jsonb NOT NULL, PRIMARY KEY(stream_id, day))',
                'CREATE TABLE stream_sort_playback (kind text NOT NULL, identity text NOT NULL, observed_at timestamptz, data jsonb NOT NULL, PRIMARY KEY(kind, identity))',
                'CREATE TABLE stream_sort_import (source text PRIMARY KEY, fingerprint text NOT NULL, last_key text)',
            ):
                cursor.execute(ddl)
            cursor.execute('INSERT INTO stream_sort_schema VALUES (1, %s)', [SCHEMA_VERSION])
        return {'schema_version': SCHEMA_VERSION, 'initialized': True}

    def acquire(self, owner, seconds=LEASE_SECONDS):
        self.check_schema()
        with self.transaction() as cursor:
            cursor.execute("INSERT INTO stream_sort_lease VALUES ('analysis', %s, clock_timestamp() + %s * interval '1 second') ON CONFLICT (name) DO UPDATE SET owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at WHERE stream_sort_lease.expires_at <= clock_timestamp() RETURNING owner", [owner, seconds])
            if not cursor.fetchone():
                raise StorageError('Another PostgreSQL analysis lease is active')
            # Abandoned components are private and never become terminal state.
            cursor.execute('DELETE FROM stream_sort_component WHERE (owner, stream_id) IN (SELECT owner, stream_id FROM stream_sort_component WHERE owner <> %s LIMIT 1000)', [owner])

    def fence(self, cursor, owner):
        cursor.execute("SELECT owner FROM stream_sort_lease WHERE name = 'analysis' AND owner = %s AND expires_at > clock_timestamp() FOR UPDATE", [owner])
        if not cursor.fetchone():
            raise LeaseLost('PostgreSQL analysis lease expired or changed owner; results were not committed')

    def heartbeat(self, owner, seconds=LEASE_SECONDS):
        with self.transaction() as cursor:
            self.fence(cursor, owner)
            cursor.execute("UPDATE stream_sort_lease SET expires_at = clock_timestamp() + %s * interval '1 second' WHERE name = 'analysis' AND owner = %s", [seconds, owner])

    def release(self, owner):
        with self.transaction() as cursor:
            cursor.execute("DELETE FROM stream_sort_lease WHERE name = 'analysis' AND owner = %s", [owner])

    def load(self, items, *, owner=None, history=False):
        result = {}
        for batch in _chunks(items):
            ids = [int(item['id']) for item in batch]
            with self.transaction() as cursor:
                if owner:
                    self.fence(cursor, owner)
                cursor.execute('SELECT stream_id, provider_id, data FROM stream_sort_current WHERE stream_id = ANY(%s)', [ids])
                rows = cursor.fetchall()
                if owner:
                    cursor.execute('SELECT stream_id, provider_id, data FROM stream_sort_component WHERE owner = %s AND stream_id = ANY(%s)', [owner, ids])
                    rows += cursor.fetchall()
                providers = {int(item['id']): item.get('account_id') for item in batch}
                for sid, provider, data in rows:
                    if provider == providers[sid]:
                        result[str(sid)] = _decode(data)
                rollup_window = 'day >= CURRENT_DATE - 365' if history else 'day = CURRENT_DATE'
                cursor.execute(f'SELECT stream_id, day, data FROM stream_sort_rollup WHERE stream_id = ANY(%s) AND {rollup_window}', [ids])
                for sid, day, data in cursor.fetchall():
                    result.setdefault(str(sid), {}).setdefault('health_daily_rollups', {})[str(day)] = _decode(data)
                if history:
                    cursor.execute("SELECT wanted.sid, h.kind, h.data FROM unnest(%s::bigint[]) wanted(sid) CROSS JOIN LATERAL (SELECT kind, data, observed_at, event_id FROM stream_sort_history WHERE stream_id = wanted.sid AND kind = 'health_check_history' AND observed_at >= clock_timestamp() - interval '90 days' ORDER BY observed_at DESC, event_id DESC LIMIT 10000) h UNION ALL SELECT wanted.sid, h.kind, h.data FROM unnest(%s::bigint[]) wanted(sid) CROSS JOIN LATERAL (SELECT kind, data, observed_at, event_id FROM stream_sort_history WHERE stream_id = wanted.sid AND kind = 'throughput_check_history' AND observed_at >= clock_timestamp() - interval '90 days' ORDER BY observed_at DESC, event_id DESC LIMIT 512) h", [ids, ids])
                    histories = cursor.fetchall()
                    histories.sort(key=lambda row: str(_decode(row[2]).get('checked_at') or ''))
                    for sid, kind, data in histories:
                        result.setdefault(str(sid), {}).setdefault(kind, []).append(_decode(data))
        return result

    def flush(self, entries, owner, *, terminal=False):
        for batch in _chunks(entries.items()):
            with self.transaction() as cursor:
                self.fence(cursor, owner)
                for sid, entry in batch:
                    provider = entry.get('m3u_account_id')
                    if terminal:
                        cursor.execute('INSERT INTO stream_sort_current VALUES (%s, %s, %s::jsonb) ON CONFLICT (stream_id) DO UPDATE SET provider_id = EXCLUDED.provider_id, data = EXCLUDED.data', [int(sid), provider, _dump(_current(entry))])
                        self._history(cursor, sid, entry, owner)
                        cursor.execute('DELETE FROM stream_sort_component WHERE owner = %s AND stream_id = %s', [owner, int(sid)])
                    else:
                        cursor.execute('INSERT INTO stream_sort_component VALUES (%s, %s, %s, %s::jsonb) ON CONFLICT (owner, stream_id) DO UPDATE SET provider_id = EXCLUDED.provider_id, data = EXCLUDED.data', [owner, int(sid), provider, _dump(_current(entry))])
                # Recheck after all statements, not merely before the batch.
                self.fence(cursor, owner)

    def _history(self, cursor, sid, entry, run):
        for kind in ('health_check_history', 'throughput_check_history'):
            for row in entry.get(kind) or []:
                event_id = hashlib.sha256(_dump([run, sid, kind, row]).encode()).hexdigest()
                cursor.execute('INSERT INTO stream_sort_history VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb) ON CONFLICT DO NOTHING', [event_id, run, int(sid), entry.get('m3u_account_id'), kind, row.get('checked_at'), _dump(row)])
        observed_days = {str(row.get('checked_at') or '')[:10] for row in entry.get('health_check_history') or []}
        for day, data in (entry.get('health_daily_rollups') or {}).items():
            if day not in observed_days:
                continue
            cursor.execute('INSERT INTO stream_sort_rollup VALUES (%s, %s, %s::jsonb) ON CONFLICT (stream_id, day) DO UPDATE SET data = stream_sort_rollup.data || EXCLUDED.data', [int(sid), day, _dump(data)])

    def playback(self, ids):
        result = {'schema_version': 2, 'streams': {}, 'channels': {}, 'scoring_enabled': True}
        for batch in _chunks(ids):
            with self.transaction() as cursor:
                cursor.execute("SELECT identity, data FROM stream_sort_playback WHERE kind = 'streams' AND identity = ANY(%s)", [[str(sid) for sid in batch]])
                result['streams'].update({key: _decode(data) for key, data in cursor.fetchall()})
                # One latest eligible row per stream is enough for component
                # freshness; event processing loads no historical arrays.
                cursor.execute("SELECT wanted.sid, h.data FROM unnest(%s::bigint[]) AS wanted(sid) CROSS JOIN LATERAL (SELECT data FROM stream_sort_history WHERE stream_id = wanted.sid AND kind = 'playback_throughput_history' AND data->>'eligible_for_throughput' = 'true' AND observed_at >= clock_timestamp() - interval '90 days' ORDER BY observed_at DESC LIMIT 1) h", [[int(sid) for sid in batch]])
                for sid, value in cursor.fetchall():
                    result['streams'].setdefault(str(sid), {})['playback_throughput_history'] = [_decode(value)]
        return result

    @contextmanager
    def playback_event(self, payload, resolver):
        """Serialize only playback writers. Scan writes never touch these rows."""
        with self.transaction() as cursor:
            cursor.execute('SELECT pg_advisory_xact_lock(1936946022)')
            data = {'schema_version': 2, 'channels': {}, 'streams': {}, 'scoring_enabled': True}
            keys = [f"id:{payload.get('channel_id')}", f"name:{payload.get('channel_name')}"]
            cursor.execute("SELECT identity, data FROM stream_sort_playback WHERE kind = 'channels' AND identity = ANY(%s)", [keys])
            data['channels'] = {key: _decode(value) for key, value in cursor.fetchall()}
            prior_channel_keys = set(data['channels'])
            ids = {payload.get('stream_id'), payload.get('previous_stream_id')}
            resolved = resolver(payload.get('stream_id'), payload)
            if resolved:
                ids.add(resolved.get('stream_id'))
            for state in data['channels'].values():
                ids.update(state.get(key) for key in ('active_stream_id', 'last_switch_previous_stream_id', 'last_switch_stream_id'))
            ids.discard(None)
            data['streams'] = self.playback(ids)['streams']
            yield data
            removed_keys = list(prior_channel_keys - set(data['channels']))
            if removed_keys:
                cursor.execute("DELETE FROM stream_sort_playback WHERE kind = 'channels' AND identity = ANY(%s)", [removed_keys])
            for kind in ('channels', 'streams'):
                for identity, value in data[kind].items():
                    observed = value.get('updated_at') or value.get('last_event_at')
                    cursor.execute('INSERT INTO stream_sort_playback VALUES (%s, %s, %s, %s::jsonb) ON CONFLICT (kind, identity) DO UPDATE SET observed_at = EXCLUDED.observed_at, data = EXCLUDED.data WHERE stream_sort_playback.observed_at IS NULL OR EXCLUDED.observed_at >= stream_sort_playback.observed_at', [kind, identity, observed, _dump(_current(value))])
                    # Keep events in rows, never a growing history JSONB blob.
                    for history_kind in ('recent_events', 'playback_throughput_history'):
                        for row in value.get(history_kind) or []:
                            event_id = hashlib.sha256(_dump([identity, history_kind, row]).encode()).hexdigest()
                            cursor.execute('INSERT INTO stream_sort_history VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb) ON CONFLICT DO NOTHING', [event_id, 'playback', int(identity), value.get('m3u_account_id'), history_kind, row.get('timestamp') or row.get('observed_at'), _dump(row)])

    def retain(self, owner, limit=1000):
        # Bounded maintenance, deliberately separate from scanning.
        with self.transaction() as cursor:
            self.fence(cursor, owner)
            cursor.execute("DELETE FROM stream_sort_history WHERE event_id IN (SELECT event_id FROM stream_sort_history WHERE observed_at < clock_timestamp() - interval '90 days' ORDER BY observed_at LIMIT %s)", [min(1000, max(1, limit))])
            removed = cursor.rowcount
            cursor.execute("DELETE FROM stream_sort_rollup WHERE (stream_id, day) IN (SELECT stream_id, day FROM stream_sort_rollup WHERE day < CURRENT_DATE - 365 ORDER BY day LIMIT %s)", [min(1000, max(1, limit))])
            total_removed = removed + cursor.rowcount
            self.fence(cursor, owner)
            return total_removed

    def persist_core(self, results, owner, logger):
        from apps.channels.models import Stream
        from . import analyzer
        from .incremental import _parse_datetime
        for batch in _chunks(results.items()):
            with self.transaction() as cursor:
                self.fence(cursor, owner)
                locked = {stream.id: stream for stream in Stream.objects.filter(
                    id__in=[sid for sid, _ in batch]).select_for_update()}
                for sid, result in batch:
                    stream = locked.get(sid)
                    if stream is None:
                        continue
                    previous = _parse_datetime(getattr(stream, 'stream_stats_updated_at', None))
                    tested = _parse_datetime(result.get('tested_at'))
                    if previous is not None and (tested is None or previous > tested):
                        continue
                    # Keep model.save/update_fields and its signals; no bulk
                    # update until Dispatcharr signal equivalence is proven.
                    if not analyzer._persist_dispatcharr_result(sid, result, logger):
                        raise StorageError(f'Core metadata update failed for stream {sid}')
                self.fence(cursor, owner)

    def reset(self, owner, *, include_history):
        with self.transaction() as cursor:
            self.fence(cursor, owner)
            for table in ('stream_sort_current', 'stream_sort_component', 'stream_sort_rollup', 'stream_sort_import'):
                cursor.execute(f'DELETE FROM {table}')
            if include_history:
                cursor.execute('DELETE FROM stream_sort_playback')
                cursor.execute('DELETE FROM stream_sort_history')
            else:
                cursor.execute("DELETE FROM stream_sort_history WHERE kind IN ('health_check_history', 'throughput_check_history')")
            self.fence(cursor, owner)


class RunLease:
    def __init__(self, store):
        self.store = store
        self.owner = uuid.uuid4().hex
        self.lost = threading.Event()
        self.stop = threading.Event()
        self.thread = None

    def __enter__(self):
        self.store.acquire(self.owner)
        self.thread = threading.Thread(target=self._loop, name='stream-sort-db-lease', daemon=True)
        self.thread.start()
        return self

    def _loop(self):
        from django.db import close_old_connections, connections
        try:
            while not self.stop.wait(HEARTBEAT_SECONDS):
                try:
                    close_old_connections()
                    PostgresStore(self.store.alias).heartbeat(self.owner)
                except Exception:
                    self.lost.set()
                    return
                finally:
                    close_old_connections()
        finally:
            connections[self.store.alias].close()

    def ensure(self):
        if self.lost.is_set():
            raise LeaseLost('PostgreSQL heartbeat failed; new provider work and writes stopped')

    def __exit__(self, *exc):
        self.stop.set()
        self.thread.join(timeout=12)
        try:
            self.store.release(self.owner)
        except Exception:
            if exc[0] is None:
                raise


def provider_work_allowed():
    return _active_lease is None or not _active_lease.lost.is_set()


def ensure_provider_work():
    if _active_lease is not None:
        _active_lease.ensure()
        # The heartbeat may not yet have noticed expiry after a process pause.
        try:
            PostgresStore(_active_lease.store.alias).heartbeat(_active_lease.owner)
        except Exception:
            _active_lease.lost.set()
            raise


def postgres_scan(function):
    @wraps(function)
    def wrapped(settings, *args, **kwargs):
        global _active_lease
        store = selected_store(settings)
        if store is None:
            return function(settings, *args, **kwargs)
        with RunLease(store) as lease:
            _active_lease = lease
            try:
                return function(settings, *args, **kwargs)
            finally:
                _active_lease = None
    return wrapped


def active_run():
    return _active_lease
