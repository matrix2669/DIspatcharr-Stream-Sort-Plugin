"""Explicit resumable legacy analysis import; originals are never modified."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .storage import BATCH_SIZE, RunLease, StorageError, _current, _dump

MAX_ENTRY_BYTES = 16 * 1024 * 1024


def iter_entries(path):
    """Stream top-level JSON entries with a hard per-entry memory limit.

    The legacy format nests history inside each stream. Oversized individual
    entries are refused explicitly instead of loading the whole cache.
    """
    decoder = json.JSONDecoder()
    with open(path, encoding='utf-8') as source:
        buffer = ''
        ended = False

        def refill():
            nonlocal buffer, ended
            chunk = source.read(64 * 1024)
            buffer += chunk
            ended = not chunk
            if len(buffer.encode()) > MAX_ENTRY_BYTES:
                raise StorageError('Legacy stream entry exceeds 16 MiB import limit')

        def whitespace():
            nonlocal buffer
            buffer = buffer.lstrip()
            while not buffer and not ended:
                refill()
                buffer = buffer.lstrip()

        def consume(character):
            nonlocal buffer
            whitespace()
            if not buffer.startswith(character):
                raise StorageError('Invalid legacy analysis JSON')
            buffer = buffer[1:]

        def value():
            nonlocal buffer
            whitespace()
            while True:
                try:
                    parsed, end = decoder.raw_decode(buffer)
                    buffer = buffer[end:]
                    return parsed
                except json.JSONDecodeError as exc:
                    if ended:
                        raise StorageError('Truncated legacy analysis JSON') from exc
                    refill()

        consume('{')
        whitespace()
        if buffer.startswith('}'):
            consume('}')
            return
        while True:
            key = value()
            if not isinstance(key, str):
                raise StorageError('Legacy stream keys must be strings')
            consume(':')
            entry = value()
            if not key.isdigit() or not isinstance(entry, dict):
                raise StorageError('Legacy analysis requires numeric stream IDs and object entries')
            yield key, entry
            whitespace()
            if buffer.startswith('}'):
                consume('}')
                whitespace()
                if buffer:
                    raise StorageError('Trailing legacy analysis JSON')
                break
            consume(',')


def import_analysis(store, path):
    path = Path(path)
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    fingerprint = digest.hexdigest()
    source_key = 'legacy_analysis'
    imported = 0
    with RunLease(store) as run:
        with store.transaction() as cursor:
            store.fence(cursor, run.owner)
            cursor.execute('SELECT fingerprint, last_key FROM stream_sort_import WHERE source = %s', [source_key])
            checkpoint = cursor.fetchone()
            if checkpoint and checkpoint[0] != fingerprint:
                raise StorageError('Legacy analysis changed since import started; refusing to mix snapshots')
            completed = int(checkpoint[1]) if checkpoint and checkpoint[1] is not None else -1
            cursor.execute('INSERT INTO stream_sort_import VALUES (%s, %s, NULL) ON CONFLICT DO NOTHING', [source_key, fingerprint])
        for position, (sid, entry) in enumerate(iter_entries(path)):
            if position <= completed:
                continue
            if entry.get('stream_id') is not None and str(entry['stream_id']) != sid:
                raise StorageError('Legacy stream identity does not match its key')
            for kind in ('health_check_history', 'throughput_check_history'):
                rows = entry.get(kind) or []
                for offset in range(0, len(rows), BATCH_SIZE):
                    run.ensure()
                    with store.transaction() as cursor:
                        store.fence(cursor, run.owner)
                        for row in rows[offset:offset + BATCH_SIZE]:
                            # Deterministic record identity survives retries and
                            # partial imports; every legacy health row has its
                            # own historical run identity.
                            event_id = hashlib.sha256(_dump([fingerprint, sid, kind, row]).encode()).hexdigest()
                            cursor.execute('INSERT INTO stream_sort_history VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb) ON CONFLICT DO NOTHING', [event_id, f'legacy:{event_id}', int(sid), entry.get('m3u_account_id'), kind, row.get('checked_at'), _dump(row)])
                        store.fence(cursor, run.owner)
            with store.transaction() as cursor:
                store.fence(cursor, run.owner)
                cursor.execute('INSERT INTO stream_sort_current VALUES (%s, %s, %s::jsonb) ON CONFLICT (stream_id) DO NOTHING', [int(sid), entry.get('m3u_account_id'), _dump(_current(entry))])
                for day, data in (entry.get('health_daily_rollups') or {}).items():
                    cursor.execute('INSERT INTO stream_sort_rollup VALUES (%s, %s, %s::jsonb) ON CONFLICT DO NOTHING', [int(sid), day, _dump(data)])
                cursor.execute('INSERT INTO stream_sort_import VALUES (%s, %s, %s) ON CONFLICT (source) DO UPDATE SET last_key = EXCLUDED.last_key', [source_key, fingerprint, str(position)])
                store.fence(cursor, run.owner)
            imported += 1
    return {'imported_streams': imported, 'fingerprint': fingerprint, 'original_preserved': True,
            'scope': 'analysis_and_direct_history_only',
            'limitations': 'Playback and sort legacy histories are not imported; PostgreSQL sorting/report actions remain disabled.'}
