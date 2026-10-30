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
            'scope': 'analysis_and_direct_history'}


class _LegacyReader:
    """Bounded JSON values; large maps/arrays are visited one entry at a time."""
    def __init__(self, handle):
        self.handle, self.buffer, self.eof = handle, '', False
        self.decoder = json.JSONDecoder()

    def peek(self):
        self.buffer = self.buffer.lstrip()
        while not self.buffer and not self.eof:
            chunk = self.handle.read(65536)
            self.eof = not chunk
            self.buffer += chunk
            self.buffer = self.buffer.lstrip()
        return self.buffer[:1]

    def take(self, expected):
        if self.peek() != expected:
            raise StorageError('Malformed legacy JSON')
        self.buffer = self.buffer[1:]

    def value(self):
        self.peek()
        while True:
            try:
                value, offset = self.decoder.raw_decode(self.buffer)
                # Numeric tokens may straddle read boundaries (e.g. 1.2e+5).
                # Do not accept a valid numeric prefix before its delimiter.
                numeric = isinstance(value, (int, float)) and not isinstance(value, bool)
                if numeric and not self.eof and (offset == len(self.buffer) or self.buffer[offset] not in ',]}: \t\r\n'):
                    chunk = self.handle.read(65536)
                    self.eof = not chunk
                    self.buffer += chunk
                    if len(self.buffer.encode()) > MAX_ENTRY_BYTES:
                        raise StorageError('Legacy entry exceeds 16 MiB import limit')
                    continue
                self.buffer = self.buffer[offset:]
                return value
            except json.JSONDecodeError as exc:
                if self.eof:
                    raise StorageError('Truncated legacy JSON') from exc
                chunk = self.handle.read(65536)
                self.eof = not chunk
                self.buffer += chunk
                if len(self.buffer.encode()) > MAX_ENTRY_BYTES:
                    raise StorageError('Legacy entry exceeds 16 MiB import limit')

    def entries(self, container):
        end = '}' if container == '{' else ']'
        self.take(container)
        position = 0
        if self.peek() == end:
            self.take(end)
            return
        while True:
            key = self.value() if container == '{' else position
            if container == '{':
                self.take(':')
            yield str(key), self.value()
            position += 1
            if self.peek() == end:
                self.take(end)
                return
            self.take(',')


def iter_legacy_records(handle, kind):
    reader = _LegacyReader(handle)
    reader.take('{')
    if reader.peek() == '}':
        reader.take('}')
        return
    sections = ({'streams': '{', 'channels': '{'} if kind == 'reliability' else
                {'sort_history': '[', 'last_applied_snapshot': '[',
                 'sort_daily_rollups': '{', 'channels': '['})
    while True:
        section = reader.value()
        reader.take(':')
        if section in sections:
            for key, value in reader.entries(sections[section]):
                if not isinstance(value, dict):
                    raise StorageError('Legacy record must be an object')
                yield section, key, value
        else:
            value = reader.value()
            if kind == 'sort' and section == 'mode':
                yield '_metadata', 'mode', {'value': value}
        if reader.peek() == '}':
            reader.take('}')
            if reader.peek():
                raise StorageError('Trailing legacy JSON')
            return
        reader.take(',')


def _import_checkpoint(store, owner, source, fingerprint):
    with store.transaction() as cursor:
        store.fence(cursor, owner)
        cursor.execute('SELECT fingerprint, last_key FROM stream_sort_import WHERE source = %s', [source])
        row = cursor.fetchone()
        if row and row[0] != fingerprint:
            raise StorageError(f'{source} changed since import started; refusing mixed snapshots')
        cursor.execute('INSERT INTO stream_sort_import VALUES (%s, %s, NULL) ON CONFLICT DO NOTHING', [source, fingerprint])
        return int(row[1]) if row and row[1] is not None else -1


def _advance(cursor, source, position):
    cursor.execute('UPDATE stream_sort_import SET last_key = %s WHERE source = %s', [str(position), source])


def import_auxiliary(store, path, *, kind):
    """Import reliability or sort records from one stable, open source inode."""
    from .sorter import _applied_snapshot
    source = 'legacy_' + kind
    imported = 0
    with open(path, encoding='utf-8') as handle:
        digest = hashlib.sha256()
        for chunk in iter(lambda: handle.read(65536), ''):
            digest.update(chunk.encode())
        fingerprint = digest.hexdigest()
        handle.seek(0)
        mode, explicit_snapshot = None, False
        if kind == 'sort':
            for section, key, value in iter_legacy_records(handle, kind):
                if section == '_metadata' and key == 'mode':
                    mode = value['value']
                explicit_snapshot = explicit_snapshot or section == 'last_applied_snapshot'
            handle.seek(0)
        with RunLease(store) as run:
            completed = _import_checkpoint(store, run.owner, source, fingerprint)
            for position, (section, key, value) in enumerate(iter_legacy_records(handle, kind)):
                if position <= completed:
                    continue
                if kind == 'reliability':
                    if section == 'streams' and (not key.isdigit() or str(value.get('stream_id', key)) != key):
                        raise StorageError('Legacy playback identity mismatch')
                    for history_kind in ('recent_events', 'playback_throughput_history') if section == 'streams' else ():
                        rows = value.get(history_kind) or []
                        for offset in range(0, len(rows), BATCH_SIZE):
                            with store.transaction() as cursor:
                                store.fence(cursor, run.owner)
                                for row in rows[offset:offset + BATCH_SIZE]:
                                    event_id = hashlib.sha256(_dump([key, history_kind, row]).encode()).hexdigest()
                                    cursor.execute('INSERT INTO stream_sort_history VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb) ON CONFLICT DO NOTHING', [event_id, 'playback', int(key), value.get('m3u_account_id'), history_kind, row.get('timestamp') or row.get('observed_at'), _dump(row)])
                                store.fence(cursor, run.owner)
                    with store.transaction() as cursor:
                        store.fence(cursor, run.owner)
                        cursor.execute('SELECT pg_advisory_xact_lock(1936946022)')
                        observed = value.get('updated_at') or value.get('last_event_at')
                        cursor.execute('INSERT INTO stream_sort_playback VALUES (%s, %s, %s, %s::jsonb) ON CONFLICT (kind, identity) DO UPDATE SET observed_at = EXCLUDED.observed_at, data = EXCLUDED.data WHERE stream_sort_playback.observed_at IS NULL OR EXCLUDED.observed_at > stream_sort_playback.observed_at', [section, key, observed, _dump(_current(value))])
                        _advance(cursor, source, position)
                        store.fence(cursor, run.owner)
                else:
                    if section == 'sort_history':
                        event_id = 'legacy:' + hashlib.sha256(_dump(value).encode()).hexdigest()
                        summary = {k: v for k, v in value.items() if k != 'changed_channels'}
                        summary['legacy_import'] = True
                        with store.transaction() as cursor:
                            store.fence(cursor, run.owner)
                            cursor.execute('INSERT INTO stream_sort_sort_run VALUES (%s, %s, %s::jsonb) ON CONFLICT DO NOTHING', [event_id, value.get('generated_at'), _dump(summary)])
                        channels = value.get('changed_channels') or []
                        for offset in range(0, len(channels), BATCH_SIZE):
                            with store.transaction() as cursor:
                                store.fence(cursor, run.owner)
                                for channel in channels[offset:offset + BATCH_SIZE]:
                                    channel = {**channel, 'changed': True, 'legacy_attribution': 'changed_channel_only'}
                                    cursor.execute('INSERT INTO stream_sort_sort_channel VALUES (%s, %s, %s::jsonb) ON CONFLICT DO NOTHING', [event_id, channel['channel_id'], _dump(channel)])
                                store.fence(cursor, run.owner)
                    with store.transaction() as cursor:
                        store.fence(cursor, run.owner)
                        if section in {'last_applied_snapshot', 'channels'}:
                            snapshot = value if section == 'last_applied_snapshot' else _applied_snapshot([value])[0]
                            # An explicit last-applied snapshot wins over a dry-run
                            # proposal; source record ordering never changes it.
                            if section == 'last_applied_snapshot' or (section == 'channels' and mode == 'apply' and not explicit_snapshot):
                                cursor.execute('INSERT INTO stream_sort_sort_snapshot VALUES (%s, %s::jsonb) ON CONFLICT (channel_id) DO UPDATE SET data = EXCLUDED.data WHERE stream_sort_sort_snapshot.data->>\'_legacy_import\' = \'true\'', [snapshot['channel_id'], _dump({**snapshot, '_legacy_import': True})])
                        elif section == 'sort_daily_rollups':
                            cursor.execute('INSERT INTO stream_sort_sort_legacy_daily VALUES (%s, %s::jsonb) ON CONFLICT DO NOTHING', [key, _dump(value)])
                        _advance(cursor, source, position)
                        store.fence(cursor, run.owner)
                imported += 1
    return {'imported_records': imported, 'fingerprint': fingerprint, 'original_preserved': True}


def import_legacy(store, analysis_path, reliability_path, sort_path):
    result = {'originals_preserved': True, 'sources': {}}
    for kind, path in (('analysis', analysis_path), ('reliability', reliability_path), ('sort', sort_path)):
        if not Path(path).exists():
            result['sources'][kind] = {'status': 'absent'}
        elif kind == 'analysis':
            result['sources'][kind] = import_analysis(store, path)
        else:
            result['sources'][kind] = import_auxiliary(store, path, kind=kind)
    result['legacy_attribution_note'] = 'Historical global sort daily totals remain explicitly unscoped; missing channel attribution is not invented.'
    return result
