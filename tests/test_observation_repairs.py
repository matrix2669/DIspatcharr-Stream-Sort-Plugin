"""Review regressions: real scheduler/collector paths with offline I/O doubles."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import subprocess
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from stream_sorter import analyzer, incremental, observations as obs, storage
from test_observations import archive, completed, probe_data, rows, ITEM, SECRET, URL
from test_storage_backend import pg_scan


def test_mid_phase_archive_commit_longer_than_lock_timeout_does_not_lose_lease(monkeypatch):
    batch_started, renewal_waiting = threading.Event(), threading.Event()
    background_waiting = threading.Event()
    # Model the actual PostgreSQL row lock and its two-second timeout. Use the
    # production store methods/decorators, Archive, RunLease and fair scheduler.
    class Cursor:
        def __init__(self, store):
            self.store, self.held, self.row = store, False, None
        def execute(self, sql, params):
            if sql.startswith('SELECT owner FROM stream_sort_lease'):
                if not self.held:
                    if not self.store.row_lock.acquire(timeout=2):
                        raise TimeoutError('lease row lock timeout')
                    self.held = True
                self.row = (params[0],) if params[0] == self.store.owner else None
            elif sql.startswith('INSERT INTO stream_sort_observation VALUES'):
                self.store.ids.add(params[0])
                if not self.store.delayed:
                    self.store.delayed = True
                    batch_started.set()
                    assert renewal_waiting.wait(5)
                    assert background_waiting.wait(5)
                    time.sleep(2.3)
            elif sql.startswith('UPDATE stream_sort_lease'):
                self.store.renewals += 1
        def fetchone(self):
            return self.row
    class RowLockStore(storage.PostgresStore):
        alias = 'default'
        def __init__(self):
            self.row_lock = threading.Lock()
            self.thread_state = threading.local()
            self.ids, self.delayed, self.renewals = set(), False, 0
            self.close_worker_connection = Mock()
        @contextmanager
        def transaction(self):
            cursor = Cursor(self)
            self.thread_state.in_transaction = True
            try:
                yield cursor
            finally:
                if cursor.held:
                    self.row_lock.release()
                self.thread_state.in_transaction = False
    store = RowLockStore()
    lease = storage.RunLease(store)
    store.owner = lease.owner
    monkeypatch.setattr(storage, '_active_lease', lease)
    monkeypatch.setattr(storage, 'PostgresStore', lambda *a: store)
    monkeypatch.setattr(obs, 'provenance', lambda *a: {'archive_schema': 3})
    monkeypatch.setattr(incremental, 'analysis_cancel_requested', lambda: False)
    items = [{'id': i, 'account_id': 7} for i in range(101)]
    lease.observations = obs.Archive(store, lease.owner, {}, items)
    capacity = SimpleNamespace(try_acquire=lambda item: (True, item['id']),
        prepare_item=lambda item, _: dict(item), release=Mock())
    @obs.probe('metadata', 'direct_ffprobe')
    def actual():
        return {'status': 'alive'}
    def worker(item):
        if item['id'] == 0:
            assert batch_started.wait(10)
            renewal_waiting.set()
            storage.ensure_provider_work()
            # The transaction guard has ended before this provider-work slot.
            assert not store.thread_state.in_transaction
        return actual()
    background_failures = []
    def background_renewal():
        try:
            assert batch_started.wait(10)
            background_waiting.set()
            store.heartbeat(lease.owner)
        except Exception as exc:
            background_failures.append(exc)
    background = threading.Thread(target=background_renewal)
    background.start()
    try:
        results = [future.result() for _, future in incremental._fair_account_futures(
            items, worker, max_workers=2, thread_name_prefix='stream-sort-media', capacity_manager=capacity)]
    finally:
        background.join(5)
    assert not background.is_alive() and not background_failures
    assert results == [{'status': 'alive'}] * 101
    assert not lease.lost.is_set() and store.owner == lease.owner
    assert store.renewals == 103 and len(store.ids) == 101
    assert capacity.release.call_count == 101
    assert sorted(call.args[0] for call in capacity.release.call_args_list) == list(range(101))
    assert lease.observations.snapshot()['committed_rows'] == 101


@pytest.mark.parametrize('failure', [storage.LeaseLost('fixture lease loss'), storage.StorageError('fixture DB failure')])
def test_storage_failure_never_enters_media_handler_or_persistent_dead(pg_scan, monkeypatch, failure):
    monkeypatch.setattr(analyzer, 'analyze_stream', Mock(side_effect=failure))
    log_media = Mock()
    monkeypatch.setattr(incremental, '_log_media_progress', log_media)
    with pytest.raises(type(failure), match='fixture'):
        pg_scan.run()
    log_media.assert_not_called()
    pg_scan.store.persist_core.assert_not_called()
    assert pg_scan.store.current['42']['error_type'] == 'black_screen'
    assert pg_scan.capacity.release.call_count == 1


def test_scheduler_storage_abort_joins_inflight_work_before_releasing_all_slots(monkeypatch):
    other_started, other_finished = threading.Event(), threading.Event()
    monkeypatch.setattr(storage, '_active_lease', None)
    monkeypatch.setattr(incremental, 'analysis_cancel_requested', lambda: False)
    def release(reservation):
        if reservation == 1:
            assert other_finished.is_set()
    capacity = SimpleNamespace(try_acquire=lambda item: (True, item['id']),
        prepare_item=lambda item, _: dict(item), release=Mock(side_effect=release))
    def worker(item):
        if item['id'] == 0:
            assert other_started.wait(5)
            raise storage.LeaseLost('fixture loss')
        other_started.set()
        time.sleep(.05)
        other_finished.set()
    with pytest.raises(storage.LeaseLost, match='fixture loss'):
        list(incremental._fair_account_futures([{'id': 0}, {'id': 1}], worker,
            max_workers=2, thread_name_prefix='stream-sort-media', capacity_manager=capacity))
    assert sorted(call.args[0] for call in capacity.release.call_args_list) == [0, 1]


@pytest.mark.parametrize('component,scenario', [
    ('metadata', 'missing'), ('metadata', 'url_changed'), ('metadata', 'provider_changed'),
    ('metadata', 'ttl_forced'), ('metadata', 'missing_timestamp'), ('metadata', 'ttl_expired'),
    ('metadata', 'dead_retry_pending'), ('metadata', 'dead_ttl_expired'),
    ('metadata', 'status_skipped'), ('metadata', 'status_unknown'),
    ('content', 'provider_missing'), ('content', 'health_revalidation'),
    ('content', 'content_recovery_required'), ('content', 'missing'), ('content', 'ttl_forced'),
    ('throughput', 'status_marginal'), ('throughput', 'status_insufficient'),
    ('throughput', 'status_unknown'), ('throughput', 'provider_missing'),
    ('throughput', 'ttl_forced'), ('throughput', 'missing_timestamp'),
    ('throughput', 'url_changed'), ('throughput', 'provider_changed'),
])
def test_real_due_reasons_survive_attempt_archive(archive, component, scenario):
    now = datetime.now(timezone.utc)
    old = (now - timedelta(days=10)).isoformat()
    entry = {'status': 'alive', 'url_hash': 'current', 'm3u_account_id': 7,
        'ffprobe_checked_at': old, 'content_checked_at': old, 'content_m3u_account_id': 7,
        'throughput': {'status': 'healthy', 'url_hash': 'current', 'm3u_account_id': 7, 'checked_at': old}}
    ttl = 1
    if scenario == 'missing':
        entry = None
    elif scenario == 'url_changed':
        entry['url_hash'] = 'other'
        entry['throughput']['url_hash'] = 'other'
    elif scenario == 'provider_changed':
        entry['m3u_account_id'] = 8
        entry['throughput']['m3u_account_id'] = 8
    elif scenario == 'provider_missing':
        entry['content_m3u_account_id'] = None
        entry['throughput']['m3u_account_id'] = None
    elif scenario == 'ttl_forced':
        ttl = 0
    elif scenario == 'missing_timestamp':
        entry.pop('ffprobe_checked_at')
        entry['throughput'].pop('checked_at')
    elif scenario in ('dead_retry_pending', 'dead_ttl_expired'):
        entry.update(status='dead', retry_pending=scenario == 'dead_retry_pending')
    elif scenario == 'health_revalidation':
        entry['status'] = 'skipped'
    elif scenario == 'content_recovery_required':
        entry['content_recovery_required'] = True
    elif scenario.startswith('status_'):
        status = scenario.removeprefix('status_')
        entry['status'] = status
        entry['throughput']['status'] = status
    kwargs = dict(url_hash='current', ttl_hours=ttl, dead_ttl_hours=1,
                  provider_id=7, ttl_jitter_percent=0, now=now)
    if component == 'metadata':
        reason = incremental._analysis_reason(entry, health_ttl_hours=ttl,
            content_ttl_hours=ttl, metadata_ttl_hours=ttl,
            **{k: v for k, v in kwargs.items() if k != 'ttl_hours'})
        assert reason == 'ffprobe_' + scenario
    else:
        function = incremental.content_check_reason if component == 'content' else incremental.throughput_check_reason
        reason = function(entry, **kwargs)
        assert reason == scenario
    @obs.probe(component, 'direct_ffprobe')
    def actual():
        return {'status': 'alive'}
    item = {**ITEM, '_observation_reasons': {component: reason}}
    with obs.worker_context(archive, item, 'stream-sort-media', 1):
        actual()
    archive.flush(force=True)
    assert rows(archive)[0]['reason'] == reason


@pytest.mark.parametrize('reason', ['placeholder_recovered', 'http://user:' + SECRET + '@private.invalid'])
def test_internal_recovery_reason_and_untrusted_reason_are_distinct(archive, reason):
    @obs.probe('content', 'remote_decode')
    def actual():
        return {'status': 'alive'}
    with obs.worker_context(archive, {**ITEM, '_observation_reasons': {'content': reason}}, 'stream-sort-content', 1):
        actual()
    archive.flush(force=True)
    assert rows(archive)[0]['reason'] == ('placeholder_recovered' if reason == 'placeholder_recovered' else 'unknown')
    assert SECRET not in json.dumps(rows(archive))


@pytest.mark.parametrize('declared,matching,expected,selection', [
    (None, False, 400, 'all_packets_fallback'),
    ('800000', False, 400, 'all_packets_fallback'),
    ('800000', True, 400, 'video_packet_durations'),
    ('800000', True, 800, 'declared_video_bitrate'),
])
def test_archive_records_exact_bitrate_selection_including_all_packet_fallback(
        archive, monkeypatch, declared, matching, expected, selection):
    data = probe_data()
    data['streams'][0]['bit_rate'] = declared
    data['packets'] = [{'stream_index': 0 if matching else 1, 'size': '1000', 'duration_time': '.02'}
                       for _ in range(1 if expected == 800 else 40)]
    monkeypatch.setattr(analyzer.subprocess, 'run', Mock(return_value=completed(json.dumps(data))))
    with obs.worker_context(archive, ITEM, 'stream-sort-media', 1):
        result = analyzer.analyze_stream(URL, stream_id=42, stream_name='', settings={}, include_content=False)
    archive.flush(force=True)
    row = rows(archive)[0]
    assert result['stats']['video_bitrate'] == expected
    assert row['selected_stats']['video_bitrate'] == expected
    assert row['primary_video']['selected_bitrate_kbps'] == expected
    assert row['primary_video']['selection'] == selection
    assert row['error_type'] == ('low_bitrate' if expected < 500 else None)
    if not matching:
        assert row['primary_video']['packet_selection'] == 'all_packets_fallback'
        assert row['packets']['video_count'] == 0
        assert row['packets']['calculated_video_bitrate_kbps'] is None
    assert SECRET not in json.dumps(row)


@pytest.mark.parametrize('binary', [False, True])
def test_timeout_archives_bounded_partial_measurements_without_changing_health(archive, monkeypatch, binary):
    stdout = 'out_time_us=2000000\nout_time_us=5000000\n' + URL
    stderr = ('black_start:0 black_end:3 black_duration:3\n'
              'lavfi.freezedetect.freeze_start: 2\n' * 140) + 'mean_volume: -75.5 dB\n' + URL
    if binary:
        stdout, stderr = stdout.encode(), stderr.encode()
    timeout = subprocess.TimeoutExpired(['ffmpeg', URL], 20, output=stdout, stderr=stderr)
    monkeypatch.setattr(analyzer.subprocess, 'run', Mock(side_effect=timeout))
    with obs.worker_context(archive, ITEM, 'stream-sort-content', 1):
        result = analyzer.apply_content_analysis({'details': {'has_audio': True}}, URL,
                                                settings={}, logger=Mock())
    archive.flush(force=True)
    row = rows(archive)[0]
    assert result['status'] == 'dead' and result['error_type'] == 'timeout'
    assert row['measured'] is False and row['error_type'] == 'timeout'
    assert row['decoded_duration_seconds'] == 5 and row['partial_output'] is True
    assert row['partial_mean_volume_db'] == -75.5
    assert row['black_intervals'][0] == [0, 3, 3]
    assert len(row['black_intervals']) + len(row['freeze_markers']) == 128
    assert row['truncations']['black_intervals'] == row['truncations']['freeze_markers'] == 76
    assert SECRET not in json.dumps(row) and len(obs.encoded(row)) <= obs.PAYLOAD_BYTES


@pytest.mark.parametrize('stage', ['renewal', 'connection_cleanup'])
def test_raw_database_failures_propagate_as_infrastructure_not_media(pg_scan, monkeypatch, stage):
    monkeypatch.setattr(analyzer, 'analyze_stream', Mock(return_value={
        'status': 'alive', 'stats': {}, 'details': {}}))
    if stage == 'renewal':
        pg_scan.store.heartbeat = Mock(side_effect=TimeoutError(URL))
    else:
        pg_scan.store.close_worker_connection = Mock(side_effect=OSError(URL))
    log_media = Mock()
    monkeypatch.setattr(incremental, '_log_media_progress', log_media)
    with pytest.raises(storage.StorageError):
        pg_scan.run()
    log_media.assert_not_called()
    pg_scan.store.persist_core.assert_not_called()
    assert pg_scan.store.current['42']['error_type'] == 'black_screen'
    assert pg_scan.capacity.release.call_count == 1


@pytest.mark.parametrize('failure_type', [storage.StorageError, storage.LeaseLost])
def test_coordinator_archive_failure_drains_and_releases_every_pending_native_slot(
        archive, monkeypatch, failure_type):
    all_peers_started, flush_failed = threading.Event(), threading.Event()
    state_lock = threading.Lock()
    started, finished = set(), set()
    lease = SimpleNamespace(owner=archive.owner, store=archive.store,
                            observations=archive, ensure=Mock())
    monkeypatch.setattr(storage, '_active_lease', lease)
    monkeypatch.setattr(storage, 'ensure_provider_work', lambda: None)
    monkeypatch.setattr(incremental, 'analysis_cancel_requested', lambda: False)
    def release(reservation):
        with state_lock:
            assert reservation in finished, 'released before its actual provider operation ended'
    capacity = SimpleNamespace(try_acquire=lambda item: (True, item['id']),
        prepare_item=lambda item, _: dict(item), release=Mock(side_effect=release))
    @obs.probe('metadata', 'direct_ffprobe')
    def actual():
        return {'status': 'alive'}
    def worker(item):
        sid = item['id']
        if sid == 0:
            assert all_peers_started.wait(5)
            # Trigger a real Archive.flush threshold while all three other
            # native reservations still belong to unfinished provider work.
            for _ in range(100):
                actual()
        else:
            with state_lock:
                started.add(sid)
                if len(started) == 3:
                    all_peers_started.set()
            assert flush_failed.wait(5)
            time.sleep(.02)
            actual()
        with state_lock:
            finished.add(sid)
    def fail_batch(owner, batch):
        assert len(batch) == 100
        with state_lock:
            assert started == {1, 2, 3} and finished == {0}
        flush_failed.set()
        raise failure_type('fixture coordinator archive failure')
    archive.store.observation_batch = Mock(side_effect=fail_batch)
    with pytest.raises(failure_type, match='fixture coordinator archive failure'):
        list(incremental._fair_account_futures([{'id': sid} for sid in range(4)], worker,
            max_workers=4, thread_name_prefix='stream-sort-media', capacity_manager=capacity))
    assert finished == {0, 1, 2, 3}
    assert sorted(call.args[0] for call in capacity.release.call_args_list) == [0, 1, 2, 3]
    assert archive.snapshot()['pending_rows'] == 103  # uncommitted evidence is still present
    assert archive.snapshot()['committed_rows'] == 0
    archive.store.persist_core.assert_not_called()


@pytest.fixture
def four_captures(tmp_path, monkeypatch):
    """Actual capture_stream_sample with bounded, synchronized subprocess doubles."""
    from pathlib import Path
    from stream_sorter import throughput
    state_lock = threading.Lock()
    clock = threading.local()
    all_started = threading.Barrier(4)
    finish_pending = threading.Event()
    created, finished = {}, set()
    def monotonic():
        value = getattr(clock, 'value', 0)
        clock.value = value + 8
        return value
    def popen(command, **kwargs):
        sid = int(command[command.index('-i') + 1].rsplit('/', 1)[1])
        path = Path(command[-1])
        path.write_bytes(b'x' * 65536)
        with state_lock:
            created[sid] = path
        def communicate(**kwargs):
            all_started.wait(5)
            if sid != 0:
                assert finish_pending.wait(5)
                time.sleep(.02)
            return '', ''
        return SimpleNamespace(returncode=0, communicate=communicate)
    monkeypatch.setattr(throughput, 'time', SimpleNamespace(monotonic=monotonic))
    monkeypatch.setattr(throughput.subprocess, 'Popen', popen)
    def capture(url, **kwargs):
        measured, path = throughput.capture_stream_sample(url, **kwargs)
        assert path is not None
        sid = int(url.rsplit('/', 1)[1])
        with state_lock:
            finished.add(sid)
        return measured, path
    def worker(item):
        measured, path = capture('http://fixture/' + str(item['id']), nominal_video_kbps=1000,
            duration_seconds=6, temp_directory=str(tmp_path))
        return measured, path, 1000
    def release(sid):
        with state_lock:
            assert sid in finished, 'released before actual capture finished'
    capacity = SimpleNamespace(try_acquire=lambda item: (True, item['id']),
        prepare_item=lambda item, _: dict(item), release=Mock(side_effect=release))
    fixture = SimpleNamespace(capture=capture, worker=worker, capacity=capacity,
        created=created, finished=finished, finish_pending=finish_pending,
        tmp_path=tmp_path, items=[{'id': sid, 'account_id': sid + 7} for sid in range(4)])
    yield fixture
    finish_pending.set()


@pytest.mark.parametrize('target', ['stream-sort-combined-capture', 'stream-sort-combined-retry'])
def test_production_capture_flush_abort_disposes_all_four_unconsumed_samples(
        pg_scan, four_captures, monkeypatch, target):
    from pathlib import Path
    import copy
    case = four_captures
    original_stream = pg_scan.rows[0].stream
    old = (pg_scan.now - timedelta(days=10)).isoformat()
    pg_scan.rows.clear()
    pg_scan.store.current.clear()
    for sid in range(4):
        stream = copy.copy(original_stream)
        stream.id, stream.url, stream.m3u_account_id = sid, 'http://fixture/' + str(sid), sid + 7
        pg_scan.rows.append(SimpleNamespace(channel_id=sid + 1, stream=stream))
        pg_scan.store.current[str(sid)] = {'status': 'alive', 'stream_id': sid, 'm3u_account_id': sid + 7,
            'url_hash': analyzer._stream_url_hash(stream.url), 'ffprobe_checked_at': pg_scan.now.isoformat(),
            'content_checked_at': old, 'content_m3u_account_id': sid + 7,
            'stats': {'width': 1920, 'height': 1080, 'video_bitrate': 4000}}
    monkeypatch.setattr(obs, 'provenance', lambda *a: {'archive_schema': 3})
    monkeypatch.setattr(incremental, '_select_capture_temp_directory', lambda *a, **kw: str(case.tmp_path))
    monkeypatch.setattr(incremental, 'content_check_reason', lambda *a, **kw: 'content_missing')
    monkeypatch.setattr(incremental, 'throughput_check_reason', lambda *a, **kw: 'throughput_missing')
    monkeypatch.setattr(analyzer, 'analyze_stream', Mock(side_effect=AssertionError('metadata is fresh')))
    decoder = Mock(side_effect=AssertionError('no capture result should be yielded'))
    monkeypatch.setattr(incremental, '_analyze_local_capture', decoder)
    phase, yielded = {'value': None}, {}
    fair = incremental._fair_account_futures
    def scheduler(items, worker, **kwargs):
        name = kwargs['thread_name_prefix']
        phase['value'] = name
        if name == target:
            assert kwargs['discard_result'] is incremental._discard_combined_capture_result
            archive = storage.active_run().observations
            for _ in range(99):
                archive.collect({'stream_id': 0, 'provider_id': 7, 'started_at': obs.utc()})
        for result in fair(items, worker, **kwargs):
            yielded[name] = yielded.get(name, 0) + 1
            yield result
    monkeypatch.setattr(incremental, '_fair_account_futures', scheduler)
    def capture(url, **kwargs):
        if phase['value'] != target:
            # A failed initial capture produces no sample; the production retry
            # path must own/dispose the four successful retry captures instead.
            return {'status': 'unknown', 'error_type': 'stream_unreachable'}, None
        return case.capture(url, **kwargs)
    monkeypatch.setattr(incremental, 'capture_stream_sample', capture)
    def release(token):
        name, sid = token
        if name == target:
            assert sid in case.finished
    pg_scan.capacity.try_acquire = lambda item: (True, (phase['value'], item['id']))
    pg_scan.capacity.release = Mock(side_effect=release)
    first_failure = threading.Event()
    def fail_batch(owner, batch):
        if not first_failure.is_set():
            assert len(batch) == 100
            assert case.finished == {0} and len(case.created) == 4
            assert sum(path.stat().st_size for path in case.created.values()) == 262144
            first_failure.set()
        case.finish_pending.set()
        raise storage.StorageError('fixture capture archive abort')
    pg_scan.store.observation_batch = fail_batch
    with pytest.raises(storage.StorageError, match='fixture capture archive abort'):
        pg_scan.run(analysis_workers=4, analysis_retries=1)
    assert yielded.get(target, 0) == 0
    assert case.finished == {0, 1, 2, 3}
    assert not list(case.tmp_path.glob('*.ts'))
    assert not any(path.exists() for path in case.created.values())
    releases = [call.args[0] for call in pg_scan.capacity.release.call_args_list]
    assert sorted(sid for name, sid in releases if name == target) == [0, 1, 2, 3]
    assert len(releases) == (4 if target == 'stream-sort-combined-capture' else 8)
    decoder.assert_not_called()
    pg_scan.store.persist_core.assert_not_called()


@pytest.mark.parametrize('cancel', [False, True])
def test_close_disposes_only_unconsumed_results_and_preserves_active_decoder(
        four_captures, monkeypatch, cancel):
    from concurrent.futures import ThreadPoolExecutor
    from pathlib import Path
    case = four_captures
    canceled = threading.Event()
    monkeypatch.setattr(storage, '_active_lease', None)
    monkeypatch.setattr(incremental, 'analysis_cancel_requested', canceled.is_set)
    source = incremental._fair_account_futures(case.items, case.worker, max_workers=4,
        thread_name_prefix='stream-sort-combined-capture', capacity_manager=case.capacity,
        discard_result=incremental._discard_combined_capture_result)
    item, future = next(source)
    measured, path, nominal = future.result()
    assert item['id'] == 0 and Path(path).exists()
    decoder_started, finish_decoder = threading.Event(), threading.Event()
    def decode(base, sample, **kwargs):
        decoder_started.set()
        assert finish_decoder.wait(5)
        assert Path(sample).exists(), 'scheduler deleted a sample already handed to its decoder'
        return {'status': 'alive', 'details': {'content': {'measured': True}}}
    monkeypatch.setattr(analyzer, 'apply_content_analysis', decode)
    with ThreadPoolExecutor(max_workers=1) as executor:
        local = executor.submit(incremental._analyze_local_capture, item, {}, path, settings={}, logger=Mock())
        try:
            assert decoder_started.wait(5)
            if cancel:
                canceled.set()
            case.finish_pending.set()
            source.close()
            assert Path(path).exists()
            assert sorted(case.tmp_path.glob('*.ts')) == [Path(path)]
            assert case.finished == {0, 1, 2, 3}
            assert case.capacity.release.call_count == 4
        finally:
            case.finish_pending.set()
            finish_decoder.set()
            source.close()
        assert local.result()['status'] == 'alive'
    assert not list(case.tmp_path.glob('*.ts'))


@pytest.mark.parametrize('cancel_after_first', [False, True])
def test_normal_capture_delivery_transfers_every_sample_without_scheduler_unlink(
        four_captures, monkeypatch, cancel_after_first):
    from pathlib import Path
    case = four_captures
    monkeypatch.setattr(storage, '_active_lease', None)
    canceled = threading.Event()
    monkeypatch.setattr(incremental, 'analysis_cancel_requested', canceled.is_set)
    case.finish_pending.set()
    source = incremental._fair_account_futures(case.items, case.worker, max_workers=4,
        thread_name_prefix='stream-sort-combined-capture', capacity_manager=case.capacity,
        discard_result=incremental._discard_combined_capture_result)
    paths = []
    for _, future in source:
        paths.append(Path(future.result()[1]))
        if cancel_after_first:
            canceled.set()
    assert len(paths) == 4 and all(path.exists() for path in paths)
    assert case.capacity.release.call_count == 4
    for path in paths:
        path.unlink()  # The caller now owns each yielded sample.


def test_generic_results_never_imply_unlink_permission(tmp_path, monkeypatch):
    path = tmp_path / 'user-owned.ts'
    path.write_bytes(b'user-owned')
    monkeypatch.setattr(storage, '_active_lease', None)
    monkeypatch.setattr(incremental, 'analysis_cancel_requested', lambda: False)
    source = incremental._fair_account_futures([{'id': sid} for sid in range(4)],
        lambda item: ({}, str(path), 1000), max_workers=4, thread_name_prefix='generic-work')
    next(source)
    source.close()
    assert path.read_bytes() == b'user-owned'


def test_capture_result_survives_future_cleanup_failure_until_scheduler_disposes_it(
        archive, four_captures, monkeypatch):
    case = four_captures
    lease = SimpleNamespace(owner=archive.owner, store=archive.store, observations=None)
    monkeypatch.setattr(storage, '_active_lease', lease)
    monkeypatch.setattr(storage, 'ensure_provider_work', lambda: None)
    monkeypatch.setattr(incremental, 'analysis_cancel_requested', lambda: False)
    def cleanup_failure():
        case.finish_pending.set()
        raise OSError('fixture connection cleanup failure after capture returned')
    archive.store.close_worker_connection = Mock(side_effect=cleanup_failure)
    with pytest.raises(storage.StorageError, match='connection cleanup failed'):
        list(incremental._fair_account_futures(case.items, case.worker, max_workers=4,
            thread_name_prefix='stream-sort-combined-capture', capacity_manager=case.capacity,
            discard_result=incremental._discard_combined_capture_result))
    assert case.finished == {0, 1, 2, 3}
    assert case.capacity.release.call_count == 4
    assert not list(case.tmp_path.glob('*.ts'))


def test_disposal_failure_still_attempts_every_other_capture_and_releases_every_slot(
        archive, four_captures, monkeypatch):
    case = four_captures
    lease = SimpleNamespace(owner=archive.owner, store=archive.store, observations=archive, ensure=Mock())
    monkeypatch.setattr(storage, '_active_lease', lease)
    monkeypatch.setattr(storage, 'ensure_provider_work', lambda: None)
    monkeypatch.setattr(incremental, 'analysis_cancel_requested', lambda: False)
    for _ in range(99):
        archive.collect({'stream_id': 0, 'provider_id': 7, 'started_at': obs.utc()})
    def fail_batch(owner, batch):
        case.finish_pending.set()
        raise storage.StorageError('fixture archive failure')
    archive.store.observation_batch = fail_batch
    disposed = []
    def dispose(result):
        disposed.append(result[1])
        if result[1] == str(case.created[0]):
            raise PermissionError('fixture unlink failure')
        incremental._discard_combined_capture_result(result)
    with pytest.raises(storage.StorageError, match='Unconsumed capture result cleanup failed'):
        list(incremental._fair_account_futures(case.items, case.worker, max_workers=4,
            thread_name_prefix='stream-sort-combined-capture', capacity_manager=case.capacity,
            discard_result=dispose))
    assert len(disposed) == 4 and case.capacity.release.call_count == 4
    assert case.finished == {0, 1, 2, 3}
    assert list(case.tmp_path.glob('*.ts')) == [case.created[0]]
    case.created[0].unlink()
