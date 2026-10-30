"""Offline fixtures for immutable attempt evidence; no provider requests."""
import copy
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from stream_sorter import analyzer, incremental, observations as obs, storage, throughput
from test_storage_backend import MemoryStore, pg_scan

SECRET = 'credential-marker'
URL = f'http://user:{SECRET}@provider.invalid/private/{SECRET}|Authorization={SECRET}'
ITEM = {'id': 42, 'account_id': 7, 'm3u_profile_id': 9,
        'name': SECRET, 'url': URL,
        'channels': [{'channel_id': 1, 'channel_number': '23.1', 'channel_name': SECRET}]}


@pytest.fixture
def archive(monkeypatch):
    monkeypatch.setattr(obs, 'provenance', lambda settings, scope: {
        'archive_schema': 3, 'scope': scope, 'configuration': obs.configuration(settings)})
    store = MemoryStore({})
    store.acquire('test-run')
    return obs.Archive(store, 'test-run', {}, [ITEM])


def rows(archive):
    return [json.loads(row[4]) for row in archive.store.observation_rows.values()]


def payload(**overrides):
    return {'started_at': obs.utc(), 'stream_id': 42, 'provider_id': 7,
            'component': 'metadata', **overrides}


def probe_data(track_count=1):
    video = {'index': 0, 'codec_type': 'video', 'codec_name': 'h264',
        'width': 1920, 'height': 1080, 'r_frame_rate': '60000/1001',
        'avg_frame_rate': '30000/1001', 'time_base': '1/90000', 'field_order': 'tt',
        'profile': 'High', 'bit_rate': '2500000', 'color_space': 'bt709', 'pix_fmt': 'yuv420p',
        'tags': {'secret': URL}, 'codec_long_name': URL}
    return {'streams': [copy.deepcopy(video) for _ in range(track_count)],
            'format': {'duration': None, 'bit_rate': '3000000', 'filename': URL, 'tags': {'x': SECRET}},
            'packets': [{'stream_index': 0, 'size': '10000', 'duration_time': '0.02',
                         'pts_time': str(i * .02), 'data': SECRET} for i in range(100)]}


def completed(stdout='', stderr='', returncode=0):
    return SimpleNamespace(stdout=stdout, stderr=stderr, returncode=returncode)


def test_metadata_allowlists_rates_bitrate_provenance_and_packet_summary(archive, monkeypatch):
    data = probe_data(70)
    data['streams'][1].update(codec_name=SECRET, profile=URL, r_frame_rate=URL)
    run = Mock(return_value=completed(json.dumps(data)))
    monkeypatch.setattr(analyzer.subprocess, 'run', run)
    with obs.worker_context(archive, ITEM, 'stream-sort-media', 1):
        result = analyzer.analyze_stream(URL, stream_id=42, stream_name=SECRET,
                                        settings={}, include_content=False)
    archive.flush(force=True)
    row = rows(archive)[0]
    assert result['status'] == 'alive'
    assert row['profile_id'] == 9 and row['channels'] == [{'channel_id': 1, 'channel_number': 23.1}]
    assert 1 < len(row['tracks']) <= 64
    assert row['truncations']['tracks'] == 70 - len(row['tracks'])
    assert len(obs.encoded(row)) <= obs.PAYLOAD_BYTES
    assert row['tracks'][0]['r_frame_rate'] == '60000/1001'
    assert row['tracks'][0]['field_order'] == 'tt'
    assert row['tracks'][1]['codec_name'] is None and row['tracks'][1]['r_frame_rate'] is None
    assert row['primary_video']['declared_bitrate_bps'] == 2500000
    assert row['primary_video']['calculated_bitrate_kbps'] == pytest.approx(4000)
    assert row['packets']['video_count'] == 100 and row['packets']['video_bytes'] == 1000000
    assert 'packets' not in row['packets'] and row['tool_returncode'] == 0
    assert row['started_at'] <= row['finished_at'] and row['elapsed_seconds'] >= 0
    assert SECRET not in json.dumps(row)
    assert run.call_count == 1


def test_content_intervals_and_partial_decoder_progress_retained_on_failure(archive, monkeypatch):
    stderr = ('black_start:0 black_end:3 black_duration:3\n'
              'lavfi.freezedetect.freeze_start: 2\n') * 140 + URL
    monkeypatch.setattr(analyzer.subprocess, 'run', Mock(return_value=completed(
        'out_time_us=2000000\nout_time_us=3000000\n', stderr, 1)))
    with obs.worker_context(archive, ITEM, 'stream-sort-content-retry', 2):
        result = analyzer.apply_content_analysis({'details': {'has_audio': True}}, URL,
                                                 settings={}, logger=Mock())
    archive.flush(force=True)
    row = rows(archive)[0]
    assert result['status'] == 'dead'
    assert len(row['black_intervals']) == len(row['freeze_markers']) == 64
    assert row['truncations'] == {'black_intervals': 76, 'freeze_markers': 76}
    assert row['decoded_duration_seconds'] == 3 and row['tool_returncode'] == 1
    assert row['measured'] is False and SECRET not in json.dumps(row)


def test_local_content_uses_measured_progress_and_deletes_fixture(archive, monkeypatch, tmp_path):
    sample = tmp_path / (SECRET + '.ts')
    sample.write_bytes(b'fixture')
    monkeypatch.setattr(analyzer.subprocess, 'run', Mock(return_value=completed('out_time_us=6000000\n')))
    monkeypatch.setattr(storage, '_active_lease', SimpleNamespace(observations=archive, store=archive.store))
    item = {**ITEM, '_observation_profile_id': 9, '_observation_phase': 'stream-sort-combined-retry', '_observation_wave': 2}
    result = incremental._analyze_local_capture(item, {'details': {}}, str(sample), settings={}, logger=Mock())
    archive.flush(force=True)
    row = rows(archive)[0]
    assert result['status'] == 'alive' and not sample.exists()
    assert row['mode'] == 'local_sample_validation' and row['phase'] == 'stream-sort-combined-retry-local'
    assert row['profile_id'] == 9 and row['decoded_duration_seconds'] == 6
    assert SECRET not in json.dumps(row)


def test_direct_throughput_actual_elapsed_bytes_source_and_hashed_edge(archive, monkeypatch):
    response = Mock()
    response.geturl.return_value = f'http://user:{SECRET}@edge.invalid/{SECRET}'
    response.read.side_effect = [b'x' * 65536, b'x' * 65536, b'']
    open_request = Mock(return_value=response)
    monkeypatch.setattr(throughput.urllib.request, 'urlopen', open_request)
    with obs.worker_context(archive, ITEM, 'stream-sort-throughput', 1):
        result = throughput.probe_stream(URL, nominal_video_kbps=1000)
    archive.flush(force=True)
    row = rows(archive)[0]
    assert row['measurement_bytes'] == 131072
    assert row['measurement_elapsed_seconds'] == result['elapsed_seconds']
    assert row['measurement_source'] == 'direct_http_read'
    assert len(row['edge_host_sha256']) == 64 and SECRET not in json.dumps(row)
    assert open_request.call_count == 1
    entry = incremental._append_throughput_history(ITEM, {}, result)
    history = entry['throughput_check_history'][0]
    assert history['duration_seconds'] == result['elapsed_seconds']
    assert history['bytes'] == 131072 and history['measurement_source'] == 'direct_http_read'


@pytest.mark.parametrize('exception', [TimeoutError(URL), OSError(URL)])
def test_throughput_failures_retain_bytes_elapsed_without_exception_text(archive, monkeypatch, exception):
    monkeypatch.setattr(throughput.urllib.request, 'urlopen', Mock(side_effect=exception))
    with obs.worker_context(archive, ITEM, 'stream-sort-throughput', 1):
        result = throughput.probe_stream(URL, nominal_video_kbps=1000)
    archive.flush(force=True)
    row = rows(archive)[0]
    assert result['status'] == 'unknown' and row['status'] == 'unknown'
    assert row['measurement_bytes'] == 0 and row['measurement_elapsed_seconds'] >= 0
    assert SECRET not in json.dumps(row)


@pytest.mark.parametrize('kind', ['threshold', 'completion', 'cancel'])
def test_coordinator_bounded_batches_stable_ids_and_metrics(archive, kind):
    sizes, threads = [], []
    original = archive.store.observation_batch
    def batch(owner, values):
        sizes.append(len(values)); threads.append(threading.get_ident())
        original(owner, values)
    archive.store.observation_batch = batch
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: archive.collect(payload()), range(235)))
    archive.flush(force=kind != 'threshold', state='canceled' if kind == 'cancel' else 'completed')
    assert sizes == [100, 100, 35]
    assert set(threads) == {threading.get_ident()}
    assert len(rows(archive)) == 235
    summary = archive.snapshot()
    assert summary['committed_rows'] == summary['collected_rows'] == 235
    assert summary['committed_payload_bytes'] == sum(row[5] for row in archive.store.observation_rows.values())
    assert summary['pending_rows'] == 0 and summary['batches'] == 3
    assert summary['flush_elapsed_seconds'] >= 0


def test_unknown_commit_preserves_batch_for_idempotent_retry(archive):
    archive.collect(payload())
    original = archive.store.observation_batch
    def uncertain(owner, values):
        original(owner, values)
        raise OSError(URL)
    archive.store.observation_batch = uncertain
    with pytest.raises(storage.StorageError, match='unacknowledged'):
        archive.flush(force=True)
    pending = list(archive.queue)
    assert len(pending) == len(rows(archive)) == 1
    archive.store.observation_batch = original
    archive.flush(force=True)
    assert len(rows(archive)) == 1 and not archive.queue
    assert rows(archive)[0]['observation_id'] == pending[0][0]


def test_non_coordinator_cannot_write(archive):
    archive.collect(payload())
    with ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(RuntimeError, match='coordinator'):
            pool.submit(archive.flush, force=True).result()
    assert len(archive.queue) == 1 and not rows(archive)


def test_payload_bounds_and_explicit_truncation_prioritize_identity(archive):
    data = payload(tracks=[{key: 'x' * 80 for key in range(20)} for _ in range(64)],
                   channels=[{'channel_id': i} for i in range(1000)])
    archive.collect(obs.bounded(data))
    archive.flush(force=True)
    row = rows(archive)[0]
    assert len(obs.encoded(row)) <= 32768 and row['stream_id'] == 42
    assert row['truncations']['tracks'] > 0 and row['truncations']['channels'] >= 872


def test_optional_telemetry_error_retains_attempt_and_original_result(archive):
    @obs.probe('metadata', 'direct_ffprobe')
    def measured():
        obs.details(invalid_numeric=float('inf'))
        return {'status': 'alive'}
    with obs.worker_context(archive, ITEM, 'stream-sort-media', 1):
        result = measured()
    archive.flush(force=True)
    assert result['status'] == 'alive'
    assert rows(archive)[0]['outcome'] == 'telemetry_unavailable'
    assert archive.metrics['collection_errors'] == 1


def test_scheduler_flushes_during_large_phase_without_worker_database_writes(archive, monkeypatch):
    monkeypatch.setattr(storage, '_active_lease', SimpleNamespace(observations=archive, store=archive.store))
    monkeypatch.setattr(storage, 'ensure_provider_work', lambda: None)
    monkeypatch.setattr(incremental, 'analysis_cancel_requested', lambda: False)
    @obs.probe('metadata', 'direct_ffprobe')
    def actual():
        return {'status': 'alive'}
    def worker(item):
        actual()
    items = [{**ITEM, 'id': index} for index in range(260)]
    completed_items = 0
    for item, future in incremental._fair_account_futures(items, worker, max_workers=8,
                                                        thread_name_prefix='stream-sort-media'):
        future.result(); completed_items += 1
        if completed_items >= 150:
            assert archive.metrics['batches'] > 0
    assert archive.metrics['committed_rows'] == 260
    assert archive.metrics['max_buffer_rows'] <= 108


@pytest.mark.parametrize('case,expected', [('normal', 1), ('retry', 2), ('placeholder', 1), ('recovery', 2)])
def test_scan_archives_actual_metadata_gates_retries_and_full_recovery(pg_scan, monkeypatch, case, expected):
    context = pg_scan
    context.capacity.prepare_item = lambda item, _: {**item, 'm3u_profile_id': 9}
    monkeypatch.setattr(incremental, 'content_check_reason', lambda *a, **kw: None)
    monkeypatch.setattr(incremental, 'throughput_check_reason', lambda *a, **kw: None)
    if case in ('placeholder', 'recovery'):
        context.store.current['42']['error_type'] = 'placeholder_file'
    calls = []
    def run(command, **kwargs):
        if '-version' in command:
            return completed('ffprobe version 7.1\n' if 'ffprobe' in command[0] else 'ffmpeg version 7.1\n')
        calls.append(command)
        if case == 'retry' and len(calls) == 1:
            return completed('', URL, 1)
        data = probe_data()
        if case == 'placeholder':
            data['format']['duration'] = 30
        return completed(json.dumps(data))
    monkeypatch.setattr(analyzer.subprocess, 'run', run)
    result = context.run(analysis_retries=1 if case == 'retry' else 0)
    records = [json.loads(row[4]) for row in context.store.observation_rows.values()]
    assert len(calls) == len(records) == expected
    assert all(row['profile_id'] == 9 for row in records)
    assert len({row['observation_id'] for row in records}) == expected
    if case in ('placeholder', 'recovery'):
        assert records[0]['mode'] == 'placeholder_confirm_1s'
    if case == 'recovery':
        assert records[1]['mode'] == 'direct_ffprobe'
    if case == 'retry':
        assert records[1]['phase'] == 'stream-sort-media-retry'
    assert len(context.store.current['42']['health_check_history']) == 1
    assert result['probe_observations']['committed_rows'] == expected
    assert context.store.observation_runs['own-run'][2] == 'completed'


def test_cooperative_cancel_persists_finished_evidence_without_terminal_double_count(pg_scan, monkeypatch):
    stop = threading.Event()
    def run(command, **kwargs):
        if '-version' in command:
            return completed()
        stop.set()
        return completed(json.dumps(probe_data()))
    monkeypatch.setattr(analyzer.subprocess, 'run', run)
    monkeypatch.setattr(incremental, 'analysis_cancel_requested', stop.is_set)
    with pytest.raises(incremental.AnalysisCancelled) as error:
        pg_scan.run()
    assert len(pg_scan.store.observation_rows) == 1
    assert pg_scan.store.observation_runs['own-run'][2] == 'canceled'
    assert error.value.result['probe_observations']['committed_rows'] == 1


def test_disabled_collection_preserves_existing_rows_and_skips_versions(pg_scan, monkeypatch):
    pg_scan.store.observation_rows['existing'] = ('retained',)
    monkeypatch.setattr(obs, 'provenance', Mock(side_effect=AssertionError('disabled must not collect versions')))
    monkeypatch.setattr(analyzer, 'analyze_stream', Mock(return_value={'status': 'dead', 'details': {}, 'stats': {}}))
    result = pg_scan.run(retain_probe_observations=False)
    assert pg_scan.store.observation_rows == {'existing': ('retained',)}
    assert pg_scan.store.observation_runs == {} and 'probe_observations' not in result


def test_capacity_deferral_is_not_failed_attempt(pg_scan, monkeypatch):
    pg_scan.capacity.try_acquire.return_value = (False, None)
    monkeypatch.setattr(analyzer, 'analyze_stream', Mock(side_effect=AssertionError('deferral is not a probe')))
    result = pg_scan.run()
    assert not pg_scan.store.observation_rows
    assert result['probe_observations']['collected_rows'] == 0


def test_provenance_is_bounded_numeric_and_versions_run_once(monkeypatch):
    calls = []
    def version(command, **kwargs):
        calls.append(command)
        tool = 'ffprobe' if 'ffprobe' in command[0] else 'ffmpeg'
        return completed(f'{tool} version 7.1.2-secretbuild\n{URL}')
    monkeypatch.setattr(obs.subprocess, 'run', version)
    result = obs.provenance({'analysis_ffmpeg_path': '/private/' + SECRET,
        'password': URL, 'analysis_retries': 99, 'analysis_workers': 99}, {'selected_streams': 1, 'selected_channels': 1})
    assert len(calls) == 2 and all(command[-1] == '-version' for command in calls)
    assert result['configuration']['analysis_retries'] == 5
    assert result['configuration']['analysis_workers'] == 16
    assert result['source']['git_sha'] is None and len(result['source']['package_sha256']) == 64
    assert result['tools'] == {'ffprobe': '7.1.2', 'ffmpeg': '7.1.2'}
    assert len(obs.encoded(result)) <= obs.PROVENANCE_BYTES and SECRET not in json.dumps(result)


def test_ui_default_and_schema_are_explicit():
    manifest = json.loads((Path(__file__).parents[1] / 'stream_sorter/plugin.json').read_text())
    field = next(row for row in manifest['fields'] if row['id'] == 'retain_probe_observations')
    assert field['type'] == 'boolean' and field['default'] is True
    assert storage.SCHEMA_VERSION == 3


@pytest.mark.parametrize('success', [False, True])
def test_actual_combined_stream_copy_archives_partial_failure_and_elapsed(archive, monkeypatch, tmp_path, success):
    clock = SimpleNamespace(now=0)
    child = Mock(returncode=0 if success else 1)
    def communicate(**kwargs):
        clock.now = 8
        return '', URL
    child.communicate.side_effect = communicate
    def popen(command, **kwargs):
        Path(command[-1]).write_bytes(b'x' * 65536)
        return child
    monkeypatch.setattr(throughput.subprocess, 'Popen', popen)
    monkeypatch.setattr(throughput, 'time', SimpleNamespace(monotonic=lambda: clock.now))
    with obs.worker_context(archive, ITEM, 'stream-sort-combined-capture', 1):
        result, path = throughput.capture_stream_sample(URL, nominal_video_kbps=1000,
                                                       duration_seconds=6, temp_directory=str(tmp_path))
    if path:
        Path(path).unlink()
    archive.flush(force=True)
    row = rows(archive)[0]
    assert row['measurement_bytes'] == 65536
    assert row['measurement_elapsed_seconds'] == 8
    assert row['measurement_source'] == 'ffmpeg_stream_copy'
    assert row['elapsed_source'] == 'provider_capture_wall'
    assert row['tool_returncode'] == (0 if success else 1)
    assert not list(tmp_path.iterdir()) and SECRET not in json.dumps(row)
    assert bool(path) is success


@pytest.mark.parametrize('success', [False, True])
def test_actual_bounded_capture_call_archives_acquisition_code_and_partial_values(archive, monkeypatch, tmp_path, success):
    child = Mock(returncode=0)
    def communicate(**kwargs):
        config = json.loads(kwargs['input'])
        Path(config['path']).write_bytes(b'x' * 65536)
        return json.dumps({'complete': success, 'capture_code': 'capture_ok' if success else 'capture_eof',
                           'bytes': 65536, 'elapsed_seconds': 2}), None
    child.communicate.side_effect = communicate
    child.poll.return_value = 0
    monkeypatch.setattr(throughput.subprocess, 'Popen', Mock(return_value=child))
    with obs.worker_context(archive, ITEM, 'stream-sort-media', 1):
        result, path = throughput.capture_bounded_sample(URL, duration_seconds=1,
            timeout_seconds=3, temp_directory=str(tmp_path))
    if path:
        Path(path).unlink()
    archive.flush(force=True)
    row = rows(archive)[0]
    assert row['measurement_bytes'] == 65536 and row['measurement_elapsed_seconds'] == 2
    assert row['capture_code'] == ('capture_ok' if success else 'capture_eof')
    assert row['measurement_source'] == 'raw_http_capture' and row['elapsed_source'] == 'acquisition'
    assert not list(tmp_path.iterdir()) and SECRET not in json.dumps(row)


def test_collector_failure_never_overrides_actual_media_result(archive, monkeypatch):
    monkeypatch.setattr(archive, 'collect', Mock(side_effect=OSError(URL)))
    @obs.probe('metadata', 'direct_ffprobe')
    def success():
        return {'status': 'alive'}
    with obs.worker_context(archive, ITEM, 'stream-sort-media', 1):
        assert success() == {'status': 'alive'}
    assert archive.metrics['collection_errors'] > 0


def test_one_second_user_sample_is_not_labeled_as_placeholder_gate(archive, monkeypatch):
    monkeypatch.setattr(analyzer.subprocess, 'run', Mock(return_value=completed(json.dumps(probe_data()))))
    with obs.worker_context(archive, ITEM, 'stream-sort-media', 1):
        analyzer.analyze_stream(URL, stream_id=42, stream_name='',
            settings={'analysis_duration_seconds': 1}, include_content=False)
    archive.flush(force=True)
    assert rows(archive)[0]['mode'] == 'direct_ffprobe'
    assert rows(archive)[0]['requested_sample_seconds'] == 1


def test_json_calls_do_not_allocate_archive_or_read_versions(monkeypatch):
    monkeypatch.setattr(obs, 'Archive', Mock(side_effect=AssertionError('no JSON archive')))
    monkeypatch.setattr(obs, 'provenance', Mock(side_effect=AssertionError('no JSON versions')))
    monkeypatch.setattr(analyzer.subprocess, 'run', Mock(return_value=completed(json.dumps(probe_data()))))
    result = analyzer.analyze_stream(URL, stream_id=42, stream_name='', settings={}, include_content=False)
    assert result['status'] == 'alive' and obs._worker.get() is None


@pytest.mark.parametrize('short_sample', [False, True])
def test_shared_local_validation_and_direct_fallback_archive_actual_calls(pg_scan, monkeypatch, short_sample):
    context = pg_scan
    selected = []
    def prepare(item, reservation):
        profile = 90 + len(selected)
        selected.append(profile)
        return {**item, 'm3u_profile_id': profile}
    context.capacity.prepare_item = prepare
    monkeypatch.setattr(incremental, '_select_capture_temp_directory', lambda *a, **kw: str(context.tmp_path))
    monkeypatch.setattr(incremental, 'content_check_reason', lambda *a, **kw: 'content_missing')
    monkeypatch.setattr(incremental, 'throughput_check_reason', lambda *a, **kw: 'throughput_missing')
    child = Mock(returncode=0)
    def communicate(**kwargs):
        config = json.loads(kwargs['input'])
        Path(config['path']).write_bytes(b'x' * 65536)
        return json.dumps({'complete': True, 'capture_code': 'capture_ok', 'bytes': 65536, 'elapsed_seconds': 8}), None
    child.communicate.side_effect = communicate
    child.poll.return_value = 0
    monkeypatch.setattr(throughput.subprocess, 'Popen', Mock(return_value=child))
    def run(command, **kwargs):
        if '-version' in command:
            return completed()
        if '-show_streams' in command:
            data = probe_data()
            if '-protocol_whitelist' in command:
                data['format']['duration'] = .2 if short_sample else 8
            return completed(json.dumps(data))
        return completed('out_time_us=6000000\n')
    monkeypatch.setattr(analyzer.subprocess, 'run', run)
    response = Mock()
    response.geturl.return_value = URL
    response.read.side_effect = [b'x' * 65536, b'x' * 65536, b'']
    monkeypatch.setattr(throughput.urllib.request, 'urlopen', Mock(return_value=response))
    monkeypatch.setattr(incremental, 'probe_stream', throughput.probe_stream)
    result = context.run(bounded_capture_reuse=True)
    records = [json.loads(row[4]) for row in context.store.observation_rows.values()]
    assert len(records) == (5 if short_sample else 3)
    assert result['throughput_attempted'] == result['throughput_checked'] == 1
    assert records[0]['component'] == 'throughput' and records[0]['mode'] == 'bounded_http_capture'
    assert records[1]['component'] == 'metadata' and records[1]['mode'] == 'local_sample_validation'
    assert records[1]['profile_id'] == selected[0]
    if short_sample:
        assert records[2]['mode'] == 'direct_ffprobe'
        assert records[2]['profile_id'] == selected[1]
    else:
        assert records[2]['component'] == 'content' and records[2]['mode'] == 'local_sample_validation'
    assert not list(context.tmp_path.glob('*.sample'))
    assert len(context.store.current['42']['health_check_history']) == 1


def test_actual_content_retry_archive_does_not_inflate_terminal_history(pg_scan, monkeypatch):
    monkeypatch.setattr(incremental, 'throughput_check_reason', lambda *a, **kw: None)
    contents = 0
    def run(command, **kwargs):
        nonlocal contents
        if '-version' in command:
            return completed()
        if '-show_streams' in command:
            return completed(json.dumps(probe_data()))
        contents += 1
        return completed('out_time_us=6000000\n',
            'black_start:0 black_end:6 black_duration:6' if contents == 1 else '')
    monkeypatch.setattr(analyzer.subprocess, 'run', run)
    result = pg_scan.run(analysis_retries=1)
    records = [json.loads(row[4]) for row in pg_scan.store.observation_rows.values()]
    assert len(records) == 3 and contents == 2
    assert records[1]['status'] == 'dead' and records[2]['status'] == 'alive'
    assert records[2]['phase'] == 'stream-sort-content-retry'
    assert len(pg_scan.store.current['42']['health_check_history']) == 1
    assert result['content_checked'] == 1


def test_provider_controlled_track_index_cannot_leak_into_primary_metadata(archive, monkeypatch):
    data = probe_data()
    data['streams'][0]['index'] = URL
    monkeypatch.setattr(analyzer.subprocess, 'run', Mock(return_value=completed(json.dumps(data))))
    with obs.worker_context(archive, ITEM, 'stream-sort-media', 1):
        analyzer.analyze_stream(URL, stream_id=42, stream_name='', settings={}, include_content=False)
    archive.flush(force=True)
    row = rows(archive)[0]
    assert row['primary_video']['stream_index'] is None
    assert SECRET not in json.dumps(row)


def test_edge_identity_drops_credentials_and_port_before_hashing():
    assert obs.edge_fingerprint('user:' + SECRET + '@edge.invalid:8080') == obs.edge_fingerprint('edge.invalid')
    assert obs.edge_fingerprint('different:credential@EDGE.INVALID:9090') == obs.edge_fingerprint('edge.invalid')
    assert obs.edge_fingerprint(None) is None
    assert obs.edge_fingerprint('[') is None
