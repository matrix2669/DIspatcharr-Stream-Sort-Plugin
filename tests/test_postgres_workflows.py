from contextlib import nullcontext
from datetime import datetime, timezone
import sys
import types
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from stream_sorter import analyzer, plugin, sorter, storage
from test_storage_backend import pg_scan


@pytest.fixture
def workflow(pg_scan, monkeypatch):
    context = pg_scan
    channel = SimpleNamespace(id=10, channel_number=100, name='Selected channel')
    first = context.rows[0]
    first.id, first.stream_id, first.order = 1, 42, 0
    first.channel, first.channel_id = channel, 10
    second = SimpleNamespace(**{**vars(first.stream), 'id': 43, 'url': 'http://example.test/second'})
    context.rows.append(SimpleNamespace(id=2, channel_id=10, channel=channel, stream=second, stream_id=43, order=1))
    context.rows.bulk_update = Mock()
    context.store.current['42'].update({'stats': {'width': 3840, 'height': 2160}})
    context.store.current['43'] = {'status': 'alive', 'stream_id': 43, 'm3u_account_id': 7,
        'url_hash': analyzer._stream_url_hash(second.url), 'stats': {'width': 1280, 'height': 720},
        'metadata_updated_at': context.now.isoformat()}
    context.store.sort_snapshots = Mock(return_value=[])
    context.store.save_sort = Mock()
    context.store.check_schema = Mock()
    context.store.fence = Mock()
    context.store.transaction = lambda: nullcontext(Mock())
    context.store.latest_scan = Mock(return_value=(context.now, {}))
    context.store.sort_report = Mock(return_value={'sort_history': [{
        'generated_at': context.now.isoformat(), 'channels_changed': 1,
        'changed_channels': [{'movements': [{'stream_id': 43}]}]}]})
    scope = {'match_mode': 'channel_profile', 'selected_channel_count': 1}
    monkeypatch.setattr(sorter, 'resolve_channel_scope', lambda *a, **kw: ({10}, scope))
    monkeypatch.setattr(sorter, 'scoped_rows', lambda *a, **kw: (list(context.rows), scope))
    monkeypatch.setattr(sorter, '_load_sort_report', Mock(side_effect=AssertionError('PG must not read JSON report')))
    monkeypatch.setattr(sorter, '_write_json_atomic', Mock(side_effect=AssertionError('PG must not write JSON report')))
    monkeypatch.setattr(sorter, 'load_cache', Mock(side_effect=AssertionError('PG must not read JSON evidence')))
    monkeypatch.setattr(plugin, '_load_json', Mock(side_effect=AssertionError('PG must not read JSON reports')))
    monkeypatch.setattr(plugin, '_save_json', Mock(side_effect=AssertionError('PG must not write JSON reports')))
    db = types.ModuleType('django.db')
    db.transaction = SimpleNamespace(atomic=nullcontext)
    monkeypatch.setitem(sys.modules, 'django.db', db)
    return context


def test_pg_sort_uses_terminal_current_and_writes_order_and_incremental_history(workflow):
    result = sorter.sort_channels({'storage_backend': 'postgresql'}, apply=True, logger=Mock())
    assert result['channels'][0]['proposed_stream_ids'] == [43, 42]
    assert result['channels'][0]['streams'][-1]['viability'] == 'known_dead'
    assert result['channels_changed'] == 1 and result['rows_changed'] == 2
    assert result['report_path'] is None
    workflow.rows.bulk_update.assert_called_once()
    assert workflow.rows.bulk_update.call_args.args[1] == ['order']
    workflow.store.sort_snapshots.assert_called_once_with([10])
    workflow.store.save_sort.assert_called_once()
    assert workflow.store.calls[0] == ('load', [42, 43], None, False)


def test_pg_dry_run_never_changes_orders_or_retained_applied_history(workflow):
    result = sorter.sort_channels({'storage_backend': 'postgresql'}, apply=False, logger=Mock())
    assert result['mode'] == 'dry_run'
    assert result['channels'][0]['proposed_stream_ids'] == [43, 42]
    workflow.rows.bulk_update.assert_not_called()
    workflow.store.save_sort.assert_not_called()


def test_pg_health_ttl_and_sort_history_actions_use_scoped_database(workflow):
    settings = {'storage_backend': 'postgresql'}
    health = plugin._run_health_report_action(settings)
    assert health['status'] == 'ok'
    assert health['report_path'] is None
    assert health['result']['selected_streams'] == 2
    assert workflow.store.calls[-1] == ('load', [42, 43], None, True)
    recommendation = plugin._run_ttl_recommendation_action(settings)
    assert recommendation['status'] == 'ok'
    assert recommendation['recommendation_path'] is None
    history = plugin._run_sort_history_action(settings)
    assert history['status'] == 'ok' and history['result']['applied_runs'] == 1
    workflow.store.sort_report.assert_called_once_with([10])


def test_pg_analyze_and_sort_action_is_wired_without_legacy_rejection(workflow, monkeypatch):
    instance = object.__new__(plugin.Plugin)
    instance._m3u_source_score_account_ids = None
    monkeypatch.setattr(plugin, '_settings_with_dynamic_source_scores', lambda settings, **kw: settings)
    start = Mock(return_value={'status': 'ok'})
    monkeypatch.setattr(plugin, '_start_background_job', start)
    response = instance.run('analyze_and_sort', {}, {'settings': {'storage_backend': 'postgresql'}})
    assert response['status'] == 'ok'
    start.assert_called_once_with({'storage_backend': 'postgresql'}, kind='analyze', sort_after=True)


def test_background_pg_analyze_and_sort_completes_with_database_sort(workflow, monkeypatch):
    sys.modules["django.db"].close_old_connections = Mock()
    monkeypatch.setattr(plugin, 'analyze_assigned_streams', Mock(return_value={
        'streams_analyzed': 2, 'status_counts': {'alive': 1, 'dead': 1},
        'health_summary': 'alive=1 dead=1'}))
    status = Mock()
    monkeypatch.setattr(plugin, '_update_status', status)
    for name in ('_notify', '_release_job_lock', '_finish_schedule_job'):
        monkeypatch.setattr(plugin, name, Mock())
    plugin._background_analyze_job({'storage_backend': 'postgresql'}, None,
                                 sort_after=True, job_id='pg-workflow')
    assert status.call_args.kwargs['status'] == 'completed'
    assert status.call_args.kwargs['result']['channels_changed'] == 1
    workflow.store.save_sort.assert_called_once()


def test_legacy_numeric_metadata_crosses_read_boundaries_without_corruption():
    import io
    from stream_sorter.storage_import import iter_legacy_records
    class TinyReads(io.StringIO):
        def read(self, size=-1):
            return super().read(min(2, size))
    source = TinyReads('{"counter":1.2345e+12,"streams":{"42":{"playback_starts":12}}}')
    assert list(iter_legacy_records(source, 'reliability')) == [
        ('streams', '42', {'playback_starts': 12})]
