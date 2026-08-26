import collections
import itertools
import sys
import threading
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from stream_sorter import analyzer, incremental
from stream_sorter.incremental import (
    _fair_account_futures,
    _build_health_report,
    _media_stats_changed_for_throughput,
    _is_significant_bitrate_change,
    _sync_runtime_playback_health,
    analyze_assigned_streams,
    content_check_reason,
    health_check_reason,
    metadata_check_reason,
    throughput_check_reason,
)


def _now():
    return datetime(2026, 8, 17, 20, 0, tzinfo=timezone.utc)


def _scheduled_accounts(items, workers):
    starts = []
    starts_lock = threading.Lock()
    first_wave = threading.Barrier(workers)

    def worker(item):
        with starts_lock:
            position = len(starts)
            starts.append(item["account_id"])
        if position < workers:
            first_wave.wait(timeout=2)
        return item["id"]

    completed = list(
        _fair_account_futures(
            items,
            worker,
            max_workers=workers,
            thread_name_prefix="test-fair-scheduler",
        )
    )
    assert sorted(future.result() for _item, future in completed) == sorted(item["id"] for item in items)
    return starts


def test_placeholder_is_dead_health_with_separate_report_classification():
    now = _now()
    item = {"id": 42, "name": "Event placeholder", "account_id": 7, "account_name": "Provider", "channels": []}
    cache = {
        "42": {
            "status": "dead",
            "error_type": "placeholder_file",
            "health_check_history": [
                {
                    "checked_at": (now - timedelta(hours=2)).isoformat(),
                    "previous_status": "unknown",
                    "status": "alive",
                    "error_type": "",
                    "terminal": True,
                },
                {
                    "checked_at": (now - timedelta(hours=1)).isoformat(),
                    "previous_status": "alive",
                    "status": "dead",
                    "error_type": "placeholder_file",
                    "terminal": True,
                },
            ],
        }
    }

    report = _build_health_report(
        [item],
        cache,
        now=now,
        media_reason_counts={},
        throughput_reason_counts={},
        channels_selected=0,
    )

    placeholder = report["status_patterns"]["placeholders"]["current_streams"][0]
    assert report["status_counts"]["dead"] == 1
    assert placeholder["last_status"] == "dead"
    assert placeholder["health_class"] == "placeholder"
    assert report["observations"]["history_rows"] == 1
    assert report["observations"]["raw_history_rows"] == 2


def test_placeholder_rollups_do_not_increment_general_health_or_retry_counters():
    entry = {}
    incremental._append_health_history(
        entry,
        reason="ffprobe_dead_ttl_expired",
        previous_status="dead",
        new_status="dead",
        tested_at=_now().isoformat(),
        result={
            "status": "dead",
            "error_type": "placeholder_file",
            "retry_telemetry": {"retry_attempts": 3, "retries_exhausted": True},
        },
    )

    bucket = entry["health_daily_rollups"][_now().date().isoformat()]
    assert bucket["placeholder_completed_checks"] == 1
    assert bucket["placeholder_retry_attempts"] == 3
    assert bucket["placeholder_retry_exhaustions"] == 1
    assert "completed_checks" not in bucket
    assert "dead_checks" not in bucket
    assert "retry_attempts" not in bucket


def test_terminal_content_dead_preserves_and_advances_existing_dead_streak():
    now = _now().isoformat()
    item = {"id": 42, "name": "Example", "url": "http://example.test/live", "account_id": 7, "account_name": "Provider"}
    previous = {
        "status": "dead",
        "error_type": "black_screen",
        "consecutive_dead_results": 4,
        "url_hash": analyzer._stream_url_hash(item["url"]),
    }
    media_alive = {
        "tested_at": now,
        "status": "alive",
        "error_type": None,
        "error": "",
        "stats": {"resolution": "1920x1080", "video_bitrate": 4000},
        "details": {},
    }
    intermediate = incremental._merge_media_result(
        item,
        previous,
        media_alive,
        record_history=False,
        history_previous_status="dead",
    )
    content_dead = {
        "tested_at": now,
        "status": "dead",
        "error_type": "black_screen",
        "error": "Stream decodes to a black screen",
        "details": {"content": {"measured": True, "tested_at": now}},
    }
    terminal = incremental._merge_content_result(
        item,
        intermediate,
        content_dead,
        history_previous_status="dead",
    )

    assert intermediate["consecutive_dead_results"] == 4
    assert terminal["status"] == "dead"
    assert terminal["consecutive_dead_results"] == 5


def test_parallel_tests_use_distinct_m3u_sources_before_reusing_one():
    items = [
        {"id": 1, "account_id": 10},
        {"id": 2, "account_id": 10},
        {"id": 3, "account_id": 20},
        {"id": 4, "account_id": 20},
        {"id": 5, "account_id": 30},
        {"id": 6, "account_id": 30},
    ]
    starts = _scheduled_accounts(items, workers=2)
    assert len(set(starts[:2])) == 2


def test_parallel_tests_split_extra_workers_evenly_across_m3u_sources():
    items = [
        {"id": index, "account_id": account_id}
        for index, account_id in enumerate([10, 10, 10, 10, 20, 20, 20, 20], start=1)
    ]
    starts = _scheduled_accounts(items, workers=5)
    assert collections.Counter(starts[:5]) == {10: 3, 20: 2}


def test_parallel_tests_reassign_capacity_when_an_m3u_source_runs_out():
    items = [
        {"id": 1, "account_id": 10},
        {"id": 2, "account_id": 20},
        {"id": 3, "account_id": 20},
        {"id": 4, "account_id": 20},
        {"id": 5, "account_id": 20},
    ]
    starts = _scheduled_accounts(items, workers=3)
    assert collections.Counter(starts[:3]) == {20: 2, 10: 1}


def test_throughput_parallelism_is_limited_per_source_not_globally(tmp_path, monkeypatch):
    class QuerySet(list):
        def select_related(self, *_args):
            return self

        def order_by(self, *_args):
            return self

        def filter(self, **_kwargs):
            return self

    accounts = [
        SimpleNamespace(id=10, name="Provider A", get_user_agent_string=lambda: "test"),
        SimpleNamespace(id=20, name="Provider B", get_user_agent_string=lambda: "test"),
    ]
    streams = [
        SimpleNamespace(
            id=index,
            name=f"Stream {index}",
            url=f"http://example.test/{index}",
            m3u_account=account,
            m3u_account_id=account.id,
            stream_stats={},
            stream_stats_updated_at=None,
        )
        for index, account in enumerate(accounts, start=1)
    ]
    rows = QuerySet(
        SimpleNamespace(channel_id=index, stream=stream)
        for index, stream in enumerate(streams, start=1)
    )
    models_module = types.ModuleType("apps.channels.models")
    models_module.ChannelStream = SimpleNamespace(objects=rows)
    monkeypatch.setitem(sys.modules, "apps", types.ModuleType("apps"))
    monkeypatch.setitem(sys.modules, "apps.channels", types.ModuleType("apps.channels"))
    monkeypatch.setitem(sys.modules, "apps.channels.models", models_module)
    django_module = types.ModuleType("django")
    django_utils_module = types.ModuleType("django.utils")
    django_timezone_module = types.ModuleType("django.utils.timezone")
    django_timezone_module.now = lambda: _now()
    django_utils_module.timezone = django_timezone_module
    monkeypatch.setitem(sys.modules, "django", django_module)
    monkeypatch.setitem(sys.modules, "django.utils", django_utils_module)
    monkeypatch.setitem(sys.modules, "django.utils.timezone", django_timezone_module)

    now = datetime.now(timezone.utc).isoformat()
    cache = {
        str(stream.id): {
            "status": "alive",
            "url_hash": analyzer._stream_url_hash(stream.url),
            "m3u_account_id": stream.m3u_account_id,
            "health_checked_at": now,
            "ffprobe_checked_at": now,
            "content_checked_at": now,
            "content_m3u_account_id": stream.m3u_account_id,
            "metadata_updated_at": now,
            "stats": {"resolution": "1920x1080", "source_fps": 30},
        }
        for stream in streams
    }
    monkeypatch.setattr(incremental.analyzer, "load_analysis_cache", lambda _path: cache)
    monkeypatch.setattr(incremental.analyzer, "save_analysis_cache", lambda *_args: None)
    monkeypatch.setattr(incremental, "load_throughput_cache", lambda _path: {})
    monkeypatch.setattr(
        incremental,
        "probe_stream",
        lambda url, *_args, **_kwargs: (
            {"status": "unknown", "tested_at": now, "error": "no measurement"}
            if str(url).endswith("/2")
            else {"status": "healthy", "tested_at": now, "measured_mbps": 10}
        ),
    )
    monkeypatch.setattr("stream_sorter.sorter.resolve_channel_scope", lambda _settings: (None, {}))

    class UnlimitedCapacity:
        def try_acquire(self, _item):
            return True, None

        def release(self, _reservation):
            pass

    monkeypatch.setattr(
        incremental,
        "build_capacity_manager",
        lambda _items, logger: UnlimitedCapacity(),
    )

    limiter_waits = []

    class RecordingLimiter:
        def __init__(self, delay_seconds):
            self.delay_seconds = delay_seconds

        def wait(self, account_id):
            limiter_waits.append(account_id)

    monkeypatch.setattr(incremental.analyzer, "_PerAccountStartLimiter", RecordingLimiter)
    result = analyze_assigned_streams(
        {
            "analysis_workers": 2,
            "playback_health_reuse": False,
            "probe_rate_per_minute": 1,
            "probe_per_account_delay_seconds": 1,
        },
        logger=SimpleNamespace(
            info=lambda *_args, **_kwargs: None,
            warning=lambda *_args, **_kwargs: None,
        ),
        cache_path=str(tmp_path / "analysis.json"),
    )

    assert result["media_checked"] == 0
    assert result["throughput_attempted"] == 2
    assert result["throughput_checked"] == 1
    assert result["analysis_health_report_path"] == str(
        tmp_path / "dispatcharr_stream_sort_health_report.json"
    )
    assert (tmp_path / "dispatcharr_stream_sort_health_report.json").exists()
    assert collections.Counter(limiter_waits) == {10: 1, 20: 1}
    assert None not in limiter_waits


def test_incremental_analyzer_is_installed_for_plugin_compatibility():
    assert analyzer.analyze_assigned_streams is analyze_assigned_streams


def test_health_and_metadata_have_independent_ttls():
    now = _now()
    entry = {
        "status": "alive",
        "url_hash": "abc",
        "health_checked_at": (now - timedelta(hours=20)).isoformat(),
        "metadata_updated_at": (now - timedelta(hours=2)).isoformat(),
        "stats": {"resolution": "1920x1080"},
    }
    assert health_check_reason(entry, url_hash="abc", ttl_hours=24, now=now) is None
    assert metadata_check_reason(entry, url_hash="abc", ttl_hours=12, now=now) is None
    assert health_check_reason(entry, url_hash="abc", ttl_hours=12, now=now) == "ttl_expired"


def test_content_ttl_is_independent_and_confirmed_dead_uses_dead_ttl():
    now = _now()
    alive = {
        "status": "alive",
        "url_hash": "same",
        "content_checked_at": (now - timedelta(hours=8)).isoformat(),
    }
    dead = {
        **alive,
        "status": "dead",
        "content_checked_at": now.isoformat(),
        "dead_checked_at": now.isoformat(),
    }

    assert content_check_reason(
        alive, url_hash="same", ttl_hours=7, dead_ttl_hours=1, now=now,
    ) == "ttl_expired"
    assert content_check_reason(
        dead, url_hash="same", ttl_hours=168, dead_ttl_hours=1, now=now,
    ) is None
    assert content_check_reason(
        dead, url_hash="same", ttl_hours=168, dead_ttl_hours=1, now=now + timedelta(hours=1),
    ) == "dead_ttl_expired"


def test_dead_skipped_and_unknown_health_are_rechecked_even_when_fresh():
    now = _now()
    for status in ("dead", "skipped", "unknown"):
        entry = {"status": status, "url_hash": "abc", "health_checked_at": now.isoformat()}
        assert health_check_reason(entry, url_hash="abc", ttl_hours=24, now=now) == f"status_{status}"


def test_dead_health_uses_dead_ttl_before_recheck():
    now = _now()
    entry = {
        "status": "dead",
        "url_hash": "abc",
        "health_checked_at": (now - timedelta(minutes=30)).isoformat(),
    }
    assert health_check_reason(entry, url_hash="abc", ttl_hours=24, dead_ttl_hours=1, now=now) == "status_dead_ttl"
    expired = {"status": "dead", "url_hash": "abc", "health_checked_at": (now - timedelta(hours=2)).isoformat()}
    assert health_check_reason(expired, url_hash="abc", ttl_hours=24, dead_ttl_hours=1, now=now) == "status_dead"


def test_dead_result_with_incomplete_retries_bypasses_dead_ttl():
    now = _now()
    entry = {
        "status": "dead",
        "url_hash": "abc",
        "health_checked_at": (now - timedelta(minutes=1)).isoformat(),
        "retry_pending": True,
    }
    assert health_check_reason(
        entry,
        url_hash="abc",
        ttl_hours=24,
        dead_ttl_hours=1,
        now=now,
    ) == "status_dead_retry_pending"


def test_dead_ttl_defers_the_complete_media_analysis_reason():
    now = _now()
    entry = {
        "status": "dead",
        "url_hash": "abc",
        "health_checked_at": (now - timedelta(minutes=30)).isoformat(),
    }
    assert incremental._analysis_reason(
        entry,
        url_hash="abc",
        health_ttl_hours=24,
        content_ttl_hours=168,
        metadata_ttl_hours=12,
        dead_ttl_hours=1,
        now=now,
    ) is None


def test_dead_ttl_does_not_use_jitter(monkeypatch):
    now = _now()
    entry = {
        "status": "dead",
        "url_hash": "abc",
        "health_checked_at": (now - timedelta(minutes=30)).isoformat(),
    }
    monkeypatch.setattr(incremental, "_ttl_with_jitter", lambda *_args, **_kwargs: 0)
    assert health_check_reason(
        entry,
        url_hash="abc",
        ttl_hours=24,
        dead_ttl_hours=1,
        ttl_jitter_percent=99,
        now=now,
    ) == "status_dead_ttl"


def test_ttl_jitter_assigns_stable_distinct_values_inside_the_configured_range():
    first = incremental._ttl_with_jitter(10, url_hash="stream-a", jitter_percent=20)
    second = incremental._ttl_with_jitter(10, url_hash="stream-b", jitter_percent=20)
    assert 8 <= first <= 12
    assert 8 <= second <= 12
    assert first != second
    assert first == incremental._ttl_with_jitter(10, url_hash="stream-a", jitter_percent=20)


def test_health_report_includes_interval_guidance_from_history():
    now = _now()
    stream_one = {
        "id": 1,
        "name": "Stream 1",
    }
    stream_two = {
        "id": 2,
        "name": "Stream 2",
    }
    cache = {
        "1": {
            "status": "alive",
            "health_check_history": [
                {"checked_at": (now - timedelta(hours=3)).isoformat(), "status": "alive", "reason": "ttl_expired"},
                {"checked_at": (now - timedelta(hours=1)).isoformat(), "status": "dead", "reason": "media_ttl_expired"},
                {"checked_at": now.isoformat(), "status": "alive", "reason": "media_ttl_expired"},
            ],
        },
        "2": {
            "status": "alive",
            "health_check_history": [
                {"checked_at": (now - timedelta(hours=4)).isoformat(), "status": "dead", "reason": "media_ttl_expired"},
                {"checked_at": now.isoformat(), "status": "dead", "reason": "media_ttl_expired"},
            ],
        },
    }
    report = _build_health_report(
        [stream_one, stream_two],
        cache,
        now=now,
        media_reason_counts={"health_ttl_expired": 3},
        throughput_reason_counts={"missing": 1},
        channels_selected=2,
    )
    assert report["observations"]["history_rows"] == 5
    assert report["observations"]["dead_checks"] == 3
    assert report["observations"]["status_changes"] == 2
    assert report["observations"]["dead_recovery_duration_hours"]["samples"] == 1
    assert report["observations"]["transition_counts"]["dead_to_alive"] == 1
    assert report["observations"]["check_interval_hours"]["p90"] is not None
    assert report["ttl_tuning_guidance"]["suggested_health_ttl_hours"] is not None


def test_terminal_history_uses_scan_start_status_across_media_and_content_phases():
    checked_at = _now().isoformat()

    def terminal_entry(stream_id, final_status):
        item = {
            "id": stream_id,
            "name": f"Stream {stream_id}",
            "url": f"http://example.test/{stream_id}",
            "account_id": 10,
            "account_name": "Provider",
        }
        media = incremental._merge_media_result(
            item,
            {"status": "dead", "dead_checked_at": checked_at},
            {"status": "alive", "tested_at": checked_at, "stats": {}, "details": {}},
            analysis_reason="ffprobe_dead_ttl_expired",
            record_history=False,
            history_previous_status="dead",
        )
        content = {
            "status": final_status,
            "tested_at": checked_at,
            "error_type": "frozen_video" if final_status == "dead" else None,
            "error": "frozen" if final_status == "dead" else "",
            "details": {"content": {"measured": True, "tested_at": checked_at}},
        }
        return item, incremental._merge_content_result(
            item,
            media,
            content,
            analysis_reason="dead_ttl_expired",
            history_previous_status="dead",
        )

    alive_item, alive_entry = terminal_entry(1, "alive")
    dead_item, dead_entry = terminal_entry(2, "dead")
    assert alive_entry["health_check_history"][-1]["previous_status"] == "dead"
    assert dead_entry["health_check_history"][-1]["previous_status"] == "dead"

    report = _build_health_report(
        [alive_item, dead_item],
        {"1": alive_entry, "2": dead_entry},
        now=_now(),
        media_reason_counts={},
        throughput_reason_counts={},
        channels_selected=2,
    )
    transitions = report["observations"]["transition_counts"]
    assert transitions["dead_to_alive"] == 1
    assert transitions.get("alive_to_dead", 0) == 0


def test_throughput_checked_uses_only_measurements_retained_after_terminal_health():
    cache = {
        "1": {"status": "alive", "throughput": {"status": "healthy", "measured_mbps": 12.5}},
        "2": {"status": "alive", "throughput": {"status": "unknown", "error": "no measurement"}},
        "3": {"status": "dead", "throughput": {"status": "unknown", "error": "invalidated by content"}},
        "4": {"status": "alive", "throughput": {"status": "healthy", "measured_mbps": 8.0}},
    }
    retained = incremental._retained_throughput_measurement_ids({1, 2, 3}, cache)
    assert retained == {1}


def test_problematic_streams_require_more_than_75_percent_dead_across_full_scope():
    now = _now()
    items = [
        {
            "id": index,
            "name": f"Stream {index}",
            "account_id": 10,
            "account_name": "Provider",
            "channels": [
                {
                    "channel_id": index,
                    "channel_name": f"Channel {index}",
                    "channel_number": index,
                }
            ],
        }
        for index in range(1, 26)
    ]
    cache = {}
    for item in items:
        statuses = (["dead"] * 15) + (["alive"] * 5)
        if item["id"] == 25:
            statuses = (["dead"] * 16) + (["alive"] * 4)
        cache[str(item["id"])] = {
            "status": statuses[-1],
            "health_check_history": [
                {
                    "checked_at": (now - timedelta(days=8) + timedelta(hours=index * 10)).isoformat(),
                    "previous_status": statuses[index - 1] if index else "unknown",
                    "status": value,
                    "reason": "ttl_expired",
                }
                for index, value in enumerate(statuses)
            ],
        }
    report = _build_health_report(
        items,
        cache,
        now=now,
        media_reason_counts={},
        throughput_reason_counts={},
        channels_selected=25,
    )
    assert report["top_metrics"]["dead_dominant_streams"] == [25]
    assert report["status_patterns"]["problematic_streams"][0]["stream_id"] == 25
    assert report["status_patterns"]["problematic_streams"][0]["source_name"] == "Provider"
    assert report["status_patterns"]["problematic_streams"][0]["channels"] == [
        {"channel_id": 25, "channel_name": "Channel 25", "channel_number": 25}
    ]


def test_health_report_preserves_all_channel_attachments_for_current_dead_stream():
    now = _now()
    stream = SimpleNamespace(id=42)
    rows = [
        SimpleNamespace(
            channel_id=100,
            channel=SimpleNamespace(id=100, name="Event One", channel_number=501),
            stream=stream,
        ),
        SimpleNamespace(
            channel_id=200,
            channel=SimpleNamespace(id=200, name="Event Two", channel_number=502),
            stream=stream,
        ),
    ]
    channels = incremental._channel_attachments_by_stream(rows)[42]
    item = {
        "id": 42,
        "name": "Shared Event Stream",
        "account_id": 10,
        "account_name": "Provider",
        "channels": channels,
    }
    report = _build_health_report(
        [item],
        {
            "42": {
                "status": "dead",
                "health_check_history": [
                    {
                        "checked_at": now.isoformat(),
                        "previous_status": "unknown",
                        "status": "dead",
                        "reason": "health_missing",
                        "terminal": True,
                    }
                ],
            }
        },
        now=now,
        media_reason_counts={"health_missing": 1},
        throughput_reason_counts={},
        channels_selected=2,
    )
    dead = report["status_patterns"]["current_dead_streams"]
    assert len(dead) == 1
    assert dead[0]["stream_id"] == 42
    assert dead[0]["status_changes"] == 0
    assert dead[0]["source_name"] == "Provider"
    assert dead[0]["channels"] == [
        {"channel_id": 100, "channel_name": "Event One", "channel_number": 501},
        {"channel_id": 200, "channel_name": "Event Two", "channel_number": 502},
    ]
    assert report["observations"]["status_changes"] == 0
    assert report["observations"]["transition_counts"] == {}


def test_initial_throughput_missing_reason_is_not_relabelled_by_media_refresh():
    assert incremental._normalize_throughput_check_reason(
        "missing",
        media_changed=True,
    ) == "throughput_missing"
    assert incremental._normalize_throughput_check_reason(
        None,
        media_changed=True,
    ) == "media_changed"


def test_persist_dispatcharr_result_does_not_write_provider_stale_state(monkeypatch):
    saved = {}
    stream = SimpleNamespace(
        stream_stats={},
        is_stale=True,
        save=lambda update_fields: saved.update(update_fields=list(update_fields)),
    )

    class Query:
        def first(self):
            return stream

    models_module = types.ModuleType("apps.channels.models")
    models_module.Stream = SimpleNamespace(objects=SimpleNamespace(filter=lambda **_kwargs: Query()))
    monkeypatch.setitem(sys.modules, "apps", types.ModuleType("apps"))
    monkeypatch.setitem(sys.modules, "apps.channels", types.ModuleType("apps.channels"))
    monkeypatch.setitem(sys.modules, "apps.channels.models", models_module)
    django_module = types.ModuleType("django")
    django_utils_module = types.ModuleType("django.utils")
    django_timezone_module = types.ModuleType("django.utils.timezone")
    django_timezone_module.now = lambda: _now()
    django_utils_module.timezone = django_timezone_module
    monkeypatch.setitem(sys.modules, "django", django_module)
    monkeypatch.setitem(sys.modules, "django.utils", django_utils_module)
    monkeypatch.setitem(sys.modules, "django.utils.timezone", django_timezone_module)
    warnings = []
    assert analyzer._persist_dispatcharr_result(
        42,
        {"status": "alive", "stats": {"resolution": "1920x1080"}},
        SimpleNamespace(warning=lambda *args, **_kwargs: warnings.append(args)),
    ), warnings
    assert stream.is_stale is True
    assert "is_stale" not in saved["update_fields"]


def test_url_change_invalidates_health_and_metadata():
    now = _now()
    entry = {
        "status": "alive",
        "url_hash": "old",
        "health_checked_at": now.isoformat(),
        "metadata_updated_at": now.isoformat(),
        "stats": {"resolution": "1920x1080"},
    }
    assert health_check_reason(entry, url_hash="new", ttl_hours=24, now=now) == "url_changed"
    assert metadata_check_reason(entry, url_hash="new", ttl_hours=12, now=now) == "url_changed"


def test_fresh_healthy_throughput_is_cached():
    now = _now()
    entry = {"throughput": {"status": "healthy", "url_hash": "abc", "checked_at": (now - timedelta(hours=2)).isoformat()}}
    assert throughput_check_reason(entry, url_hash="abc", ttl_hours=6, now=now) is None


def test_nonhealthy_throughput_uses_status_specific_jittered_ttls():
    now = _now()
    for status, status_ttl in (("marginal", 12), ("insufficient", 12), ("unknown", 4)):
        entry = {"throughput": {"status": status, "url_hash": "abc", "checked_at": now.isoformat()}}
        assert throughput_check_reason(
            entry,
            url_hash="abc",
            ttl_hours=24,
            degraded_ttl_hours=12,
            unknown_ttl_hours=4,
            ttl_jitter_percent=0,
            now=now,
        ) is None
        assert throughput_check_reason(
            entry,
            url_hash="abc",
            ttl_hours=24,
            degraded_ttl_hours=12,
            unknown_ttl_hours=4,
            ttl_jitter_percent=0,
            now=now + timedelta(hours=status_ttl),
        ) == f"status_{status}"


def test_provider_attributed_playback_reuses_profiles_but_not_other_providers():
    from stream_sorter import incremental

    now = _now()
    item = {
        "id": 42,
        "name": "Example",
        "url": "http://provider.test/live/base",
        "account_id": 7,
        "dispatcharr_stats": {"resolution": "1920x1080", "fps": 30},
    }
    url_hash = analyzer._stream_url_hash(item["url"])
    cache = {"42": {"status": "alive", "url_hash": url_hash, "stats": item["dispatcharr_stats"]}}
    reliability = {
        "streams": {
            "42": {
                "m3u_account_id": 7,
                "last_clean_playback_at": now.isoformat(),
                "last_clean_playback_seconds": 300,
                "last_clean_playback_m3u_account_id": 7,
                "playback_throughput_history": [{
                    "observed_at": now.isoformat(),
                    "kind": "clean_playback",
                    "runtime_seconds": 300,
                    "measured_mbps": 20.0,
                    "m3u_account_id": 7,
                    "eligible_for_throughput": True,
                }],
            }
        }
    }

    assert incremental._sync_runtime_playback_evidence(
        [item], cache, reliability,
        min_clean_playback_seconds=60,
        min_throughput_playback_seconds=300,
    ) == 1
    assert cache["42"]["content_source"] == "dispatcharr_playback_assumed"
    assert cache["42"]["content_m3u_account_id"] == 7
    assert cache["42"]["throughput"]["source"] == "dispatcharr_playback"
    assert content_check_reason(
        cache["42"],
        url_hash=url_hash,
        ttl_hours=168,
        dead_ttl_hours=1,
        provider_id=8,
        now=now,
    ) == "provider_changed"
    assert throughput_check_reason(
        cache["42"],
        url_hash=url_hash,
        ttl_hours=6,
        dead_ttl_hours=1,
        provider_id=8,
        now=now,
    ) == "provider_changed"

    other_provider_cache = {"42": {"status": "alive", "url_hash": url_hash, "stats": item["dispatcharr_stats"]}}
    reliability["streams"]["42"]["last_clean_playback_m3u_account_id"] = 8
    reliability["streams"]["42"]["playback_throughput_history"][0]["m3u_account_id"] = 8
    incremental._sync_runtime_playback_evidence(
        [item], other_provider_cache, reliability,
        min_clean_playback_seconds=60,
        min_throughput_playback_seconds=300,
    )
    assert "content_checked_at" not in other_provider_cache["42"]
    assert "throughput" not in other_provider_cache["42"]
    assert other_provider_cache["42"]["playback_throughput_history"][0]["provider_attribution_valid"] is False


def test_terminal_content_dead_uses_dead_ttl_for_every_phase():
    from stream_sorter import incremental

    now = _now()
    item = {"id": 9, "name": "Dead", "url": "http://provider.test/dead", "account_id": 7}
    url_hash = analyzer._stream_url_hash(item["url"])
    previous = {
        "status": "alive",
        "url_hash": url_hash,
        "ffprobe_checked_at": (now - timedelta(minutes=5)).isoformat(),
        "throughput": {"status": "healthy", "url_hash": url_hash, "checked_at": now.isoformat()},
    }
    dead = {
        "status": "dead",
        "error_type": "black_screen",
        "error": "black",
        "tested_at": now.isoformat(),
        "retry_pending": False,
        "details": {"content": {"measured": True, "tested_at": now.isoformat(), "black": True}},
    }
    merged = incremental._merge_content_result(item, previous, dead, analysis_reason="ttl_expired")

    assert incremental.ffprobe_check_reason(
        merged, url_hash=url_hash, ttl_hours=12, dead_ttl_hours=1, ttl_jitter_percent=30, now=now,
    ) is None
    assert content_check_reason(
        merged, url_hash=url_hash, ttl_hours=168, dead_ttl_hours=1, ttl_jitter_percent=30, now=now,
    ) is None
    assert throughput_check_reason(
        merged, url_hash=url_hash, ttl_hours=6, dead_ttl_hours=1, ttl_jitter_percent=30, now=now,
    ) is None
    assert "health_checked_at" not in merged
    assert incremental.ffprobe_check_reason(
        merged, url_hash=url_hash, ttl_hours=12, dead_ttl_hours=1, ttl_jitter_percent=30, now=now + timedelta(hours=1),
    ) == "dead_ttl_expired"


def test_no_applicable_content_detectors_complete_without_provider_work():
    result = {
        "status": "alive",
        "stats": {},
        "details": {"has_audio": False},
    }
    settings = {
        "black_screen_detection": False,
        "frozen_video_detection": False,
        "silent_audio_detection": True,
    }

    assert incremental._content_checks_applicable(result, settings) is False
    skipped = incremental._content_skipped_result(result)
    assert skipped["status"] == "alive"
    assert skipped["details"]["content"]["measured"] is True
    assert skipped["details"]["content"]["skip_reason"] == "no_applicable_detectors"


def test_shared_memory_capture_directory_requires_worker_headroom(tmp_path, monkeypatch):
    class EnoughSpace:
        f_frsize = 1
        f_bsize = 1
        f_bavail = 4 * 1024 * 1024 * 1024

    root = tmp_path / "stream-sorter"
    monkeypatch.setattr(incremental.os, "statvfs", lambda _path: EnoughSpace())
    assert incremental._select_capture_temp_directory(12, shared_memory_root=str(root)) == str(root)
    assert root.is_dir()

    EnoughSpace.f_bavail = 1
    assert incremental._select_capture_temp_directory(12, shared_memory_root=str(root)) is None


def test_shared_memory_capture_directory_falls_back_when_runtime_user_cannot_write(tmp_path, monkeypatch):
    class EnoughSpace:
        f_frsize = 1
        f_bsize = 1
        f_bavail = 4 * 1024 * 1024 * 1024

    warnings = []
    root = tmp_path / "stream-sorter"
    root.mkdir()
    monkeypatch.setattr(incremental.os, "statvfs", lambda _path: EnoughSpace())

    def denied(*_args, **_kwargs):
        raise PermissionError(13, "Permission denied", str(root))

    monkeypatch.setattr(incremental.tempfile, "mkstemp", denied)
    assert incremental._select_capture_temp_directory(
        12,
        shared_memory_root=str(root),
        logger=SimpleNamespace(warning=lambda *args: warnings.append(args)),
    ) is None
    assert warnings
    assert "falling back" in warnings[0][0]


def _run_single_combined_scan(tmp_path, monkeypatch, capture_stream_sample):
    class QuerySet(list):
        def select_related(self, *_args):
            return self

        def order_by(self, *_args):
            return self

        def filter(self, **_kwargs):
            return self

    account = SimpleNamespace(id=10, name="Provider", get_user_agent_string=lambda: "test")
    stream = SimpleNamespace(
        id=42,
        name="Stream 42",
        url="http://example.test/42",
        m3u_account=account,
        m3u_account_id=account.id,
        stream_stats={},
        stream_stats_updated_at=None,
    )
    rows = QuerySet([SimpleNamespace(channel_id=1, stream=stream)])
    models_module = types.ModuleType("apps.channels.models")
    models_module.ChannelStream = SimpleNamespace(objects=rows)
    monkeypatch.setitem(sys.modules, "apps", types.ModuleType("apps"))
    monkeypatch.setitem(sys.modules, "apps.channels", types.ModuleType("apps.channels"))
    monkeypatch.setitem(sys.modules, "apps.channels.models", models_module)
    django_module = types.ModuleType("django")
    django_utils_module = types.ModuleType("django.utils")
    django_timezone_module = types.ModuleType("django.utils.timezone")
    django_timezone_module.now = lambda: _now()
    django_utils_module.timezone = django_timezone_module
    monkeypatch.setitem(sys.modules, "django", django_module)
    monkeypatch.setitem(sys.modules, "django.utils", django_utils_module)
    monkeypatch.setitem(sys.modules, "django.utils.timezone", django_timezone_module)

    now = datetime.now(timezone.utc).isoformat()
    cache = {
        "42": {
            "status": "alive",
            "url_hash": analyzer._stream_url_hash(stream.url),
            "m3u_account_id": stream.m3u_account_id,
            "health_checked_at": now,
            "ffprobe_checked_at": now,
            "metadata_updated_at": now,
            "stats": {"resolution": "1920x1080", "source_fps": 30},
        }
    }
    monkeypatch.setattr(incremental.analyzer, "load_analysis_cache", lambda _path: cache)
    monkeypatch.setattr(incremental.analyzer, "save_analysis_cache", lambda *_args: None)
    monkeypatch.setattr(incremental.analyzer, "_persist_dispatcharr_result", lambda *_args: None)
    monkeypatch.setattr(incremental, "load_throughput_cache", lambda _path: {})
    monkeypatch.setattr(incremental, "capture_stream_sample", capture_stream_sample)
    monkeypatch.setattr(
        incremental,
        "_analyze_local_capture",
        lambda _item, base, _path, **_kwargs: {
            **dict(base),
            "status": "alive",
            "details": {"content": {"measured": True, "tested_at": now}},
        },
    )
    monkeypatch.setattr(
        incremental,
        "probe_stream",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected throughput-only fallback")),
    )
    monkeypatch.setattr("stream_sorter.sorter.resolve_channel_scope", lambda _settings: (None, {}))

    class UnlimitedCapacity:
        def try_acquire(self, _item):
            return True, None

        def release(self, _reservation):
            pass

    monkeypatch.setattr(incremental, "build_capacity_manager", lambda _items, logger: UnlimitedCapacity())
    ticks = itertools.count()
    monkeypatch.setattr(incremental.time, "monotonic", lambda: float(next(ticks)))
    messages = {"info": [], "warning": []}
    logger = SimpleNamespace(
        info=lambda *args, **_kwargs: messages["info"].append(args),
        warning=lambda *args, **_kwargs: messages["warning"].append(args),
    )
    result = analyze_assigned_streams(
        {"analysis_workers": 1, "playback_health_reuse": False},
        logger=logger,
        cache_path=str(tmp_path / "analysis.json"),
    )
    return result, cache, messages


def test_failed_combined_capture_retries_both_and_preserves_no_false_throughput_check(tmp_path, monkeypatch):
    calls = []

    def capture(*_args, **_kwargs):
        calls.append(1)
        if len(calls) == 1:
            return {
                "status": "unknown",
                "tested_at": _now().isoformat(),
                "error_type": "stream_unreachable",
                "error": "PermissionError: denied",
            }, None
        return {
            "status": "healthy",
            "tested_at": _now().isoformat(),
            "measured_mbps": 12.0,
        }, str(tmp_path / "capture.ts")

    result, cache, messages = _run_single_combined_scan(tmp_path, monkeypatch, capture)
    assert len(calls) == 2
    assert result["content_checked"] == 1
    assert result["throughput_attempted"] == 1
    assert result["throughput_checked"] == 1
    assert cache["42"]["status"] == "alive"
    assert cache["42"]["throughput"]["status"] == "healthy"
    assert any("content and throughput remain incomplete" in args[0] for args in messages["warning"])


def test_exhausted_combined_capture_retries_mark_dead_without_throughput_ttl(tmp_path, monkeypatch):
    calls = []

    def capture(*_args, **_kwargs):
        calls.append(1)
        return {
            "status": "unknown",
            "tested_at": _now().isoformat(),
            "error_type": "stream_unreachable",
            "error": "PermissionError: denied",
        }, None

    result, cache, _messages = _run_single_combined_scan(tmp_path, monkeypatch, capture)
    assert len(calls) == 4
    assert result["content_checked"] == 0
    assert result["throughput_attempted"] == 1
    assert result["throughput_checked"] == 0
    assert cache["42"]["status"] == "dead"
    assert cache["42"]["retry_pending"] is False
    assert "throughput" not in cache["42"]


def test_local_combined_analysis_always_deletes_capture(tmp_path, monkeypatch):
    sample_path = tmp_path / "stream-sort-capture-test.ts"
    sample_path.write_bytes(b"sample")

    def analyze(_base_result, path, **_kwargs):
        assert Path(path).exists()
        return {"status": "alive", "details": {"content": {"measured": True}}}

    monkeypatch.setattr(incremental.analyzer, "apply_content_analysis", analyze)
    result = incremental._analyze_local_capture(
        {"id": 42, "user_agent": "test"},
        {"status": "alive"},
        str(sample_path),
        settings={},
        logger=SimpleNamespace(),
    )

    assert result["status"] == "alive"
    assert not sample_path.exists()


def test_dispatcharr_metadata_refresh_does_not_refresh_health_or_throughput():
    now = _now()
    url = "http://example.test/live"
    url_hash = analyzer._stream_url_hash(url)
    previous_health = (now - timedelta(hours=10)).isoformat()
    previous_throughput = (now - timedelta(hours=2)).isoformat()
    item = {
        "id": 42,
        "name": "Example",
        "url": url,
        "account_id": 3,
        "account_name": "Provider",
        "dispatcharr_stats": {"resolution": "1920x1080", "source_fps": 59.94, "video_bitrate": 5000},
        "dispatcharr_stats_updated_at": now.isoformat(),
    }
    cache = {
        "42": {
            "status": "alive",
            "url_hash": url_hash,
            "health_checked_at": previous_health,
            "metadata_updated_at": (now - timedelta(hours=13)).isoformat(),
            "stats": {"resolution": "1280x720", "source_fps": 29.97, "video_bitrate": 2500},
            "throughput": {"status": "healthy", "url_hash": url_hash, "checked_at": previous_throughput},
        }
    }
    refreshed, changed_ids = incremental._sync_dispatcharr_metadata([item], cache)
    entry = cache["42"]
    assert refreshed == 1
    assert changed_ids == {42}
    assert entry["metadata_source"] == "dispatcharr_stream_stats"
    assert entry["metadata_updated_at"] == now.isoformat()
    assert entry["stats"]["resolution"] == "1920x1080"
    assert entry["health_checked_at"] == previous_health
    assert entry["throughput"]["checked_at"] == previous_throughput


def test_older_dispatcharr_metadata_is_ignored():
    now = _now()
    url = "http://example.test/live"
    url_hash = analyzer._stream_url_hash(url)
    item = {
        "id": 42,
        "url": url,
        "dispatcharr_stats": {"resolution": "1920x1080"},
        "dispatcharr_stats_updated_at": (now - timedelta(hours=3)).isoformat(),
    }
    cache = {"42": {"status": "alive", "url_hash": url_hash, "metadata_updated_at": (now - timedelta(hours=1)).isoformat(), "stats": {"resolution": "1280x720"}}}
    refreshed, changed_ids = incremental._sync_dispatcharr_metadata([item], cache)
    assert refreshed == 0
    assert changed_ids == set()
    assert cache["42"]["stats"]["resolution"] == "1280x720"


def test_media_stats_bitrate_change_is_treated_as_significant_only_above_tolerance():
    assert not _is_significant_bitrate_change(5000.0, 5600.0)
    assert _is_significant_bitrate_change(5000.0, 7000.0)
    assert not _is_significant_bitrate_change(None, 5000.0)
    assert not _is_significant_bitrate_change(5000.0, None)


def test_media_stats_changed_for_throughput_uses_signature_and_bitrate_tolerance():
    previous = {"resolution": "1920x1080", "source_fps": 60, "video_bitrate": 5000}
    noisy_change = {"resolution": "1920x1080", "source_fps": 60, "video_bitrate": 5600}
    clear_change = {"resolution": "1920x1080", "source_fps": 60, "video_bitrate": 7000}
    fps_change = {"resolution": "1920x1080", "source_fps": 30, "video_bitrate": 5600}
    bitrate_history = [{"stats": previous}, {"stats": previous}, {"stats": previous}, {"stats": clear_change}]
    fps_history = [{"stats": previous}, {"stats": previous}, {"stats": previous}, {"stats": fps_change}]

    assert not _media_stats_changed_for_throughput(previous, noisy_change, media_history=bitrate_history)
    assert _media_stats_changed_for_throughput(previous, clear_change, media_history=bitrate_history)
    assert _media_stats_changed_for_throughput(previous, fps_change, media_history=fps_history)
    assert not _media_stats_changed_for_throughput(previous, {})
    assert not _media_stats_changed_for_throughput({}, previous)


def test_media_change_thresholds_are_configurable():
    previous = {"resolution": "1920x1080", "source_fps": 60, "video_bitrate": 5000}
    minor_change = {"resolution": "1920x1080", "source_fps": 60, "video_bitrate": 5600}
    assert not _media_stats_changed_for_throughput(
        previous,
        minor_change,
        media_bitrate_relative_tolerance=0.05,
        media_history=[{"stats": previous}, {"stats": previous}, {"stats": previous}, {"stats": previous}],
    )
    assert _media_stats_changed_for_throughput(
        previous,
        minor_change,
        media_bitrate_relative_tolerance=0.05,
        media_history=[{"stats": previous}, {"stats": previous}, {"stats": previous}, {"stats": minor_change}],
    )


def test_dispatcharr_metadata_uses_configured_bitrate_change_thresholds():
    now = _now()
    url = "http://example.test/live"
    item = {
        "id": 42,
        "url": url,
        "dispatcharr_stats": {"resolution": "1920x1080", "source_fps": 60, "video_bitrate": 5600},
        "dispatcharr_stats_updated_at": now.isoformat(),
    }
    cache = {"42": {
        "url_hash": analyzer._stream_url_hash(url),
        "metadata_updated_at": (now - timedelta(hours=1)).isoformat(),
        "stats": {"resolution": "1920x1080", "source_fps": 60, "video_bitrate": 5000},
    }}
    refreshed, changed = incremental._sync_dispatcharr_metadata(
        [item], cache,
        media_bitrate_relative_tolerance=0.30,
    )
    assert refreshed == 1
    assert changed == set()

    item["dispatcharr_stats"] = {"resolution": "1920x1080", "source_fps": 60, "video_bitrate": 7500}
    item["dispatcharr_stats_updated_at"] = (now + timedelta(minutes=1)).isoformat()
    refreshed, changed = incremental._sync_dispatcharr_metadata(
        [item], cache,
        media_bitrate_relative_tolerance=0.30,
    )
    assert refreshed == 1
    assert changed == set()


def test_skipped_media_result_preserves_previous_metadata():
    now = _now().isoformat()
    item = {"id": 42, "url": "http://example.test/live", "name": "Example", "account_id": 3, "account_name": "Provider"}
    previous = {"status": "alive", "stats": {"resolution": "1920x1080"}, "metadata_updated_at": now}
    result = {"tested_at": now, "status": "skipped", "error_type": "rate_limited", "error": "429", "stats": {}, "details": {}}
    merged = incremental._merge_media_result(item, previous, result)
    assert merged["status"] == "skipped"
    assert merged["stats"]["resolution"] == "1920x1080"
    assert merged["metadata_updated_at"] == now


def test_matching_legacy_throughput_is_migrated(monkeypatch):
    url = "http://example.test/live"
    url_hash = analyzer._stream_url_hash(url)
    item = {"id": 42, "url": url, "account_id": 3, "account_name": "Provider"}
    cache = {"42": {"status": "alive", "url_hash": url_hash, "stats": {"height": 1080}}}
    monkeypatch.setattr(incremental, "load_throughput_cache", lambda _path: {"42": {"status": "healthy", "measured_mbps": 12.3, "tested_at": _now().isoformat()}})
    assert incremental._migrate_legacy_throughput([item], cache) == 1
    assert cache["42"]["throughput"]["status"] == "healthy"
    assert "expires_at" not in cache["42"]["throughput"]


def test_clean_runtime_playback_reuses_reachability_and_defers_content_check():
    now = _now()
    url = "http://example.test/live"
    item = {
        "id": 42,
        "name": "Example",
        "url": url,
        "account_id": 3,
        "account_name": "Provider",
        "dispatcharr_stats": {"resolution": "1280x720", "source_fps": 59.94},
        "dispatcharr_stats_updated_at": now.isoformat(),
    }
    reliability = {
        "streams": {
            "42": {
                "last_clean_playback_at": now.isoformat(),
                "last_clean_playback_seconds": 61,
                "reliability_evidence": {"updated_at": now.isoformat(), "playback_seconds": 61},
            }
        }
    }
    cache = {}
    refreshed = _sync_runtime_playback_health(
        [item], cache, reliability,
        min_playback_seconds=300,
        min_clean_playback_seconds=60,
        ttl_hours=6,
        now=now,
    )
    assert refreshed == 1
    assert cache["42"]["status"] == "alive"
    assert cache["42"]["health_source"] == "runtime_playback"
    assert health_check_reason(
        cache["42"], url_hash=analyzer._stream_url_hash(url), ttl_hours=6,
        content_ttl_hours=168, now=now + timedelta(hours=1),
    ) is None
    assert health_check_reason(
        cache["42"], url_hash=analyzer._stream_url_hash(url), ttl_hours=999,
        content_ttl_hours=168, now=now + timedelta(hours=169),
    ) == "content_missing"


def test_short_unfinished_playback_does_not_replace_reachability_probe():
    now = _now()
    item = {"id": 42, "name": "Example", "url": "http://example.test/live"}
    reliability = {"streams": {"42": {"reliability_evidence": {"updated_at": now.isoformat(), "playback_seconds": 59}}}}
    cache = {}
    assert _sync_runtime_playback_health(
        [item], cache, reliability,
        min_playback_seconds=300,
        min_clean_playback_seconds=60,
        ttl_hours=6,
        now=now,
    ) == 0
    assert cache == {}


def test_runtime_playback_never_clears_confirmed_dead_health():
    now = _now()
    item = {"id": 42, "name": "Example", "url": "http://example.test/live"}
    reliability = {"streams": {"42": {
        "last_clean_playback_at": (now - timedelta(minutes=1)).isoformat(),
        "last_clean_playback_seconds": 120,
        "reliability_evidence": {
            "updated_at": (now - timedelta(minutes=1)).isoformat(),
            "playback_seconds": 120,
        },
    }}}
    cache = {"42": {
        "status": "dead",
        "url_hash": analyzer._stream_url_hash(item["url"]),
        "health_checked_at": now.isoformat(),
    }}
    assert _sync_runtime_playback_health(
        [item], cache, reliability,
        min_playback_seconds=300,
        min_clean_playback_seconds=60,
        ttl_hours=6,
        now=now,
    ) == 0
    assert cache["42"]["status"] == "dead"
