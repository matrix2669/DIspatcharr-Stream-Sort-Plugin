import collections
import sys
import threading
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from stream_sorter import analyzer, incremental
from stream_sorter.incremental import (
    _fair_account_futures,
    _build_health_report,
    _media_stats_changed_for_throughput,
    _is_significant_bitrate_change,
    _sync_runtime_playback_health,
    analyze_assigned_streams,
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
            "health_checked_at": now,
            "content_checked_at": now,
            "metadata_updated_at": now,
            "stats": {"resolution": "1920x1080", "source_fps": 30},
        }
        for stream in streams
    }
    monkeypatch.setattr(incremental.analyzer, "load_analysis_cache", lambda _path: cache)
    monkeypatch.setattr(incremental.analyzer, "save_analysis_cache", lambda *_args: None)
    monkeypatch.setattr(incremental, "load_throughput_cache", lambda _path: {})
    monkeypatch.setattr(incremental, "probe_stream", lambda *_args, **_kwargs: {
        "status": "healthy",
        "tested_at": now,
        "measured_mbps": 10,
    })
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
    assert result["throughput_checked"] == 2
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


def test_problematic_streams_require_more_than_75_percent_dead_across_full_scope():
    now = _now()
    items = [{"id": index, "name": f"Stream {index}"} for index in range(1, 26)]
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


def test_nonhealthy_throughput_is_rechecked_even_when_fresh():
    now = _now()
    for status in ("marginal", "insufficient", "unknown"):
        entry = {"throughput": {"status": status, "url_hash": "abc", "checked_at": now.isoformat()}}
        assert throughput_check_reason(entry, url_hash="abc", ttl_hours=6, now=now) == f"status_{status}"


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
    fps_change = {"resolution": "1920x1080", "source_fps": 59.9, "video_bitrate": 5600}

    assert not _media_stats_changed_for_throughput(previous, noisy_change)
    assert _media_stats_changed_for_throughput(previous, clear_change)
    assert _media_stats_changed_for_throughput(previous, fps_change)
    assert not _media_stats_changed_for_throughput(previous, {})
    assert _media_stats_changed_for_throughput({}, previous)


def test_media_change_thresholds_are_configurable():
    previous = {"resolution": "1920x1080", "source_fps": 60, "video_bitrate": 5000}
    minor_change = {"resolution": "1920x1080", "source_fps": 60, "video_bitrate": 5600}
    assert not _media_stats_changed_for_throughput(
        previous,
        minor_change,
        media_bitrate_relative_tolerance=0.05,
        media_bitrate_absolute_tolerance_kbps=1000.0,
    )
    assert _media_stats_changed_for_throughput(
        previous,
        minor_change,
        media_bitrate_relative_tolerance=0.05,
        media_bitrate_absolute_tolerance_kbps=100.0,
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
        media_bitrate_absolute_tolerance_kbps=500,
    )
    assert refreshed == 1
    assert changed == set()

    item["dispatcharr_stats"] = {"resolution": "1920x1080", "source_fps": 60, "video_bitrate": 7500}
    item["dispatcharr_stats_updated_at"] = (now + timedelta(minutes=1)).isoformat()
    refreshed, changed = incremental._sync_dispatcharr_metadata(
        [item], cache,
        media_bitrate_relative_tolerance=0.30,
        media_bitrate_absolute_tolerance_kbps=500,
    )
    assert refreshed == 1
    assert changed == {42}


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
    assert incremental._migrate_legacy_throughput([item], cache, ttl_hours=6) == 1
    assert cache["42"]["throughput"]["status"] == "healthy"
    assert "expires_at" in cache["42"]["throughput"]


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
