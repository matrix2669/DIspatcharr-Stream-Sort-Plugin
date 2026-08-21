from datetime import datetime, timedelta, timezone

from stream_sorter import analyzer, incremental
from stream_sorter.incremental import (
    _sync_runtime_playback_health,
    analyze_assigned_streams,
    health_check_reason,
    metadata_check_reason,
    throughput_check_reason,
)


def _now():
    return datetime(2026, 8, 17, 20, 0, tzinfo=timezone.utc)


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
