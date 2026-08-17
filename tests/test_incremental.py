from datetime import datetime, timedelta, timezone

from stream_sorter import analyzer
from stream_sorter.incremental import (
    analyze_assigned_streams,
    media_check_reason,
    throughput_check_reason,
)


def _now():
    return datetime(2026, 8, 17, 20, 0, tzinfo=timezone.utc)


def test_incremental_analyzer_is_installed_for_plugin_compatibility():
    assert analyzer.analyze_assigned_streams is analyze_assigned_streams


def test_fresh_alive_media_is_cached():
    now = _now()
    entry = {"status": "alive", "url_hash": "abc", "media_checked_at": (now - timedelta(hours=2)).isoformat()}
    assert media_check_reason(entry, url_hash="abc", ttl_hours=12, now=now) is None


def test_dead_and_skipped_media_are_rechecked_even_when_fresh():
    now = _now()
    for status in ("dead", "skipped", "unknown"):
        entry = {"status": status, "url_hash": "abc", "media_checked_at": now.isoformat()}
        assert media_check_reason(entry, url_hash="abc", ttl_hours=12, now=now) == f"status_{status}"


def test_alive_media_rechecks_when_ttl_expires():
    now = _now()
    entry = {"status": "alive", "url_hash": "abc", "media_checked_at": (now - timedelta(hours=12, seconds=1)).isoformat()}
    assert media_check_reason(entry, url_hash="abc", ttl_hours=12, now=now) == "ttl_expired"


def test_url_change_invalidates_media_immediately():
    now = _now()
    entry = {"status": "alive", "url_hash": "old", "media_checked_at": now.isoformat()}
    assert media_check_reason(entry, url_hash="new", ttl_hours=12, now=now) == "url_changed"


def test_fresh_healthy_throughput_is_cached():
    now = _now()
    entry = {"throughput": {"status": "healthy", "url_hash": "abc", "checked_at": (now - timedelta(hours=2)).isoformat()}}
    assert throughput_check_reason(entry, url_hash="abc", ttl_hours=6, now=now) is None


def test_nonhealthy_throughput_is_rechecked_even_when_fresh():
    now = _now()
    for status in ("marginal", "insufficient", "unknown"):
        entry = {"throughput": {"status": status, "url_hash": "abc", "checked_at": now.isoformat()}}
        assert throughput_check_reason(entry, url_hash="abc", ttl_hours=6, now=now) == f"status_{status}"


def test_healthy_throughput_rechecks_when_ttl_expires():
    now = _now()
    entry = {"throughput": {"status": "healthy", "url_hash": "abc", "checked_at": (now - timedelta(hours=6, seconds=1)).isoformat()}}
    assert throughput_check_reason(entry, url_hash="abc", ttl_hours=6, now=now) == "ttl_expired"


def test_zero_ttl_forces_recheck():
    now = _now()
    media = {"status": "alive", "url_hash": "abc", "media_checked_at": now.isoformat()}
    throughput = {"throughput": {"status": "healthy", "url_hash": "abc", "checked_at": now.isoformat()}}
    assert media_check_reason(media, url_hash="abc", ttl_hours=0, now=now) == "ttl_forced"
    assert throughput_check_reason(throughput, url_hash="abc", ttl_hours=0, now=now) == "ttl_forced"
