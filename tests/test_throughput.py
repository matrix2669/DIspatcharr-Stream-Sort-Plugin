import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from stream_sorter.throughput import _split_url_headers, load_cache, probe_stream, save_cache


def test_split_m3u_url_headers():
    url, headers = _split_url_headers(
        "http://example.test/live|User-Agent=VLC%2F3.0&Referer=http%3A%2F%2Ffoo.test%2F"
    )
    assert url == "http://example.test/live"
    assert headers["User-Agent"] == "VLC/3.0"
    assert headers["Referer"] == "http://foo.test/"


def test_cache_round_trip(tmp_path: Path):
    path = tmp_path / "cache.json"
    payload = {"42": {"status": "healthy", "measured_mbps": 12.3}}
    save_cache(payload, str(path))
    assert load_cache(str(path)) == payload
    assert json.loads(path.read_text()) == payload


def test_unified_cache_extracts_nested_throughput_and_ignores_legacy_ttl_timestamp(tmp_path: Path):
    path = tmp_path / "analysis.json"
    expires = datetime.now(timezone.utc) + timedelta(hours=2)
    path.write_text(
        json.dumps(
            {
                "42": {
                    "status": "alive",
                    "stats": {"height": 1080},
                    "throughput": {
                        "status": "healthy",
                        "measured_mbps": 12.3,
                        "tested_at": datetime.now(timezone.utc).isoformat(),
                        "expires_at": expires.isoformat(),
                    },
                }
            }
        )
    )
    loaded = load_cache(str(path))
    assert loaded["42"]["status"] == "healthy"
    assert loaded["42"]["measured_mbps"] == 12.3
    assert "tested_at" not in loaded["42"]


def test_unified_cache_marks_expired_throughput_unknown(tmp_path: Path):
    path = tmp_path / "analysis.json"
    expired = datetime.now(timezone.utc) - timedelta(seconds=1)
    path.write_text(
        json.dumps(
            {
                "42": {
                    "status": "alive",
                    "throughput": {
                        "status": "healthy",
                        "measured_mbps": 12.3,
                        "expires_at": expired.isoformat(),
                    },
                }
            }
        )
    )
    assert load_cache(str(path))["42"]["status"] == "unknown"


def test_corrupt_cache_returns_empty(tmp_path: Path):
    path = tmp_path / "cache.json"
    path.write_text("not-json")
    assert load_cache(str(path)) == {}


def test_missing_url_probe_is_unknown_not_dead():
    result = probe_stream("", nominal_video_kbps=6000)
    assert result["status"] == "unknown"
    assert "error" in result
