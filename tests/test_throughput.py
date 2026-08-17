import json
from pathlib import Path

from stream_sorter.throughput import _split_url_headers, load_cache, save_cache


def test_split_m3u_url_headers():
    url, headers = _split_url_headers("http://example.test/live|User-Agent=VLC%2F3.0&Referer=http%3A%2F%2Ffoo.test%2F")
    assert url == "http://example.test/live"
    assert headers["User-Agent"] == "VLC/3.0"
    assert headers["Referer"] == "http://foo.test/"


def test_cache_round_trip(tmp_path: Path):
    path = tmp_path / "cache.json"; payload = {"42": {"status": "healthy", "measured_mbps": 12.3}}
    save_cache(payload, str(path)); assert load_cache(str(path)) == payload; assert json.loads(path.read_text()) == payload


def test_corrupt_cache_returns_empty(tmp_path: Path):
    path = tmp_path / "cache.json"; path.write_text("not-json"); assert load_cache(str(path)) == {}
