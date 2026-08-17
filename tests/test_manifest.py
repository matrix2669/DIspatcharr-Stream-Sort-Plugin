import json
from pathlib import Path


def test_plugin_manifest_is_valid_and_matches_version():
    root = Path(__file__).parents[1]
    manifest = json.loads((root / "stream_sorter" / "plugin.json").read_text())
    assert manifest["name"] == "Dispatcharr Stream Sort"
    assert manifest["version"] == "0.2.1"
    assert {a["id"] for a in manifest["actions"]} == {
        "analyze_streams",
        "dry_run",
        "sort_streams",
        "analyze_and_sort",
    }
    field_ids = {field["id"] for field in manifest["fields"]}
    assert "stream_data_ttl_hours" in field_ids
    assert "healthy_throughput_ttl_hours" in field_ids
    assert "throughput_cache_ttl_minutes" not in field_ids
