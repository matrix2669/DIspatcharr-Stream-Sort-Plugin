import json
from pathlib import Path


def test_plugin_manifest_is_valid_and_matches_version():
    root = Path(__file__).parents[1]
    manifest = json.loads((root / "stream_sorter" / "plugin.json").read_text())
    assert manifest["name"] == "Dispatcharr Stream Sort"
    assert manifest["version"] == "0.1.1"
    assert {a["id"] for a in manifest["actions"]} == {"dry_run", "sort_streams", "probe_throughput", "probe_and_sort"}
