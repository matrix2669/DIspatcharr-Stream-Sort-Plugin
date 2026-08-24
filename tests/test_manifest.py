import json
import tomllib
from pathlib import Path


def test_plugin_manifest_is_valid_and_matches_version():
    root = Path(__file__).parents[1]
    manifest = json.loads((root / "stream_sorter" / "plugin.json").read_text())
    project = tomllib.loads((root / "pyproject.toml").read_text())
    version = (root / "VERSION").read_text().strip()
    assert manifest["name"] == "Dispatcharr Stream Sort"
    assert version == manifest["version"] == project["project"]["version"]
    assert {a["id"] for a in manifest["actions"]} == {
        "analyze_streams",
        "check_analysis_status",
        "stop_analysis",
        "apply_schedule",
        "schedule_status",
            "remove_schedule",
            "health_report",
            "recommend_ttls",
            "reset_statistics",
        "dry_run",
        "sort_streams",
        "analyze_and_sort",
        "record_runtime_event",
    }
    reliability_action = next(a for a in manifest["actions"] if a["id"] == "record_runtime_event")
    assert set(reliability_action["events"]) == {
        "channel_start",
        "channel_stop",
        "channel_buffering",
        "channel_reconnect",
        "channel_error",
        "channel_failover",
        "stream_switch",
    }
    field_ids = {field["id"] for field in manifest["fields"]}
    assert "stream_data_ttl_hours" in field_ids
    assert "health_content_ttl_hours" not in field_ids
    assert "dead_content_ttl_hours" in field_ids
    assert "media_bitrate_relative_tolerance_percent" in field_ids
    assert "media_bitrate_absolute_tolerance_kbps" in field_ids
    assert "analysis_ttl_jitter_percent" in field_ids
    jitter_field = next(field for field in manifest["fields"] if field["id"] == "analysis_ttl_jitter_percent")
    assert jitter_field["default"] == 30
    assert "playback_health_reuse" in field_ids
    assert "playback_health_clean_min_seconds" in field_ids
    assert "playback_health_ttl_hours" not in field_ids
    assert "content_validation_ttl_hours" in field_ids
    assert "healthy_throughput_ttl_hours" in field_ids
    assert "reset_statistics_include_history" in field_ids
    assert "probe_per_account_delay_seconds" in field_ids
    assert "probe_rate_per_minute" not in field_ids
    assert "reliability_info" in field_ids
    assert "reliability_scoring_enabled" in field_ids
    assert "throughput_cache_ttl_minutes" not in field_ids
