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
        "reset_scan_statistics",
        "reset_all_statistics",
        "dry_run",
        "sort_streams",
        "analyze_and_sort",
        "record_runtime_event",
    }
    assert [action["id"] for action in manifest["actions"]] == [
        "analyze_streams",
        "sort_streams",
        "analyze_and_sort",
        "check_analysis_status",
        "stop_analysis",
        "dry_run",
        "apply_schedule",
        "remove_schedule",
        "schedule_status",
        "health_report",
        "recommend_ttls",
        "reset_scan_statistics",
        "reset_all_statistics",
        "record_runtime_event",
    ]
    reliability_action = next(a for a in manifest["actions"] if a["id"] == "record_runtime_event")
    assert reliability_action["label"] == "Runtime Reliability (automatic)"
    assert reliability_action["button_label"] == "Automatic only"
    assert "informational no-op" in reliability_action["description"]
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
    assert "source_scores" not in field_ids
    assert [field["id"] for field in manifest["fields"]] == [
        "scoring_info",
        "filter_info",
        "channel_filter_type",
        "analyze_sort_filter",
        "analyze_only_filter",
        "name_score_rules",
        "analysis_info",
        "analysis_ffprobe_path",
        "analysis_ffmpeg_path",
        "analysis_duration_seconds",
        "analysis_connection_timeout_seconds",
        "analysis_probe_timeout_seconds",
        "media_bitrate_change_settings",
        "media_bitrate_relative_tolerance_percent",
        "minimum_video_bitrate_kbps",
        "placeholder_file_detection",
        "content_sample_seconds",
        "content_ffmpeg_timeout_seconds",
        "black_screen_detection",
        "black_screen_min_seconds",
        "frozen_video_detection",
        "frozen_video_min_seconds",
        "silent_audio_detection",
        "silent_audio_max_db",
        "throughput_info",
        "probe_duration_seconds",
        "probe_timeout_seconds",
        "analysis_retries",
        "analysis_per_account_delay_seconds",
        "probe_per_account_delay_seconds",
        "stream_data_ttl_hours",
        "content_validation_ttl_hours",
        "healthy_throughput_ttl_hours",
        "degraded_throughput_ttl_hours",
        "unknown_throughput_ttl_hours",
        "analysis_ttl_jitter_percent",
        "dead_content_ttl_hours",
        "playback_health_reuse",
        "playback_health_clean_min_seconds",
        "playback_health_min_seconds",
        "reliability_info",
        "reliability_scoring_enabled",
        "analysis_workers",
        "stream_sort_schedule_cron",
        "stream_sort_apply_sort_after_scheduled_scan",
        "stream_sort_allow_parallel_checks_on_scheduled_scan",
    ]
    assert "paths_info" not in field_ids
    name_rules = next(field for field in manifest["fields"] if field["id"] == "name_score_rules")
    assert "," in name_rules["default"]
    assert "\n" not in name_rules["default"]
    assert "channel_filter_type" in field_ids
    assert "analyze_sort_filter" in field_ids
    assert "analyze_only_filter" in field_ids
    assert "channel_group_filter" not in field_ids
    assert "channel_profile_filter" not in field_ids
    filter_type = next(field for field in manifest["fields"] if field["id"] == "channel_filter_type")
    assert {option["value"] for option in filter_type["options"]} == {
        "channel_group",
        "channel_profile",
    }
    assert "analysis_max_streams" not in field_ids
    assert "analysis_max_streams" not in (root / "stream_sorter" / "incremental.py").read_text()
    assert "stream_data_ttl_hours" in field_ids
    assert "health_content_ttl_hours" not in field_ids
    assert "dead_content_ttl_hours" in field_ids
    assert "media_bitrate_relative_tolerance_percent" in field_ids
    assert "media_bitrate_absolute_tolerance_kbps" not in field_ids
    minimum_bitrate = next(field for field in manifest["fields"] if field["id"] == "minimum_video_bitrate_kbps")
    assert minimum_bitrate["default"] == 500
    assert "analysis_ttl_jitter_percent" in field_ids
    jitter_field = next(field for field in manifest["fields"] if field["id"] == "analysis_ttl_jitter_percent")
    assert jitter_field["default"] == 30
    assert "playback_health_reuse" in field_ids
    assert "playback_health_clean_min_seconds" in field_ids
    assert "playback_health_ttl_hours" not in field_ids
    assert "content_validation_ttl_hours" in field_ids
    assert "healthy_throughput_ttl_hours" in field_ids
    assert next(field for field in manifest["fields"] if field["id"] == "healthy_throughput_ttl_hours")["default"] == 24
    assert next(field for field in manifest["fields"] if field["id"] == "degraded_throughput_ttl_hours")["default"] == 12
    assert next(field for field in manifest["fields"] if field["id"] == "unknown_throughput_ttl_hours")["default"] == 4
    assert "reset_statistics_include_history" not in field_ids
    assert "probe_per_account_delay_seconds" in field_ids
    assert "probe_rate_per_minute" not in field_ids
    assert "reliability_info" in field_ids
    assert "reliability_scoring_enabled" in field_ids
    assert "throughput_cache_ttl_minutes" not in field_ids
