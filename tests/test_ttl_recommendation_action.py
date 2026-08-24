import json
from datetime import datetime, timezone

from stream_sorter import plugin


def _build_report():
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "selected_streams": 4,
        "observations": {
            "history_rows": 80,
            "history_span_hours": 96.0,
            "status_changes": 6,
            "dead_checks": 4,
            "status_changes_per_check_ratio": 0.06,
            "dead_check_ratio": 0.07,
            "alive_episode_duration_hours": {"samples": 6, "p25": 36.0, "p50": 48.0},
            "dead_recovery_duration_hours": {"samples": 6, "p50": 2.0, "p90": 4.0},
            "check_concentration": {"busiest_minute_ratio": 0.12},
        },
        "status_patterns": {
            "hourly_dead_ratio": [
                {"hour": 1, "dead_ratio": 0.01},
                {"hour": 7, "dead_ratio": 0.17},
            ]
        },
        "reasons": {
            "media_due": {"media_ttl_expired": 6},
        },
    }


def test_recommend_ttl_action_requires_health_report(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin, "ANALYSIS_HEALTH_REPORT_PATH", str(tmp_path / "missing.json"))

    result = plugin._run_ttl_recommendation_action({})
    assert result["status"] == "error"
    assert "No analysis health report found" in result["message"]


def test_recommend_ttl_action_writes_recommendation_file(tmp_path, monkeypatch):
    health_path = tmp_path / "health.json"
    recommend_path = tmp_path / "ttl-recommendations.json"
    health_path.write_text(json.dumps(_build_report()), encoding="utf-8")

    monkeypatch.setattr(plugin, "ANALYSIS_HEALTH_REPORT_PATH", str(health_path))
    monkeypatch.setattr(plugin, "TTL_RECOMMENDATION_PATH", str(recommend_path))

    result = plugin._run_ttl_recommendation_action(
        {
            "stream_data_ttl_hours": "24",
            "dead_content_ttl_hours": "1",
            "analysis_ttl_jitter_percent": "0",
        }
    )

    assert result["status"] == "ok"
    assert recommend_path.exists()
    assert result["recommendation_path"] == str(recommend_path)
    loaded = json.loads(recommend_path.read_text(encoding="utf-8"))
    assert loaded["recommended_ttls"]["stream_data_ttl_hours"] == 36.0
    assert loaded["recommended_ttls"]["dead_content_ttl_hours"] == 2.0
    assert loaded["confidence"] == "medium"


def test_health_report_action_returns_problem_stream_summary(tmp_path, monkeypatch):
    health_path = tmp_path / "health.json"
    report = _build_report()
    report["status_patterns"]["problematic_streams"] = [{"stream_id": 9, "dead_check_ratio": 0.9}]
    report["observations"]["transition_counts"] = {"alive_to_dead": 3, "dead_to_alive": 2}
    health_path.write_text(json.dumps(report), encoding="utf-8")
    monkeypatch.setattr(plugin, "ANALYSIS_HEALTH_REPORT_PATH", str(health_path))

    result = plugin._run_health_report_action()
    assert result["status"] == "ok"
    assert "problematic (>75% dead, minimum 20 checks over 7 days)=1" in result["message"]
    assert result["result"]["observations"]["transition_counts"]["dead_to_alive"] == 2


def test_recommend_ttl_action_rejects_stale_report(tmp_path, monkeypatch):
    health_path = tmp_path / "health.json"
    report = _build_report()
    report["generated_at"] = "2026-01-01T00:00:00+00:00"
    health_path.write_text(json.dumps(report), encoding="utf-8")
    monkeypatch.setattr(plugin, "ANALYSIS_HEALTH_REPORT_PATH", str(health_path))

    result = plugin._run_ttl_recommendation_action({})
    assert result["status"] == "error"
    assert "older than seven days" in result["message"]
