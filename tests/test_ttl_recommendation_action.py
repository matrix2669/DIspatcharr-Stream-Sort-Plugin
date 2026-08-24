import json

from stream_sorter import plugin


def _build_report():
    return {
        "selected_streams": 4,
        "observations": {
            "history_rows": 80,
            "history_span_hours": 48.0,
            "status_changes": 6,
            "dead_checks": 4,
            "checks_per_status_change_ratio": 0.06,
            "dead_check_ratio": 0.07,
            "check_interval_hours": {"p50": 12.0, "p90": 28.0},
            "status_change_interval_hours": {"p50": 60.0, "p90": 80.0},
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
            "health_content_ttl_hours": "24",
            "dead_content_ttl_hours": "1",
            "analysis_ttl_jitter_percent": "0",
        }
    )

    assert result["status"] == "ok"
    assert recommend_path.exists()
    assert result["recommendation_path"] == str(recommend_path)
    loaded = json.loads(recommend_path.read_text(encoding="utf-8"))
    assert loaded["recommended_ttls"]["health_content_ttl_hours"] == 35.0
    assert loaded["confidence"] == "medium"
