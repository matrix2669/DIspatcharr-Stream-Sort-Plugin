import json
from datetime import datetime, timedelta, timezone

from stream_sorter import plugin
from stream_sorter import sorter


def _channel(*, changed=True, score=20.0, bitrate_score=10.0):
    return {
        "channel_id": 10,
        "channel_number": 100,
        "channel_name": "Example",
        "changed": changed,
        "current_stream_ids": [1, 2],
        "proposed_stream_ids": [2, 1] if changed else [1, 2],
        "streams": [
            {
                "stream_id": 2,
                "name": "Second",
                "score": score,
                "score_breakdown": {"bitrate": bitrate_score, "throughput": 10.0},
                "viability": "usable",
                "resolution_tier": 1080,
                "throughput_status": "healthy",
                "reliability_status": "neutral",
            },
            {
                "stream_id": 1,
                "name": "First",
                "score": 15.0,
                "score_breakdown": {"bitrate": 5.0, "throughput": 10.0},
                "viability": "usable",
                "resolution_tier": 1080,
                "throughput_status": "healthy",
                "reliability_status": "neutral",
            },
        ],
    }


def _payload(now, *, mode="apply", changed=True):
    return {
        "generated_at": now.isoformat(),
        "mode": mode,
        "channels_evaluated": 1,
        "channels_changed": int(changed),
        "rows_changed": 2 if changed else 0,
        "channels": [_channel(changed=changed)],
    }


def test_applied_sort_history_retains_movement_and_score_deltas():
    previous_at = datetime(2026, 8, 27, tzinfo=timezone.utc)
    previous = sorter._merge_sort_history({}, _payload(previous_at), now=previous_at)
    current_at = previous_at + timedelta(days=1)
    current_payload = _payload(current_at)
    current_payload["channels"][0]["streams"][0]["score"] = 25.0
    current_payload["channels"][0]["streams"][0]["score_breakdown"]["bitrate"] = 15.0

    merged = sorter._merge_sort_history(previous, current_payload, now=current_at)
    movement = merged["sort_history"][-1]["changed_channels"][0]["movements"][0]

    assert len(merged["sort_history"]) == 2
    assert movement["stream_id"] == 2
    assert movement["old_position"] == 2
    assert movement["new_position"] == 1
    assert movement["score_delta"] == 5.0
    assert movement["score_breakdown_delta"] == {"bitrate": 5.0}
    assert merged["sort_daily_rollups"][current_at.date().isoformat()]["applied_runs"] == 1


def test_dry_run_preserves_but_does_not_append_applied_history():
    now = datetime(2026, 8, 27, tzinfo=timezone.utc)
    previous = sorter._merge_sort_history({}, _payload(now), now=now)
    dry_run = sorter._merge_sort_history(previous, _payload(now + timedelta(hours=1), mode="dry_run"), now=now + timedelta(hours=1))

    assert dry_run["sort_history"] == previous["sort_history"]
    assert dry_run["sort_daily_rollups"] == previous["sort_daily_rollups"]
    assert dry_run["last_applied_snapshot"] == previous["last_applied_snapshot"]


def test_sort_history_action_returns_retained_summary(tmp_path, monkeypatch):
    now = datetime(2026, 8, 27, tzinfo=timezone.utc)
    report = sorter._merge_sort_history({}, _payload(now), now=now)
    report_path = tmp_path / "sort-report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    monkeypatch.setattr(plugin, "REPORT_PATH", str(report_path))

    response = plugin._run_sort_history_action()

    assert response["status"] == "ok"
    assert response["result"]["applied_runs"] == 1
    assert response["result"]["channels_changed"] == 1
    assert response["result"]["stream_movements"] == 2
