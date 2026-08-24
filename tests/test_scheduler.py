from datetime import datetime, timezone

from stream_sorter import plugin


def test_cron_field_parser_supports_common_steps_and_lists():
    assert plugin._parse_cron_field("*/15", minimum=0, maximum=59) == {0, 15, 30, 45}
    assert plugin._parse_cron_field("1,2,5", minimum=0, maximum=59) == {1, 2, 5}
    assert plugin._parse_cron_field("1-3", minimum=0, maximum=59) == {1, 2, 3}
    assert plugin._parse_cron_field("*", minimum=0, maximum=6) == {0, 1, 2, 3, 4, 5, 6}


def test_cron_match_evaluates_expression_with_utc_minute_boundary():
    now = datetime(2026, 8, 24, 12, 45, 10, tzinfo=timezone.utc)
    assert plugin._cron_matches("*/15 * * * *", now)
    assert not plugin._cron_matches("*/20 * * * *", now)


def test_apply_schedule_action_stores_only_relevant_settings(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin, "SCHEDULE_STATE_PATH", str(tmp_path / "schedule.json"))

    result = plugin._apply_schedule_action(
        {
            "stream_sort_schedule_cron": "*/30 * * * *",
            "stream_sort_apply_sort_after_scheduled_scan": "false",
            "stream_sort_allow_parallel_checks_on_scheduled_scan": "true",
            "channel_group_filter": "Live",
        }
    )

    assert result["status"] == "ok"
    state = plugin._load_schedule_state()
    assert state["enabled"] is True
    assert state["apply_sort_after_analysis"] is False
    assert state["allow_parallel_checks"] is True
    assert state["cron"] == "*/30 * * * *"
    assert state["settings"]["channel_group_filter"] == "Live"
    assert "stream_sort_schedule_cron" not in state["settings"]


def test_scheduled_scan_forces_single_worker_when_parallel_disabled(tmp_path, monkeypatch):
    captured = {}
    monkeypatch.setattr(plugin, "SCHEDULE_STATE_PATH", str(tmp_path / "schedule.json"))

    def fake_start_background_job(settings, kind, sort_after):
        captured["settings"] = settings
        captured["kind"] = kind
        captured["sort_after"] = sort_after
        return {
            "status": "ok",
            "message": "ok",
            "job_id": "abc",
        }

    monkeypatch.setattr(plugin, "_start_background_job", fake_start_background_job)

    state = {
        "enabled": True,
        "cron": "* * * * *",
        "apply_sort_after_analysis": True,
        "allow_parallel_checks": False,
        "settings": {"analysis_workers": 8},
    }

    result = plugin._run_scheduled_scan(state, now=datetime(2026, 8, 24, tzinfo=timezone.utc))
    assert result["status"] == "ok"
    assert captured["kind"] == "analyze"
    assert captured["sort_after"] is True
    assert captured["settings"]["analysis_workers"] == 1


def test_check_schedule_tick_only_runs_once_per_minute_when_due(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin, "SCHEDULE_STATE_PATH", str(tmp_path / "schedule.json"))
    ran = {"count": 0}

    def fake_run_scheduled_scan(state, now):
        ran["count"] += 1
        return {"status": "ok", "message": "triggered", "job_id": f"job-{ran['count']}"}

    monkeypatch.setattr(plugin, "_run_scheduled_scan", fake_run_scheduled_scan)

    state = plugin._load_schedule_state()
    state.update({
        "enabled": True,
        "cron": "* * * * *",
        "apply_sort_after_analysis": True,
        "allow_parallel_checks": True,
        "settings": {},
        "last_scheduled_minute": None,
    })
    plugin._save_schedule_state(state)

    tick_time = datetime(2026, 8, 24, 12, 0, 15, tzinfo=timezone.utc)
    first = plugin._check_schedule_tick(tick_time)
    second = plugin._check_schedule_tick(tick_time)

    assert first["status"] == "ok"
    assert second["status"] == "skipped"
    assert ran["count"] == 1
