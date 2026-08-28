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


def test_cron_accepts_sunday_seven_and_uses_standard_dom_dow_or_semantics():
    sunday = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)
    monday_matching_dom = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    assert plugin._cron_matches("0 12 * * 7", sunday)
    assert plugin._cron_matches("0 12 24 * 0", monday_matching_dom)


def test_apply_schedule_action_stores_only_relevant_settings(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin, "SCHEDULE_STATE_PATH", str(tmp_path / "schedule.json"))
    monkeypatch.setattr(plugin, "SCHEDULE_STATE_LOCK_PATH", str(tmp_path / "schedule.lock"))

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
    assert "settings" not in state
    assert state["generation"] == 1


def test_scheduled_scan_forces_single_worker_when_parallel_disabled(tmp_path, monkeypatch):
    captured = {}
    monkeypatch.setattr(plugin, "SCHEDULE_STATE_PATH", str(tmp_path / "schedule.json"))

    def fake_start_background_job(settings, kind, sort_after, schedule_generation=None):
        captured["settings"] = settings
        captured["kind"] = kind
        captured["sort_after"] = sort_after
        captured["schedule_generation"] = schedule_generation
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
        "generation": 7,
    }

    result = plugin._run_scheduled_scan(
        state,
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
        settings={"analysis_workers": 8, "channel_group_filter": "Current"},
    )
    assert result["status"] == "ok"
    assert captured["kind"] == "analyze"
    assert captured["sort_after"] is True
    assert captured["settings"]["analysis_workers"] == 1
    assert captured["settings"]["channel_group_filter"] == "Current"
    assert captured["schedule_generation"] == 7


def test_apply_schedule_action_uses_hourly_default_when_cron_is_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin, "SCHEDULE_STATE_PATH", str(tmp_path / "schedule.json"))
    monkeypatch.setattr(plugin, "SCHEDULE_STATE_LOCK_PATH", str(tmp_path / "schedule.lock"))

    result = plugin._apply_schedule_action({})

    assert result["status"] == "ok"
    state = plugin._load_schedule_state()
    assert state["cron"] == "18 * * * *"
    assert state["apply_sort_after_analysis"] is True
    assert state["allow_parallel_checks"] is False


def test_check_schedule_tick_only_runs_once_per_minute_when_due(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin, "SCHEDULE_STATE_PATH", str(tmp_path / "schedule.json"))
    monkeypatch.setattr(plugin, "SCHEDULE_STATE_LOCK_PATH", str(tmp_path / "schedule.lock"))
    ran = {"count": 0}
    claims = {"count": 0}

    def fake_run_scheduled_scan(state, now, settings):
        ran["count"] += 1
        return {"status": "ok", "message": "triggered", "job_id": f"job-{ran['count']}"}

    def fake_claim(_generation, _minute):
        claims["count"] += 1
        return claims["count"] == 1

    monkeypatch.setattr(plugin, "_run_scheduled_scan", fake_run_scheduled_scan)
    monkeypatch.setattr(plugin, "_claim_schedule_minute", fake_claim)

    state = plugin._load_schedule_state()
    state.update({
        "enabled": True,
        "cron": "* * * * *",
        "apply_sort_after_analysis": True,
        "allow_parallel_checks": True,
        "generation": 1,
        "last_scheduled_minute": None,
    })
    plugin._save_schedule_state(state)

    tick_time = datetime(2026, 8, 24, 12, 0, 15, tzinfo=timezone.utc)
    first = plugin._check_schedule_tick(tick_time, settings={"analysis_workers": 3})
    second = plugin._check_schedule_tick(tick_time, settings={"analysis_workers": 3})

    assert first["status"] == "ok"
    assert second["status"] == "skipped"
    assert ran["count"] == 1


def test_schedule_completion_updates_only_matching_generation_and_job(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin, "SCHEDULE_STATE_PATH", str(tmp_path / "schedule.json"))
    monkeypatch.setattr(plugin, "SCHEDULE_STATE_LOCK_PATH", str(tmp_path / "schedule.lock"))
    state = plugin._load_schedule_state()
    state.update({"enabled": True, "generation": 4, "last_job_id": "job-4"})
    plugin._save_schedule_state(state)

    plugin._finish_schedule_job(4, "job-4", status="completed", message="complete")
    completed_state = plugin._load_schedule_state()
    assert completed_state["last_run_status"] == "completed"
    assert completed_state["history"][-1]["status"] == "completed"
    assert completed_state["history"][-1]["job_id"] == "job-4"

    plugin._finish_schedule_job(3, "job-4", status="failed", message="stale")
    stale_state = plugin._load_schedule_state()
    assert stale_state["last_run_status"] == "completed"
    assert len(stale_state["history"]) == 1
