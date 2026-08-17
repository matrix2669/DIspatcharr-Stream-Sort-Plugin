from datetime import datetime, timedelta, timezone

import pytest

from stream_sorter.scoring import (
    StreamCandidate,
    classify_throughput,
    estimate_nominal_throughput_kbps,
    parse_name_rules,
    parse_source_rules,
    rank_candidates,
)


def c(
    stream_id,
    name,
    order,
    *,
    height=1080,
    width=1920,
    fps=60,
    bitrate=6000,
    source_id=1,
    source_name="Provider",
    throughput=None,
    stats=True,
    stats_updated_at=None,
    stale=False,
    active=True,
    url="http://example.invalid/live",
):
    stream_stats = (
        {
            "width": width,
            "height": height,
            "source_fps": fps,
            "video_bitrate": bitrate,
        }
        if stats is True
        else stats
    )
    return StreamCandidate(
        stream_id=stream_id,
        name=name,
        original_order=order,
        stats=stream_stats,
        stats_updated_at=stats_updated_at,
        m3u_account_id=source_id,
        m3u_account_name=source_name,
        m3u_account_active=active,
        is_stale=stale,
        url=url,
        throughput=throughput,
    )


def test_resolution_is_hard_tier_over_score():
    rules = parse_name_rules("ROKU=-100\nUS=100")
    ranked = rank_candidates(
        [
            c(1, "ROKU | ESPN", 0, height=1080, width=1920, bitrate=2500),
            c(2, "US | ESPN", 1, height=720, width=1280, bitrate=8000),
        ],
        name_rules=rules,
    )
    assert [x.stream_id for x in ranked] == [1, 2]


def test_us_bonus_beats_small_roku_bitrate_advantage():
    rules = parse_name_rules("US=20\nROKU=-20")
    ranked = rank_candidates(
        [
            c(1, "US | ESPN", 0, bitrate=6000),
            c(2, "ROKU | ESPN", 1, bitrate=7500),
        ],
        name_rules=rules,
    )
    assert [x.stream_id for x in ranked] == [1, 2]
    assert ranked[0].total_score > ranked[1].total_score


def test_drastic_quality_difference_can_overcome_roku_penalty_inside_tier():
    rules = parse_name_rules("US=20\nROKU=-20")
    ranked = rank_candidates(
        [
            c(1, "US | ESPN", 0, fps=30, bitrate=2100),
            c(2, "ROKU | ESPN", 1, fps=60, bitrate=9000),
        ],
        name_rules=rules,
    )
    assert [x.stream_id for x in ranked] == [2, 1]


def test_source_score_combines_with_name_score():
    name_rules = parse_name_rules("US=20\nROKU=-20")
    source_rules = parse_source_rules("Preferred=15\nBackup=-5")
    ranked = rank_candidates(
        [
            c(1, "ROKU | ESPN", 0, source_name="Preferred"),
            c(2, "US | ESPN", 1, source_name="Backup"),
        ],
        name_rules=name_rules,
        source_rules=source_rules,
    )
    # Preferred/ROKU = -5 net preference; Backup/US = +15 net preference.
    assert [x.stream_id for x in ranked] == [2, 1]


def test_source_can_match_numeric_id():
    source_rules = parse_source_rules("id:7=25\n3=-10")
    ranked = rank_candidates(
        [
            c(1, "A", 0, source_id=3, source_name="X"),
            c(2, "B", 1, source_id=7, source_name="Y"),
        ],
        source_rules=source_rules,
    )
    assert [x.stream_id for x in ranked] == [2, 1]


def test_multiple_regex_rules_are_additive():
    rules = parse_name_rules("10::^US\n-7::\\bBACKUP\\b")
    ranked = rank_candidates([c(1, "US | ESPN BACKUP", 0)], name_rules=rules)
    assert ranked[0].breakdown["name_rules"] == 3
    assert ranked[0].matched_name_rules == ["^US", r"\bBACKUP\b"]


def test_content_starved_stream_crosses_resolution_boundary():
    ranked = rank_candidates(
        [
            c(1, "1080 placeholder", 0, height=1080, width=1920, bitrate=300),
            c(2, "720 healthy", 1, height=720, width=1280, bitrate=3500),
        ]
    )
    assert [x.stream_id for x in ranked] == [2, 1]
    assert ranked[1].viability == "content_starved"


def test_known_dead_empty_stats_crosses_resolution_boundary():
    now = datetime.now(timezone.utc)
    ranked = rank_candidates(
        [
            c(1, "dead", 0, stats={}, stats_updated_at=now),
            c(2, "alive", 1, height=480, width=720, bitrate=1500),
        ]
    )
    assert [x.stream_id for x in ranked] == [2, 1]
    assert ranked[1].viability == "known_dead"


def test_unprobed_none_stats_is_not_marked_dead():
    ranked = rank_candidates([c(1, "unprobed", 0, stats=None)])
    assert ranked[0].viability == "usable"
    assert ranked[0].resolution_tier == 0


def test_inactive_or_stale_stream_is_demoted():
    ranked = rank_candidates(
        [
            c(1, "inactive 4k", 0, height=2160, width=3840, active=False),
            c(2, "active 480", 1, height=480, width=720),
        ]
    )
    assert [x.stream_id for x in ranked] == [2, 1]



def test_nominal_throughput_uses_resolution_and_fps_not_measured_bitrate():
    assert estimate_nominal_throughput_kbps(1080, 60) == 6000
    assert estimate_nominal_throughput_kbps(1080, 30) == 4000
    assert estimate_nominal_throughput_kbps(720, 60) == 4000
    assert estimate_nominal_throughput_kbps(720, 30) == 2500
    assert estimate_nominal_throughput_kbps(None, None) == 2500

def test_throughput_classifier_thresholds():
    assert classify_throughput(9.0, 6000) == "healthy"
    assert classify_throughput(6.7, 6000) == "marginal"
    assert classify_throughput(6.0, 6000) == "insufficient"
    assert classify_throughput(None, 6000) == "unknown"


def test_throughput_score_changes_order_inside_resolution_tier():
    now = datetime.now(timezone.utc)
    ranked = rank_candidates(
        [
            c(1, "A", 0, throughput={"status": "insufficient", "tested_at": now.isoformat()}),
            c(2, "B", 1, throughput={"status": "healthy", "tested_at": now.isoformat()}),
        ],
        now=now,
    )
    assert [x.stream_id for x in ranked] == [2, 1]


def test_stale_throughput_cache_becomes_unknown():
    now = datetime.now(timezone.utc)
    old = now - timedelta(minutes=60)
    ranked = rank_candidates(
        [
            c(1, "A", 0, throughput={"status": "insufficient", "tested_at": old.isoformat()}),
            c(2, "B", 1),
        ],
        throughput_cache_ttl_minutes=30,
        now=now,
    )
    assert ranked[0].stream_id == 1  # tie falls back to existing order
    assert ranked[0].throughput_status == "unknown"
    assert any("stale" in note for note in ranked[0].notes)


def test_equal_candidates_preserve_existing_order():
    ranked = rank_candidates([c(3, "same", 7), c(2, "same", 2), c(1, "same", 5)])
    assert [x.stream_id for x in ranked] == [2, 1, 3]


def test_invalid_rules_fail_cleanly():
    with pytest.raises(ValueError):
        parse_name_rules("not a rule")
    with pytest.raises(ValueError):
        parse_name_rules("10::[")
    with pytest.raises(ValueError):
        parse_source_rules("Provider")
