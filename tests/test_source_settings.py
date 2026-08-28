import pytest

from stream_sorter.plugin import (
    _build_m3u_source_score_fields,
    _settings_with_dynamic_source_scores,
)
from stream_sorter.scoring import parse_source_rules


def test_dynamic_m3u_fields_use_bounded_selectors_and_account_ids():
    fields = _build_m3u_source_score_fields(
        [
            {"id": 9, "name": "Zulu", "is_active": True},
            {"id": 3, "name": "Alpha", "is_active": True},
            {"id": 7, "name": "Backup", "is_active": False},
            {"id": 1, "name": "custom", "is_active": True, "locked": True},
        ]
    )

    assert [field["id"] for field in fields] == [
        "m3u_source_score_3",
        "m3u_source_score_7",
        "m3u_source_score_9",
    ]
    assert all(field["type"] == "select" for field in fields)
    assert all(field["default"] == 0 for field in fields)
    assert [option["value"] for option in fields[0]["options"]] == list(range(-5, 6))
    assert fields[0]["options"][5]["label"] == "0 (neutral)"
    assert fields[0]["label"] == "Alpha"
    assert fields[1]["label"] == "Backup (inactive)"


def test_dynamic_scores_translate_to_existing_source_rule_format():
    settings = _settings_with_dynamic_source_scores(
        {
            "m3u_source_score_7": 20,
            "m3u_source_score_3": -10,
            "m3u_source_score_11": 0,
        }
    )
    rules = parse_source_rules(settings["source_scores"])

    assert [(rule.key, rule.score) for rule in rules] == [
        ("id:3", -5.0),
        ("id:7", 5.0),
        ("id:11", 0.0),
    ]


def test_legacy_source_scores_are_preserved_until_dynamic_fields_are_saved():
    settings = _settings_with_dynamic_source_scores(
        {"source_scores": "Preferred=15\nid:4=-5"}
    )
    assert settings["source_scores"] == "Preferred=15\nid:4=-5"


def test_dynamic_source_fields_override_legacy_source_scores():
    settings = _settings_with_dynamic_source_scores(
        {
            "source_scores": "Preferred=99",
            "m3u_source_score_4": 0,
        }
    )
    assert settings["source_scores"] == "id:4=0"


def test_dynamic_source_scores_round_to_nearest_selector_value():
    settings = _settings_with_dynamic_source_scores(
        {"m3u_source_score_4": 3.6, "m3u_source_score_5": -2.6}
    )
    assert settings["source_scores"] == "id:4=4\nid:5=-3"
    assert settings["m3u_source_score_4"] == 4
    assert settings["m3u_source_score_5"] == -3


def test_locked_or_removed_dynamic_source_scores_are_ignored():
    settings = _settings_with_dynamic_source_scores(
        {
            "m3u_source_score_1": -20,
            "m3u_source_score_3": 10,
        },
        allowed_account_ids={3},
    )
    assert settings["source_scores"] == "id:3=5"


def test_invalid_dynamic_source_score_fails_cleanly():
    with pytest.raises(ValueError, match="M3U source ID 4"):
        _settings_with_dynamic_source_scores({"m3u_source_score_4": "bad"})
