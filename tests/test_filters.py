import pytest

from stream_sorter.sorter import (
    _partition_channel_scope,
    _resolve_filter_tokens,
    _split_filter_values,
)


def test_split_filter_values_accepts_commas_newlines_semicolons_and_dedupes():
    assert _split_filter_values("Local, Sports\nNews;local") == ["Local", "Sports", "News"]


def test_resolve_filter_tokens_by_name_and_id():
    records = [
        {"id": 3, "name": "Local"},
        {"id": 7, "name": "Sports"},
        {"id": 12, "name": "123"},
    ]
    ids, resolved = _resolve_filter_tokens(
        ["local", "id:7", "name:123"],
        records,
        label="channel group",
    )
    assert ids == {3, 7, 12}
    assert resolved == [
        {"id": 3, "name": "Local"},
        {"id": 7, "name": "Sports"},
        {"id": 12, "name": "123"},
    ]


def test_plain_numeric_filter_is_treated_as_id():
    records = [
        {"id": 12, "name": "Other"},
        {"id": 99, "name": "12"},
    ]
    ids, _resolved = _resolve_filter_tokens(["12"], records, label="channel profile")
    assert ids == {12}


def test_resolve_filter_tokens_supports_case_insensitive_name_wildcards():
    records = [
        {"id": 3, "name": "Event Sports"},
        {"id": 7, "name": "EVENT News"},
        {"id": 12, "name": "Regional"},
    ]
    ids, resolved = _resolve_filter_tokens(
        ["event*"],
        records,
        label="channel profile",
    )
    assert ids == {3, 7}
    assert resolved == [
        {"id": 3, "name": "Event Sports"},
        {"id": 7, "name": "EVENT News"},
    ]


def test_analyze_only_overrides_sort_for_overlapping_matches():
    analysis_ids, sort_ids = _partition_channel_scope(
        {1, 2, 3, 4},
        {1, 2},
        {2, 3},
        analyze_sort_filtered=True,
    )
    assert analysis_ids == {1, 2, 3}
    assert sort_ids == {1}


def test_analyze_only_excludes_matches_when_analyze_sort_is_unfiltered():
    analysis_ids, sort_ids = _partition_channel_scope(
        {1, 2, 3, 4},
        set(),
        {2, 3},
        analyze_sort_filtered=False,
    )
    assert analysis_ids is None
    assert sort_ids == {1, 4}


def test_unknown_filter_value_fails_loudly():
    with pytest.raises(ValueError, match="Unknown channel group filter value"):
        _resolve_filter_tokens(
            ["Does Not Exist"],
            [{"id": 1, "name": "Local"}],
            label="channel group",
        )
