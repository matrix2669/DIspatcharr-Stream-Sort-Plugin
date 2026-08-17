import pytest

from stream_sorter.sorter import _resolve_filter_tokens, _split_filter_values


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


def test_unknown_filter_value_fails_loudly():
    with pytest.raises(ValueError, match="Unknown channel group filter value"):
        _resolve_filter_tokens(
            ["Does Not Exist"],
            [{"id": 1, "name": "Local"}],
            label="channel group",
        )
