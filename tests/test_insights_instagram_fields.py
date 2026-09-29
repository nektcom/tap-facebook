"""Unit tests for the opt-in Instagram profile metrics group (NEKT-5558).

Fully offline. The contract being pinned: a source that does not enable
`include_insights_instagram_fields` keeps the exact schema, request and report
parts it had before, and a source that enables it gets only the two columns,
carried by the core report, so it costs no extra report creation.
"""

from __future__ import annotations

import pytest
from facebook_business.adobjects.adsinsights import AdsInsights

from tap_facebook.streams.ad_insights import (
    BASIC_FIELDS,
    EXTRA_FIELD_TYPES,
    INSTAGRAM_FIELDS,
    STANDARD_FIELDS,
    AdsInsightStream,
)
from tap_facebook.tap import TapFacebook

BASE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
    "enable_advanced_reports": True,
}


def _stream(**config) -> AdsInsightStream:
    tap = TapFacebook(config={**BASE_CONFIG, **config}, validate_config=False)
    return next(s for s in tap.streams.values() if isinstance(s, AdsInsightStream))


def _parts(stream: AdsInsightStream) -> dict[str, list[str]]:
    stream._split_mode = True
    return dict(stream._report_parts(stream._get_selected_columns()))


def test_group_is_off_by_default():
    stream = _stream()
    assert "include_insights_instagram_fields" not in stream.enabled_field_groups
    assert "instagram_profile_follow" not in stream.schema["properties"]
    assert "instagram_profile_visits" not in stream.schema["properties"]


def test_visits_stays_in_standard_group():
    """Removing it from STANDARD would drop the column for sources that already have it."""
    assert "instagram_profile_visits" in STANDARD_FIELDS
    assert "instagram_profile_visits" in _stream(include_insights_standard_fields=True).schema["properties"]


def test_enabling_adds_only_the_two_columns():
    before = _stream().schema["properties"]
    after = _stream(include_insights_instagram_fields=True).schema["properties"]
    assert [c for c in after if c not in before] == INSTAGRAM_FIELDS
    assert {c: after[c] for c in before} == before
    assert after["instagram_profile_follow"] == {"type": ["string", "null"]}


def test_follow_is_typed_although_the_sdk_does_not_list_it():
    stream = _stream(include_insights_instagram_fields=True)
    assert "instagram_profile_follow" in stream.insights_fields
    if "instagram_profile_follow" not in AdsInsights._field_types:  # SDK 25.x
        assert EXTRA_FIELD_TYPES["instagram_profile_follow"] == "string"


def test_with_standard_enabled_visits_is_not_duplicated():
    stream = _stream(include_insights_standard_fields=True, include_insights_instagram_fields=True)
    assert stream.insights_fields.count("instagram_profile_visits") == 1


def test_split_mode_adds_no_report_part_basic_only():
    """BASIC + Instagram has no optional part to split off: still a single report."""
    parts = _parts(_stream(include_insights_instagram_fields=True))
    assert list(parts) == ["all"]
    assert set(INSTAGRAM_FIELDS) <= set(parts["all"])


@pytest.mark.parametrize(
    "groups",
    [
        {"include_insights_results_fields": True},
        {"include_insights_standard_fields": True},
        {"include_insights_standard_fields": True, "include_insights_results_fields": True},
    ],
)
def test_split_mode_keeps_the_same_parts(groups):
    without = _parts(_stream(**groups))
    with_instagram = _parts(_stream(**groups, include_insights_instagram_fields=True))
    assert list(with_instagram) == list(without)
    assert "instagram_profile_follow" in with_instagram["core"]
    if "include_insights_standard_fields" in groups:
        # Brought in by STANDARD first, it stays where it already was.
        assert "instagram_profile_visits" not in with_instagram["core"]
    else:
        assert "instagram_profile_visits" in with_instagram["core"]
    assert not _stream(include_insights_instagram_fields=True)._has_optional_columns(
        list(BASIC_FIELDS) + INSTAGRAM_FIELDS
    )


def test_drift_check_does_not_report_follow_as_gone_or_unclassified(caplog):
    stream = _stream(include_insights_instagram_fields=True)
    with caplog.at_level("INFO"):
        stream._log_schema_drift()
    for record in caplog.records:
        if "no longer exist" in record.getMessage() or "belong to no group" in record.getMessage():
            assert "instagram_profile_follow" not in record.getMessage()


def test_customer_log_does_not_advertise_the_group():
    """A source that never asked for it keeps the same run log."""
    from unittest import mock

    stream = _stream(include_insights_standard_fields=True)
    stream._optional_groups_logged = False
    with mock.patch("tap_facebook.streams.ad_insights.user_logger") as user:
        stream._log_schema_drift()
    for call in user.info.call_args_list:
        assert "Instagram" not in str(call.args[0])
