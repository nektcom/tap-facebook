"""Unit tests for v1.89: fields that skew a report aggregated above the ad are not requested.

Fully offline.

Background (NEKT-5768, 2026-10-08). A customer's campaign_insights held 3-8% of
the spend shown in Ads Manager for every period after ~2024-09, with no error in
any run. Bisecting the 192 fields of the report in the Graph API Explorer: with
`creative_media_type` in an async report at level=campaign, Facebook summed only
part of the ads of each campaign (3 of 34 for one campaign-day, 1.4% of its
spend). The same report without the field, the field at level=ad, and the
synchronous endpoint all returned the full numbers.
"""

from __future__ import annotations

from unittest import mock

import pendulum
import pytest

from tap_facebook.streams.ad_insights import FIELDS_THAT_SKEW_AGGREGATED_REPORTS, AdsInsightStream
from tap_facebook.tap import TapFacebook

CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
    "enable_campaign_insights": True,
    "include_insights_standard_fields": True,
    "include_insights_messaging_fields": True,
    "include_insights_commerce_fields": True,
    "include_insights_beta_fields": True,
    "include_insights_results_fields": True,
    "include_insights_attribution_fields": True,
}
USER = "tap_facebook.streams.ad_insights.user_logger"
SKEWING = sorted(FIELDS_THAT_SKEW_AGGREGATED_REPORTS)


@pytest.fixture(autouse=True)
def _fresh_process_state():
    def reset(started_without_state: bool) -> None:
        AdsInsightStream._incomplete_without_history = []
        AdsInsightStream._columns_refused_on_read = set()
        AdsInsightStream._run_started_without_state = started_without_state
        AdsInsightStream._account_not_building = False
        AdsInsightStream._not_completed = {}
        AdsInsightStream._account_limit_spent = False

    reset(started_without_state=False)
    yield
    reset(started_without_state=True)


def stream(name: str, config: dict | None = None) -> AdsInsightStream:
    return TapFacebook(config=config or CONFIG).streams[name]


def with_level(level: str) -> dict:
    return {**CONFIG, "report_definition": {"level": level}}


def run_get_records(insights: AdsInsightStream) -> mock.Mock:
    """Run get_records with no report created; return the patched user logger."""
    with (
        mock.patch.object(insights, "_initialize_client"),
        mock.patch.object(insights, "_create_report_batch", return_value=[]),
        mock.patch.object(insights, "_advance_batch", side_effect=lambda current, inc, n, end: end.add(days=1)),
        mock.patch.object(insights, "_fail_if_nothing_extracted"),
        mock.patch.object(insights, "_get_start_date", return_value=pendulum.today().date()),
        mock.patch(USER) as user,
    ):
        list(insights.get_records(None))
    return user


def told_left_empty(user: mock.Mock) -> list[str]:
    return [c.args[0] for c in user.info.call_args_list if "left empty" in c.args[0]]


class TestTheList:
    def test_it_holds_the_field_found_on_2026_10_08(self):
        assert SKEWING == ["creative_media_type"]


class TestCampaignInsights:
    @pytest.mark.parametrize("field", SKEWING)
    def test_the_field_stays_in_the_schema_but_leaves_the_request(self, field):
        campaign = stream("campaign_insights")
        assert field in campaign.schema["properties"], "the column keeps existing, it arrives empty"
        assert field not in campaign._get_selected_columns()

    def test_every_other_standard_field_is_still_requested(self):
        campaign = stream("campaign_insights")
        ad = stream("adsinsights")
        assert set(ad._get_selected_columns()) - set(campaign._get_selected_columns()) == set(SKEWING)

    @pytest.mark.parametrize("field", SKEWING)
    def test_insights_included_fields_does_not_bring_it_back(self, field):
        campaign = stream("campaign_insights", {**CONFIG, "insights_included_fields": [field]})
        assert field not in campaign._get_selected_columns()

    def test_the_customer_is_told_once_that_the_column_stays_empty(self):
        said = told_left_empty(run_get_records(stream("campaign_insights")))
        assert len(said) == 1
        assert said[0].startswith("[campaign_insights] Column(s) creative_media_type left empty")
        assert "in a report by campaign" in said[0]
        assert "ad-level insights table" in said[0], "it says where the per-ad values are"

    def test_no_part_of_a_split_report_carries_it(self):
        campaign = stream("campaign_insights")
        campaign._split_mode = True
        parts = campaign._report_parts(campaign._get_selected_columns())
        assert len(parts) > 1
        assert all("creative_media_type" not in columns for _, columns in parts)

    def test_a_field_the_source_already_excludes_is_not_blamed_on_facebook(self):
        campaign = stream("campaign_insights", {**CONFIG, "insights_excluded_fields": ["creative_media_type"]})
        assert "creative_media_type" not in campaign._get_selected_columns()
        assert told_left_empty(run_get_records(campaign)) == []

    def test_without_the_standard_group_there_is_nothing_to_tell(self):
        campaign = stream("campaign_insights", {**CONFIG, "include_insights_standard_fields": False})
        assert "creative_media_type" not in campaign._get_selected_columns()
        assert told_left_empty(run_get_records(campaign)) == []


class TestAdLevel:
    @pytest.mark.parametrize("field", SKEWING)
    def test_the_field_is_still_requested(self, field):
        assert field in stream("adsinsights")._get_selected_columns()

    def test_nothing_is_said(self):
        assert told_left_empty(run_get_records(stream("adsinsights"))) == []

    def test_every_breakdown_stream_still_requests_it(self):
        tap = TapFacebook(config={**CONFIG, "enable_advanced_reports": True})
        breakdowns = [name for name in tap.streams if name.startswith("adsinsights_")]
        assert breakdowns, "the advanced reports add breakdown streams at level=ad"
        for name in breakdowns:
            assert "creative_media_type" in tap.streams[name]._get_selected_columns(), name

    def test_the_class_default_is_left_untouched_by_a_run(self):
        run_get_records(stream("campaign_insights"))
        assert AdsInsightStream._left_empty_at_this_level == frozenset()


class TestOtherStreamsConfiguredAboveTheAd:
    @pytest.mark.parametrize("level", ["campaign", "adset", "account"])
    def test_the_field_leaves_the_request(self, level):
        insights = stream("adsinsights", with_level(level))
        assert "creative_media_type" not in insights._get_selected_columns()

    def test_an_explicit_ad_level_keeps_it(self):
        assert "creative_media_type" in stream("adsinsights", with_level("ad"))._get_selected_columns()
