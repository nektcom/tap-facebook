"""Unit tests for the cadence and channel of the tap's log lines.

Fully offline.

Background: the promotion of WARNING to INFO on 2026-08-04 multiplied log
volume across the fleet, and tap-facebook became the largest emitter (about
2,067 lines a day at the reference org on 2026-09-22) from four places. Each of
them looked like "once per stream" and ran once per request, per poll or per
group. These tests pin the cadence by counting lines over many calls, which is
what caught the same defect in tap-vtex, tap-nuvemshop and tap-googleads.

The rule: the customer channel only carries what they can act on and never per
request, page or poll; the internal channel keeps the detail at debug, with
info reserved for milestones; every customer line has an internal pair.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest
from facebook_business.adobjects.adreportrun import AdReportRun

from tap_facebook.api_helper import has_reached_api_limit
from tap_facebook.streams.ad_insights import AdsInsightStream
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
}

USAGE_HEADERS = {"x-app-usage": json.dumps({"call_count": 10, "total_cputime": 5, "total_time": 5})}
REQUESTS = 25


def make_stream(**overrides) -> AdsInsightStream:
    stream = TapFacebook(config={**SAMPLE_CONFIG, **overrides}).streams["adsinsights"]
    stream._reset_run_state()
    return stream


def messages(logger_method: mock.Mock) -> list[str]:
    return [str(call.args[0]) for call in logger_method.call_args_list]


class TestTheUsageHeadersAreNotLoggedPerRequest:
    """has_reached_api_limit runs on every HTTP 200 and its result is discarded there."""

    def test_no_info_line_across_many_responses(self):
        with mock.patch("tap_facebook.api_helper.internal_logger") as log:
            for _ in range(REQUESTS):
                has_reached_api_limit(headers=USAGE_HEADERS, account_id="123")

        assert log.info.call_count == 0, f"expected no info line, got {log.info.call_count}"

    def test_the_detail_is_still_there_at_debug(self):
        with mock.patch("tap_facebook.api_helper.internal_logger") as log:
            has_reached_api_limit(headers=USAGE_HEADERS, account_id="123")

        assert any("Call Count" in line for line in messages(log.debug))

    def test_a_response_without_usage_headers_is_not_a_warning(self):
        with mock.patch("tap_facebook.api_helper.internal_logger") as log:
            for _ in range(REQUESTS):
                has_reached_api_limit(headers={}, account_id="123")

        assert log.warning.call_count == 0, f"expected no warning, got {log.warning.call_count}"


class TestTheThrottleSemanticsAreUntouched:
    """Only the log level moved. The return values are pinned so this change cannot alter them.

    The over-quota case returns False on purpose here -- it is a known oddity
    tracked separately, not something this change is allowed to fix.
    """

    def test_below_the_threshold_is_false(self):
        assert has_reached_api_limit(headers=USAGE_HEADERS, account_id="123") is False

    def test_above_the_threshold_is_true(self):
        headers = {"x-app-usage": json.dumps({"call_count": 95})}
        assert has_reached_api_limit(headers=headers, account_id="123") is True

    def test_an_explicit_over_quota_still_returns_false(self):
        headers = {"x-ad-account-usage": json.dumps({"acc_id_util_pct": 99, "reset_time_duration": 300})}
        assert has_reached_api_limit(headers=headers, account_id="123") is False

    def test_no_headers_is_false(self):
        assert has_reached_api_limit(headers={}, account_id="123") is False


def job_progress(polls_before_done: int) -> list[dict]:
    running = [
        {"id": "r1", "async_status": "Job Running", "async_percent_completion": 10 * i}
        for i in range(polls_before_done)
    ]
    return [*running, {"id": "r1", "async_status": "Job Completed", "async_percent_completion": 100}]


class TestPollingAJobIsNotNarratedToTheCustomer:
    POLLS = 11

    def run_one_job(self, *, quiet: bool = False):
        stream = make_stream()
        with (
            mock.patch.object(AdReportRun, "api_get", side_effect=job_progress(self.POLLS)),
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
            mock.patch("tap_facebook.streams.ad_insights.user_logger") as user,
            mock.patch("tap_facebook.streams.ad_insights.internal_logger") as internal,
        ):
            stream._run_job_to_completion(AdReportRun("r1"), "2026-09-20", quiet=quiet)
        return user, internal

    def test_the_customer_gets_one_line_when_the_report_is_ready(self):
        user, _ = self.run_one_job()

        assert user.info.call_count == 1, f"expected one line, got {user.info.call_count}"
        assert "is ready" in messages(user.info)[0]

    def test_the_customer_never_sees_poll_progress(self):
        user, _ = self.run_one_job()

        assert not any("% done" in line for line in messages(user.info))

    def test_poll_and_sleep_lines_stay_out_of_internal_info(self):
        _, internal = self.run_one_job()

        noisy = [line for line in messages(internal.info) if "% done" in line or "Sleeping for" in line]
        assert noisy == [], f"expected no per-poll info line, got {len(noisy)}"

    def test_the_completion_has_an_internal_pair_with_the_job_id(self):
        _, internal = self.run_one_job()

        assert any("r1" in line and "completed" in line for line in messages(internal.info))

    def test_a_quiet_job_says_nothing_to_the_customer(self):
        user, _ = self.run_one_job(quiet=True)

        assert user.info.call_count == 0
        assert user.error.call_count == 0


class TestASplitPeriodIsAnnouncedOnce:
    PARTS = [
        {"name": "core", "report_run_id": "a"},
        {"name": "standard", "report_run_id": "b"},
        {"name": "beta", "report_run_id": "c"},
    ]

    def test_three_parts_give_the_customer_one_line(self):
        stream = make_stream()
        progress = {
            part["report_run_id"]: iter(
                [
                    {"id": part["report_run_id"], "async_status": "Job Running", "async_percent_completion": 50},
                    {"id": part["report_run_id"], "async_status": "Job Completed", "async_percent_completion": 100},
                ]
            )
            for part in self.PARTS
        }

        def api_get(self_, *args, **kwargs):
            return next(progress[self_["id"]])

        parts = [dict(part) for part in self.PARTS]
        with (
            mock.patch.object(AdReportRun, "api_get", autospec=True, side_effect=api_get),
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
            mock.patch("tap_facebook.streams.ad_insights.user_logger") as user,
            mock.patch("tap_facebook.streams.ad_insights.internal_logger"),
        ):
            jobs = stream._run_parts_to_completion(parts, "2026-09-20")

        assert jobs is not None
        assert user.info.call_count == 1, f"expected one line, got {user.info.call_count}"
        assert messages(user.info) == ["[adsinsights] Report for 2026-09-20 is ready."]

    def test_each_part_stays_at_internal_debug(self):
        stream = make_stream()
        progress = {
            part["report_run_id"]: iter(
                [{"id": part["report_run_id"], "async_status": "Job Completed", "async_percent_completion": 100}]
            )
            for part in self.PARTS
        }

        def api_get(self_, *args, **kwargs):
            return next(progress[self_["id"]])

        with (
            mock.patch.object(AdReportRun, "api_get", autospec=True, side_effect=api_get),
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
            mock.patch("tap_facebook.streams.ad_insights.user_logger"),
            mock.patch("tap_facebook.streams.ad_insights.internal_logger") as internal,
        ):
            stream._run_parts_to_completion([dict(part) for part in self.PARTS], "2026-09-20")

        per_part = [line for line in messages(internal.info) if "Insights job" in line]
        assert per_part == [], f"expected no per-part info line, got {len(per_part)}"
        assert sum("All 3 part(s) built" in line for line in messages(internal.info)) == 1


class TestTheOptionalGroupsAreToldToTheCustomerOnce:
    def drift(self, stream: AdsInsightStream, times: int = 1):
        stream._optional_groups_logged = False
        with (
            mock.patch("tap_facebook.streams.ad_insights.user_logger") as user,
            mock.patch("tap_facebook.streams.ad_insights.internal_logger") as internal,
        ):
            for _ in range(times):
                stream._log_schema_drift()
        return user, internal

    def test_one_customer_line_for_all_groups(self):
        user, _ = self.drift(make_stream())

        lines = [line for line in messages(user.info) if "optional metric groups" in line]
        assert len(lines) == 1, f"expected one line, got {len(lines)}"
        assert "Ads Insights: Include beta metrics" in lines[0]
        assert "Ads Insights: Include commerce metrics" in lines[0]

    def test_it_is_not_repeated_for_the_same_stream(self):
        user, _ = self.drift(make_stream(), times=3)

        lines = [line for line in messages(user.info) if "optional metric groups" in line]
        assert len(lines) == 1, f"expected one line, got {len(lines)}"

    def test_the_customer_line_names_settings_not_config_keys(self):
        user, _ = self.drift(make_stream())

        line = next(line for line in messages(user.info) if "optional metric groups" in line)
        assert "include_insights_" not in line

    def test_the_internal_pair_keeps_the_field_names_in_one_line(self):
        _, internal = self.drift(make_stream())

        pairs = [line for line in messages(internal.info) if "not requested, by setting" in line]
        assert len(pairs) == 1, f"expected one line, got {len(pairs)}"
        assert "include_insights_beta_fields=" in pairs[0]

    def test_with_every_group_on_nothing_is_said(self):
        stream = make_stream(
            include_insights_standard_fields=True,
            include_insights_messaging_fields=True,
            include_insights_commerce_fields=True,
            include_insights_beta_fields=True,
            include_insights_results_fields=True,
            include_insights_attribution_fields=True,
        )
        user, _ = self.drift(stream)

        assert not any("optional metric groups" in line for line in messages(user.info))


def test_the_titles_match_the_source_form():
    """The customer line quotes these titles, so they must be the ones in nekt.config.json."""
    config = json.loads((Path(__file__).parent.parent / "nekt.config.json").read_text())
    found: dict[str, str] = {}

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(value, dict) and key in AdsInsightStream.OPTIONAL_FIELD_GROUP_TITLES:
                    found[key] = value.get("title")
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(config)
    assert found == AdsInsightStream.OPTIONAL_FIELD_GROUP_TITLES
    assert set(found) == set(AdsInsightStream.OPTIONAL_FIELD_GROUPS)


if __name__ == "__main__":
    pytest.main([__file__])
