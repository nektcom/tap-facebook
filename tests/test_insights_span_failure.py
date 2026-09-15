"""Unit tests for what happens after the single-window (span) report fails.

Fully offline: report creation and job polling are mocked, so no credentials
and no calls against the ad account quota these tests are about.

Background (NEKT-5249): since 11/09/2026 Facebook puts some ad accounts in a
state where every async insights job fails at 0% and report creation is refused
with #613 after a handful of calls. The tap used to answer a failed span job by
creating one report per day (13-30 creations) and recreating each failed report
ten times -- all of which failed the same way, kept the account throttled and
crashed the run. It also discarded the error fields Facebook attaches to the
failed AdReportRun. These tests pin the new behaviour: retry the span once,
probe with a single-slice report, stop cleanly when the probe fails too, share
that verdict with the other insights streams, and log Facebook's own reason.
"""

from __future__ import annotations

from unittest import mock

import pendulum
import pytest
from facebook_business.adobjects.adreportrun import AdReportRun

from tap_facebook.streams.ad_insights import (
    BASIC_FIELDS,
    PER_SLICE_RETRIES,
    SPAN_RETRIES,
    AdsInsightStream,
)
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
    "enable_advanced_reports": True,
}

START = pendulum.date(2026, 9, 2)
UNTIL = START.add(days=13)
COLUMNS = list(BASIC_FIELDS)


@pytest.fixture(autouse=True)
def _fresh_process_state():
    """The 'account not building' verdict lives on the class, i.e. per process."""
    AdsInsightStream._account_not_building = False
    yield
    AdsInsightStream._account_not_building = False


@pytest.fixture(autouse=True)
def _no_sleep():
    with mock.patch("tap_facebook.streams.ad_insights.time.sleep"):
        yield


def make_stream(name: str = "adsinsights") -> AdsInsightStream:
    tap = TapFacebook(config=SAMPLE_CONFIG)
    stream = tap.streams[name]
    stream._reset_run_state()
    return stream


def span_report() -> dict:
    return {
        "report_run_id": "span-1",
        "date": f"{START} to {UNTIL}",
        "date_obj": START,
        "until_obj": UNTIL,
        "next_date": UNTIL.add(days=1),
    }


def slice_report(date: pendulum.Date) -> dict:
    return {
        "report_run_id": f"slice-{date}",
        "date": date.to_date_string(),
        "date_obj": date,
        "until_obj": None,
        "next_date": date.add(days=1),
    }


def built_job() -> mock.Mock:
    job = mock.Mock(spec=AdReportRun)
    job.get_result.return_value = []
    return job


FAILED = None  # what _run_job_to_completion returns for a job that did not build


class TestSpanFailure:
    def test_a_failed_span_is_recreated_once_and_can_still_succeed(self):
        stream = make_stream()

        with mock.patch.object(stream, "_run_job_to_completion", side_effect=[FAILED, built_job()]) as poll, \
             mock.patch.object(stream, "_create_single_report", return_value="span-2") as create:
            list(stream._process_report_batch([span_report()], COLUMNS, 1))

        assert create.call_count == SPAN_RETRIES == 1
        assert create.call_args.kwargs["until"] == UNTIL, "the retry must keep the span shape"
        assert poll.call_count == 2
        assert stream._span_mode is True
        assert AdsInsightStream._account_not_building is False

    def test_two_span_failures_probe_one_slice_and_then_fall_back(self):
        stream = make_stream()
        probe = built_job()

        with mock.patch.object(stream, "_run_job_to_completion", side_effect=[FAILED, FAILED, probe]), \
             mock.patch.object(stream, "_create_single_report", side_effect=["span-2", "probe-1"]) as create:
            list(stream._process_report_batch([span_report()], COLUMNS, 1))

        # 1 span retry + 1 single-slice probe. Not 14 per-day reports.
        assert create.call_count == 2
        probe_call = create.call_args_list[1]
        assert probe_call.args[0] == START
        assert probe_call.kwargs.get("until") is None, "the probe is a single slice"
        # The probe built, so the account works: per-slice is the right fallback,
        # resuming right after the slice the probe already delivered.
        assert stream._span_mode is False
        assert stream._span_failed_from == START.add(days=1)
        assert AdsInsightStream._account_not_building is False
        assert stream._dates_failed == 0

    def test_when_the_probe_fails_too_the_stream_stops_without_the_per_slice_burst(self):
        stream = make_stream()

        with mock.patch.object(stream, "_run_job_to_completion", side_effect=[FAILED, FAILED, FAILED]), \
             mock.patch.object(stream, "_create_single_report", side_effect=["span-2", "probe-1"]) as create, \
             mock.patch("tap_facebook.streams.ad_insights.user_logger") as user_log:
            records = list(stream._process_report_batch([span_report()], COLUMNS, 1))

        assert records == []
        # The whole cost of learning the account is not building: 3 creations
        # (span, span retry, probe) instead of 3 + 14 + 10 per date.
        assert create.call_count == 2
        assert AdsInsightStream._account_not_building is True
        assert stream._dates_failed == 1
        assert stream._span_mode is True, "no fallback to per-slice was entered"
        message = user_log.error.call_args.args[0]
        assert "not building performance reports" in message
        assert "left untouched" in message

    def test_a_refused_probe_creation_is_a_throttle_not_a_verdict(self):
        stream = make_stream()

        calls = []

        def create(*args, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                return "span-2"  # the span retry is accepted
            stream._throttled = True  # the probe creation is refused with 613
            return None

        with mock.patch.object(stream, "_run_job_to_completion", side_effect=[FAILED, FAILED]), \
             mock.patch.object(stream, "_create_single_report", side_effect=create):
            list(stream._process_report_batch([span_report()], COLUMNS, 1))

        assert len(calls) == 2

        assert AdsInsightStream._account_not_building is False
        assert stream._throttled_from == START


class TestTheVerdictIsSharedAcrossStreams:
    def test_later_insights_streams_skip_without_spending_a_call(self):
        AdsInsightStream._account_not_building = True
        other = make_stream("adsinsights_by_country")

        with mock.patch.object(other, "_initialize_client"), \
             mock.patch.object(other, "_create_report_batch") as create, \
             pytest.raises(SystemExit):
            list(other.get_records(None))

        create.assert_not_called()

    def test_a_fresh_process_does_not_skip(self):
        other = make_stream("adsinsights_by_country")
        assert AdsInsightStream._account_not_building is False
        assert other._account_not_building is False


class TestFacebookReasonIsKept:
    def test_a_failed_job_logs_facebook_error_fields_and_flags_quota_codes(self):
        stream = make_stream()
        report = mock.Mock()
        report.api_get.return_value = {
            "id": "job-1",
            "async_status": "Job Failed",
            "async_percent_completion": 0,
            "error_code": 613,
            "error_subcode": 1234,
            "error_message": "Calls to this api have exceeded the rate limit.",
            "error_user_title": "Report limit reached",
            "error_user_msg": "Try again later.",
        }

        with mock.patch("tap_facebook.streams.ad_insights.user_logger") as user_log, \
             mock.patch("tap_facebook.streams.ad_insights.internal_logger") as internal_log:
            result = stream._run_job_to_completion(report_instance=report, report_date="2026-09-02")

        assert result is None
        assert stream._last_job_error == {
            "error_code": 613,
            "error_subcode": 1234,
            "error_message": "Calls to this api have exceeded the rate limit.",
            "error_user_title": "Report limit reached",
            "error_user_msg": "Try again later.",
        }
        assert stream._throttled is True, "a quota code on the job is a throttle"
        customer_line = user_log.error.call_args.args[0]
        assert "Facebook says: Report limit reached" in customer_line
        assert "Try again later." in customer_line
        engineer_line = internal_log.error.call_args.args[0]
        assert "error_code=613" in engineer_line and "act_123" in engineer_line

    def test_a_failed_job_without_error_fields_keeps_the_generic_hint(self):
        stream = make_stream()
        report = mock.Mock()
        report.api_get.return_value = {"id": "job-2", "async_status": "Job Failed", "async_percent_completion": 0}

        with mock.patch("tap_facebook.streams.ad_insights.user_logger") as user_log:
            stream._run_job_to_completion(report_instance=report, report_date="2026-09-02")

        assert stream._last_job_error == {}
        assert stream._throttled is False
        assert "intermittent error" in user_log.error.call_args.args[0]


class TestPerSliceRetriesAreCapped:
    def test_a_slice_that_never_builds_is_recreated_at_most_twice(self):
        stream = make_stream()
        stream._give_up_on_span(START, "label")

        with mock.patch.object(stream, "_run_job_to_completion", return_value=FAILED), \
             mock.patch.object(stream, "_create_single_report", side_effect=["r2", "r3", "r4", "r5"]) as create, \
             mock.patch.object(stream, "_drop_columns_failing_the_job", return_value=False):
            list(stream._process_report_batch([slice_report(START)], COLUMNS, 1))

        assert create.call_count == PER_SLICE_RETRIES == 2
        assert stream._dates_failed == 1


if __name__ == "__main__":
    pytest.main([__file__])
