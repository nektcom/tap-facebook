"""Unit tests for telling Facebook's transient failures apart from real limits.

Fully offline: report creation and job polling are mocked, so no credentials
and no calls against the ad account quota.

Background (NEKT-5249, v1.72): once the tap stopped bursting on a failed span
(v1.71), the first full day on the fleet showed the opposite mistake -- ending
a run on errors that clear within minutes:

* the APP-level limit (code 4, "Application request limit reached") was treated
  like the per-account 613 and killed four healthy runs at the top of the hour;
* a single HTTP 500 / code 1 on the first creation ended a stream with 0 rows;
* a dropped connection during creation was an uncaught exception;
* the span-then-probe verdict was reached in ~2 minutes, so a short Facebook
  outage put a healthy account (uqda) in the "not building" state for the run
  while its sibling (IfNM), probing one minute later, sailed through;
* the probe used the oldest day of the window, which for a backfill starting at
  the retention edge fails on age alone (3du2).

These tests pin the new behaviour: wait and retry on what is Facebook's to fix,
back off before the probe, probe the newest slice, and keep 613 as the only
code that ends the stream on the spot.
"""

from __future__ import annotations

from unittest import mock

import pendulum
import pytest
import requests
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.exceptions import FacebookRequestError

from tap_facebook.streams.ad_insights import (
    APP_THROTTLE_RETRIES,
    BASIC_FIELDS,
    PROBE_BACKOFF_SECONDS,
    TRANSIENT_CREATE_RETRIES,
    AdsInsightStream,
)
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
    "enable_advanced_reports": True,
}

START = pendulum.date(2026, 9, 4)
UNTIL = pendulum.date(2026, 9, 15)
COLUMNS = list(BASIC_FIELDS)
FAILED = None  # what _run_job_to_completion returns for a job that did not build


@pytest.fixture(autouse=True)
def _fresh_process_state():
    AdsInsightStream._account_not_building = False
    yield
    AdsInsightStream._account_not_building = False


@pytest.fixture(autouse=True)
def sleep():
    with mock.patch("tap_facebook.streams.ad_insights.time.sleep") as patched:
        yield patched


def make_stream() -> AdsInsightStream:
    tap = TapFacebook(config=SAMPLE_CONFIG)
    stream = tap.streams["adsinsights"]
    stream._reset_run_state()
    return stream


def fb_error(code: int, *, http_status: int = 400, subcode: int | None = None, message: str = "x") -> FacebookRequestError:
    body = {"error": {"code": code, "message": message}}
    if subcode is not None:
        body["error"]["error_subcode"] = subcode
    import json

    return FacebookRequestError(
        message="Call was not successful",
        request_context={},
        http_status=http_status,
        http_headers={},
        body=json.dumps(body),
    )


def accepted(report_run_id: str = "r-1") -> mock.Mock:
    response = mock.Mock()
    response.status.return_value = 200
    response.json.return_value = {"report_run_id": report_run_id}
    response._headers = {}
    return response


APP_LIMIT = fb_error(4, subcode=1504022, message="Application request limit reached")
ACCOUNT_LIMIT = fb_error(613, message="(#613) Custom Analytics metrics exceeded the rate limit of 5 calls per 6 hours")
META_500 = fb_error(1, http_status=500, subcode=99, message="An unknown error occurred")


class TestAppLimitIsWaitedOut:
    def test_a_refused_creation_is_retried_after_a_wait_and_then_accepted(self, sleep):
        stream = make_stream()

        with mock.patch.object(
            stream, "_trigger_async_insight_report_creation", side_effect=[APP_LIMIT, accepted("r-ok")]
        ) as trigger:
            report_run_id = stream._create_single_report(START, COLUMNS, 1)

        assert report_run_id == "r-ok"
        assert trigger.call_count == 2
        assert sleep.call_count == 1 and sleep.call_args.args[0] >= 60
        # The app limit is not the account's quota: nothing was marked as spent.
        assert stream._throttled is False

    def test_the_retries_are_bounded_and_then_the_window_is_given_up_with_the_app_wording(self, sleep):
        stream = make_stream()

        with mock.patch.object(
            stream, "_trigger_async_insight_report_creation", side_effect=APP_LIMIT
        ) as trigger, mock.patch("tap_facebook.streams.ad_insights.user_logger") as user_log:
            report_run_id = stream._create_single_report(START, COLUMNS, 1)
            stream._warn_throttle_is_unrecoverable()

        assert report_run_id is None
        assert trigger.call_count == APP_THROTTLE_RETRIES + 1
        assert stream._throttled is True, "after the retries the caller stops the batch as before"
        assert stream._last_throttle_code == 4
        message = user_log.warning.call_args.args[0]
        assert "Nekt application" in message and "not specific to your account" in message

    def test_the_account_limit_is_still_final_on_the_first_refusal(self, sleep):
        stream = make_stream()

        with mock.patch.object(
            stream, "_trigger_async_insight_report_creation", side_effect=ACCOUNT_LIMIT
        ) as trigger:
            report_run_id = stream._create_single_report(START, COLUMNS, 1)

        assert report_run_id is None
        assert trigger.call_count == 1, "613 does not clear within a run; no point retrying"
        assert stream._throttled is True
        sleep.assert_not_called()

    def test_an_app_limit_on_the_failed_job_is_transient_not_a_throttle(self):
        stream = make_stream()
        report = mock.Mock()
        report.api_get.return_value = {
            "id": "job-1",
            "async_status": "Job Failed",
            "async_percent_completion": 0,
            "error_code": 4,
            "error_subcode": 1504022,
            "error_message": "Application request limit reached",
        }

        result = stream._run_job_to_completion(report_instance=report, report_date="2026-09-04")

        assert result is None
        assert stream._throttled is False, "the retry ladder recreates it; the window is not given up"
        assert stream._last_job_error["error_code"] == 4


class TestFacebookHiccupsAreRetriedOnce:
    def test_a_500_on_creation_is_retried_and_succeeds(self, sleep):
        stream = make_stream()

        with mock.patch.object(
            stream, "_trigger_async_insight_report_creation", side_effect=[META_500, accepted("r-ok")]
        ) as trigger:
            report_run_id = stream._create_single_report(START, COLUMNS, 1)

        assert report_run_id == "r-ok"
        assert trigger.call_count == TRANSIENT_CREATE_RETRIES + 1
        assert sleep.call_count == 1

    def test_a_persistent_500_still_ends_the_creation_without_hanging(self, sleep):
        stream = make_stream()

        with mock.patch.object(
            stream, "_trigger_async_insight_report_creation", side_effect=META_500
        ) as trigger, mock.patch("tap_facebook.streams.ad_insights.user_logger"):
            report_run_id = stream._create_single_report(START, COLUMNS, 1)

        assert report_run_id is None
        assert trigger.call_count == TRANSIENT_CREATE_RETRIES + 1
        assert stream._throttled is False

    def test_a_dropped_connection_is_retried_and_then_skips_the_date_instead_of_crashing(self, sleep):
        stream = make_stream()
        boom = requests.exceptions.ConnectionError("Remote end closed connection without response")

        with mock.patch.object(
            stream, "_trigger_async_insight_report_creation", side_effect=boom
        ) as trigger, mock.patch("tap_facebook.streams.ad_insights.user_logger"):
            reports = stream._create_report_batch(
                start_date=START, batch_size=1, end_date=START, columns=COLUMNS, time_increment=1
            )

        assert reports == []
        assert trigger.call_count == TRANSIENT_CREATE_RETRIES + 1
        # The floor at the end of the run sees this as a failed date.
        assert stream._dates_failed == 1

    def test_a_bad_request_is_not_retried(self, sleep):
        stream = make_stream()
        bad_field = fb_error(100, message="Invalid parameter")

        with mock.patch.object(
            stream, "_trigger_async_insight_report_creation", side_effect=bad_field
        ) as trigger, mock.patch("tap_facebook.streams.ad_insights.user_logger"):
            report_run_id = stream._create_single_report(START, COLUMNS, 1)

        assert report_run_id is None
        assert trigger.call_count == 1
        sleep.assert_not_called()


class TestTheProbeWaitsAndLooksAtTheNewestSlice:
    def test_the_probe_backs_off_and_asks_for_the_last_day_of_the_window(self, sleep):
        stream = make_stream()
        probe = mock.Mock(spec=AdReportRun)

        with mock.patch.object(stream, "_run_job_to_completion", return_value=probe), \
             mock.patch.object(stream, "_create_single_report", return_value="probe-1") as create:
            stream._leave_span(START, f"{START} to {UNTIL}", COLUMNS, 1, until=UNTIL)

        assert PROBE_BACKOFF_SECONDS in [c.args[0] for c in sleep.call_args_list]
        assert create.call_args.args[0] == UNTIL
        assert create.call_args.kwargs.get("quiet") is True, "the probe is a diagnostic, not customer work"
        # The probe's rows are not consumed: the per-slice pass covers the whole window.
        probe.get_result.assert_not_called()
        assert stream._span_failed_from == START
        assert stream._span_mode is False
        assert AdsInsightStream._account_not_building is False

    def test_a_probe_that_fails_after_the_wait_is_the_verdict(self, sleep):
        stream = make_stream()

        with mock.patch.object(stream, "_run_job_to_completion", return_value=FAILED), \
             mock.patch.object(stream, "_create_single_report", return_value="probe-1"), \
             mock.patch("tap_facebook.streams.ad_insights.internal_logger") as internal_log:
            stream._leave_span(START, f"{START} to {UNTIL}", COLUMNS, 1, until=UNTIL)

        assert AdsInsightStream._account_not_building is True
        assert stream._dates_failed == 1
        engineer_line = internal_log.error.call_args.args[0]
        assert UNTIL.to_date_string() in engineer_line and f"{PROBE_BACKOFF_SECONDS}s later" in engineer_line

    def test_without_a_span_end_the_probe_falls_back_to_the_start_date(self, sleep):
        stream = make_stream()

        with mock.patch.object(stream, "_run_job_to_completion", return_value=mock.Mock(spec=AdReportRun)), \
             mock.patch.object(stream, "_create_single_report", return_value="probe-1") as create:
            stream._leave_span(START, START.to_date_string(), COLUMNS, 1)

        assert create.call_args.args[0] == START


if __name__ == "__main__":
    pytest.main([__file__])
