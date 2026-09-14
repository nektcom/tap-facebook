"""Unit tests for how the insights streams behave once the ad account is throttled.

Fully offline: the report-creation request and the job polling are mocked, so
no Facebook credentials and -- more to the point -- no calls against the very
quota these tests are about.

NEKT-5173 taught `_create_report_batch` to stop on a throttling code and ask for
the whole window in a single report. These tests cover the two paths it left
spending calls on an empty budget (NEKT-5202): the per-date retry ladder in
`_process_report_batch`, and the column bisect that would otherwise read a quota
failure as evidence against the requested fields.
"""

from __future__ import annotations

import pendulum
import pytest
from facebook_business.exceptions import FacebookRequestError

from tap_facebook.streams.ad_insights import (
    BASIC_FIELDS,
    AdsInsightStream,
)
from tap_facebook.tap import TapFacebook
from unittest import mock

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
    "enable_advanced_reports": True,
}

THROTTLE_BODY = (
    '{"error": {"code": 613, "message": "(#613) Custom Analytics metrics exceeded '
    'the rate limit of 5 calls per 6 hours for this ad account."}}'
)

A_DATE = pendulum.date(2026, 9, 1)
ANOTHER_DATE = pendulum.date(2026, 9, 2)


def make_stream() -> AdsInsightStream:
    tap = TapFacebook(config=SAMPLE_CONFIG)
    stream = tap.streams["adsinsights"]
    # Only get_records calls this, and these tests exercise the helpers directly.
    stream._reset_run_state()
    return stream


def make_throttle_error() -> FacebookRequestError:
    return FacebookRequestError(
        message="Call was not successful",
        request_context={},
        http_status=400,
        http_headers={},
        body=THROTTLE_BODY,
    )


def make_report(date: pendulum.Date) -> dict:
    return {
        "report_run_id": f"report-{date}",
        "date": date.to_date_string(),
        "date_obj": date,
        "until_obj": None,
        "next_date": date.add(days=1),
    }


class TestCreationThrottle:
    def test_quota_refusal_is_recorded_and_not_shouted_at_the_customer(self):
        stream = make_stream()

        with mock.patch.object(
            stream, "_trigger_async_insight_report_creation", side_effect=make_throttle_error()
        ), mock.patch("tap_facebook.streams.ad_insights.user_logger") as user_log:
            report_run_id = stream._create_single_report(A_DATE, list(BASIC_FIELDS), 1)

        assert report_run_id is None
        assert stream._throttled is True
        # The caller emits the one message that explains the run; a line per
        # refused date is what flooded the customer log before.
        user_log.warning.assert_not_called()

    def test_a_non_quota_error_still_warns_and_does_not_set_the_flag(self):
        stream = make_stream()
        other_error = FacebookRequestError(
            message="Call was not successful",
            request_context={},
            http_status=400,
            http_headers={},
            body='{"error": {"code": 100, "message": "Invalid parameter"}}',
        )

        with mock.patch.object(
            stream, "_trigger_async_insight_report_creation", side_effect=other_error
        ), mock.patch("tap_facebook.streams.ad_insights.user_logger") as user_log:
            report_run_id = stream._create_single_report(A_DATE, list(BASIC_FIELDS), 1)

        assert report_run_id is None
        assert stream._throttled is False
        user_log.warning.assert_called()


@mock.patch("tap_facebook.streams.ad_insights.time.sleep")
class TestRetryLadderStopsOnThrottle:
    def test_one_refused_creation_ends_the_batch(self, mock_sleep):
        stream = make_stream()
        batch = [make_report(A_DATE), make_report(ANOTHER_DATE)]

        with mock.patch.object(stream, "_run_job_to_completion", return_value=None), mock.patch.object(
            stream, "_trigger_async_insight_report_creation", side_effect=make_throttle_error()
        ) as trigger:
            records = list(stream._process_report_batch(batch, list(BASIC_FIELDS), 1))

        assert records == []
        # The ladder allows ten retries per date and the batch holds two dates.
        # Exactly one creation must be attempted: the rest would be refused the
        # same way and keep the six-hour budget empty.
        assert trigger.call_count == 1
        assert stream._throttled_from == A_DATE

    def test_the_second_date_is_never_reached(self, mock_sleep):
        stream = make_stream()
        batch = [make_report(A_DATE), make_report(ANOTHER_DATE)]
        seen: list[str] = []

        def remember(report_instance, report_date, quiet=False):  # noqa: ARG001, FBT002
            seen.append(report_date)
            return None

        with mock.patch.object(stream, "_run_job_to_completion", side_effect=remember), mock.patch.object(
            stream, "_trigger_async_insight_report_creation", side_effect=make_throttle_error()
        ):
            list(stream._process_report_batch(batch, list(BASIC_FIELDS), 1))

        assert seen == [A_DATE.to_date_string()]


class TestThrottleDoesNotBlameTheColumns:
    def test_bisect_is_skipped_while_the_quota_is_spent(self):
        stream = make_stream()
        stream._throttled = True
        columns = [*BASIC_FIELDS, "an_optional_column"]

        with mock.patch.object(stream, "_bisect_failing_columns") as bisect:
            dropped = stream._drop_columns_failing_the_job(A_DATE, columns, 1)

        assert dropped is False
        bisect.assert_not_called()
        assert not stream._rejected_columns
        assert stream._auto_drops == 0

    def test_a_throttle_during_the_probes_leaves_the_field_set_untouched(self):
        stream = make_stream()
        columns = [*BASIC_FIELDS, "an_optional_column"]

        def bisect_hits_the_limit(*_args, **_kwargs) -> list[str]:
            # What really happens: the BASIC_FIELDS probe is refused for quota,
            # so the bisect cannot isolate anything and returns empty-handed.
            stream._throttled = True
            return []

        with mock.patch.object(stream, "_bisect_failing_columns", side_effect=bisect_hits_the_limit):
            dropped = stream._drop_columns_failing_the_job(A_DATE, columns, 1)

        # Without the guard this fell through to "could not isolate a single
        # column", dropping every optional column for the rest of the run.
        assert dropped is False
        assert not stream._rejected_columns
        assert stream._auto_drops == 0


if __name__ == "__main__":
    pytest.main([__file__])
