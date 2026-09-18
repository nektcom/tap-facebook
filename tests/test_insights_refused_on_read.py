"""Unit tests for a column the account takes at creation and refuses at read.

Fully offline: the built report is a mock, so nothing here touches an ad
account or its quota.

Background (NEKT-5249, v1.77): some ad accounts accept `adset_start` /
`adset_end` when the async report is created, complete the job, and then answer
"(#100) Tried accessing nonexisting summary field (adset_end)" when the result
is read. Until v1.76 the stream dropped one refused column and recreated the
whole report, so a single stream spent three creations instead of one and the
run hit the "5 calls per 6 hours" limit before the last stream was served.

Verified in the Graph API Explorer on 2026-09-18 (report 1750585912883579,
v26.0): reading it with no field list answers #100, and reading the very same
report with an explicit `fields=` that leaves the refused column out returns the
rows. So the remedy is to read again, not to build again. These tests pin that,
and pin that the refusal is remembered for the other streams of the same run.
"""

from __future__ import annotations

from unittest import mock

import pendulum
import pytest
from facebook_business.exceptions import FacebookRequestError

from tap_facebook.streams.ad_insights import AdsInsightStream
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
    "enable_advanced_reports": True,
    "include_insights_standard_fields": True,
}

COLUMNS = ["ad_id", "impressions", "spend", "adset_start", "adset_end"]
REPORT_DATE = "2026-09-16"
DATE_OBJ = pendulum.date(2026, 9, 16)
PARTS = [{"name": "all", "columns": COLUMNS, "report_run_id": "report-1"}]


@pytest.fixture(autouse=True)
def _fresh_process_state():
    AdsInsightStream._columns_refused_on_read = set()
    yield
    AdsInsightStream._columns_refused_on_read = set()


def make_stream() -> AdsInsightStream:
    tap = TapFacebook(config=SAMPLE_CONFIG)
    stream = tap.streams["adsinsights"]
    stream._reset_run_state()
    return stream


def refusal(column: str) -> FacebookRequestError:
    return FacebookRequestError(
        message="Call was not successful",
        request_context={},
        http_status=400,
        http_headers={},
        body=(
            '{"error": {"code": 100, "type": "OAuthException", "message": '
            f'"(#100) Tried accessing nonexisting summary field ({column})"}}}}'
        ),
    )


class TestARefusedColumnIsAnsweredByReadingAgain:
    def test_the_report_is_read_again_without_the_refused_column(self):
        stream = make_stream()
        rows = [{"ad_id": "1", "impressions": "10"}]
        with mock.patch.object(stream, "_merge_part_results", return_value=rows) as merge:
            got = stream._reread_without_refused_columns(
                refusal("adset_end"), PARTS, [mock.Mock()], COLUMNS, REPORT_DATE
            )

        assert got == rows
        asked_for = merge.call_args.kwargs["fields"]
        assert "adset_end" not in asked_for
        assert asked_for == ["ad_id", "impressions", "spend", "adset_start"]

    def test_no_report_is_recreated(self):
        """The whole point: the read is retried, the creation path is not entered."""
        stream = make_stream()
        with (
            mock.patch.object(stream, "_merge_part_results", return_value=[]),
            mock.patch.object(stream, "_create_single_report") as create,
        ):
            stream._reread_without_refused_columns(
                refusal("adset_end"), PARTS, [mock.Mock()], COLUMNS, REPORT_DATE
            )

        create.assert_not_called()

    def test_facebook_naming_one_column_at_a_time_keeps_narrowing_the_read(self):
        stream = make_stream()
        rows = [{"ad_id": "1"}]
        with mock.patch.object(
            stream, "_merge_part_results", side_effect=[refusal("adset_start"), rows]
        ) as merge:
            got = stream._reread_without_refused_columns(
                refusal("adset_end"), PARTS, [mock.Mock()], COLUMNS, REPORT_DATE
            )

        assert got == rows
        assert merge.call_count == 2
        assert merge.call_args.kwargs["fields"] == ["ad_id", "impressions", "spend"]

    def test_an_error_that_is_not_a_refused_column_is_left_to_the_caller(self):
        stream = make_stream()
        throttled = FacebookRequestError(
            message="Call was not successful",
            request_context={},
            http_status=400,
            http_headers={},
            body='{"error": {"code": 613, "message": "(#613) Custom Analytics metrics exceeded the rate limit"}}',
        )
        with mock.patch.object(stream, "_merge_part_results") as merge:
            got = stream._reread_without_refused_columns(
                throttled, PARTS, [mock.Mock()], COLUMNS, REPORT_DATE
            )

        assert got is None
        merge.assert_not_called()

    def test_a_100_that_names_no_requested_column_is_left_to_the_caller(self):
        stream = make_stream()
        with mock.patch.object(stream, "_merge_part_results") as merge:
            got = stream._reread_without_refused_columns(
                refusal("something_else"), PARTS, [mock.Mock()], COLUMNS, REPORT_DATE
            )

        assert got is None
        merge.assert_not_called()

    def test_every_column_refused_falls_back_to_the_caller(self):
        stream = make_stream()
        only_one = ["adset_end"]
        parts = [{"name": "all", "columns": only_one, "report_run_id": "report-1"}]
        with mock.patch.object(stream, "_merge_part_results") as merge:
            got = stream._reread_without_refused_columns(
                refusal("adset_end"), parts, [mock.Mock()], only_one, REPORT_DATE
            )

        assert got is None
        merge.assert_not_called()

    def test_a_part_is_only_asked_for_the_columns_it_was_created_with(self):
        """Each report holds its own columns; asking one part for another's is what #100 punishes."""
        stream = make_stream()
        core = {"name": "core", "columns": ["ad_id", "impressions"], "report_run_id": "r1"}
        extra = {"name": "standard", "columns": ["ad_id", "spend", "adset_end"], "report_run_id": "r2"}
        jobs = [mock.Mock(), mock.Mock()]
        jobs[0].get_result.return_value = []
        jobs[1].get_result.return_value = []

        stream._merge_part_results([core, extra], jobs, REPORT_DATE, fields=["ad_id", "impressions", "spend"])

        assert jobs[0].get_result.call_args.kwargs["fields"] == ["ad_id", "impressions"]
        assert jobs[1].get_result.call_args.kwargs["fields"] == ["ad_id", "spend"]

    def test_a_plain_read_still_lets_facebook_serve_the_created_field_set(self):
        stream = make_stream()
        job = mock.Mock()
        job.get_result.return_value = []

        stream._merge_part_results(PARTS, [job], REPORT_DATE)

        assert job.get_result.call_args.kwargs["fields"] is None

    def test_a_part_with_no_readable_column_left_is_skipped_not_read_wide(self):
        stream = make_stream()
        core = {"name": "core", "columns": ["ad_id", "impressions"], "report_run_id": "r1"}
        extra = {"name": "standard", "columns": ["adset_end"], "report_run_id": "r2"}
        jobs = [mock.Mock(), mock.Mock()]
        jobs[0].get_result.return_value = []
        jobs[1].get_result.return_value = []

        stream._merge_part_results([core, extra], jobs, REPORT_DATE, fields=["ad_id", "impressions"])

        jobs[1].get_result.assert_not_called()

    def test_the_core_part_losing_every_column_falls_back_to_the_caller(self):
        stream = make_stream()
        core = {"name": "core", "columns": ["adset_end"], "report_run_id": "r1"}
        extra = {"name": "standard", "columns": ["spend"], "report_run_id": "r2"}
        with mock.patch.object(stream, "_merge_part_results") as merge:
            got = stream._reread_without_refused_columns(
                refusal("adset_end"), [core, extra], [mock.Mock(), mock.Mock()], ["adset_end", "spend"], REPORT_DATE
            )

        assert got is None
        merge.assert_not_called()


class TestTheRefusalIsRememberedForTheRestOfTheRun:
    def test_the_refused_columns_are_recorded_for_the_whole_process(self):
        stream = make_stream()
        with mock.patch.object(stream, "_merge_part_results", return_value=[]):
            stream._reread_without_refused_columns(
                refusal("adset_end"), PARTS, [mock.Mock()], COLUMNS, REPORT_DATE
            )

        assert AdsInsightStream._columns_refused_on_read == {"adset_end"}

    def test_a_later_stream_does_not_request_them_again(self):
        AdsInsightStream._columns_refused_on_read = {"adset_start", "adset_end"}
        stream = make_stream()

        columns = stream._get_selected_columns()

        assert "adset_start" not in columns
        assert "adset_end" not in columns
        assert "impressions" in columns

    def test_nothing_is_dropped_when_the_account_refused_nothing(self):
        stream = make_stream()
        columns = stream._get_selected_columns()
        assert "adset_start" in columns

    def test_a_refusal_is_not_carried_into_the_next_run(self):
        """The set lives on the class for the process only -- a new run rediscovers."""
        assert AdsInsightStream._columns_refused_on_read == set()


if __name__ == "__main__":
    pytest.main([__file__])
