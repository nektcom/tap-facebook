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

from tap_facebook.streams.ad_insights import (
    DEFAULT_INSIGHTS_MAX_WAIT_TO_START_SECONDS,
    AdsInsightStream,
)
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



class TestAJobThatHasNotStartedIsWaitedForNotRecreated:
    """Polling costs nothing; giving up costs a creation against the account's budget."""

    def poll(self, stream, *, elapsed: float, percent: int) -> tuple:
        job = {"async_status": "Job Running", "async_percent_completion": percent, "id": "job-1"}
        instance = mock.Mock()
        instance.api_get.return_value = job
        channel = mock.Mock()
        state = {
            "channel": channel,
            "report_date": REPORT_DATE,
            "instance": instance,
            "start": 0.0,
            "poll_failures": 0,
            "sleep": 0,
        }
        with mock.patch("tap_facebook.streams.ad_insights.time.time", return_value=elapsed):
            return stream._poll_job_once(state), channel

    def test_the_default_budget_is_twenty_minutes(self):
        assert DEFAULT_INSIGHTS_MAX_WAIT_TO_START_SECONDS == 20 * 60

    def test_five_minutes_at_zero_percent_is_still_pending(self):
        """The old limit: this used to fail the part and cost a recreation."""
        stream = make_stream()
        (status, _), channel = self.poll(stream, elapsed=301, percent=0)
        assert status == "pending"
        channel.error.assert_not_called()

    def test_past_the_budget_at_zero_percent_the_job_is_given_up(self):
        stream = make_stream()
        (status, _), channel = self.poll(stream, elapsed=DEFAULT_INSIGHTS_MAX_WAIT_TO_START_SECONDS + 1, percent=0)
        assert status == "failed"
        assert "insights_max_wait_to_start_seconds" in channel.error.call_args.args[0]

    def test_a_job_that_did_start_is_not_judged_by_this_budget(self):
        stream = make_stream()
        (status, _), _ = self.poll(stream, elapsed=DEFAULT_INSIGHTS_MAX_WAIT_TO_START_SECONDS + 1, percent=5)
        assert status == "pending"

    def test_the_source_can_raise_the_budget(self):
        tap = TapFacebook(config={**SAMPLE_CONFIG, "insights_max_wait_to_start_seconds": 3600})
        stream = tap.streams["adsinsights"]
        stream._reset_run_state()
        (status, _), _ = self.poll(stream, elapsed=1800, percent=0)
        assert status == "pending"


class TestTheRefusalIsNotRediscoveredOnEveryWindow:
    """No refusal, no second read: each window pays one request, not two."""

    def batch(self, stream):
        report = {"report_run_id": "r1", "date": REPORT_DATE, "date_obj": DATE_OBJ, "next_date": DATE_OBJ.add(days=1)}
        with (
            mock.patch.object(stream, "_run_parts_to_completion", return_value=[mock.Mock()]),
            mock.patch.object(stream, "_merge_part_results", return_value=[]) as merge,
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
        ):
            list(stream._process_report_batch([report], COLUMNS, 1))
        return merge

    def test_the_first_read_already_leaves_out_what_the_account_refused(self):
        AdsInsightStream._columns_refused_on_read = {"adset_end"}
        merge = self.batch(make_stream())

        fields = merge.call_args.kwargs["fields"]
        assert fields is not None
        assert "adset_end" not in fields
        assert "adset_start" in fields

    def test_without_a_known_refusal_the_read_stays_as_it_was(self):
        merge = self.batch(make_stream())
        assert merge.call_args.kwargs["fields"] is None


class TestAccountQuotaEndsTheStreamInsteadOfWalkingOn:
    """The spent budget belongs to the ad account and does not come back mid-run."""

    def run_until_throttled(self, code: int):
        stream = make_stream()

        def refused(*args, **kwargs):
            stream._throttled = True
            stream._last_throttle_code = code
            return []

        with (
            mock.patch.object(stream, "_initialize_client"),
            mock.patch.object(stream, "_create_report_batch", side_effect=refused),
            mock.patch.object(
                stream, "_advance_batch", side_effect=lambda *a, **k: pendulum.today().date().add(days=1)
            ) as advance,
            mock.patch.object(stream, "_fail_if_nothing_extracted") as floor,
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
        ):
            list(stream.get_records(None))
        return advance, floor

    def test_a_613_stops_the_stream_and_asks_for_no_further_window(self):
        advance, _ = self.run_until_throttled(613)
        advance.assert_not_called()

    def test_an_app_level_throttle_keeps_the_old_behaviour(self):
        """Codes 4 and 17 are the shared app limit, not this account's budget."""
        advance, _ = self.run_until_throttled(4)
        assert advance.called

    def test_the_floor_still_runs_when_the_stream_stops_early(self):
        _, floor = self.run_until_throttled(613)
        floor.assert_called_once()


class TestARefusalThatNamesKeyColumnsIsNotTrusted:
    """Facebook sometimes echoes the whole field list instead of naming the culprit.

    Seen on facebook-ads-WhFu (2026-09-18): the #100 message carried 13 names,
    including date_start and the join keys. Narrowing the read to drop them
    returned rows with no replication key and the run died with KeyError.
    """

    def refusal_naming_keys(self) -> FacebookRequestError:
        named = "account_id, ad_id, adset_end, adset_id, adset_start, campaign_id, date_start, date_stop"
        return FacebookRequestError(
            message="Call was not successful",
            request_context={},
            http_status=400,
            http_headers={},
            body=(
                '{"error": {"code": 100, "type": "OAuthException", "message": '
                f'"(#100) Tried accessing nonexisting summary field ({named})"}}}}'
            ),
        )

    def test_the_read_is_not_narrowed_when_a_key_is_named(self):
        stream = make_stream()
        columns = [*COLUMNS, "campaign_id", "date_start", "date_stop", "account_id"]
        with mock.patch.object(stream, "_merge_part_results") as merge:
            got = stream._reread_without_refused_columns(
                self.refusal_naming_keys(), PARTS, [mock.Mock()], columns, REPORT_DATE
            )

        assert got is None
        merge.assert_not_called()

    def test_nothing_is_remembered_from_such_a_refusal(self):
        stream = make_stream()
        columns = [*COLUMNS, "campaign_id", "date_start", "date_stop", "account_id"]
        stream._reread_without_refused_columns(
            self.refusal_naming_keys(), PARTS, [mock.Mock()], columns, REPORT_DATE
        )
        assert AdsInsightStream._columns_refused_on_read == set()

    def test_the_recreate_path_also_refuses_to_drop_keys(self):
        stream = make_stream()
        columns = [*COLUMNS, "campaign_id", "date_start", "date_stop", "account_id"]
        acted = stream._record_columns_refused_while_reading(
            self.refusal_naming_keys(), columns, REPORT_DATE, DATE_OBJ
        )
        assert acted is False
        assert stream._rejected_columns == []

    def test_a_later_pass_naming_keys_stops_the_loop(self):
        """The regression that disabled facebook-ads-TJaE and -WhFu on 2026-09-19/20.

        The first refusal names one harmless column, so the re-read starts. The
        read of the narrowed report then answers with the whole field list,
        keys included. Until v1.81 only the first refusal was checked, so the
        loop dropped the keys on this second pass and the rows came back with no
        `date_start`.
        """
        stream = make_stream()
        columns = [*COLUMNS, "campaign_id", "date_start", "date_stop", "account_id"]
        with mock.patch.object(
            stream, "_merge_part_results", side_effect=self.refusal_naming_keys()
        ) as merge:
            got = stream._reread_without_refused_columns(
                refusal("adset_end"), PARTS, [mock.Mock()], columns, REPORT_DATE
            )

        assert got is None
        assert merge.call_count == 1
        # And nothing is carried to the other streams of this run.
        assert AdsInsightStream._columns_refused_on_read == set()

    def test_a_later_pass_naming_a_plain_column_keeps_narrowing(self):
        """The loop itself is untouched: only a refusal naming a key stops it."""
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
        assert AdsInsightStream._columns_refused_on_read == {"adset_end", "adset_start"}

    def test_a_refusal_naming_only_a_plain_column_still_works(self):
        stream = make_stream()
        rows = [{"ad_id": "1"}]
        with mock.patch.object(stream, "_merge_part_results", return_value=rows) as merge:
            got = stream._reread_without_refused_columns(
                refusal("adset_end"), PARTS, [mock.Mock()], COLUMNS, REPORT_DATE
            )
        assert got == rows
        assert merge.called


if __name__ == "__main__":
    pytest.main([__file__])
