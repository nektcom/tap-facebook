"""Unit tests for v1.88: periods that were skipped without a word are now failed dates.

Fully offline.

Background (NEKT-5249, sweep of the runs from 2026-09-30 to 10-07):

* A report that could not be created -- HTTP 502/503, or a Graph error other
  than a quota or a refused column -- left its window behind uncounted: no
  warning named it, the bookmark walked past it and no run asked for it again
  (GcvQ, TIno, ZpnZ, all green).
* A monthly full sync clamped its start to Facebook's 37 months and then moved
  it back to the 1st of that month: every report was refused with #3018 and the
  six insights streams of facebook-ads-9huQ skipped 2023-09 to 2026-03 (green).
* In split mode on facebook-ads-WYkS (2026-10-07) none of the 47,985 rows of the
  seven optional parts matched the 953 core rows: the core rows were written
  with ~140 columns empty over days that had been loaded whole.
"""

from __future__ import annotations

import json
from unittest import mock

import pendulum
import pytest
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.exceptions import FacebookRequestError

from tap_facebook.streams.ad_insights import BASIC_FIELDS, STANDARD_FIELDS, AdsInsightStream, OptionalPartsDidNotJoin
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
    "enable_advanced_reports": True,
    "include_insights_standard_fields": True,
}
USER = "tap_facebook.streams.ad_insights.user_logger"
START = pendulum.date(2026, 9, 29)
UNTIL = pendulum.date(2026, 10, 7)
KEYS = ["date_start", "date_stop", "campaign_id", "adset_id", "ad_id"]
CORE = list(BASIC_FIELDS)
STANDARD = KEYS + STANDARD_FIELDS[:4]
TODAY = pendulum.today().date()


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


def make_stream(config: dict | None = None) -> AdsInsightStream:
    stream = TapFacebook(config=config or SAMPLE_CONFIG).streams["adsinsights"]
    stream._reset_run_state()
    stream._sync_context = None
    return stream


def row(ad_id: str) -> dict:
    return {"date_start": "2026-09-29", "date_stop": "2026-09-29", "campaign_id": "c", "adset_id": "s", "ad_id": ad_id}


def built(rows: list[dict]) -> mock.Mock:
    job = mock.Mock(spec=AdReportRun)
    objects = []
    for values in rows:
        obj = AdsInsights()
        for key, value in values.items():
            obj[key] = value
        objects.append(obj)
    job.get_result.return_value = objects
    return job


def fb_error(code: int, *, http_status: int = 400, message: str = "x") -> FacebookRequestError:
    return FacebookRequestError(
        message="Call was not successful",
        request_context={},
        http_status=http_status,
        http_headers={},
        body=json.dumps({"error": {"code": code, "message": message}}),
    )


PARTS = [
    {"name": "core", "columns": CORE, "report_run_id": "c-1"},
    {"name": "standard", "columns": STANDARD, "report_run_id": "s-1"},
    {"name": "results", "columns": STANDARD, "report_run_id": "r-1"},
]


def span_report() -> dict:
    return {
        "report_run_id": "c-1",
        "parts": [dict(part) for part in PARTS],
        "date": f"{START} to {UNTIL}",
        "date_obj": START,
        "until_obj": UNTIL,
        "next_date": UNTIL.add(days=1),
    }


class TestPartsThatDoNotJoinAreNotWritten:
    def test_no_optional_row_matching_a_core_row_is_a_mismatch(self):
        stream = make_stream()
        jobs = [built([row("a"), row("b")]), built([row("x"), row("y")]), built([row("z")])]
        with mock.patch("tap_facebook.streams.ad_insights.internal_logger"), pytest.raises(
            OptionalPartsDidNotJoin
        ) as raised:
            stream._merge_part_results(PARTS, jobs, "2026-09-29 to 2026-10-07")
        assert raised.value.core_rows == 2
        assert raised.value.part_rows == {"standard": 2, "results": 1}

    def test_a_part_with_some_rows_joined_is_merged_as_before(self):
        stream = make_stream()
        jobs = [built([row("a")]), built([row("a"), row("ghost")]), built([])]
        with mock.patch("tap_facebook.streams.ad_insights.internal_logger"):
            rows = stream._merge_part_results(PARTS, jobs, "2026-09-29 to 2026-10-07")
        assert [r["ad_id"] for r in rows] == ["a"]

    def test_optional_parts_with_no_rows_at_all_are_not_a_mismatch(self):
        """An account with nothing in those groups: the core rows are the period."""
        stream = make_stream()
        jobs = [built([row("a")]), built([]), built([])]
        rows = stream._merge_part_results(PARTS, jobs, "2026-09-29 to 2026-10-07")
        assert [r["ad_id"] for r in rows] == ["a"]

    def test_the_period_is_given_up_and_counted(self):
        stream = make_stream()
        stream._split_mode = True
        jobs = [built([row("a")]), built([row("x")]), built([row("y")])]
        with (
            mock.patch.object(stream, "_run_parts_to_completion", return_value=jobs),
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
            mock.patch(USER) as user,
        ):
            emitted = list(stream._process_report_batch([span_report()], CORE, 1))
        assert emitted == []
        assert stream._dates_failed == 1
        said = user.warning.call_args.args[0]
        assert "does not match the main report" in said
        assert "rather than written with those columns empty" in said

    def test_the_reread_path_gives_the_period_up_too(self):
        """WYkS: the read refused two columns, the re-read without them did not join."""
        stream = make_stream()
        stream._split_mode = True
        jobs = [built([row("a")]), built([row("x")]), built([row("y")])]
        mismatch = OptionalPartsDidNotJoin("2026-09-29 to 2026-10-07", 953, {"standard": 6855})
        with (
            mock.patch.object(stream, "_run_parts_to_completion", return_value=jobs),
            mock.patch.object(stream, "_merge_part_results", side_effect=fb_error(100, message="adset_end")),
            mock.patch.object(stream, "_reread_without_refused_columns", side_effect=mismatch),
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
            mock.patch(USER) as user,
        ):
            emitted = list(stream._process_report_batch([span_report()], CORE, 1))
        assert emitted == []
        assert stream._dates_failed == 1
        assert "none of their 6855 row(s) matched the 953 row(s)" in user.warning.call_args.args[0]

    def test_the_reread_does_not_swallow_the_mismatch(self):
        stream = make_stream()
        mismatch = OptionalPartsDidNotJoin("w", 1, {"standard": 1})
        with (
            mock.patch.object(stream, "_what_a_read_refusal_leaves_out", return_value=(["adset_end"], [])),
            mock.patch.object(stream, "_merge_part_results", side_effect=mismatch),
            pytest.raises(OptionalPartsDidNotJoin),
        ):
            stream._reread_without_refused_columns(
                fb_error(100, message="adset_end"), PARTS, [built([])] * 3, CORE + ["adset_end"], "w"
            )


class TestAReportThatCouldNotBeCreatedIsAFailedDate:
    def queue(self, stream: AdsInsightStream, answer) -> list[dict]:
        with (
            mock.patch.object(stream, "_request_report_creation", side_effect=[answer]),
            mock.patch.object(stream, "_check_facebook_api_usage"),
            mock.patch(USER),
        ):
            return stream._queue_report_parts(START, UNTIL, "w", CORE, 1)

    def test_an_answer_other_than_200(self):
        stream = make_stream()
        bad_gateway = mock.Mock()
        bad_gateway.status.return_value = 502
        bad_gateway._headers = {}
        assert self.queue(stream, bad_gateway) == []
        assert stream._dates_failed == 1

    def test_a_graph_error_that_is_not_a_quota_or_a_refused_column(self):
        stream = make_stream()
        assert self.queue(stream, fb_error(1, http_status=500, message="An unknown error has occurred.")) == []
        assert stream._dates_failed == 1

    def test_a_spent_quota_is_still_left_to_the_caller(self):
        """The caller counts it once, as before (see get_records)."""
        stream = make_stream()
        assert self.queue(stream, fb_error(613)) == []
        assert stream._dates_failed == 0
        assert stream._throttled is True

    def test_the_window_becomes_a_period_asked_for_again(self):
        stream = make_stream()
        stream._tracking_missing = True
        stream._window_from = None
        stream._step_window(START)
        self.queue(stream, fb_error(1, http_status=500))
        stream._step_window(UNTIL.add(days=1))
        assert stream._missing_found == [(START, UNTIL)]


class TestMonthlyStartsInsideFacebooksRetention:
    MONTHLY = {**SAMPLE_CONFIG, "performance_granularity": "monthly"}

    def test_a_start_clamped_mid_month_moves_to_the_next_whole_month(self):
        stream = make_stream(self.MONTHLY)
        oldest = TODAY.subtract(months=37)
        with mock.patch(USER) as user:
            start = stream._first_whole_month(oldest)
        if oldest.day == 1:
            assert start == oldest
            user.info.assert_not_called()
        else:
            assert start == oldest.start_of("month").add(months=1)
            assert "Facebook keeps 37 months" in user.info.call_args.args[0]

    def test_a_start_inside_the_retention_is_the_first_of_its_month(self):
        stream = make_stream(self.MONTHLY)
        with mock.patch(USER) as user:
            start = stream._first_whole_month(pendulum.date(2025, 6, 17))
        assert start == pendulum.date(2025, 6, 1)
        user.info.assert_not_called()

    def test_a_full_sync_from_long_ago_never_asks_beyond_37_months(self):
        """facebook-ads-9huQ: #3018 on every report of a monthly full sync."""
        stream = make_stream({**self.MONTHLY, "start_date": "2020-01-01T00:00:00Z"})
        starts = []

        def created(*, start_date, end_date, **kwargs):
            starts.append(start_date)
            return []

        with (
            mock.patch.object(stream, "_initialize_client"),
            mock.patch.object(stream, "_create_report_batch", side_effect=created),
            mock.patch.object(stream, "_advance_batch", side_effect=lambda current, inc, n, end: end.add(days=1)),
            mock.patch.object(stream, "_fail_if_nothing_extracted"),
            mock.patch(USER),
        ):
            list(stream.get_records(None))
        assert starts[0] >= TODAY.subtract(months=37)
        assert starts[0].day == 1
