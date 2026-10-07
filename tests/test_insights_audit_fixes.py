"""Unit tests for the fixes from the audit of v1.86-v1.88 before release (NEKT-5249, 2026-10-07).

Fully offline. No source ran v1.86 or v1.87, so a source upgrading from
v1.84/v1.85 gets all three at once; the audit compared them with v1.85.
"""

from __future__ import annotations

import copy
from unittest import mock

import pendulum
import pytest
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsinsights import AdsInsights
from nekt_singer_sdk import Tap

from tap_facebook.streams.ad_insights import (
    MISSING_PERIOD_ATTEMPTS,
    MISSING_PERIODS_KEPT,
    MISSING_PERIODS_STATE_KEY,
    SPAN_WIDTH_STATE_KEY,
    AdsInsightStream,
    OptionalPartsDidNotJoin,
)
from tap_facebook.tap import TapFacebook

CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "t",
    "account_id": "123",
    "report_definition": {"lookback_window": 7, "action_report_time": "impression"},
}
USER = "tap_facebook.streams.ad_insights.user_logger"
TODAY = pendulum.today().date()
BEHIND = pendulum.date(2025, 6, 1)


@pytest.fixture(autouse=True)
def _fresh_process_state():
    def reset(started_without_state: bool) -> None:
        AdsInsightStream._incomplete_without_history = []
        AdsInsightStream._columns_refused_on_read = set()
        AdsInsightStream._run_started_without_state = started_without_state
        AdsInsightStream._account_not_building = False
        AdsInsightStream._account_split_mode = False
        AdsInsightStream._not_completed = {}
        AdsInsightStream._account_limit_spent = False

    reset(started_without_state=False)
    yield
    reset(started_without_state=True)


def make(stream_state: dict, config: dict | None = None) -> AdsInsightStream:
    tap = TapFacebook(config=config or CONFIG, state={"bookmarks": {"adsinsights": copy.deepcopy(stream_state)}})
    stream = tap.streams["adsinsights"]
    stream._write_starting_replication_value(None)
    return stream


def bookmark_at(date: pendulum.Date, **extra) -> dict:
    return {"replication_key": "date_start", "replication_key_value": date.to_date_string(), **extra}


def run(stream: AdsInsightStream, budget: int = 99, fails=lambda start: False):
    """get_records with the real batching; the account builds `budget` reports and then answers #613.

    `fails(start)` makes the creation of the window starting there fail (HTTP 5xx).
    Returns the windows created and the finalized state.
    """
    created: list[tuple] = []

    def queue(current_date, span_until, label, columns, ti):
        if fails(current_date):
            stream._dates_failed += 1
            return []
        if len(created) >= budget:
            stream._throttled = True
            stream._last_throttle_code = 613
            return []
        created.append((current_date, span_until))
        return [{"name": "all", "columns": columns, "report_run_id": str(len(created))}]

    def process(batch, columns, ti):
        for report in batch:
            yield {"id": report["report_run_id"], "date_start": (report["until_obj"] or report["date_obj"]).to_date_string()}

    with (
        mock.patch.object(stream, "_initialize_client"),
        mock.patch.object(stream, "_queue_report_parts", side_effect=queue),
        mock.patch.object(stream, "_process_report_batch", side_effect=process),
        mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
        mock.patch(USER),
    ):
        for record in stream.get_records(None):
            stream._increment_stream_state(record, context=None)
        state = stream.get_context_state(None)
        stream._finalize_state(state)
    return created, state


class TestAnUpToDateStreamCostsOneReport:
    def test_new_dates_and_lookback_that_fit_in_one_report_are_one_report(self):
        """v1.85 paid one report; v1.86-v1.88 split it into two on every run."""
        created, _ = run(make(bookmark_at(TODAY.subtract(days=1))))
        assert created == [(TODAY.subtract(days=7), TODAY)]

    def test_a_narrow_width_climbs_back_on_an_up_to_date_stream(self):
        """Only the new dates counted for the climb, so 1-2 new days never widened a 3-day width again."""
        marker = {"slices": 3, "since": TODAY.subtract(days=10).to_date_string()}
        stream = make(bookmark_at(TODAY.subtract(days=1), **{SPAN_WIDTH_STATE_KEY: marker}))
        _, state = run(stream)
        assert state[SPAN_WIDTH_STATE_KEY]["slices"] > 3
        # The next run covers new dates and lookback (8 days) in one report.
        created, state = run(make({k: v for k, v in state.items() if k != "insights_last_served"}))
        assert created == [(TODAY.subtract(days=7), TODAY)]


class TestAStopDoesNotHideAGap:
    def test_a_date_that_failed_before_the_budget_ran_out_is_kept(self):
        """Per-slice batches: a date fails, later ones are written, then #613 ends the stream."""
        stream = make(bookmark_at(BEHIND))
        stream._reset_run_state()
        stream._tracking_missing = True
        stream._window_from = BEHIND
        stream._failed_before_window = 0
        stream._dates_failed = 2  # the failed date, then the stop itself
        stream._last_emitted = BEHIND.add(days=10).to_date_string()
        stream._close_window_at_stop()
        assert stream._missing_found == [(BEHIND, BEHIND.add(days=10))]

    def test_get_records_keeps_the_gap_when_the_budget_ends_a_per_slice_batch(self):
        stream = make(bookmark_at(BEHIND))
        calls = {"n": 0}

        def queue(current_date, span_until, label, columns, ti):
            return [{"name": "all", "columns": columns, "report_run_id": current_date.to_date_string()}]

        def process(batch, columns, ti):
            calls["n"] += 1
            if calls["n"] == 1:  # the span report gives way to one report per day
                with mock.patch(USER):
                    stream._give_up_on_span(batch[0]["date_obj"], batch[0]["date"])
                return
            for i, report in enumerate(batch):
                if i == 5:
                    stream._dates_failed += 1  # this day given up after its retries; the batch goes on
                    continue
                if i == 11:
                    stream._throttled = True
                    stream._last_throttle_code = 613
                    stream._throttled_from = report["date_obj"]
                    return
                yield {"id": str(i), "date_start": report["date_obj"].to_date_string()}

        with (
            mock.patch.object(stream, "_initialize_client"),
            mock.patch.object(stream, "_queue_report_parts", side_effect=queue),
            mock.patch.object(stream, "_process_report_batch", side_effect=process),
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
            mock.patch(USER),
        ):
            for record in stream.get_records(None):
                stream._increment_stream_state(record, context=None)
            state = stream.get_context_state(None)
            stream._finalize_state(state)
        assert state["replication_key_value"] == BEHIND.add(days=10).to_date_string()
        assert state[MISSING_PERIODS_STATE_KEY] == [
            {"from": BEHIND.to_date_string(), "until": BEHIND.add(days=9).to_date_string(), "attempts": 0}
        ]

    def test_the_stop_alone_is_not_a_gap(self):
        stream = make(bookmark_at(BEHIND))
        stream._reset_run_state()
        stream._tracking_missing = True
        stream._window_from = BEHIND
        stream._dates_failed = 1
        stream._last_emitted = BEHIND.add(days=10).to_date_string()
        stream._close_window_at_stop()
        assert stream._missing_found == []


class TestOnePartThatJoinsNothingIsEnough:
    @staticmethod
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

    def test_the_period_is_not_written_when_one_optional_part_joins_none_of_its_rows(self):
        def row(ad_id: str, **metrics) -> dict:
            keys = {"date_start": "2026-09-29", "date_stop": "2026-09-29", "campaign_id": "c", "adset_id": "s"}
            return {**keys, "ad_id": ad_id, **metrics}

        stream = make(bookmark_at(BEHIND))
        stream._reset_run_state()
        parts = [
            {"name": "core", "columns": [], "report_run_id": "c-1"},
            {"name": "standard", "columns": [], "report_run_id": "s-1"},
            {"name": "results", "columns": [], "report_run_id": "r-1"},
        ]
        jobs = [
            self.built([row("a", spend="1")]),
            self.built([row("a", reach="10")]),
            self.built([row(f"other-{i}", results="5") for i in range(3)]),
        ]
        with mock.patch("tap_facebook.streams.ad_insights.internal_logger"), pytest.raises(
            OptionalPartsDidNotJoin
        ) as raised:
            stream._merge_part_results(parts, jobs, "w")
        assert raised.value.part_rows == {"results": 3}


class TestMissingPeriods:
    PERIOD = {"from": "2025-04-01", "until": "2025-05-01", "attempts": 0}

    def test_a_retry_cut_by_the_budget_resumes_where_it_stopped(self):
        """A period needing more reports than a run gets used to start over every run and never end."""
        narrow = {"slices": 15, "since": TODAY.to_date_string(), "wider_failed_on": TODAY.to_date_string()}
        state = bookmark_at(BEHIND, **{MISSING_PERIODS_STATE_KEY: [self.PERIOD], SPAN_WIDTH_STATE_KEY: narrow})
        created, state = run(make(state), budget=2)
        assert created == [
            (pendulum.date(2025, 4, 1), pendulum.date(2025, 4, 15)),
            (pendulum.date(2025, 4, 16), pendulum.date(2025, 4, 30)),
        ]
        assert state[MISSING_PERIODS_STATE_KEY] == [{"from": "2025-05-01", "until": "2025-05-01", "attempts": 0}]

    def test_a_period_that_failed_today_waits_for_tomorrow(self):
        failed_today = {**self.PERIOD, "attempts": 1, "failed_on": TODAY.to_date_string()}
        created, state = run(make(bookmark_at(BEHIND, **{MISSING_PERIODS_STATE_KEY: [failed_today]})))
        assert all(start >= BEHIND for start, _ in created)
        assert state[MISSING_PERIODS_STATE_KEY] == [failed_today]

    def test_attempts_are_counted_once_a_day(self):
        april = lambda start: start.year == 2025 and start.month == 4  # noqa: E731
        yesterday = {**self.PERIOD, "attempts": 1, "failed_on": TODAY.subtract(days=1).to_date_string()}
        _, state = run(make(bookmark_at(BEHIND, **{MISSING_PERIODS_STATE_KEY: [yesterday]})), fails=april)
        assert state[MISSING_PERIODS_STATE_KEY][0]["attempts"] == 2
        assert state[MISSING_PERIODS_STATE_KEY][0]["failed_on"] == TODAY.to_date_string()

    def test_facebook_not_building_while_on_a_period_is_an_attempt(self):
        stream = make(bookmark_at(BEHIND))
        stream._reset_run_state()
        period = {"from": pendulum.date(2025, 4, 1), "until": pendulum.date(2025, 4, 10), "attempts": 0}
        stream._missing_periods = [period]
        AdsInsightStream._account_not_building = True
        with mock.patch(USER):
            stream._settle_missing_period(period, failed=False, stopped=True)
        assert period["attempts"] == 1

    def test_the_budget_spent_on_a_period_does_not_name_it_as_where_the_next_run_continues(self):
        state = bookmark_at(BEHIND, **{MISSING_PERIODS_STATE_KEY: [self.PERIOD]})
        run(make(state), budget=0)
        note = AdsInsightStream._not_completed["adsinsights"]["dates"]
        assert "2025-04-01" not in note
        assert "continues from where it stopped" in note

    def test_a_period_given_up_is_named_and_not_promised(self):
        april = lambda start: start.year == 2025 and start.month == 4  # noqa: E731
        last = {**self.PERIOD, "attempts": MISSING_PERIOD_ATTEMPTS - 1}
        _, state = run(make(bookmark_at(BEHIND, **{MISSING_PERIODS_STATE_KEY: [last]})), fails=april)
        assert MISSING_PERIODS_STATE_KEY not in state
        notes = AdsInsightStream._not_completed["adsinsights"]
        assert "dates" not in notes, "the new dates were all extracted"
        assert notes["given_up"] == "no longer asked for, Facebook did not build them: 2025-04-01 to 2025-05-01."

    def test_too_many_periods_are_told_once(self):
        stream = make(bookmark_at(BEHIND))
        stream._reset_run_state()
        stream._tracking_missing = True
        stream._missing_found = [
            (BEHIND.subtract(days=2 * i + 2), BEHIND.subtract(days=2 * i + 1)) for i in range(MISSING_PERIODS_KEPT + 2)
        ]
        state = stream.get_context_state(None)
        with mock.patch(USER) as user:
            for _ in range(3):  # the SDK finalizes a stream more than once
                stream._finalize_state(state)
        assert user.warning.call_count == 1
        assert len(state[MISSING_PERIODS_STATE_KEY]) == MISSING_PERIODS_KEPT
        assert "no longer asked for" in AdsInsightStream._not_completed["adsinsights"]["given_up"]

    def test_a_weekly_window_that_failed_is_kept_too(self):
        """Until the audit only daily slices were tracked; the bookmark walked ~7 months past the gap."""
        weekly = {**CONFIG, "report_definition": {**CONFIG["report_definition"], "time_increment_days": 7}}
        stream = make(bookmark_at(pendulum.date(2025, 1, 1)), config=weekly)
        first = []
        _, state = run(stream, fails=lambda start: not first and not first.append(start))
        assert state[MISSING_PERIODS_STATE_KEY][0]["from"] == first[0].to_date_string()


class TestTheWidthIsOnlyConfirmedByAWrittenWindow:
    def test_a_wider_window_that_was_given_up_does_not_raise_the_width(self):
        """A later window written at that width does confirm it (see the v1.87 tests)."""
        marker = {"slices": 3, "since": TODAY.subtract(days=1).to_date_string()}
        stream = make(bookmark_at(BEHIND, **{SPAN_WIDTH_STATE_KEY: marker}))
        windows = []

        def queue(current_date, span_until, label, columns, ti):
            windows.append((current_date, span_until))
            return [{"name": "all", "columns": columns, "report_run_id": str(len(windows))}]

        def process(batch, columns, ti):
            stream._dates_failed += 1  # every window given up (split parts / parts not joining)
            yield from ()

        with (
            mock.patch.object(stream, "_initialize_client"),
            mock.patch.object(stream, "_queue_report_parts", side_effect=queue),
            mock.patch.object(stream, "_process_report_batch", side_effect=process),
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
            mock.patch(USER),
        ):
            list(stream.get_records(None))
        assert windows[0][1] == windows[0][0].add(days=6), "the run tried one step wider"
        assert stream.get_context_state(None)[SPAN_WIDTH_STATE_KEY]["slices"] != 7


class TestTheSummaryIsToldEvenWhenTheRunStops:
    def test_a_stream_that_exits_does_not_swallow_what_the_others_left(self):
        def streams_ran(*args, **kwargs):
            AdsInsightStream._not_completed = {"adsinsights": {"dates": "not updated in this run z."}}
            raise SystemExit(1)

        with (
            mock.patch.object(Tap, "sync_all", side_effect=streams_ran),
            mock.patch("tap_facebook.tap.user_logger") as user,
            pytest.raises(SystemExit),
        ):
            TapFacebook(config=CONFIG, state={}).sync_all()
        assert user.warning.call_args.args[0].startswith("**Not completed in this run**")
