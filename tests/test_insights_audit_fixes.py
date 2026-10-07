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
    LAST_SERVED_STATE_KEY,
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


def bookmark_at(date: pendulum.Date, *, upgraded: bool = True, **extra) -> dict:
    """A stream's state handed a bookmark; `upgraded=False` is a state last written by v1.85."""
    state = {"replication_key": "date_start", "replication_key_value": date.to_date_string(), **extra}
    if upgraded:
        state[LAST_SERVED_STATE_KEY] = "2026-10-01T00:00:00Z"
    return state


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
        assert created == [(TODAY.subtract(days=8), TODAY)]

    def test_a_narrow_width_climbs_back_on_an_up_to_date_stream(self):
        """Only the new dates counted for the climb, so 1-2 new days never widened a 3-day width again."""
        marker = {"slices": 3, "since": TODAY.subtract(days=10).to_date_string()}
        stream = make(bookmark_at(TODAY.subtract(days=1), **{SPAN_WIDTH_STATE_KEY: marker}))
        _, state = run(stream)
        assert state[SPAN_WIDTH_STATE_KEY]["slices"] > 3
        # The next run (bookmark now today) covers new dates and lookback in one report.
        created, state = run(make(state))
        assert created == [(TODAY.subtract(days=7), TODAY)]


class TestAStopDoesNotHideAGap:
    def test_a_date_that_failed_before_the_budget_ran_out_is_kept(self):
        """Per-slice batches: a date fails, later ones are written, then #613 ends the stream."""
        stream = make(bookmark_at(BEHIND))
        stream._reset_run_state()
        stream._tracking_missing = True
        stream._window_from = BEHIND
        stream._dates_failed = 1  # a day of the window, before any later day was written
        stream._last_emitted = BEHIND.add(days=10).to_date_string()
        stream._dates_failed += 1  # the stop itself
        stream._close_window_at_stop()
        # Up to the day before the last one written: that one is in the table.
        assert stream._missing_found == [(BEHIND, BEHIND.add(days=9), [None])]

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
        stream._last_emitted = BEHIND.add(days=10).to_date_string()
        stream._dates_failed = 1
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
        up_to_date = TODAY.subtract(days=1)
        state = bookmark_at(up_to_date, **{MISSING_PERIODS_STATE_KEY: [self.PERIOD], SPAN_WIDTH_STATE_KEY: narrow})
        created, state = run(make(state), budget=3)
        assert created == [
            (TODAY.subtract(days=8), TODAY),  # the new dates and the lookback, in one report
            (pendulum.date(2025, 4, 1), pendulum.date(2025, 4, 15)),
            (pendulum.date(2025, 4, 16), pendulum.date(2025, 4, 30)),
        ]
        assert state[MISSING_PERIODS_STATE_KEY] == [
            {"from": "2025-05-01", "until": "2025-05-01", "attempts": 0, "tried_on": TODAY.to_date_string()}
        ]

    def test_a_period_that_failed_today_waits_for_tomorrow(self):
        failed_today = {**self.PERIOD, "attempts": 1, "tried_on": TODAY.to_date_string()}
        created, state = run(make(bookmark_at(BEHIND, **{MISSING_PERIODS_STATE_KEY: [failed_today]})))
        assert all(start >= BEHIND for start, _ in created)
        assert state[MISSING_PERIODS_STATE_KEY] == [failed_today]

    def test_attempts_are_counted_once_a_day(self):
        april = lambda start: start.year == 2025 and start.month == 4  # noqa: E731
        yesterday = {**self.PERIOD, "attempts": 1, "tried_on": TODAY.subtract(days=1).to_date_string()}
        _, state = run(make(bookmark_at(BEHIND, **{MISSING_PERIODS_STATE_KEY: [yesterday]})), fails=april)
        assert state[MISSING_PERIODS_STATE_KEY][0]["attempts"] == 2
        assert state[MISSING_PERIODS_STATE_KEY][0]["tried_on"] == TODAY.to_date_string()

    def test_facebook_not_building_while_on_a_period_postpones_it_without_an_attempt(self):
        """An account-wide outage clears in a few days; it must not give a period up."""
        stream = make(bookmark_at(BEHIND))
        stream._reset_run_state()
        period = {"from": pendulum.date(2025, 4, 1), "until": pendulum.date(2025, 4, 10), "attempts": 0}
        stream._missing_periods = [period]
        AdsInsightStream._account_not_building = True
        with mock.patch(USER):
            stream._settle_missing_period(period, failed=False, stopped=True)
        assert period["attempts"] == 0
        assert period["tried_on"] == TODAY
        assert not stream._due_today(period)

    def test_the_budget_spent_on_a_period_does_not_name_it_as_where_the_next_run_continues(self):
        state = bookmark_at(BEHIND, **{MISSING_PERIODS_STATE_KEY: [self.PERIOD]})
        run(make(state), budget=0)
        note = AdsInsightStream._not_completed["adsinsights"]["dates"]
        assert "2025-04-01" not in note
        assert f"continues from {BEHIND}" in note

    def test_a_period_failing_again_is_named_as_asked_for_again_not_as_new_dates(self):
        april = lambda start: start.year == 2025 and start.month == 4  # noqa: E731
        last = {**self.PERIOD, "attempts": MISSING_PERIOD_ATTEMPTS - 1}
        _, state = run(make(bookmark_at(BEHIND, **{MISSING_PERIODS_STATE_KEY: [last]})), fails=april)
        assert state[MISSING_PERIODS_STATE_KEY][0]["attempts"] == MISSING_PERIOD_ATTEMPTS
        notes = AdsInsightStream._not_completed["adsinsights"]
        assert "dates" not in notes, "the new dates were all extracted"
        assert "given_up" not in notes
        assert "2025-04-01 to 2025-05-01" in notes["missing"]

    def test_too_many_periods_are_told_once(self):
        stream = make(bookmark_at(BEHIND))
        stream._reset_run_state()
        stream._tracking_missing = True
        stream._last_emitted = BEHIND.to_date_string()
        stream._missing_found = [
            (BEHIND.subtract(days=2 * i + 2), BEHIND.subtract(days=2 * i + 1), [None])
            for i in range(MISSING_PERIODS_KEPT + 2)
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
        # The failed window began with the lookback; days before the bookmark the run
        # was handed were loaded before, so the period starts there, on a slice start.
        assert first[0] < pendulum.date(2025, 1, 1)
        assert state[MISSING_PERIODS_STATE_KEY][0]["from"] == "2025-01-01"


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


class TestSecondReview:
    """Second pre-release review (2026-10-08): what the first round of fixes left or caused."""

    def test_a_failed_reread_of_loaded_days_is_not_a_missing_period(self):
        """Daily, up to date: one report covers lookback and new dates; a 5xx on it is not a gap."""
        bookmark = TODAY.subtract(days=1)
        stream = make(bookmark_at(bookmark))
        created, state = run(stream, fails=lambda start: start < bookmark)
        assert MISSING_PERIODS_STATE_KEY not in state
        assert state["replication_key_value"] == bookmark.to_date_string()

    def test_the_budget_on_an_up_to_date_stream_names_the_bookmark_not_the_lookback(self):
        bookmark = TODAY.subtract(days=1)
        run(make(bookmark_at(bookmark)), budget=0)
        note = AdsInsightStream._not_completed["adsinsights"]["dates"]
        assert TODAY.subtract(days=7).to_date_string() not in note

    def test_a_widening_too_large_on_a_one_report_plan_still_reads_the_new_dates_first(self):
        marker = {"slices": 7, "since": TODAY.subtract(days=10).to_date_string()}
        stream = make(bookmark_at(TODAY.subtract(days=1), **{SPAN_WIDTH_STATE_KEY: marker}))
        created = []
        emitted = []

        def queue(current_date, span_until, label, columns, ti):
            if len(created) >= 2:  # the failed wider report, and one more
                stream._throttled = True
                stream._last_throttle_code = 613
                return []
            created.append((current_date, span_until))
            return [{"name": "all", "columns": columns, "report_run_id": str(len(created))}]

        def process(batch, columns, ti):
            for report in batch:
                start, until = report["date_obj"], report["until_obj"]
                if stream._slices_between(start, until, 1) > 7:
                    stream._shrink_span_after_too_large(start, until, report["date"], 1)
                    return
                record = {"id": report["report_run_id"], "date_start": until.to_date_string()}
                emitted.append(record)
                yield record

        with (
            mock.patch.object(stream, "_initialize_client"),
            mock.patch.object(stream, "_queue_report_parts", side_effect=queue),
            mock.patch.object(stream, "_process_report_batch", side_effect=process),
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
            mock.patch(USER),
        ):
            list(stream.get_records(None))
        assert created[1][0] == TODAY.subtract(days=1), "after the refused wider report, the new dates come first"
        assert TODAY.to_date_string() in {record["date_start"] for record in emitted}

    def test_a_period_with_its_own_failure_and_then_the_budget_waits_for_tomorrow(self):
        """Before: no attempt, no shrink -- asked for first on every run, never ending, new dates never reached."""
        narrow = {"slices": 15, "since": TODAY.to_date_string(), "wider_failed_on": TODAY.to_date_string()}
        period = {"from": "2025-04-01", "until": "2025-05-01", "attempts": 0}
        up_to_date = TODAY.subtract(days=1)
        state = bookmark_at(up_to_date, **{MISSING_PERIODS_STATE_KEY: [period], SPAN_WIDTH_STATE_KEY: narrow})
        _, state = run(make(state), budget=2, fails=lambda start: start == pendulum.date(2025, 4, 16))
        kept = state[MISSING_PERIODS_STATE_KEY][0]
        assert kept["attempts"] == 1 and kept["tried_on"] == TODAY.to_date_string()
        created, _ = run(make(state), budget=5)
        assert all(start >= TODAY.subtract(days=8) for start, _ in created), "not asked for again today"

    def test_a_failure_after_the_last_written_day_is_not_a_gap(self):
        stream = make(bookmark_at(BEHIND))
        stream._reset_run_state()
        stream._tracking_missing = True
        stream._window_from = BEHIND
        stream._last_emitted = BEHIND.add(days=3).to_date_string()
        stream._dates_failed += 1  # a day after the last one written
        stream._dates_failed += 1  # the stop
        stream._close_window_at_stop()
        stream._bookmark_handed_in = BEHIND
        state = stream.get_context_state(None)
        state["replication_key_value"] = BEHIND.add(days=3).to_date_string()
        stream._save_missing_periods(state, BEHIND.add(days=3))
        assert MISSING_PERIODS_STATE_KEY not in state

    def test_a_monthly_gap_ends_on_a_month_end(self):
        """A period cut mid-month would write a short row over a whole month (same row id)."""
        monthly = {**CONFIG, "performance_granularity": "monthly"}
        stream = make(bookmark_at(pendulum.date(2025, 3, 1)), config=monthly)
        stream._reset_run_state()
        stream._tracking_missing = True
        stream._bookmark_handed_in = pendulum.date(2025, 3, 1)
        stream._window_from = pendulum.date(2025, 3, 1)
        stream._last_emitted = "2025-03-01"
        stream._dates_failed += 1  # April
        stream._last_emitted = "2025-05-01"  # May written
        stream._dates_failed += 1  # June: the budget
        stream._close_window_at_stop()
        state = stream.get_context_state(None)
        stream._save_missing_periods(state, pendulum.date(2025, 5, 1))
        assert state[MISSING_PERIODS_STATE_KEY] == [{"from": "2025-03-01", "until": "2025-04-30", "attempts": 0}]

    def test_a_monthly_period_at_the_37_month_edge_starts_on_a_whole_month(self):
        monthly = {**CONFIG, "performance_granularity": "monthly"}
        oldest = TODAY.subtract(months=37)
        period = {"from": oldest.start_of("month").to_date_string(), "until": oldest.end_of("month").add(months=1).to_date_string()}
        stream = make(bookmark_at(BEHIND, **{MISSING_PERIODS_STATE_KEY: [period]}), config=monthly)
        with mock.patch(USER):
            periods = stream._read_missing_periods(None)
        assert all(p["from"].day == 1 and p["from"] >= oldest for p in periods)

    def test_only_whole_weekly_slices_inside_the_limits_are_kept(self):
        weekly = {**CONFIG, "report_definition": {**CONFIG["report_definition"], "time_increment_days": 7}}
        stream = make(bookmark_at(BEHIND), config=weekly)
        jan = pendulum.date(2025, 1, 1)
        # The slice of the 15th ends on the 21st, past the bookmark finalized on the 20th.
        assert stream._whole_slices(jan, jan.add(days=27), after=None, before=jan.add(days=19)) == (jan, jan.add(days=13))
        # A window ending mid-slice keeps the slices it holds whole.
        assert stream._whole_slices(jan, jan.add(days=16), after=None, before=jan.add(days=60)) == (jan, jan.add(days=13))
        # A slice that straddles the bookmark the run was handed holds days never loaded: kept whole.
        assert stream._whole_slices(jan, jan.add(days=27), after=jan.add(days=3), before=jan.add(days=60)) == (
            jan,
            jan.add(days=27),
        )
        # Slices that end before it were loaded by an earlier run.
        assert stream._whole_slices(jan, jan.add(days=27), after=jan.add(days=8), before=jan.add(days=60)) == (
            jan.add(days=7),
            jan.add(days=27),
        )


class TestFinalReview:
    """Final pre-release review (2026-10-08): simulation of 31k runs against v1.85."""

    def test_the_first_run_after_the_upgrade_keeps_what_v185_would_have_reread(self):
        """v1.85 walked past failed windows and relied on the lookback before the bookmark to read them again."""
        stream = make(bookmark_at(BEHIND, upgraded=False))
        _, state = run(stream, budget=0)
        assert state[MISSING_PERIODS_STATE_KEY] == [
            {"from": BEHIND.subtract(days=7).to_date_string(), "until": BEHIND.subtract(days=1).to_date_string(), "attempts": 0}
        ]

    def test_the_kept_lookback_is_read_after_the_new_dates_and_not_kept_again(self):
        stream = make(bookmark_at(BEHIND, upgraded=False))
        created, state = run(stream)
        assert created[-1] == (BEHIND.subtract(days=7), BEHIND.subtract(days=1))
        assert MISSING_PERIODS_STATE_KEY not in state
        # The first report created marks the stream as on this version (here the
        # creation is mocked, so the mark is set by hand).
        state[LAST_SERVED_STATE_KEY] = "2026-10-08T00:00:00Z"
        created, _ = run(make({**state, "replication_key_value": BEHIND.to_date_string()}))
        assert all(start >= BEHIND for start, _ in created)

    def test_an_up_to_date_stream_upgraded_from_v185_rereads_nothing_more(self):
        created, state = run(make(bookmark_at(TODAY.subtract(days=1), upgraded=False)))
        assert created == [(TODAY.subtract(days=8), TODAY)]
        assert MISSING_PERIODS_STATE_KEY not in state

    def test_a_period_whose_job_fails_and_whose_retry_meets_the_budget_counts_an_attempt(self):
        """Before: no attempt, asked for again every run and never given up."""
        stream = make(bookmark_at(TODAY.subtract(days=1)))
        stream._reset_run_state()
        period = {"from": pendulum.date(2025, 4, 1), "until": pendulum.date(2025, 4, 10), "attempts": 0}
        stream._missing_periods = [period]
        stream._a_job_failed = True
        with mock.patch(USER):
            stream._settle_missing_period(period, failed=True, stopped=True)
        assert period["attempts"] == 1 and period["tried_on"] == TODAY

    def test_a_job_that_failed_and_then_built_does_not_fail_the_period(self):
        april = {"from": "2025-04-01", "until": "2025-04-10", "attempts": 0}
        stream = make(bookmark_at(TODAY.subtract(days=1), **{MISSING_PERIODS_STATE_KEY: [april]}))
        original = stream._settle_missing_period

        def after_a_transient_job_failure(period, *, failed, stopped):
            assert stream._a_job_failed is True
            return original(period, failed=failed, stopped=stopped)

        def queue(current_date, span_until, label, columns, ti):
            if current_date.year == 2025:
                stream._a_job_failed = True  # failed once, then built on the retry
            return [{"name": "all", "columns": columns, "report_run_id": "1"}]

        def process(batch, columns, ti):
            for report in batch:
                yield {"id": "1", "date_start": (report["until_obj"] or report["date_obj"]).to_date_string()}

        with (
            mock.patch.object(stream, "_initialize_client"),
            mock.patch.object(stream, "_queue_report_parts", side_effect=queue),
            mock.patch.object(stream, "_process_report_batch", side_effect=process),
            mock.patch.object(stream, "_settle_missing_period", side_effect=after_a_transient_job_failure),
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
            mock.patch(USER),
        ):
            for record in stream.get_records(None):
                stream._increment_stream_state(record, context=None)
            state = stream.get_context_state(None)
            stream._finalize_state(state)
        assert MISSING_PERIODS_STATE_KEY not in state

    def test_a_weekly_report_per_slice_asks_for_the_whole_week(self):
        """A one-day report wrote a one-day row under the id of the whole week."""
        weekly = {**CONFIG, "report_definition": {**CONFIG["report_definition"], "time_increment_days": 7}}
        stream = make(bookmark_at(BEHIND), config=weekly)
        assert stream._get_time_range(pendulum.date(2025, 9, 1)) == {"since": "2025-09-01", "until": "2025-09-07"}

    def test_a_daily_report_per_slice_is_still_one_day(self):
        stream = make(bookmark_at(BEHIND))
        assert stream._get_time_range(pendulum.date(2025, 9, 1)) == {"since": "2025-09-01", "until": "2025-09-01"}
