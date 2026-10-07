"""Unit tests for v1.86: an insights stream short of reports still moves forward.

Fully offline.

Background (NEKT-5249, review of 2026-09-21 to 10-05, before upgrading the
sources still on v1.33-1.42).

* adsinsights on facebook-ads-WhFu (seven insights streams sharing the ad
  account's "5 calls per 6 hours") got 0-1 report per run. With 4-day windows
  and a 7-day lookback that one report only re-read old days, and the SDK
  finalized the latest date of the run as the bookmark: 30/09 -> 26/09 ->
  22/09 -> 18/09 -> 14/09 -> 10/09 in five green runs, no new date loaded.
* facebook-ads-egWh (LLMidia) loading 2024 data advanced 1-7 days a run: each
  run re-read the 7 days before a bookmark two years old.
* The customer saw a transient Facebook job failure as an error on a run that
  ended green, and had no word of why the windows stayed at 3 days.
* With the lookback limited to recent days, a period Facebook failed to build
  while a stream was catching up is no longer re-read by the next run's
  lookback; it is kept in the state and asked for first instead.
"""

from __future__ import annotations

from unittest import mock

import pendulum
import pytest

from tap_facebook.streams.ad_insights import (
    LAST_SERVED_STATE_KEY,
    MISSING_PERIOD_ATTEMPTS,
    MISSING_PERIODS_STATE_KEY,
    SPAN_MAX_SLICES,
    SPAN_WIDEN_RETRY_DAYS,
    SPAN_WIDTH_STATE_KEY,
    AdsInsightStream,
)
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
    "report_definition": {"lookback_window": 7, "action_report_time": "impression"},
}
USER = "tap_facebook.streams.ad_insights.user_logger"
TODAY = pendulum.today().date()


@pytest.fixture(autouse=True)
def _fresh_process_state():
    AdsInsightStream._incomplete_without_history = []
    AdsInsightStream._columns_refused_on_read = set()
    AdsInsightStream._run_started_without_state = False
    AdsInsightStream._account_not_building = False
    yield
    AdsInsightStream._incomplete_without_history = []
    AdsInsightStream._columns_refused_on_read = set()
    AdsInsightStream._run_started_without_state = True
    AdsInsightStream._account_not_building = False


def stream_with_bookmark(
    bookmark: pendulum.Date | None, name: str = "adsinsights", config: dict | None = None
) -> AdsInsightStream:
    """A stream as the SDK leaves it at the start of a run handed `bookmark`."""
    state = (
        {"bookmarks": {name: {"replication_key": "date_start", "replication_key_value": bookmark.to_date_string()}}}
        if bookmark is not None
        else {}
    )
    tap = TapFacebook(config=config or SAMPLE_CONFIG, state=state)
    stream = tap.streams[name]
    stream._reset_run_state()
    stream._sync_context = None
    stream._write_starting_replication_value(None)
    return stream


class TestTheLookbackOnlyReReadsRecentDays:
    def test_a_bookmark_far_behind_resumes_from_it(self):
        stream = stream_with_bookmark(pendulum.date(2024, 2, 10))
        with mock.patch(USER) as user:
            start = stream._get_start_date(None)
        assert start == pendulum.date(2024, 2, 10)
        said = user.info.call_args.args[0]
        assert "starting on '2024-02-10'" in said
        assert "not re-read" in said

    def test_a_recent_bookmark_rereads_the_last_days(self):
        stream = stream_with_bookmark(TODAY.subtract(days=1))
        with mock.patch(USER):
            start = stream._get_start_date(None)
        assert start == TODAY.subtract(days=7)

    def test_a_bookmark_just_outside_the_window_is_not_reread(self):
        bookmark = TODAY.subtract(days=10)
        stream = stream_with_bookmark(bookmark)
        with mock.patch(USER):
            start = stream._get_start_date(None)
        assert start == bookmark

    def test_the_message_keeps_its_shape(self):
        """Monitoring reads the bookmark out of this exact sentence."""
        stream = stream_with_bookmark(TODAY.subtract(days=2))
        with mock.patch(USER) as user:
            stream._get_start_date(None)
        said = user.info.call_args.args[0]
        assert said.startswith(
            f"[adsinsights] Incremental sync, applying lookback '7' to the bookmark start date "
            f"'{TODAY.subtract(days=2)}'. Syncing reports starting on '{TODAY.subtract(days=7)}'."
        )

    def test_monthly_slices_keep_the_full_lookback(self):
        """Moving the start would change which days a monthly row adds up."""
        bookmark = pendulum.date(2024, 2, 10)
        stream = stream_with_bookmark(bookmark, config={**SAMPLE_CONFIG, "performance_granularity": "monthly"})
        with mock.patch(USER):
            start = stream._get_start_date(None)
        assert start == bookmark.subtract(days=7)

    def test_multi_day_slices_keep_the_full_lookback(self):
        config = {**SAMPLE_CONFIG, "report_definition": {**SAMPLE_CONFIG["report_definition"], "time_increment_days": 7}}
        bookmark = pendulum.date(2024, 2, 10)
        stream = stream_with_bookmark(bookmark, config=config)
        with mock.patch(USER):
            start = stream._get_start_date(None)
        assert start == bookmark.subtract(days=7)

    def test_the_37_month_warning_names_the_date_that_was_too_old(self):
        bookmark = TODAY.subtract(months=40)
        stream = stream_with_bookmark(bookmark)
        with mock.patch(USER) as user:
            start = stream._get_start_date(None)
        assert start == TODAY.subtract(months=37)
        said = user.warning.call_args.args[0]
        assert f"'{bookmark}'" in said
        assert f"'{TODAY.subtract(months=37)}'" in said


def narrowed(stream: AdsInsightStream, slices: int = 3) -> AdsInsightStream:
    """A width Facebook refused to go beyond today, so the run cannot cover its period in one report."""
    stream.get_context_state(None)[SPAN_WIDTH_STATE_KEY] = {
        "slices": slices,
        "since": TODAY.to_date_string(),
        "wider_failed_on": TODAY.to_date_string(),
    }
    return stream


class TestNewDatesComeBeforeTheReRead:
    def test_a_recent_bookmark_splits_a_period_wider_than_one_report(self):
        stream = stream_with_bookmark(TODAY.subtract(days=2))
        stream._bookmark_handed_in = TODAY.subtract(days=2)
        stream._span_slices = 3
        ranges = stream._extraction_ranges(TODAY.subtract(days=7), TODAY)
        assert ranges == [
            (TODAY.subtract(days=2), TODAY, False),
            (TODAY.subtract(days=7), TODAY.subtract(days=3), True),
        ]

    def test_a_period_that_fits_in_one_report_is_one_report(self):
        """An up-to-date stream: v1.86-v1.88 split it and paid two reports a run for what one covers."""
        stream = stream_with_bookmark(TODAY.subtract(days=2))
        stream._bookmark_handed_in = TODAY.subtract(days=2)
        assert stream._extraction_ranges(TODAY.subtract(days=7), TODAY) == [(TODAY.subtract(days=7), TODAY, False)]

    def test_a_bookmark_far_behind_is_one_period(self):
        bookmark = pendulum.date(2024, 2, 10)
        stream = stream_with_bookmark(bookmark)
        stream._bookmark_handed_in = bookmark
        assert stream._extraction_ranges(bookmark, TODAY) == [(bookmark, TODAY, False)]

    def test_no_bookmark_is_one_period(self):
        stream = stream_with_bookmark(None)
        start = pendulum.date(2024, 1, 1)
        assert stream._extraction_ranges(start, TODAY) == [(start, TODAY, False)]

    def run_get_records(self, stream, created):
        with (
            mock.patch.object(stream, "_initialize_client"),
            mock.patch.object(stream, "_create_report_batch", side_effect=created) as create,
            mock.patch.object(stream, "_process_report_batch", side_effect=lambda *a, **k: iter([{"id": "1"}])),
            mock.patch.object(stream, "_advance_batch", side_effect=lambda current, inc, n, end: end.add(days=1)),
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
            mock.patch(USER) as user,
        ):
            records = list(stream.get_records(None))
        return create, user, records

    def test_get_records_asks_for_the_new_dates_first(self):
        bookmark = TODAY.subtract(days=2)
        stream = narrowed(stream_with_bookmark(bookmark))

        def created(*, start_date, end_date, **kwargs):
            return [{"next_date": end_date.add(days=1)}]

        create, _, _ = self.run_get_records(stream, created)
        starts = [call.kwargs["start_date"] for call in create.call_args_list]
        assert starts == [bookmark, TODAY.subtract(days=7)]
        assert create.call_args_list[1].kwargs["end_date"] == bookmark.subtract(days=1)

    def test_the_budget_running_out_on_new_dates_skips_the_reread(self):
        bookmark = TODAY.subtract(days=2)
        stream = narrowed(stream_with_bookmark(bookmark))

        def refused(**kwargs):
            stream._throttled = True
            stream._last_throttle_code = 613
            return []

        create, user, _ = self.run_get_records(stream, refused)
        assert create.call_count == 1
        assert stream._budget_spent_at == bookmark
        assert stream._reread_stopped_at is None
        said = user.warning.call_args.args[0]
        assert "not updated in this run" in said
        assert f"ran out at {bookmark}" in said

    def test_the_budget_running_out_on_the_reread_is_not_a_missing_date(self):
        bookmark = TODAY.subtract(days=2)
        stream = narrowed(stream_with_bookmark(bookmark))
        calls = []

        def created(*, start_date, end_date, **kwargs):
            calls.append(start_date)
            if len(calls) == 1:
                return [{"next_date": end_date.add(days=1)}]
            stream._throttled = True
            stream._last_throttle_code = 613
            return []

        _, user, records = self.run_get_records(stream, created)
        assert records == [{"id": "1"}]
        assert stream._dates_failed == 0
        assert stream._reread_failed == 1
        assert stream._reread_stopped_at == TODAY.subtract(days=7)
        warnings = [call.args[0] for call in user.warning.call_args_list]
        assert not any("partially updated" in text or "not updated" in text for text in warnings)
        infos = [call.args[0] for call in user.info.call_args_list]
        assert any("re-read of recent days" in text for text in infos)


class TestTheBookmarkNeverMovesBack:
    def finalize_after(self, stream: AdsInsightStream, *dates: str) -> str:
        for date in dates:
            stream._increment_stream_state({"date_start": date}, context=None)
        state = stream.get_context_state(None)
        stream._finalize_state(state)
        return state["replication_key_value"]

    def test_a_run_that_stopped_before_the_bookmark_keeps_it(self):
        """The WhFu spiral: 30/09 -> 26/09 -> 22/09 ... in green runs."""
        stream = stream_with_bookmark(pendulum.date(2026, 9, 30))
        stream._bookmark_handed_in = pendulum.date(2026, 9, 30)
        assert self.finalize_after(stream, "2026-09-23", "2026-09-26") == "2026-09-30"

    def test_a_run_that_got_past_it_moves_it_forward(self):
        stream = stream_with_bookmark(pendulum.date(2026, 9, 30))
        stream._bookmark_handed_in = pendulum.date(2026, 9, 30)
        assert self.finalize_after(stream, "2026-09-29", "2026-10-03") == "2026-10-03"

    def test_without_a_bookmark_the_run_sets_it(self):
        stream = stream_with_bookmark(None)
        stream._bookmark_handed_in = None
        assert self.finalize_after(stream, "2024-01-05") == "2024-01-05"

    def test_get_records_takes_the_floor_from_the_state(self):
        stream = stream_with_bookmark(pendulum.date(2026, 9, 30))
        with (
            mock.patch.object(stream, "_initialize_client"),
            mock.patch.object(stream, "_create_report_batch", return_value=[]),
            mock.patch.object(stream, "_advance_batch", side_effect=lambda current, inc, n, end: end.add(days=1)),
            mock.patch.object(stream, "_fail_if_nothing_extracted"),
            mock.patch(USER),
        ):
            list(stream.get_records(None))
        assert stream._bookmark_handed_in == pendulum.date(2026, 9, 30)

    def test_a_full_table_stream_has_no_floor(self):
        """Its table is replaced every run; an old bookmark in the state means nothing."""
        stream = stream_with_bookmark(pendulum.date(2026, 9, 30))
        stream.forced_replication_method = "FULL_TABLE"
        with (
            mock.patch.object(stream, "_initialize_client"),
            mock.patch.object(stream, "_create_report_batch", return_value=[]),
            mock.patch.object(stream, "_advance_batch", side_effect=lambda current, inc, n, end: end.add(days=1)),
            mock.patch.object(stream, "_fail_if_nothing_extracted"),
            mock.patch(USER),
        ):
            list(stream.get_records(None))
        assert stream._bookmark_handed_in is None


class TestTheLongestWaitingStreamGoesFirst:
    CONFIG = {**SAMPLE_CONFIG, "enable_advanced_reports": True}

    def insights_order(self, tap: TapFacebook) -> list[str]:
        return [stream.name for stream in tap.streams.values() if isinstance(stream, AdsInsightStream)]

    def test_never_served_then_oldest_first(self):
        served = {
            "adsinsights": "2026-10-05T12:00:00Z",
            "adsinsights_by_country": "2026-10-01T04:00:00Z",
            "adsinsights_by_region": "2026-10-04T04:00:00Z",
        }
        state = {"bookmarks": {name: {LAST_SERVED_STATE_KEY: when} for name, when in served.items()}}
        tap = TapFacebook(config=self.CONFIG, state=state)
        tap._serve_the_longest_waiting_insights_stream_first()
        order = self.insights_order(tap)
        never = [name for name in order if name not in served]
        assert order[: len(never)] == never
        assert order[len(never) :] == ["adsinsights_by_country", "adsinsights_by_region", "adsinsights"]

    def test_without_a_record_the_rotation_stands(self):
        tap = TapFacebook(config=self.CONFIG)
        before = self.insights_order(tap)
        tap._serve_the_longest_waiting_insights_stream_first()
        assert self.insights_order(tap) == before

    def test_the_other_streams_still_go_before_the_insights(self):
        tap = TapFacebook(config=self.CONFIG, state={"bookmarks": {"adsinsights": {LAST_SERVED_STATE_KEY: "x"}}})
        tap._serve_the_longest_waiting_insights_stream_first()
        kinds = [isinstance(stream, AdsInsightStream) for stream in tap.streams.values()]
        assert kinds == sorted(kinds)

    def test_sync_all_applies_the_order(self):
        state = {"bookmarks": {"adsinsights": {LAST_SERVED_STATE_KEY: "2026-10-05T12:00:00Z"}}}
        tap = TapFacebook(config=self.CONFIG, state=state)
        with mock.patch("nekt_singer_sdk.Tap.sync_all"):
            tap.sync_all()
        assert self.insights_order(tap)[-1] == "adsinsights"

    def test_a_stream_records_when_it_was_served_once_per_run(self):
        stream = stream_with_bookmark(None)
        stream._note_served()
        first = stream.get_context_state(None)[LAST_SERVED_STATE_KEY]
        stream._note_served()
        assert stream.get_context_state(None)[LAST_SERVED_STATE_KEY] == first
        assert pendulum.parse(first).date() == pendulum.now("UTC").date()


class TestTheCustomerIsToldWhatTheTapDecided:
    def test_the_remembered_window_width_is_shown(self):
        since = TODAY.subtract(days=2)
        stream = stream_with_bookmark(TODAY.subtract(days=1))
        stream.get_context_state(None)[SPAN_WIDTH_STATE_KEY] = {"slices": 3, "since": since.to_date_string()}
        with mock.patch(USER) as user:
            stream._restore_span_width(None)
        said = user.info.call_args.args[0]
        assert "reports of 3 day(s) each" in said
        assert f"on {since}" in said
        assert "first tries a report of up to 7 day(s) when it has more than 3 day(s) to extract" in said

    def test_the_wait_after_a_failed_widening_is_shown(self):
        failed_on = TODAY.subtract(days=2)
        stream = stream_with_bookmark(TODAY.subtract(days=1))
        stream.get_context_state(None)[SPAN_WIDTH_STATE_KEY] = {
            "slices": 3,
            "since": TODAY.subtract(days=5).to_date_string(),
            "wider_failed_on": failed_on.to_date_string(),
        }
        with mock.patch(USER) as user:
            stream._restore_span_width(None)
        said = user.info.call_args.args[0]
        assert f"A larger report is tried again from {failed_on.add(days=SPAN_WIDEN_RETRY_DAYS)}" in said

    def test_a_partial_update_names_where_the_budget_ran_out(self):
        stream = stream_with_bookmark(TODAY.subtract(days=10))
        stream._dates_failed = 1
        stream._last_throttle_code = 613
        stream._budget_spent_at = TODAY.subtract(days=4)
        with mock.patch(USER) as user:
            stream._fail_if_nothing_extracted(batches_attempted=2, reports_queued=1, records_emitted=10)
        said = user.warning.call_args.args[0]
        assert "only partially updated" in said
        assert f"ran out at {TODAY.subtract(days=4)}" in said


BEHIND = pendulum.date(2024, 2, 10)
WINDOW = 10


class TestAPeriodLeftBehindIsAskedForAgain:
    """A failed window the run walked past is a gap behind the bookmark."""

    def stream(self, bookmark=BEHIND, missing=None, config=None) -> AdsInsightStream:
        stream = stream_with_bookmark(bookmark, config=config)
        if missing is not None:
            stream.get_context_state(None)[MISSING_PERIODS_STATE_KEY] = missing
        return stream

    def run(self, stream, outcome):
        """Run get_records with 10-day windows; `outcome(start, end)` says how each one goes."""
        calls = []

        def created(*, start_date, end_date, **kwargs):
            calls.append((start_date, end_date))
            result = outcome(start_date, end_date)
            if result == "failed":
                stream._dates_failed += 1
                return []
            if result == "budget":
                stream._throttled = True
                stream._last_throttle_code = 613
                return []
            return [{"next_date": min(start_date.add(days=WINDOW), end_date.add(days=1))}]

        with (
            mock.patch.object(stream, "_initialize_client"),
            mock.patch.object(stream, "_create_report_batch", side_effect=created),
            mock.patch.object(stream, "_process_report_batch", side_effect=lambda *a, **k: iter([{"id": "1"}])),
            mock.patch.object(
                stream,
                "_advance_batch",
                side_effect=lambda current, inc, n, end: min(current.add(days=WINDOW), end.add(days=1)),
            ),
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
            mock.patch(USER) as user,
        ):
            list(stream.get_records(None))
        return calls, user

    def finalize(self, stream, *dates: pendulum.Date) -> dict:
        for date in dates:
            stream._increment_stream_state({"date_start": date.to_date_string()}, context=None)
        state = stream.get_context_state(None)
        with mock.patch(USER):
            stream._finalize_state(state)
        return state

    def test_a_window_that_failed_on_the_way_is_kept(self):
        stream = self.stream()
        self.run(stream, lambda start, end: "failed" if start == BEHIND else "ok")
        assert stream._missing_found[0] == (BEHIND, BEHIND.add(days=WINDOW - 1))
        state = self.finalize(stream, BEHIND.add(days=25))
        assert state[MISSING_PERIODS_STATE_KEY] == [
            {"from": "2024-02-10", "until": "2024-02-19", "attempts": 0},
        ]

    def test_the_window_where_the_budget_ran_out_is_not_a_gap(self):
        """The next run's new dates start right there."""
        stream = self.stream()
        self.run(stream, lambda start, end: "ok" if start == BEHIND else "budget")
        assert stream._missing_found == []
        state = self.finalize(stream, BEHIND.add(days=9))
        assert MISSING_PERIODS_STATE_KEY not in state

    def test_a_window_at_or_after_the_bookmark_is_not_kept(self):
        stream = self.stream()
        stream._tracking_missing = True
        stream._missing_found = [(BEHIND.add(days=30), BEHIND.add(days=39))]
        state = self.finalize(stream, BEHIND.add(days=30))
        assert MISSING_PERIODS_STATE_KEY not in state

    def test_a_kept_period_is_asked_for_first_and_cleared_once_extracted(self):
        missing = [{"from": "2024-01-05", "until": "2024-01-14", "attempts": 1}]
        stream = self.stream(missing=missing)
        calls, user = self.run(stream, lambda start, end: "ok")
        assert calls[0] == (pendulum.date(2024, 1, 5), pendulum.date(2024, 1, 14))
        assert calls[1][0] == BEHIND
        infos = [call.args[0] for call in user.info.call_args_list]
        assert any("Asking first for 1 period(s)" in text and "2024-01-05 to 2024-01-14" in text for text in infos)
        recovered = "2024-01-05 to 2024-01-14, which an earlier run could not extract, is now extracted"
        assert any(recovered in text for text in infos)
        state = self.finalize(stream, TODAY)
        assert MISSING_PERIODS_STATE_KEY not in state

    def test_a_retry_that_fails_counts_an_attempt(self):
        missing = [{"from": "2024-01-05", "until": "2024-01-14", "attempts": 0}]
        stream = self.stream(missing=missing)
        self.run(stream, lambda start, end: "failed" if start.year == 2024 and start.month == 1 else "ok")
        state = self.finalize(stream, TODAY)
        assert state[MISSING_PERIODS_STATE_KEY] == [
            {"from": "2024-01-05", "until": "2024-01-14", "attempts": 1, "failed_on": TODAY.to_date_string()}
        ]

    def test_a_period_still_failing_is_dropped_and_named(self):
        missing = [{"from": "2024-01-05", "until": "2024-01-14", "attempts": MISSING_PERIOD_ATTEMPTS - 1}]
        stream = self.stream(missing=missing)
        _, user = self.run(stream, lambda start, end: "failed" if start.year == 2024 and start.month == 1 else "ok")
        warnings = [call.args[0] for call in user.warning.call_args_list]
        assert any(
            "did not build the report for 2024-01-05 to 2024-01-14" in text and "no longer requested" in text
            for text in warnings
        )
        state = self.finalize(stream, TODAY)
        assert MISSING_PERIODS_STATE_KEY not in state

    def test_a_retry_cut_by_the_budget_is_not_an_attempt(self):
        missing = [{"from": "2024-01-05", "until": "2024-01-14", "attempts": 2}]
        stream = self.stream(missing=missing)
        calls, _ = self.run(stream, lambda start, end: "budget")
        assert len(calls) == 1
        state = self.finalize(stream)
        assert state[MISSING_PERIODS_STATE_KEY] == [{"from": "2024-01-05", "until": "2024-01-14", "attempts": 2}]
        assert state["replication_key_value"] == BEHIND.to_date_string()

    def test_records_of_a_retry_do_not_move_the_bookmark_back(self):
        """The retry yields dates before the bookmark: the floor keeps it."""
        missing = [{"from": "2024-01-05", "until": "2024-01-14", "attempts": 0}]
        stream = self.stream(missing=missing)
        self.run(stream, lambda start, end: "ok" if start.month == 1 else "budget")
        state = self.finalize(stream, pendulum.date(2024, 1, 14))
        assert state["replication_key_value"] == BEHIND.to_date_string()
        assert MISSING_PERIODS_STATE_KEY not in state

    def test_a_period_older_than_facebook_keeps_is_dropped_and_named(self):
        old = TODAY.subtract(months=38)
        missing = [{"from": old.to_date_string(), "until": old.add(days=3).to_date_string(), "attempts": 0}]
        stream = self.stream(missing=missing)
        with mock.patch(USER) as user:
            periods = stream._read_missing_periods(None)
        assert periods == []
        assert "older than the 37 months" in user.warning.call_args.args[0]

    def test_monthly_slices_keep_a_failed_month_too(self):
        """Until v1.88 only daily slices were tracked; a failed month behind the bookmark was never read again."""
        stream = self.stream(config={**SAMPLE_CONFIG, "performance_granularity": "monthly"})
        self.run(stream, lambda start, end: "failed" if start == BEHIND.start_of("month") else "ok")
        assert stream._tracking_missing is True
        assert stream._missing_found[0][0] == BEHIND.start_of("month")


class TestTheWindowClimbsBackOneStepAtATime:
    """3 -> 7 -> 15 -> 31, one step per run, never a whole ladder at once."""

    def stream(self, slices: int | None, failed_on: pendulum.Date | None = None) -> AdsInsightStream:
        stream = stream_with_bookmark(BEHIND)
        if slices is not None:
            marker = {"slices": slices, "since": TODAY.subtract(days=1).to_date_string()}
            if failed_on is not None:
                marker["wider_failed_on"] = failed_on.to_date_string()
            stream.get_context_state(None)[SPAN_WIDTH_STATE_KEY] = marker
        with mock.patch(USER):
            stream._restore_span_width(None)
        return stream

    def run(self, stream, too_large_at: set[int] = frozenset()):
        """get_records over a long backfill; windows as wide as `_span_slices`."""
        widths = []

        def created(*, start_date, end_date, **kwargs):
            widths.append(stream._span_slices)
            until = min(start_date.add(days=stream._span_slices - 1), end_date)
            return [{"next_date": until.add(days=1), "date_obj": start_date, "until_obj": until, "date": "w"}]

        def processed(reports, *args, **kwargs):
            report = reports[0]
            if stream._span_slices in too_large_at:
                stream._shrink_span_after_too_large(report["date_obj"], report["until_obj"], "w", 1)
                return iter([])
            return iter([{"id": "1"}])

        with (
            mock.patch.object(stream, "_initialize_client"),
            mock.patch.object(stream, "_create_report_batch", side_effect=created),
            mock.patch.object(stream, "_process_report_batch", side_effect=processed),
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
            mock.patch(USER) as user,
        ):
            list(stream.get_records(None))
        return widths, user

    def marker(self, stream) -> dict | None:
        return stream.get_context_state(None).get(SPAN_WIDTH_STATE_KEY)

    def test_a_run_with_several_windows_tries_one_step_wider_and_keeps_it(self):
        stream = self.stream(3)
        widths, user = self.run(stream)
        assert widths[:3] == [7, 7, 7]
        assert self.marker(stream) == {"slices": 7, "since": TODAY.to_date_string()}
        infos = [call.args[0] for call in user.info.call_args_list]
        assert any("Trying a report of 7 day(s) (up from 3)" in text for text in infos)
        assert any("built the report of 7 day(s)" in text and "next run tries 15" in text for text in infos)

    def test_a_wider_window_too_large_goes_back_to_what_builds_and_waits(self):
        since = TODAY.subtract(days=1).to_date_string()
        stream = self.stream(3)
        widths, user = self.run(stream, too_large_at={7})
        assert widths[:3] == [7, 3, 3]
        assert self.marker(stream) == {"slices": 3, "since": since, "wider_failed_on": TODAY.to_date_string()}
        infos = [call.args[0] for call in user.info.call_args_list]
        assert any(
            "report of 7 day(s) for w was too large" in text
            and f"tried again from {TODAY.add(days=SPAN_WIDEN_RETRY_DAYS)}" in text
            for text in infos
        )

    def test_no_attempt_during_the_wait(self):
        stream = self.stream(3, failed_on=TODAY.subtract(days=SPAN_WIDEN_RETRY_DAYS - 1))
        widths, _ = self.run(stream)
        assert set(widths) == {3}

    def test_the_attempt_comes_back_after_the_wait(self):
        stream = self.stream(3, failed_on=TODAY.subtract(days=SPAN_WIDEN_RETRY_DAYS))
        widths, _ = self.run(stream)
        assert widths[0] == 7

    def test_reaching_the_full_window_drops_the_marker(self):
        stream = self.stream(15)
        widths, _ = self.run(stream)
        assert widths[0] == SPAN_MAX_SLICES
        assert self.marker(stream) is None

    def test_one_window_to_extract_is_no_reason_to_try(self):
        """An up-to-date stream would save nothing and risk a creation."""
        stream = self.stream(7)
        stream._try_wider(TODAY.subtract(days=6), TODAY, 1)
        assert stream._span_slices == 7
        assert stream._widening_to is None
        assert stream._may_widen is False

    def test_the_attempt_is_no_wider_than_what_there_is_to_extract(self):
        stream = self.stream(7)
        stream._try_wider(TODAY.subtract(days=9), TODAY, 1)
        assert stream._span_slices == 10
        assert stream._widening_to == 10

    def test_a_cut_waits_for_the_next_run(self):
        """The cut itself was the failed attempt at the wider width."""
        stream = self.stream(15)
        with mock.patch(USER):
            stream._shrink_span_after_too_large(BEHIND, BEHIND.add(days=14), "w", 1)
        assert stream._span_slices == 7
        assert stream._may_widen is False
        assert self.marker(stream) == {"slices": 7, "since": TODAY.to_date_string()}

    def test_the_run_after_a_cut_tries_one_step_up(self):
        stream = stream_with_bookmark(BEHIND)
        stream.get_context_state(None)[SPAN_WIDTH_STATE_KEY] = {"slices": 7, "since": TODAY.to_date_string()}
        with mock.patch(USER):
            stream._restore_span_width(None)
        assert stream._may_widen is True

    def test_without_a_remembered_width_nothing_changes(self):
        stream = self.stream(None)
        widths, _ = self.run(stream)
        assert set(widths) == {SPAN_MAX_SLICES}
        assert self.marker(stream) is None
