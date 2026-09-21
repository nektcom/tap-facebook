"""Unit tests for sharing the ad account's report budget between insights streams.

Fully offline.

Background (NEKT-5249, v1.80). Only the insights streams spend the ad account's
"5 calls per 6 hours" budget, and Facebook refuses new reports once it is gone.
Two consequences were seen in production on 2026-09-18:

* facebook-ads-UQfB, on an ordinary incremental run (not a backfill), spent the
  budget on its first streams: two of them reached the current day while
  `adsinsights_by_country` stayed nine days behind. With a fixed sync order that
  stream is always last, so it would never catch up.
* The same run ended red, because a stream that extracted nothing tripped the
  floor. Three red runs in a row disable the pipeline, which stops every stream,
  including the ones that were syncing fine.

So the order rotates by calendar day, and a stream that already has a bookmark
warns instead of failing the run -- with a bookmark the loader is adding to the
table, not replacing it, so nothing can be erased.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest import mock

import pytest

import pendulum

from tap_facebook.streams.ad_insights import (
    SPAN_RETRIES,
    SPLIT_MODE_STATE_KEY,
    SPLIT_MODE_TTL_DAYS,
    SPLIT_SPAN_RETRIES,
    AdsInsightStream,
)
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
    "enable_advanced_reports": True,
}


def make_tap(**overrides) -> TapFacebook:
    return TapFacebook(config={**SAMPLE_CONFIG, **overrides})


def insights_order(tap: TapFacebook) -> list[str]:
    return [stream.name for stream in tap.streams.values() if isinstance(stream, AdsInsightStream)]


def at(year: int, month: int, day: int, hour: int):
    """Pin the clock the rotation reads."""
    patched = mock.patch("tap_facebook.tap.datetime")
    fake = patched.start()
    fake.now.return_value = datetime(year, month, day, hour, tzinfo=timezone.utc)
    return patched


class TestTheInsightsStreamsTakeTurnsGoingFirst:
    def test_the_order_rotates_over_time(self):
        seen = set()
        for day in range(1, 8):
            clock = at(2026, 9, day, 4)
            seen.add(tuple(insights_order(make_tap())))
            clock.stop()

        # Six insights streams, so a week of runs must show more than one order.
        assert len(seen) > 1

    def test_every_stream_gets_the_front_of_the_queue_within_a_cycle(self):
        first_ones = set()
        for day in range(1, 31):
            clock = at(2026, 9, day, 4)
            first_ones.add(insights_order(make_tap())[0])
            clock.stop()

        assert len(first_ones) == len(insights_order(make_tap()))

    def test_a_daily_pipeline_at_a_fixed_hour_still_rotates(self):
        """Counting total hours would freeze it: 24 is a multiple of six."""
        firsts = set()
        for day in range(1, 8):
            clock = at(2026, 9, day, 4)
            firsts.add(insights_order(make_tap())[0])
            clock.stop()

        assert len(firsts) > 1

    def test_the_three_runs_of_one_day_do_not_take_the_same_order(self):
        """facebook-ads-WYkS lost the same stream three times on 2026-09-20."""
        firsts = set()
        for hour in (4, 12, 20):
            clock = at(2026, 9, 20, hour)
            firsts.add(insights_order(make_tap())[0])
            clock.stop()

        assert len(firsts) > 1

    def test_a_retry_within_the_hour_keeps_the_same_order(self):
        """A retry must not reshuffle: the same hour is the same queue."""
        clock = at(2026, 9, 18, 4)
        try:
            assert insights_order(make_tap()) == insights_order(make_tap())
        finally:
            clock.stop()

    def test_the_insights_streams_still_come_after_the_others(self):
        names = [stream.name for stream in make_tap().streams.values()]
        insights = [name for name in names if "insight" in name]
        assert names[-len(insights) :] == insights

    def test_nothing_is_reordered_when_there_is_a_single_insights_stream(self):
        tap = make_tap(enable_advanced_reports=False)
        assert insights_order(tap) == ["adsinsights"]


class TestAStreamWithHistoryWarnsInsteadOfFailingTheRun:
    def stream(self) -> AdsInsightStream:
        stream = make_tap().streams["adsinsights"]
        stream._reset_run_state()
        stream._sync_context = None
        stream._dates_failed = 2
        return stream

    def test_a_stream_that_already_has_a_bookmark_does_not_fail_the_run(self):
        stream = self.stream()
        with (
            mock.patch.object(stream, "get_starting_replication_key_value", return_value="2026-09-09"),
            mock.patch("tap_facebook.streams.ad_insights.user_logger") as logged,
        ):
            stream._fail_if_nothing_extracted(batches_attempted=1, reports_queued=1, records_emitted=0)

        assert logged.warning.called
        assert not logged.error.called

    def test_the_customer_is_told_the_stream_did_not_advance(self):
        stream = self.stream()
        with (
            mock.patch.object(stream, "get_starting_replication_key_value", return_value="2026-09-09"),
            mock.patch("tap_facebook.streams.ad_insights.user_logger") as logged,
        ):
            stream._fail_if_nothing_extracted(batches_attempted=1, reports_queued=1, records_emitted=0)

        said = logged.warning.call_args.args[0]
        assert "not updated in this run" in said
        assert "untouched" in said

    def test_a_first_sync_with_no_bookmark_still_fails(self):
        """No bookmark means the load replaces the table; empty would erase it."""
        stream = self.stream()
        with (
            mock.patch.object(stream, "get_starting_replication_key_value", return_value=None),
            pytest.raises(SystemExit),
        ):
            stream._fail_if_nothing_extracted(batches_attempted=1, reports_queued=1, records_emitted=0)

    def test_a_run_that_extracted_rows_is_untouched(self):
        stream = self.stream()
        with mock.patch.object(stream, "get_starting_replication_key_value") as bookmark:
            stream._fail_if_nothing_extracted(batches_attempted=1, reports_queued=1, records_emitted=10)
        bookmark.assert_not_called()

    def test_an_account_that_simply_had_no_delivery_is_untouched(self):
        stream = self.stream()
        stream._dates_failed = 0
        with mock.patch.object(stream, "get_starting_replication_key_value") as bookmark:
            stream._fail_if_nothing_extracted(batches_attempted=1, reports_queued=1, records_emitted=0)
        bookmark.assert_not_called()


class TestTheWholeReportIsInsistedOnBeforePayingForParts:
    """Splitting costs eight or nine creations; another whole-report attempt costs one.

    The Explorer test on 2026-09-18 showed both accounts that had just split
    their period building all 192 fields in one report at the first attempt, so
    what the tap was reading as "too heavy" was a transient failure.
    """

    def test_the_whole_window_is_attempted_four_times(self):
        assert SPAN_RETRIES == 3

    def test_a_window_already_in_parts_keeps_the_single_retry(self):
        """There each attempt recreates every failed part, so insisting multiplies the cost."""
        assert SPLIT_SPAN_RETRIES == 1

    def test_the_verdict_is_not_carried_for_a_week(self):
        assert SPLIT_MODE_TTL_DAYS == 1

    def test_yesterdays_verdict_no_longer_starts_the_run_in_parts(self):
        stream = make_tap().streams["adsinsights"]
        stream._reset_run_state()
        stream.stream_state[SPLIT_MODE_STATE_KEY] = pendulum.today().subtract(days=2).to_date_string()

        stream._restore_split_mode(None)

        assert stream._split_mode is False
        assert SPLIT_MODE_STATE_KEY not in stream.stream_state

    def test_a_verdict_taken_today_still_holds_for_the_rest_of_the_run(self):
        stream = make_tap().streams["adsinsights"]
        stream._reset_run_state()
        stream.stream_state[SPLIT_MODE_STATE_KEY] = pendulum.today().to_date_string()

        stream._restore_split_mode(None)

        assert stream._split_mode is True


class TestAPartiallyExtractedStreamIsAnnounced:
    """A green run that lost some dates must not read as a complete one.

    Until v1.81 the floor returned in silence as soon as one record had been
    emitted, so a stream that got three days out of ten said nothing at all.
    """

    def stream(self) -> AdsInsightStream:
        stream = make_tap().streams["adsinsights"]
        stream._reset_run_state()
        stream._sync_context = None
        return stream

    def test_the_customer_is_told_when_some_dates_were_refused(self):
        stream = self.stream()
        stream._dates_failed = 3
        with mock.patch("tap_facebook.streams.ad_insights.user_logger") as logged:
            stream._fail_if_nothing_extracted(batches_attempted=1, reports_queued=4, records_emitted=120)

        said = logged.warning.call_args.args[0]
        assert "only partially updated" in said
        assert "3 date(s)" in said
        assert not logged.error.called

    def test_a_complete_run_says_nothing(self):
        stream = self.stream()
        stream._dates_failed = 0
        with mock.patch("tap_facebook.streams.ad_insights.user_logger") as logged:
            stream._fail_if_nothing_extracted(batches_attempted=1, reports_queued=4, records_emitted=120)

        assert not logged.warning.called
        assert not logged.error.called

    def test_the_partial_run_is_not_failed(self):
        stream = self.stream()
        stream._dates_failed = 3
        with mock.patch("tap_facebook.streams.ad_insights.user_logger"):
            # No SystemExit: the table moved forward and the bookmark is kept.
            stream._fail_if_nothing_extracted(batches_attempted=1, reports_queued=4, records_emitted=120)


if __name__ == "__main__":
    pytest.main([__file__])
