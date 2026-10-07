"""Unit tests for v1.88: the log closes with what the run did not complete.

Fully offline.

Background (NEKT-5249, 2026-10-06). Customers opened support tickets for green
runs they read as failures. The log view opens on the most recent lines, where
the extraction table shows a stream refused by Facebook as "0 rows", like one
with nothing new; the warnings that explained it sat further up, two per
stream -- 14 on facebook-ads-WhFu (2026-10-05) for one spent report limit.
"""

from __future__ import annotations

from unittest import mock

import pendulum
import pytest
from nekt_singer_sdk import Tap

from tap_facebook.streams.ad_insights import MISSING_PERIODS_STATE_KEY, AdsInsightStream
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
    "report_definition": {"lookback_window": 7, "action_report_time": "impression"},
}
USER = "tap_facebook.streams.ad_insights.user_logger"
TAP_USER = "tap_facebook.tap.user_logger"
BOOKMARK = pendulum.date(2024, 2, 10)


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


def stream_with_bookmark(bookmark: pendulum.Date | None, name: str = "adsinsights") -> AdsInsightStream:
    state = (
        {"bookmarks": {name: {"replication_key": "date_start", "replication_key_value": bookmark.to_date_string()}}}
        if bookmark is not None
        else {}
    )
    stream = TapFacebook(config=SAMPLE_CONFIG, state=state).streams[name]
    stream._reset_run_state()
    stream._sync_context = None
    stream._write_starting_replication_value(None)
    return stream


def refused_by_the_account_limit(stream: AdsInsightStream, spent_at: pendulum.Date | None = None) -> None:
    stream._dates_failed = 1
    stream._last_throttle_code = 613
    stream._budget_spent_at = spent_at


def left(name: str = "adsinsights") -> dict[str, str]:
    return AdsInsightStream._not_completed.get(name, {})


class TestEachStreamRecordsWhatItLeft:
    def test_a_partly_updated_stream_says_where_the_next_run_continues(self):
        stream = stream_with_bookmark(BOOKMARK)
        refused_by_the_account_limit(stream, spent_at=pendulum.date(2024, 2, 24))
        with mock.patch(USER):
            stream._fail_if_nothing_extracted(batches_attempted=2, reports_queued=1, records_emitted=10)
        note = left()["dates"]
        assert note.startswith("partly updated: 1 date(s) not extracted (the ad account's request limit")
        assert "next run picks them up from 2024-02-24" in note
        assert AdsInsightStream._account_limit_spent is True

    def test_a_stream_not_updated_keeps_its_data(self):
        stream = stream_with_bookmark(BOOKMARK)
        refused_by_the_account_limit(stream)
        with mock.patch(USER):
            stream._fail_if_nothing_extracted(batches_attempted=1, reports_queued=0, records_emitted=0)
        note = left()["dates"]
        assert note.startswith("not updated in this run")
        assert "the data loaded before is untouched" in note
        assert "continues from where it stopped" in note

    def test_a_stream_with_no_data_yet(self):
        stream = stream_with_bookmark(None)
        refused_by_the_account_limit(stream)
        with mock.patch(USER):
            stream._fail_if_nothing_extracted(batches_attempted=1, reports_queued=0, records_emitted=0)
        assert left()["dates"].startswith("no data yet")

    def test_a_partial_load_on_a_replaced_table(self):
        stream = stream_with_bookmark(None)
        AdsInsightStream._run_started_without_state = True
        refused_by_the_account_limit(stream)
        with mock.patch(USER):
            stream._fail_if_nothing_extracted(batches_attempted=2, reports_queued=1, records_emitted=10)
        assert "the run is marked as failed" in left()["dates"]

    def test_a_complete_stream_leaves_nothing(self):
        stream = stream_with_bookmark(BOOKMARK)
        with mock.patch(USER):
            stream._fail_if_nothing_extracted(batches_attempted=2, reports_queued=2, records_emitted=10)
        assert AdsInsightStream._not_completed == {}
        assert AdsInsightStream._account_limit_spent is False

    def test_an_unfinished_reread_is_listed(self):
        stream = stream_with_bookmark(BOOKMARK)
        stream._reread_stopped_at = pendulum.date(2024, 2, 5)
        with mock.patch(USER):
            stream._report_unfinished_reread()
        assert "re-read of recent days already loaded" in left()["reread"]
        assert "stopped at 2024-02-05" in left()["reread"]

    def test_periods_asked_for_first_are_listed_and_cleared(self):
        stream = stream_with_bookmark(BOOKMARK)
        stream._tracking_missing = True
        stream._missing_found = [(pendulum.date(2024, 2, 1), pendulum.date(2024, 2, 3))]
        state = stream.get_context_state(None)
        with mock.patch(USER):
            stream._finalize_state(state)
        assert left()["missing"] == (
            "1 period(s) Facebook did not build are asked for first in the next run: 2024-02-01 to 2024-02-03."
        )
        stream._missing_found = []
        stream._finalize_state(state)
        assert MISSING_PERIODS_STATE_KEY not in state
        assert AdsInsightStream._not_completed == {}

    def test_an_account_facebook_is_not_building_is_named(self):
        stream = stream_with_bookmark(BOOKMARK)
        AdsInsightStream._account_not_building = True
        assert stream._why_reports_were_refused() == (
            "Facebook is not building performance reports for this ad account right now"
        )


class TestTheCauseIsToldOncePerStream:
    def test_a_spent_account_limit_is_one_warning_not_two(self):
        """Until v1.88: one at the throttle point and one at the end of the stream."""
        stream = stream_with_bookmark(BOOKMARK)

        def refused(**kwargs):
            stream._throttled = True
            stream._last_throttle_code = 613
            return []

        with (
            mock.patch.object(stream, "_initialize_client"),
            mock.patch.object(stream, "_create_report_batch", side_effect=refused),
            mock.patch.object(stream, "_advance_batch", side_effect=lambda current, inc, n, end: end.add(days=1)),
            mock.patch(USER) as user,
        ):
            list(stream.get_records(None))
        assert user.warning.call_count == 1
        assert "not updated in this run" in user.warning.call_args.args[0]


class TestTheLogClosesWithIt:
    def tap(self) -> TapFacebook:
        return TapFacebook(config=SAMPLE_CONFIG, state={})

    def test_the_summary_lists_every_stream_left(self):
        AdsInsightStream._not_completed = {
            "adsinsights": {"dates": "partly updated: x.", "missing": "1 period(s) y."},
            "adsinsights_by_country": {"dates": "not updated in this run z."},
        }
        with mock.patch(TAP_USER) as user:
            self.tap()._tell_what_was_not_completed()
        said = user.warning.call_args.args[0]
        assert said.splitlines() == [
            "**Not completed in this run**",
            "",
            "- **adsinsights**: partly updated: x.",
            "- **adsinsights**: 1 period(s) y.",
            "- **adsinsights_by_country**: not updated in this run z.",
        ]

    def test_a_spent_account_limit_adds_what_the_customer_can_do(self):
        AdsInsightStream._not_completed = {"adsinsights": {"dates": "not updated in this run z."}}
        AdsInsightStream._account_limit_spent = True
        with mock.patch(TAP_USER) as user:
            self.tap()._tell_what_was_not_completed()
        assert "Running this source less often" in user.warning.call_args.args[0]

    def test_nothing_left_prints_nothing(self):
        with mock.patch(TAP_USER) as user:
            self.tap()._tell_what_was_not_completed()
        user.warning.assert_not_called()

    def test_sync_all_prints_it_after_every_stream_and_starts_clean(self):
        AdsInsightStream._not_completed = {"stale": {"dates": "from an earlier run in this process"}}

        def streams_ran(*args, **kwargs):
            assert AdsInsightStream._not_completed == {}, "the registry is reset before the streams run"
            AdsInsightStream._not_completed = {"adsinsights": {"dates": "not updated in this run z."}}

        with (
            mock.patch.object(Tap, "sync_all", side_effect=streams_ran),
            mock.patch(TAP_USER) as user,
        ):
            self.tap().sync_all()
        said = user.warning.call_args.args[0]
        assert said.startswith("**Not completed in this run**")
        assert "stale" not in said
