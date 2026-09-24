"""Unit tests for v1.82: the full-sync guard and the "report too large" window cut.

Fully offline.

Background (NEKT-5249, reported by Burgarelli on 2026-09-23). Two defects of
v1.80/v1.81, found on facebook-ads-TJaE ("Meta Ads - Reinaldo") and
facebook-ads-WhFu:

* A manual full sync of TJaE found the ad account's request limit spent; all
  seven insights streams extracted nothing. The floor took the SDK's starting
  value for a bookmark -- with no state the SDK seeds it with the configured
  start_date -- so it told the customer the previous data was untouched and
  ended the run green, while the loader replaced the seven tables with empty
  snapshots.
* On both accounts the insights window grew from a bookmark stuck on
  2026-09-04 and Facebook refused it every run as too large
  (error_code=-3, error_subcode=1504045, "Out of memory"). Splitting by columns
  does not help when the rows are the weight, so the stream never advanced.
"""

from __future__ import annotations

from unittest import mock

import pendulum
import pytest

from tap_facebook.streams.ad_insights import (
    SPAN_MAX_SLICES,
    SPAN_WIDTH_STATE_KEY,
    SPAN_WIDTH_TTL_DAYS,
    AdsInsightStream,
)
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2026-08-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
}
USER = "tap_facebook.streams.ad_insights.user_logger"


@pytest.fixture(autouse=True)
def _fresh_process_state():
    AdsInsightStream._incomplete_without_history = []
    yield
    AdsInsightStream._incomplete_without_history = []


def make_stream() -> AdsInsightStream:
    stream = TapFacebook(config=SAMPLE_CONFIG).streams["adsinsights"]
    stream._reset_run_state()
    stream._sync_context = None
    return stream


def as_full_sync(stream: AdsInsightStream) -> AdsInsightStream:
    """What the SDK does at stream start when no state was passed in."""
    stream.stream_state.clear()
    stream._write_starting_replication_value(None)
    return stream


def with_bookmark(stream: AdsInsightStream) -> AdsInsightStream:
    stream.stream_state["replication_key"] = "date_start"
    stream.stream_state["replication_key_value"] = "2026-09-20"
    return stream


class TestAFullSyncIsNotMistakenForHistory:
    def test_the_sdk_seeds_a_starting_value_even_without_state(self):
        """The trap: the value the old floor read is never empty on a full sync."""
        stream = as_full_sync(make_stream())

        assert stream.get_starting_replication_key_value(None)
        assert stream._has_history() is False

    def test_a_real_bookmark_is_history(self):
        assert with_bookmark(make_stream())._has_history() is True

    def test_a_full_sync_that_extracted_nothing_fails_the_run(self):
        """The TJaE run of 2026-09-23 ended green here."""
        stream = as_full_sync(make_stream())
        stream._dates_failed = 1

        with mock.patch(USER), pytest.raises(SystemExit):
            stream._fail_if_nothing_extracted(batches_attempted=1, reports_queued=0, records_emitted=0)

    def test_the_customer_is_not_told_the_old_data_is_untouched(self):
        stream = as_full_sync(make_stream())
        stream._dates_failed = 1

        with mock.patch(USER) as user, pytest.raises(SystemExit):
            stream._fail_if_nothing_extracted(batches_attempted=1, reports_queued=0, records_emitted=0)

        said = user.error.call_args.args[0]
        assert "untouched" not in said
        assert not user.warning.called

    def test_a_partial_full_sync_does_not_exit_inside_the_stream(self):
        """Exiting here would discard the progress of an unsorted stream."""
        stream = as_full_sync(make_stream())
        stream._dates_failed = 3

        with mock.patch(USER) as user:
            stream._fail_if_nothing_extracted(batches_attempted=2, reports_queued=4, records_emitted=500)

        assert AdsInsightStream._incomplete_without_history == ["adsinsights"]
        assert user.error.called
        assert not user.warning.called

    def test_an_incremental_run_is_unchanged(self):
        """With a real bookmark the loader appends, so the stream still only warns."""
        stream = with_bookmark(make_stream())
        stream._dates_failed = 1

        with mock.patch(USER) as user:
            stream._fail_if_nothing_extracted(batches_attempted=1, reports_queued=0, records_emitted=0)

        assert user.warning.called
        assert AdsInsightStream._incomplete_without_history == []


class TestTheTapFailsAPartialFullSyncAtTheEnd:
    def test_the_run_fails_after_every_stream_has_run(self):
        tap = TapFacebook(config=SAMPLE_CONFIG)

        def streams_ran(*args, **kwargs):
            AdsInsightStream._incomplete_without_history.append("adsinsights")

        with (
            mock.patch("nekt_singer_sdk.Tap.sync_all", side_effect=streams_ran),
            mock.patch("tap_facebook.tap.user_logger"),
            pytest.raises(SystemExit),
        ):
            tap.sync_all()

    def test_a_complete_run_ends_normally(self):
        tap = TapFacebook(config=SAMPLE_CONFIG)
        with mock.patch("nekt_singer_sdk.Tap.sync_all"):
            tap.sync_all()

    def test_a_previous_run_does_not_leak_into_this_one(self):
        AdsInsightStream._incomplete_without_history = ["left over"]
        tap = TapFacebook(config=SAMPLE_CONFIG)
        with mock.patch("nekt_singer_sdk.Tap.sync_all"):
            tap.sync_all()


class TestTheCustomerIsToldTheRealCause:
    def cause(self, **run_state) -> str:
        stream = with_bookmark(make_stream())
        stream._dates_failed = 1
        for key, value in run_state.items():
            setattr(stream, key, value)
        with mock.patch(USER) as user:
            stream._fail_if_nothing_extracted(batches_attempted=1, reports_queued=0, records_emitted=0)
        return user.warning.call_args.args[0]

    def test_a_spent_request_limit_is_named_as_such(self):
        assert "request limit" in self.cause(_last_throttle_code=613)

    def test_a_report_too_large_is_not_blamed_on_the_limit(self):
        said = self.cause(_report_too_large_seen=True)
        assert "too large" in said
        assert "request limit" not in said

    def test_an_unknown_cause_is_not_guessed(self):
        said = self.cause()
        assert "request limit" not in said
        assert "too large" not in said


def too_large_job() -> dict:
    return {
        "error_code": -3,
        "error_subcode": 1504045,
        "error_message": "Out of memory",
        "error_user_title": "O relatório de insights é muito grande",
    }


class TestAReportTooLargeIsCutByDays:
    START = pendulum.date(2026, 9, 4)
    UNTIL = pendulum.date(2026, 9, 23)

    def test_the_failure_is_recognised(self):
        stream = make_stream()
        with mock.patch(USER):
            stream._record_job_failure(too_large_job(), "job-1", "2026-09-04 to 2026-09-23", mock.Mock())

        assert stream._job_too_large is True
        assert stream._report_too_large_seen is True

    def test_the_window_is_halved_and_resumed_from_the_same_date(self):
        stream = make_stream()
        with mock.patch(USER):
            cut = stream._shrink_span_after_too_large(self.START, self.UNTIL, "2026-09-04 to 2026-09-23", 1)

        assert cut is True
        assert stream._span_slices == 10
        assert stream._span_resize_from == self.START

    def test_the_width_is_kept_for_the_next_runs(self):
        stream = make_stream()
        with mock.patch(USER):
            stream._shrink_span_after_too_large(self.START, self.UNTIL, "label", 1)

        assert stream.stream_state[SPAN_WIDTH_STATE_KEY]["slices"] == 10

    def test_a_single_day_is_left_to_the_usual_ladder(self):
        stream = make_stream()
        day = pendulum.date(2026, 9, 23)
        assert stream._shrink_span_after_too_large(day, day, "2026-09-23", 1) is False
        assert stream._span_slices == SPAN_MAX_SLICES

    def test_the_next_window_uses_the_narrower_width(self):
        stream = make_stream()
        stream._span_slices = 10
        with mock.patch.object(
            stream, "_queue_report_parts", return_value=[{"name": "all", "columns": [], "report_run_id": "r"}]
        ):
            batch = stream._create_report_batch(self.START, 30, self.UNTIL, [], 1)

        assert batch[0]["until_obj"] == pendulum.date(2026, 9, 13)

    def test_no_retry_is_spent_at_the_same_width(self):
        """Each retry is a creation against the account's budget, for a known answer."""
        stream = make_stream()
        report = {
            "report_run_id": "r",
            "parts": [{"name": "all", "columns": [], "report_run_id": "r"}],
            "date": "2026-09-04 to 2026-09-23",
            "date_obj": self.START,
            "until_obj": self.UNTIL,
            "next_date": pendulum.date(2026, 9, 24),
        }

        def fails_on_size(*args, **kwargs):
            stream._job_too_large = True
            stream._report_too_large_seen = True

        with (
            mock.patch.object(stream, "_run_parts_to_completion", side_effect=fails_on_size) as run,
            mock.patch.object(stream, "_recreate_failed_parts") as recreate,
            mock.patch.object(stream, "_leave_span") as leave,
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
            mock.patch(USER),
        ):
            rows = list(stream._process_report_batch([report], [], 1))

        assert rows == []
        assert run.call_count == 1
        recreate.assert_not_called()
        leave.assert_not_called()
        assert stream._span_resize_from == self.START
        assert stream._span_slices == 10


class TestTheWidthIsReadBackByTheNextRun:
    def test_a_recent_width_is_used(self):
        stream = make_stream()
        stream.stream_state[SPAN_WIDTH_STATE_KEY] = {"slices": 5, "since": pendulum.today().to_date_string()}

        stream._restore_span_width(None)

        assert stream._span_slices == 5

    def test_an_expired_width_gives_the_full_window_back(self):
        stream = make_stream()
        old = pendulum.today().subtract(days=SPAN_WIDTH_TTL_DAYS + 1).to_date_string()
        stream.stream_state[SPAN_WIDTH_STATE_KEY] = {"slices": 5, "since": old}

        stream._restore_span_width(None)

        assert stream._span_slices == SPAN_MAX_SLICES
        assert SPAN_WIDTH_STATE_KEY not in stream.stream_state

    def test_an_unreadable_marker_is_dropped(self):
        stream = make_stream()
        stream.stream_state[SPAN_WIDTH_STATE_KEY] = "garbage"

        stream._restore_span_width(None)

        assert stream._span_slices == SPAN_MAX_SLICES
        assert SPAN_WIDTH_STATE_KEY not in stream.stream_state


if __name__ == "__main__":
    pytest.main([__file__])
