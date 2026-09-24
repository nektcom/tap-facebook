"""Unit tests for v1.84: when a short insights stream fails the run, and the v1.83 follow-ups.

Fully offline.

Background (NEKT-5249, review before the fleet rollout on 2026-09-24). v1.82
failed any insights stream that came back short without a bookmark of its own,
reading "no bookmark" as "full sync". But a stream the customer just enabled,
or a new source from its second run on, has no bookmark either while the run
is an ordinary incremental one -- the destination merges and nothing is lost.
On an account short of quota such a pipeline went red every run until each
insights stream had a bookmark, and three red runs in a row disable it
(facebook-ads-p5eD and -659m, both created on 2026-09-23). Meltano passes no
state at all on a full refresh, so the state the run was handed is what tells
the two apart.

The same review found: the singular wording of the "weren't there" read
refusal (89 of 487 in a week) still recreated the report; a network error
during the re-read took the stream down; a FULL_TABLE stream with an old
bookmark counted as history; a 613 reported on the job did not stop the
stream; and the span width was lost when a replaced stream failed the run.
"""

from __future__ import annotations

from unittest import mock

import pytest
import requests
from facebook_business.exceptions import FacebookRequestError

from tap_facebook.streams.ad_insights import (
    SPAN_WIDTH_STATE_KEY,
    AdsInsightStream,
    _columns_absent_from_report,
)
from tap_facebook.tap import TapFacebook, _state_has_a_bookmark

SAMPLE_CONFIG = {
    "start_date": "2026-08-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
}
USER = "tap_facebook.streams.ad_insights.user_logger"
BOOKMARKED_STATE = {
    "bookmarks": {"adsinsights_hourly_advertiser_timezone": {"replication_key_value": "2026-09-23"}}
}


@pytest.fixture(autouse=True)
def _fresh_process_state():
    AdsInsightStream._incomplete_without_history = []
    AdsInsightStream._columns_refused_on_read = set()
    AdsInsightStream._run_started_without_state = True
    yield
    AdsInsightStream._incomplete_without_history = []
    AdsInsightStream._columns_refused_on_read = set()
    AdsInsightStream._run_started_without_state = True


def make_stream() -> AdsInsightStream:
    stream = TapFacebook(config=SAMPLE_CONFIG).streams["adsinsights"]
    stream._reset_run_state()
    stream._sync_context = None
    return stream


def without_bookmark(stream: AdsInsightStream) -> AdsInsightStream:
    """What the SDK leaves at stream start when this stream has no bookmark."""
    stream.stream_state.clear()
    stream._write_starting_replication_value(None)
    return stream


def with_bookmark(stream: AdsInsightStream) -> AdsInsightStream:
    stream.stream_state["replication_key"] = "date_start"
    stream.stream_state["replication_key_value"] = "2026-09-20"
    return stream


def incremental_run() -> None:
    AdsInsightStream._run_started_without_state = False


def lost_everything(stream: AdsInsightStream) -> None:
    stream._dates_failed = 1
    stream._fail_if_nothing_extracted(batches_attempted=1, reports_queued=0, records_emitted=0)


def lost_some(stream: AdsInsightStream) -> None:
    stream._dates_failed = 3
    stream._fail_if_nothing_extracted(batches_attempted=2, reports_queued=4, records_emitted=500)


class TestTheStateHandedInTellsAFullSyncFromANewStream:
    def test_no_state_is_no_bookmark(self):
        assert _state_has_a_bookmark(None) is False
        assert _state_has_a_bookmark({}) is False
        assert _state_has_a_bookmark({"bookmarks": {}}) is False

    def test_the_sdk_starting_value_is_not_a_bookmark(self):
        state = {"bookmarks": {"adsinsights": {"starting_replication_value": "2026-08-01", "progress_markers": {}}}}
        assert _state_has_a_bookmark(state) is False

    def test_a_finalized_bookmark_of_any_stream_counts(self):
        assert _state_has_a_bookmark(BOOKMARKED_STATE) is True

    def test_a_partition_bookmark_counts(self):
        state = {"bookmarks": {"ads": {"partitions": [{"context": {}, "replication_key_value": "2026-09-01"}]}}}
        assert _state_has_a_bookmark(state) is True

    def test_junk_is_not_a_bookmark(self):
        assert _state_has_a_bookmark({"bookmarks": {"x": "not a dict", "y": {"partitions": ["nope"]}}}) is False

    def test_a_run_with_state_is_incremental(self):
        tap = TapFacebook(config=SAMPLE_CONFIG, state=BOOKMARKED_STATE)
        with mock.patch("nekt_singer_sdk.Tap.sync_all"):
            tap.sync_all()
        assert AdsInsightStream._run_started_without_state is False

    def test_a_run_without_state_is_a_full_sync_or_a_first_run(self):
        incremental_run()  # left over from an earlier run of the process
        tap = TapFacebook(config=SAMPLE_CONFIG)
        with mock.patch("nekt_singer_sdk.Tap.sync_all"):
            tap.sync_all()
        assert AdsInsightStream._run_started_without_state is True

    def test_the_mode_is_read_before_the_streams_add_bookmarks(self):
        tap = TapFacebook(config=SAMPLE_CONFIG)

        def streams_ran(*args, **kwargs):
            tap.state.setdefault("bookmarks", {})["adsinsights"] = {"replication_key_value": "2026-09-24"}

        with mock.patch("nekt_singer_sdk.Tap.sync_all", side_effect=streams_ran):
            tap.sync_all()
        assert AdsInsightStream._run_started_without_state is True


class TestANewStreamOnAnIncrementalRunOnlyWarns:
    """The p5eD / 659m case: state handed in, this stream has no bookmark yet."""

    def test_nothing_extracted_does_not_fail_the_run(self):
        incremental_run()
        stream = without_bookmark(make_stream())
        with mock.patch(USER) as user:
            lost_everything(stream)
        assert user.warning.called
        assert not user.error.called

    def test_the_customer_is_not_told_there_was_earlier_data(self):
        incremental_run()
        stream = without_bookmark(make_stream())
        with mock.patch(USER) as user:
            lost_everything(stream)
        said = user.warning.call_args.args[0]
        assert "untouched" not in said
        assert "no data yet" in said

    def test_a_partial_extraction_does_not_fail_the_run_at_the_end(self):
        incremental_run()
        stream = without_bookmark(make_stream())
        with mock.patch(USER) as user:
            lost_some(stream)
        assert AdsInsightStream._incomplete_without_history == []
        assert user.warning.called
        assert not user.error.called


class TestARunWithoutStateStillFails:
    """Full sync or first run: the destination replaces the table, as in v1.82."""

    def test_nothing_extracted_fails_now(self):
        stream = without_bookmark(make_stream())
        with mock.patch(USER), pytest.raises(SystemExit):
            lost_everything(stream)

    def test_a_partial_extraction_fails_at_the_end(self):
        stream = without_bookmark(make_stream())
        with mock.patch(USER):
            lost_some(stream)
        assert AdsInsightStream._incomplete_without_history == ["adsinsights"]

    def test_the_span_width_is_emitted_before_the_run_fails(self):
        """Otherwise the next full sync pays the same cuts again."""
        stream = without_bookmark(make_stream())
        stream.stream_state[SPAN_WIDTH_STATE_KEY] = {"slices": 7, "since": "2026-09-24"}
        with (
            mock.patch(USER),
            mock.patch.object(stream._tap, "write_message") as write,
            pytest.raises(SystemExit),
        ):
            lost_everything(stream)

        emitted = [call.args[0].value for call in write.call_args_list]
        assert any(
            state.get("bookmarks", {}).get("adsinsights", {}).get(SPAN_WIDTH_STATE_KEY) == {"slices": 7, "since": "2026-09-24"}
            for state in emitted
        )


class TestAFullTableStreamIsAlwaysReplaced:
    def full_table(self) -> AdsInsightStream:
        stream = with_bookmark(make_stream())
        stream.forced_replication_method = "FULL_TABLE"
        return stream

    def test_an_old_bookmark_does_not_turn_an_empty_snapshot_green(self):
        incremental_run()
        with mock.patch(USER), pytest.raises(SystemExit):
            lost_everything(self.full_table())

    def test_a_partial_snapshot_fails_the_run_at_the_end(self):
        incremental_run()
        with mock.patch(USER):
            lost_some(self.full_table())
        assert AdsInsightStream._incomplete_without_history == ["adsinsights"]

    def test_an_incremental_stream_with_a_bookmark_is_unchanged(self):
        incremental_run()
        stream = with_bookmark(make_stream())
        with mock.patch(USER) as user:
            lost_everything(stream)
        assert "untouched" in user.warning.call_args.args[0]


def fb_error(message: str, code: int = 100) -> FacebookRequestError:
    return FacebookRequestError(
        message="Call was not successful",
        request_context={},
        http_status=400,
        http_headers={},
        body=f'{{"error": {{"code": {code}, "type": "OAuthException", "message": "{message}"}}}}',
    )


COLUMNS = ["date_start", "date_stop", "campaign_id", "adset_id", "ad_id", "spend", "social_spend", "adset_end"]
PARTS = [{"name": "all", "columns": COLUMNS, "report_run_id": "report-1"}]
ROWS = [{"ad_id": "1", "date_start": "2026-09-16", "spend": "3.2"}]
SINGULAR = (
    "(#100) Cannot include social_spend in fields param because it wasn't there while creating the report run. "
    "All available values are: account_id, ad_id, adset_id, campaign_id, date_start, date_stop, spend"
)


class TestTheSingularWordingIsReadToo:
    """facebook-ads-L928, 2026-09-24: breakdown streams got the one-field wording."""

    def test_the_one_field_is_named(self):
        assert _columns_absent_from_report(SINGULAR, COLUMNS) == ["social_spend"]

    def test_the_report_is_read_again_instead_of_rebuilt(self):
        stream = make_stream()
        refused = fb_error("(#100) Tried accessing nonexisting summary field (adset_end)")
        with mock.patch.object(stream, "_merge_part_results", side_effect=[fb_error(SINGULAR), ROWS]) as merge:
            got = stream._reread_without_refused_columns(refused, PARTS, [mock.Mock()], COLUMNS, "2026-09-16")

        assert got == ROWS
        fields = merge.call_args.kwargs["fields"]
        assert "social_spend" not in fields
        assert "adset_end" not in fields
        assert {"date_start", "ad_id"} <= set(fields)


class TestANetworkErrorDuringTheReReadDoesNotKillTheStream:
    def test_it_is_handed_back_as_not_absorbed(self):
        stream = make_stream()
        refused = fb_error("(#100) Tried accessing nonexisting summary field (adset_end)")
        with mock.patch.object(
            stream, "_merge_part_results", side_effect=requests.exceptions.ConnectionError("reset by peer")
        ):
            got = stream._reread_without_refused_columns(refused, PARTS, [mock.Mock()], COLUMNS, "2026-09-16")

        assert got is None

    def test_a_read_timeout_is_handed_back_too(self):
        stream = make_stream()
        refused = fb_error("(#100) Tried accessing nonexisting summary field (adset_end)")
        with mock.patch.object(stream, "_merge_part_results", side_effect=requests.exceptions.ReadTimeout()):
            got = stream._reread_without_refused_columns(refused, PARTS, [mock.Mock()], COLUMNS, "2026-09-16")

        assert got is None


class TestA613OnTheJobIsRecordedLikeOneOnCreation:
    def job(self) -> dict:
        return {"error_code": 613, "error_message": "Custom Analytics metrics exceeded the rate limit"}

    def test_the_code_is_recorded(self):
        stream = make_stream()
        with mock.patch(USER):
            stream._record_job_failure(self.job(), "job-1", "2026-09-16 to 2026-09-23", mock.Mock())

        assert stream._throttled is True
        assert stream._last_throttle_code == 613

    def test_the_customer_is_told_the_real_cause(self):
        stream = make_stream()
        with mock.patch(USER):
            stream._record_job_failure(self.job(), "job-1", "2026-09-16 to 2026-09-23", mock.Mock())

        assert "request limit" in stream._why_reports_were_refused()


if __name__ == "__main__":
    pytest.main([__file__])
