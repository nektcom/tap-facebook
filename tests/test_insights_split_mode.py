"""Unit tests for split mode: the same period requested as several smaller reports.

Fully offline: report creation and job polling are mocked, so no credentials
and no calls against the ad account quota.

Background (NEKT-5249, v1.73): Meta support's final answer on the incident was
that a job ending as "Service temporarily unavailable" (2/1504044) is a report
too heavy to build, not a blocked account. Reproduced on the Verruck account
(act_1053762787247772, 2026-09-04) in the Graph API Explorer: the 197 fields
the tap asks for failed in seconds even with limit=25, while the 51
BASIC_FIELDS built with limit=100. The field set is the lever.

These tests pin the fallback: the probe after a failed span asks for the core
metrics only; when it builds and optional groups were requested, the period is
asked for again in parts (core + one report per enabled group, all carrying
the join keys) and the rows are combined before they are emitted -- same rows,
same columns. Accounts that build the full report never pay the extra
creations, the verdict is remembered in the state for a few days, and a part
that does not build fails the period instead of emitting half a row.
"""

from __future__ import annotations

import typing as t
from unittest import mock

import pendulum
import pytest
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsinsights import AdsInsights

from tap_facebook.streams.ad_insights import (
    BASIC_FIELDS,
    RESULTS_FIELDS,
    SPLIT_MODE_STATE_KEY,
    SPLIT_MODE_TTL_DAYS,
    SPLIT_PART_MAX_COLUMNS,
    STANDARD_FIELDS,
    AdsInsightStream,
)
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
    "enable_advanced_reports": True,
    "include_insights_standard_fields": True,
    "include_insights_results_fields": True,
}

START = pendulum.date(2026, 9, 4)
UNTIL = pendulum.date(2026, 9, 15)
CORE = list(BASIC_FIELDS)
STANDARD = STANDARD_FIELDS[:4]
RESULTS = RESULTS_FIELDS[:2]
FULL = CORE + STANDARD + RESULTS
KEYS = ["date_start", "date_stop", "campaign_id", "adset_id", "ad_id"]
FAILED = None  # what _run_job_to_completion returns for a job that did not build


@pytest.fixture(autouse=True)
def _fresh_process_state():
    AdsInsightStream._account_not_building = False
    AdsInsightStream._account_split_mode = False
    yield
    AdsInsightStream._account_not_building = False
    AdsInsightStream._account_split_mode = False


@pytest.fixture(autouse=True)
def sleep():
    with mock.patch("tap_facebook.streams.ad_insights.time.sleep") as patched:
        yield patched


def make_stream() -> AdsInsightStream:
    tap = TapFacebook(config=SAMPLE_CONFIG)
    stream = tap.streams["adsinsights"]
    stream._reset_run_state()
    return stream


def span_unit(parts: list[dict] | None = None) -> dict:
    unit = {
        "report_run_id": "span-1",
        "date": f"{START} to {UNTIL}",
        "date_obj": START,
        "until_obj": UNTIL,
        "next_date": UNTIL.add(days=1),
    }
    if parts is not None:
        unit["parts"] = parts
    return unit


def slice_unit(parts: list[dict] | None = None) -> dict:
    unit = {
        "report_run_id": "slice-1",
        "date": START.to_date_string(),
        "date_obj": START,
        "until_obj": None,
        "next_date": START.add(days=1),
    }
    if parts is not None:
        unit["parts"] = parts
    return unit


def built(rows: list[dict] | None = None) -> mock.Mock:
    job = mock.Mock(spec=AdReportRun)
    objects = []
    for row in rows or []:
        obj = AdsInsights()
        for key, value in row.items():
            obj[key] = value
        objects.append(obj)
    job.get_result.return_value = objects
    return job


def poll_script(script: dict[str, list]) -> t.Callable:
    """A `_poll_job_once` stand-in: outcomes per part label, consumed in order.

    An entry is FAILED (None) or a built job; a `...` entry means "still
    pending" for that round.
    """
    queues = {label: list(outcomes) for label, outcomes in script.items()}

    def poll(state: dict) -> tuple[str, object]:
        label = state["report_date"].split("[")[-1].rstrip("]")
        outcome = queues[label].pop(0)
        if outcome is ...:
            return "pending", None
        if outcome is None:
            return "failed", None
        return "completed", outcome

    return poll


def accepted(report_run_id: str) -> mock.Mock:
    response = mock.Mock()
    response.status.return_value = 200
    response.json.return_value = {"report_run_id": report_run_id}
    response._headers = {}
    return response


class TestHowTheColumnsAreSpreadOverReports:
    def test_outside_split_mode_it_is_one_report_with_every_column(self):
        stream = make_stream()
        assert stream._report_parts(FULL) == [("all", FULL)]

    def test_in_split_mode_the_core_comes_first_and_each_group_carries_the_join_keys(self):
        stream = make_stream()
        stream._split_mode = True

        parts = dict(stream._report_parts(FULL))

        assert list(parts) == ["core", "standard", "results"]
        assert parts["core"] == CORE, "the core already holds the keys"
        assert parts["standard"] == KEYS + STANDARD
        assert parts["results"] == KEYS + RESULTS

    def test_only_core_columns_means_nothing_to_split_even_in_split_mode(self):
        stream = make_stream()
        stream._split_mode = True
        assert stream._report_parts(CORE) == [("all", CORE)]
        assert stream._has_optional_columns(CORE) is False

    def test_the_join_keys_follow_the_report_level(self):
        stream = make_stream()
        with mock.patch.object(type(stream), "report_level", new_callable=mock.PropertyMock, return_value="campaign"):
            assert stream._split_key_fields() == ["date_start", "date_stop", "campaign_id"]


class TestTheProbeOpensTheWayToSplitMode:
    def test_the_probe_asks_for_the_core_metrics_only(self):
        stream = make_stream()

        with mock.patch.object(stream, "_run_job_to_completion", side_effect=[FAILED, FAILED, built()]), \
             mock.patch.object(stream, "_create_single_report", side_effect=["span-2", "probe-1"]) as create:
            list(stream._process_report_batch([span_unit()], FULL, 1))

        probe_call = create.call_args_list[1]
        assert probe_call.args[0] == UNTIL
        assert probe_call.args[1] == CORE, "a probe with every column would fail for the same reason as the span"

    def test_when_the_core_builds_the_window_is_asked_for_again_in_parts(self):
        stream = make_stream()

        with mock.patch.object(stream, "_run_job_to_completion", side_effect=[FAILED, FAILED, built()]), \
             mock.patch.object(stream, "_create_single_report", side_effect=["span-2", "probe-1"]):
            list(stream._process_report_batch([span_unit()], FULL, 1))

        assert stream._split_mode is True
        assert stream._split_from == START, "nothing was emitted, so the window restarts where it began"
        assert stream._span_mode is True, "a span of the core metrics is still one creation instead of 13-30"
        assert stream._span_failed_from is None
        assert AdsInsightStream._account_split_mode is True, "the other insights streams start in parts"
        assert AdsInsightStream._account_not_building is False
        assert stream._dates_failed == 0
        assert stream.stream_state[SPLIT_MODE_STATE_KEY] == pendulum.today().to_date_string()

    def test_when_even_the_core_fails_the_account_is_not_building(self):
        stream = make_stream()

        with mock.patch.object(stream, "_run_job_to_completion", side_effect=[FAILED, FAILED, FAILED]), \
             mock.patch.object(stream, "_create_single_report", side_effect=["span-2", "probe-1"]), \
             mock.patch("tap_facebook.streams.ad_insights.user_logger"):
            list(stream._process_report_batch([span_unit()], FULL, 1))

        assert AdsInsightStream._account_not_building is True
        assert stream._split_mode is False
        assert SPLIT_MODE_STATE_KEY not in stream.stream_state

    def test_already_in_split_mode_a_span_whose_parts_do_not_build_is_left_for_the_next_run(self):
        stream = make_stream()
        stream._split_mode = True
        parts = [
            {"name": "core", "columns": CORE, "report_run_id": "c-1"},
            {"name": "standard", "columns": KEYS + STANDARD, "report_run_id": "s-1"},
        ]
        # Round 1: core builds, standard fails. Retry: standard fails again.
        script = poll_script({"core": [built(), built()], "standard": [FAILED, FAILED]})

        with mock.patch.object(stream, "_poll_job_once", side_effect=script), \
             mock.patch.object(stream, "_create_single_report", return_value="s-2") as create, \
             mock.patch("tap_facebook.streams.ad_insights.user_logger") as user_log:
            rows = list(stream._process_report_batch([span_unit(parts)], FULL, 1))

        # Only the failed part was recreated, once, with its own columns and shape.
        assert create.call_count == 1
        assert create.call_args.args[1] == KEYS + STANDARD
        assert create.call_args.kwargs["until"] == UNTIL
        # No probe, no per-day burst (on TJaE that was 98 creations and a #613):
        # the window is a failed date and the next window is tried as a span again.
        assert rows == []
        assert stream._dates_failed == 1
        assert stream._span_mode is True
        assert stream._span_failed_from is None
        assert stream._split_from is None
        assert AdsInsightStream._account_not_building is False
        message = user_log.error.call_args.args[0]
        assert "built 1 of 2" in message and "standard" in message

    def test_a_source_with_only_core_columns_keeps_the_old_ladder(self):
        stream = make_stream()

        with mock.patch.object(stream, "_run_job_to_completion", side_effect=[FAILED, FAILED, built()]), \
             mock.patch.object(stream, "_create_single_report", side_effect=["span-2", "probe-1"]):
            list(stream._process_report_batch([span_unit()], CORE, 1))

        assert stream._split_mode is False
        assert stream._span_mode is False
        assert stream._span_failed_from == START


class TestAPerSliceDateThatKeepsFailingIsSplitBeforeAnyColumnIsDropped:
    def test_three_failures_in_a_row_enter_split_mode(self):
        stream = make_stream()

        with mock.patch.object(stream, "_run_job_to_completion", return_value=FAILED), \
             mock.patch.object(stream, "_create_single_report", return_value="slice-2"), \
             mock.patch.object(stream, "_drop_columns_failing_the_job") as bisect:
            list(stream._process_report_batch([slice_unit()], FULL, 1))

        assert stream._split_mode is True
        assert stream._split_from == START
        bisect.assert_not_called()
        assert stream._dates_failed == 0, "the date is retried in parts, not given up"

    def test_in_split_mode_a_date_that_keeps_failing_is_given_up_without_a_column_hunt(self):
        stream = make_stream()
        stream._split_mode = True
        parts = [
            {"name": "core", "columns": CORE, "report_run_id": "c-1"},
            {"name": "standard", "columns": KEYS + STANDARD, "report_run_id": "s-1"},
        ]
        script = poll_script({"core": [built()] * 3, "standard": [FAILED] * 3})

        with mock.patch.object(stream, "_poll_job_once", side_effect=script), \
             mock.patch.object(stream, "_create_single_report", return_value="s-2"), \
             mock.patch.object(stream, "_bisect_failing_columns") as bisect, \
             mock.patch("tap_facebook.streams.ad_insights.user_logger"), \
             mock.patch("tap_facebook.streams.ad_insights.internal_logger"):
            rows = list(stream._process_report_batch([slice_unit(parts)], FULL, 1))

        bisect.assert_not_called()
        assert rows == []
        assert stream._dates_failed == 1
        assert stream._rejected_columns == []

    def test_outside_split_mode_the_column_hunt_never_drops_a_whole_group_once_in_parts(self):
        stream = make_stream()
        stream._split_mode = True

        with mock.patch.object(stream, "_bisect_failing_columns", return_value=[]), \
             mock.patch("tap_facebook.streams.ad_insights.internal_logger"):
            dropped = stream._drop_columns_failing_the_job(START, FULL, 1)

        assert dropped is False, "emitting the row without a group's columns is a silently incomplete row"
        assert stream._rejected_columns == []

    def test_nothing_to_split_returns_false(self):
        stream = make_stream()
        assert stream._split_columns_failing_the_job(START, CORE) is False
        stream._split_mode = True
        assert stream._split_columns_failing_the_job(START, FULL) is False
        assert stream._split_from is None


class TestCreationInSplitMode:
    def test_one_creation_per_part_with_that_parts_fields(self):
        stream = make_stream()
        stream._split_mode = True
        stream._span_mode = False

        with mock.patch.object(
            stream, "_request_report_creation", side_effect=[accepted("c-1"), accepted("s-1"), accepted("r-1")]
        ) as request, mock.patch.object(stream, "_check_facebook_api_usage"), \
             mock.patch("tap_facebook.streams.ad_insights.user_logger") as user_log:
            units = stream._create_report_batch(
                start_date=START, batch_size=1, end_date=START, columns=FULL, time_increment=1
            )

        assert [call.args[0]["fields"] for call in request.call_args_list] == [CORE, KEYS + STANDARD, KEYS + RESULTS]
        assert all(call.args[0]["time_range"] == {"since": "2026-09-04", "until": "2026-09-04"} for call in request.call_args_list)
        assert len(units) == 1
        assert [part["report_run_id"] for part in units[0]["parts"]] == ["c-1", "s-1", "r-1"]
        assert units[0]["report_run_id"] == "c-1"
        assert "Queued 3 reports for 2026-09-04 (core, standard, results)" in user_log.info.call_args.args[0]

    def test_outside_split_mode_creation_is_unchanged(self):
        stream = make_stream()
        stream._span_mode = False

        with mock.patch.object(stream, "_request_report_creation", return_value=accepted("r-1")) as request, \
             mock.patch.object(stream, "_check_facebook_api_usage"):
            units = stream._create_report_batch(
                start_date=START, batch_size=1, end_date=START, columns=FULL, time_increment=1
            )

        assert request.call_count == 1
        assert request.call_args.args[0]["fields"] == FULL
        assert units[0]["parts"] == [{"name": "all", "columns": FULL, "report_run_id": "r-1"}]

    def test_a_part_that_cannot_be_created_leaves_the_period_out_of_the_batch(self):
        stream = make_stream()
        stream._split_mode = True
        stream._span_mode = False
        refused = mock.Mock()
        refused.status.return_value = 500
        refused._headers = {}

        with mock.patch.object(stream, "_request_report_creation", side_effect=[accepted("c-1"), refused]), \
             mock.patch.object(stream, "_check_facebook_api_usage"), \
             mock.patch("tap_facebook.streams.ad_insights.user_logger"):
            units = stream._create_report_batch(
                start_date=START, batch_size=1, end_date=START, columns=FULL, time_increment=1
            )

        assert units == [], "half a period is never queued"


class TestTheRowsOfThePartsAreCombined:
    def test_rows_are_joined_on_the_hash_id_and_keep_every_column(self):
        stream = make_stream()
        key = {"date_start": "2026-09-04", "date_stop": "2026-09-04", "campaign_id": "c", "adset_id": "s", "ad_id": "a"}
        other = {**key, "ad_id": "b"}
        parts = [
            {"name": "core", "columns": CORE, "report_run_id": "c-1"},
            {"name": "standard", "columns": KEYS + STANDARD, "report_run_id": "s-1"},
        ]
        jobs = [
            built([{**key, "spend": "1.5"}, {**other, "spend": "2.5"}]),
            built([{**other, "buying_type": "AUCTION"}, {**key, "buying_type": "RESERVED"}]),
        ]

        rows = stream._merge_part_results(parts, jobs, START.to_date_string())

        assert [row["ad_id"] for row in rows] == ["a", "b"], "the core report decides which rows exist and their order"
        assert rows[0]["spend"] == "1.5" and rows[0]["buying_type"] == "RESERVED"
        assert rows[1]["spend"] == "2.5" and rows[1]["buying_type"] == "AUCTION"
        assert rows[0]["id"] and rows[0]["id"] != rows[1]["id"]

    def test_a_row_only_an_optional_part_returned_is_dropped_not_invented(self):
        stream = make_stream()
        key = {"date_start": "2026-09-04", "date_stop": "2026-09-04", "campaign_id": "c", "adset_id": "s", "ad_id": "a"}
        ghost = {**key, "ad_id": "ghost"}
        parts = [
            {"name": "core", "columns": CORE, "report_run_id": "c-1"},
            {"name": "standard", "columns": KEYS + STANDARD, "report_run_id": "s-1"},
        ]

        with mock.patch("tap_facebook.streams.ad_insights.internal_logger") as internal_log:
            rows = stream._merge_part_results(parts, [built([key]), built([ghost])], START.to_date_string())

        assert [row["ad_id"] for row in rows] == ["a"]
        assert "1 row(s) of the optional parts" in internal_log.warning.call_args.args[0]

    def test_a_single_part_is_what_the_stream_always_emitted(self):
        stream = make_stream()
        key = {"date_start": "2026-09-04", "date_stop": "2026-09-04", "campaign_id": "c", "adset_id": "s", "ad_id": "a"}
        parts = [{"name": "all", "columns": FULL, "report_run_id": "r-1"}]

        rows = stream._merge_part_results(parts, [built([{**key, "spend": "1"}])], START.to_date_string())

        assert len(rows) == 1 and rows[0]["spend"] == "1" and "id" in rows[0]


class TestAPartThatDoesNotBuildFailsThePeriod:
    def test_only_the_failed_part_is_recreated_and_the_rows_wait_for_it(self):
        stream = make_stream()
        stream._split_mode = True
        key = {"date_start": "2026-09-04", "date_stop": "2026-09-04", "campaign_id": "c", "adset_id": "s", "ad_id": "a"}
        parts = [
            {"name": "core", "columns": CORE, "report_run_id": "c-1"},
            {"name": "standard", "columns": KEYS + STANDARD, "report_run_id": "s-1"},
        ]
        script = poll_script(
            {
                "core": [built([{**key, "spend": "1"}]), built([{**key, "spend": "1"}])],
                "standard": [FAILED, built([{**key, "buying_type": "AUCTION"}])],
            }
        )

        with mock.patch.object(stream, "_poll_job_once", side_effect=script) as poll, \
             mock.patch.object(stream, "_create_single_report", return_value="s-2") as create:
            rows = list(stream._process_report_batch([slice_unit(parts)], FULL, 1))

        assert create.call_count == 1
        assert create.call_args.args[1] == KEYS + STANDARD
        # Attempt 2 polls both parts again: the core job was built already but
        # its rows were not emitted alone.
        assert poll.call_count == 4
        assert len(rows) == 1 and rows[0]["buying_type"] == "AUCTION" and rows[0]["spend"] == "1"

    def test_a_part_that_never_builds_gives_the_date_up_without_a_partial_row(self):
        stream = make_stream()
        stream._split_mode = True
        parts = [
            {"name": "core", "columns": CORE, "report_run_id": "c-1"},
            {"name": "standard", "columns": KEYS + STANDARD, "report_run_id": "s-1"},
        ]
        script = poll_script({"core": [built()] * 3, "standard": [FAILED] * 3})

        with mock.patch.object(stream, "_poll_job_once", side_effect=script), \
             mock.patch.object(stream, "_create_single_report", return_value="s-2"), \
             mock.patch("tap_facebook.streams.ad_insights.user_logger"), \
             mock.patch("tap_facebook.streams.ad_insights.internal_logger"):
            rows = list(stream._process_report_batch([slice_unit(parts)], FULL, 1))

        assert rows == []
        assert stream._dates_failed == 1
        assert stream._rejected_columns == []


class TestThePartsArePolledInTurn:
    def test_a_slow_part_does_not_hold_up_the_others(self, sleep):
        stream = make_stream()
        parts = [
            {"name": "core", "columns": CORE, "report_run_id": "c-1"},
            {"name": "standard", "columns": KEYS + STANDARD, "report_run_id": "s-1"},
            {"name": "results", "columns": KEYS + RESULTS, "report_run_id": "r-1"},
        ]
        core_job, standard_job, results_job = built(), built(), built()
        script = poll_script({"core": [core_job], "standard": [..., ..., standard_job], "results": [..., results_job]})
        order: list[str] = []

        def poll(state):
            order.append(state["report_date"].split("[")[-1].rstrip("]"))
            return script(state)

        with mock.patch.object(stream, "_poll_job_once", side_effect=poll):
            jobs = stream._run_parts_to_completion(parts, START.to_date_string())

        # Round 1 checks all three; round 2 the two still pending; round 3 the last one.
        assert order == ["core", "standard", "results", "standard", "results", "standard"]
        assert jobs == [core_job, standard_job, results_job], "built jobs come back in part order"
        assert sleep.call_count == 2, "one wait per round, not one per part"

    def test_a_failed_part_is_dropped_from_the_rounds_and_the_period_is_not_built(self):
        stream = make_stream()
        parts = [
            {"name": "core", "columns": CORE, "report_run_id": "c-1"},
            {"name": "standard", "columns": KEYS + STANDARD, "report_run_id": "s-1"},
        ]
        script = poll_script({"core": [..., built()], "standard": [FAILED]})

        with mock.patch.object(stream, "_poll_job_once", side_effect=script) as poll, \
             mock.patch("tap_facebook.streams.ad_insights.internal_logger"):
            jobs = stream._run_parts_to_completion(parts, START.to_date_string())

        assert jobs is None
        assert parts[1]["report_run_id"] is None, "only the failed part is recreated later"
        assert parts[0]["report_run_id"] == "c-1"
        assert poll.call_count == 3

    def test_a_single_report_still_uses_the_plain_wait(self):
        stream = make_stream()
        parts = [{"name": "all", "columns": FULL, "report_run_id": "r-1"}]
        job = built()

        with mock.patch.object(stream, "_run_job_to_completion", return_value=job) as wait:
            assert stream._run_parts_to_completion(parts, START.to_date_string()) == [job]

        assert wait.call_args.kwargs["report_date"] == START.to_date_string()


class TestBigGroupsAreCutInHalves:
    def test_the_full_standard_group_becomes_two_parts(self):
        stream = make_stream()
        stream._split_mode = True
        columns = CORE + list(STANDARD_FIELDS)

        parts = dict(stream._report_parts(columns))

        assert list(parts) == ["core", "standard-1", "standard-2"]
        halves = [c for c in parts["standard-1"] if c not in KEYS] + [c for c in parts["standard-2"] if c not in KEYS]
        assert halves == list(STANDARD_FIELDS), "nothing lost, nothing duplicated, order kept"
        assert all(
            len([c for c in parts[name] if c not in KEYS]) <= SPLIT_PART_MAX_COLUMNS for name in ("standard-1", "standard-2")
        )
        assert parts["standard-2"][: len(KEYS)] == KEYS, "each half carries the join keys"


class TestBeingThrottledAfterQueuingIsNotAnEmptyAccount:
    def test_the_run_ends_red_instead_of_green_with_zero_rows(self):
        stream = make_stream()
        stream._reset_run_state()

        def process(batch_reports, columns, time_increment):
            stream._throttled_from = batch_reports[0]["date_obj"]
            return iter(())

        with mock.patch.object(stream, "_initialize_client"), \
             mock.patch.object(stream, "_get_start_date", return_value=START), \
             mock.patch.object(stream, "_create_report_batch", return_value=[slice_unit()]), \
             mock.patch.object(stream, "_process_report_batch", side_effect=process), \
             mock.patch.object(type(stream), "config", new_callable=mock.PropertyMock,
                               return_value={**SAMPLE_CONFIG, "end_date": START.to_date_string()}), \
             mock.patch("tap_facebook.streams.ad_insights.user_logger"), \
             mock.patch("tap_facebook.streams.ad_insights.internal_logger"), \
             pytest.raises(SystemExit):
            list(stream.get_records(None))

        assert stream._dates_failed >= 1


class TestTheVerdictIsRememberedBetweenRuns:
    def test_a_recent_marker_starts_the_run_in_split_mode(self):
        stream = make_stream()
        stream.stream_state[SPLIT_MODE_STATE_KEY] = pendulum.today().subtract(days=2).to_date_string()

        stream._restore_split_mode(None)

        assert stream._split_mode is True
        assert AdsInsightStream._account_split_mode is True

    def test_an_expired_marker_is_removed_and_the_full_report_is_tried_again(self):
        stream = make_stream()
        stream.stream_state[SPLIT_MODE_STATE_KEY] = pendulum.today().subtract(days=SPLIT_MODE_TTL_DAYS + 1).to_date_string()

        stream._restore_split_mode(None)

        assert stream._split_mode is False
        assert SPLIT_MODE_STATE_KEY not in stream.stream_state

    def test_another_stream_of_the_same_process_starts_in_split_mode(self):
        AdsInsightStream._account_split_mode = True
        stream = make_stream()

        stream._restore_split_mode(None)

        assert stream._split_mode is True

    def test_without_a_marker_the_full_report_is_the_default(self):
        stream = make_stream()
        stream._restore_split_mode(None)
        assert stream._split_mode is False


if __name__ == "__main__":
    pytest.main([__file__])
