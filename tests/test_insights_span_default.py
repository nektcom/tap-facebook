"""Unit tests for the single-window report being the DEFAULT request shape.

Fully offline: the report-creation request is mocked, so no credentials and no
calls against the ad account quota these tests are about.

Background (NEKT-5213): the insights streams used to create one async report per
date, and Facebook meters those per report CREATED, not per day of data. A 7-day
lookback across two insights streams was ~32 creations per run against a budget
of 5 per 6 hours. NEKT-5173 introduced the single-window ("span") report but only
as a reaction to being throttled; this makes it the normal shape, with one report
per slice as the fallback for when Facebook cannot build the span job.
"""

from __future__ import annotations

from http import HTTPStatus
from unittest import mock

import pendulum
import pytest

from tap_facebook.streams.ad_insights import (
    SPAN_MAX_SLICES,
    AdsInsightStream,
    BASIC_FIELDS,
)
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
    "enable_advanced_reports": True,
}

START = pendulum.date(2026, 9, 8)


def make_stream() -> AdsInsightStream:
    tap = TapFacebook(config=SAMPLE_CONFIG)
    stream = tap.streams["adsinsights"]
    stream._reset_run_state()
    return stream


def accepted_response() -> mock.Mock:
    response = mock.Mock()
    response.status.return_value = HTTPStatus.OK
    response.json.return_value = {"report_run_id": "report-1"}
    response._headers = {}
    return response


def creations(stream: AdsInsightStream, **kwargs) -> mock.Mock:
    """Run one batch with the creation call mocked, and hand back the mock."""
    defaults = {
        "start_date": START,
        "batch_size": 30,
        "end_date": START.add(days=6),
        "columns": list(BASIC_FIELDS),
        "time_increment": 1,
    }
    defaults.update(kwargs)
    with mock.patch.object(
        stream, "_trigger_async_insight_report_creation", return_value=accepted_response()
    ) as trigger:
        stream._create_report_batch(**defaults)
    return trigger


class TestSpanIsTheDefault:
    def test_a_fresh_run_starts_in_span_mode(self):
        stream = make_stream()

        assert stream._span_mode is True
        assert stream._span_disabled is False

    def test_a_seven_day_window_costs_one_creation(self):
        stream = make_stream()

        trigger = creations(stream)

        # The old shape asked for one report per date: seven calls for this
        # window, and thirty for a full batch. The quota is spent per creation.
        assert trigger.call_count == 1

    def test_the_single_report_spans_the_whole_window(self):
        stream = make_stream()

        trigger = creations(stream)

        params = trigger.call_args.kwargs["params"]
        assert params["time_range"] == {"since": "2026-09-08", "until": "2026-09-14"}

    def test_time_increment_is_untouched(self):
        """The whole equivalence claim rests on this: the API still slices by it."""
        stream = make_stream()

        trigger = creations(stream)

        assert trigger.call_args.kwargs["params"]["time_increment"] == 1

    def test_a_long_backfill_is_capped_at_the_slice_ceiling(self):
        stream = make_stream()

        trigger = creations(stream, end_date=START.add(days=120))

        assert trigger.call_count == 1
        until = trigger.call_args.kwargs["params"]["time_range"]["until"]
        assert until == START.add(days=SPAN_MAX_SLICES - 1).to_date_string()


class TestPerSliceIsTheFallback:
    def test_giving_up_on_span_switches_to_one_report_per_slice(self):
        stream = make_stream()
        stream._give_up_on_span(START, "2026-09-08 to 2026-09-14")

        assert stream._span_mode is False
        assert stream._span_disabled is True

        trigger = creations(stream)

        # Seven dates in the window, one report each, now that the span shape
        # was abandoned for this run.
        assert trigger.call_count == 7

    def test_the_fallback_asks_for_one_date_at_a_time(self):
        stream = make_stream()
        stream._give_up_on_span(START, "label")

        trigger = creations(stream)

        first = trigger.call_args_list[0].kwargs["params"]["time_range"]
        assert first == {"since": "2026-09-08", "until": "2026-09-08"}

    def test_span_is_never_re_entered_once_abandoned(self):
        stream = make_stream()
        stream._give_up_on_span(START, "label")

        creations(stream)

        assert stream._span_mode is False


if __name__ == "__main__":
    pytest.main([__file__])
