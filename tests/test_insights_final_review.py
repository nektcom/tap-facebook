"""Unit tests from the final pre-release review of v1.86-v1.88 (NEKT-5249, 2026-10-08).

Fully offline. These drive the real retry ladder of get_records (span job, retries,
probe) with a per-run budget of report creations, over consecutive daily runs.
"""

from __future__ import annotations

import copy
from unittest import mock

import pendulum
import pytest
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsinsights import AdsInsights

from tap_facebook.streams.ad_insights import MISSING_PERIODS_STATE_KEY, AdsInsightStream
from tap_facebook.tap import TapFacebook

_dt = pendulum.datetime
CONFIG = {
    "start_date": "2025-01-01T00:00:00Z",
    "access_token": "t",
    "account_id": "123",
    "report_definition": {"lookback_window": 7, "action_report_time": "impression"},
}


@pytest.fixture(autouse=True)
def _fresh():
    AdsInsightStream._account_not_building = False
    AdsInsightStream._account_split_mode = False
    AdsInsightStream._run_started_without_state = False
    AdsInsightStream._incomplete_without_history = []
    AdsInsightStream._not_completed = {}
    yield
    AdsInsightStream._account_not_building = False
    AdsInsightStream._account_split_mode = False
    AdsInsightStream._run_started_without_state = True


def today_is(day):
    return mock.patch("pendulum.today", lambda tz="local": _dt(day.year, day.month, day.day, tz=tz))


def make(stream_state, config=None):
    tap = TapFacebook(config=config or CONFIG, state={"bookmarks": {"adsinsights": copy.deepcopy(stream_state)}})
    stream = tap.streams["adsinsights"]
    stream._write_starting_replication_value(None)
    return stream


def carried(stream):
    st = copy.deepcopy(stream.get_context_state(None))
    for key in ("progress_markers", "starting_replication_value", "insights_last_served"):
        st.pop(key, None)
    return st


def label_range(label):
    label = str(label).split(" [")[0]
    a, _, b = label.partition(" to ")
    return pendulum.parse(a).date(), pendulum.parse(b or a).date()


def run_real_ladder(stream, *, budget, bad):
    """get_records with the real batching and retry ladder; `budget` creations, then #613.

    A job fails when its window overlaps `bad`; any other job builds and returns one row per day.
    """
    left = [budget]
    written = []

    def spend():
        if left[0] <= 0:
            stream._throttled = True
            stream._last_throttle_code = 613
            return False
        left[0] -= 1
        return True

    def queue(current_date, span_until, label, columns, ti):
        return [{"name": "all", "columns": columns, "report_run_id": label}] if spend() else []

    def is_bad(label):
        a, b = label_range(label)
        return a <= bad[1] and b >= bad[0]

    def run_parts(parts, report_date):
        if is_bad(report_date):
            parts[0]["report_run_id"] = None
            return None
        return [mock.Mock()]

    def merge(parts, jobs, report_date, fields=None):
        a, b = label_range(report_date)
        return [{"id": str(a.add(days=i)), "date_start": str(a.add(days=i))} for i in range((b - a).days + 1)]

    with (
        mock.patch.object(stream, "_initialize_client"),
        mock.patch.object(stream, "_queue_report_parts", side_effect=queue),
        mock.patch.object(stream, "_create_single_report", side_effect=lambda d, c, t, *, until=None, quiet=False: "id" if spend() else None),
        mock.patch.object(stream, "_run_parts_to_completion", side_effect=run_parts),
        mock.patch.object(stream, "_merge_part_results", side_effect=merge),
        mock.patch.object(stream, "_run_job_to_completion", side_effect=lambda report_instance=None, report_date=None, quiet=False: None if is_bad(report_date) else mock.Mock()),
        mock.patch("tap_facebook.streams.ad_insights.AdReportRun", mock.Mock),
        mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
        mock.patch("tap_facebook.streams.ad_insights.user_logger"),
        mock.patch("tap_facebook.streams.ad_insights.internal_logger"),
    ):
        for record in stream.get_records(None):
            stream._increment_stream_state(record, context=None)
            written.append(record["date_start"])
        stream._finalize_state(stream.get_context_state(None))
    return written


class TestAFailingMissingPeriodDoesNotHoldTheNewDatesBack:
    """The job of a kept period fails and its retry is refused by #613: it used to count no attempt and,
    asked for first, take every run's budget while the new dates waited."""

    @pytest.mark.parametrize("budget", [1, 2, 3, 4])
    def test_new_dates_advance_within_a_few_days(self, budget):
        start = pendulum.date(2026, 10, 1)
        st = {
            "replication_key": "date_start",
            "replication_key_value": str(start.subtract(days=1)),
            MISSING_PERIODS_STATE_KEY: [{"from": "2026-08-03", "until": "2026-08-09", "attempts": 0}],
        }
        for day in range(6):  # one run a day
            with today_is(start.add(days=day)):
                stream = make(st)
                run_real_ladder(stream, budget=budget, bad=(pendulum.date(2026, 8, 3), pendulum.date(2026, 8, 9)))
                st = carried(stream)
        # v1.85, without kept periods, reaches 2026-10-06 with the same budget.
        assert st["replication_key_value"] == "2026-10-06", (st["replication_key_value"], st[MISSING_PERIODS_STATE_KEY])


class TestAGapLeftByV185IsReadAfterTheUpgrade:
    """State written by v1.85, which walked past a failed window and relied on its lookback to read it again."""

    def test_the_day_v185_would_have_reread_is_extracted_or_kept(self):
        # v1.85 run on 2026-10-07: windows 05-25, 05-28, 05-31 (job failed, walked past), 06-03, 06-06, then #613.
        st = {
            "replication_key": "date_start",
            "replication_key_value": "2026-06-08",
            "insights_span_width": {"slices": 3, "since": "2026-10-07"},
        }
        written = set()
        for day in range(10):
            with today_is(pendulum.date(2026, 10, 7).add(days=day)):
                stream = make(st)
                written |= set(run_real_ladder(stream, budget=1000, bad=(pendulum.date(1999, 1, 1),) * 2))
                st = carried(stream)
        kept = any(p["from"] <= "2026-06-02" <= p["until"] for p in st.get(MISSING_PERIODS_STATE_KEY) or [])
        # v1.85's next run starts at 06-08 - 7 = 06-01 and writes 06-02.
        assert "2026-06-02" in written or kept


    def test_a_period_facebook_never_builds_slows_down_to_weekly_after_three_days(self):
        """Its job fails and the retry meets the budget: each day counts, so it ends asked for once a week."""
        start = pendulum.date(2026, 10, 1)
        st = {
            "replication_key": "date_start",
            "replication_key_value": str(start.subtract(days=1)),
            MISSING_PERIODS_STATE_KEY: [{"from": "2026-08-03", "until": "2026-08-09", "attempts": 0}],
        }
        for day in range(3):
            with today_is(start.add(days=day)):
                stream = make(st)
                run_real_ladder(stream, budget=2, bad=(pendulum.date(2026, 8, 3), pendulum.date(2026, 8, 9)))
                st = carried(stream)
        assert st[MISSING_PERIODS_STATE_KEY][0]["attempts"] == 3
        with today_is(start.add(days=3)):
            assert not AdsInsightStream._due_today(
                {"attempts": 3, "tried_on": pendulum.parse(st[MISSING_PERIODS_STATE_KEY][0]["tried_on"]).date()}
            )
