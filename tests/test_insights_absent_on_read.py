"""Unit tests for a read that names columns the built report does not hold.

Fully offline: the built report is a mock, so nothing here touches an ad
account or its quota.

Background (NEKT-5249, v1.83): on the accounts that refuse `adset_start` /
`adset_end` at read, the free re-read (v1.77) never worked. Leaving the refused
column out means naming the fields on the read, and Facebook then answers:

    (#100) Cannot include cost_per_objective_result, objective_results in fields
    param because they weren't there while creating the report run. All available
    values are: account_id, ..., ad_id, adset_id, campaign_id, date_start, ...

The report was built without those two fields although the request asked for
them. The key guard of v1.81 read every name in the message, saw the keys in the
"available values" list and gave the re-read up, so the stream recreated the
report -- facebook-ads-WYkS on 23/09/2026 spent five creations on one window and
extracted nothing. In the three days to 24/09/2026, 44 pipelines went down that
path, and all 18 pipelines whose insights had stopped on the account's quota
were among them (7rNi: 12 of its 17 built reports thrown away).
"""

from __future__ import annotations

from unittest import mock

import pendulum
import pytest
from facebook_business.exceptions import FacebookRequestError

from tap_facebook.streams.ad_insights import (
    AdsInsightStream,
    _columns_absent_from_report,
    _columns_named_in_error,
)
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
    "enable_advanced_reports": True,
    "include_insights_standard_fields": True,
}

KEYS = ["date_start", "date_stop", "campaign_id", "adset_id", "ad_id"]
COLUMNS = [
    *KEYS,
    "impressions",
    "spend",
    "adset_start",
    "adset_end",
    "cost_per_objective_result",
    "objective_results",
]
REPORT_DATE = "2026-09-16 to 2026-09-23"
DATE_OBJ = pendulum.date(2026, 9, 16)
PARTS = [{"name": "all", "columns": COLUMNS, "report_run_id": "report-1"}]
ROWS = [{"ad_id": "1", "date_start": "2026-09-16", "spend": "12.5"}]


@pytest.fixture(autouse=True)
def _fresh_process_state():
    AdsInsightStream._columns_refused_on_read = set()
    yield
    AdsInsightStream._columns_refused_on_read = set()


def make_stream() -> AdsInsightStream:
    tap = TapFacebook(config=SAMPLE_CONFIG)
    stream = tap.streams["adsinsights"]
    stream._reset_run_state()
    return stream


def fb_error(message: str, code: int = 100) -> FacebookRequestError:
    return FacebookRequestError(
        message="Call was not successful",
        request_context={},
        http_status=400,
        http_headers={},
        body=f'{{"error": {{"code": {code}, "type": "OAuthException", "message": "{message}"}}}}',
    )


def refusal(column: str) -> FacebookRequestError:
    return fb_error(f"(#100) Tried accessing nonexisting summary field ({column})")


def not_in_the_report(*names: str, apostrophe: str = "'") -> FacebookRequestError:
    """The message facebook-ads-WYkS got on 23/09/2026, trimmed to the columns used here."""
    available = [c for c in COLUMNS if c not in names and c != "adset_end"]
    return fb_error(
        f"(#100) Cannot include {', '.join(names)} in fields param because they weren{apostrophe}t "
        f"there while creating the report run. All available values are: {', '.join(available)}"
    )


class TestTheMessageNamesOnlyWhatTheReportLacks:
    def test_the_culprits_are_the_names_before_in_fields_param(self):
        message = not_in_the_report("cost_per_objective_result", "objective_results").api_error_message()
        assert _columns_absent_from_report(message, COLUMNS) == ["cost_per_objective_result", "objective_results"]

    def test_read_as_one_list_it_looks_like_an_echo_of_the_keys(self):
        """Why v1.81/1.82 gave the re-read up: the available list carries every key."""
        message = not_in_the_report("cost_per_objective_result", "objective_results").api_error_message()
        assert set(KEYS) <= set(_columns_named_in_error(message, COLUMNS))

    def test_a_curly_apostrophe_is_read_the_same(self):
        message = not_in_the_report("objective_results", apostrophe="’").api_error_message()
        assert _columns_absent_from_report(message, COLUMNS) == ["objective_results"]

    def test_any_other_message_names_nothing_absent(self):
        message = refusal("adset_end").api_error_message()
        assert _columns_absent_from_report(message, COLUMNS) == []


class TestTheReportIsReadAgainInsteadOfRebuilt:
    def test_the_wyks_sequence_ends_in_rows(self):
        """adset_end refused -> re-read -> "weren't there" -> re-read without them -> rows."""
        stream = make_stream()
        with mock.patch.object(
            stream,
            "_merge_part_results",
            side_effect=[not_in_the_report("cost_per_objective_result", "objective_results"), ROWS],
        ) as merge:
            got = stream._reread_without_refused_columns(
                refusal("adset_end"), PARTS, [mock.Mock()], COLUMNS, REPORT_DATE
            )

        assert got == ROWS
        assert merge.call_count == 2
        fields = merge.call_args.kwargs["fields"]
        assert "adset_end" not in fields
        assert "objective_results" not in fields
        assert "cost_per_objective_result" not in fields
        assert set(KEYS) <= set(fields)

    def test_only_the_refused_column_is_remembered_for_the_run(self):
        """The absent ones belong to that report: another stream's report may hold them."""
        stream = make_stream()
        with mock.patch.object(
            stream,
            "_merge_part_results",
            side_effect=[not_in_the_report("cost_per_objective_result", "objective_results"), ROWS],
        ):
            stream._reread_without_refused_columns(refusal("adset_end"), PARTS, [mock.Mock()], COLUMNS, REPORT_DATE)

        assert AdsInsightStream._columns_refused_on_read == {"adset_end"}

    def test_a_read_naming_only_absent_columns_is_read_again_without_telling_the_customer(self):
        """Later windows start with a named read and hit this first; nothing changes for the data."""
        stream = make_stream()
        with (
            mock.patch.object(stream, "_merge_part_results", return_value=ROWS) as merge,
            mock.patch("tap_facebook.streams.ad_insights.user_logger") as user,
        ):
            got = stream._reread_without_refused_columns(
                not_in_the_report("objective_results"), PARTS, [mock.Mock()], COLUMNS, REPORT_DATE
            )

        assert got == ROWS
        assert "objective_results" not in merge.call_args.kwargs["fields"]
        user.warning.assert_not_called()
        assert AdsInsightStream._columns_refused_on_read == set()

    def test_what_the_account_already_refused_stays_out_of_the_re_read(self):
        AdsInsightStream._columns_refused_on_read = {"adset_end", "adset_start"}
        stream = make_stream()
        with mock.patch.object(stream, "_merge_part_results", return_value=ROWS) as merge:
            stream._reread_without_refused_columns(
                not_in_the_report("objective_results"), PARTS, [mock.Mock()], COLUMNS, REPORT_DATE
            )

        assert merge.call_count == 1
        fields = merge.call_args.kwargs["fields"]
        assert "adset_end" not in fields
        assert "adset_start" not in fields

    def test_a_message_saying_a_key_is_absent_is_not_trusted(self):
        stream = make_stream()
        with mock.patch.object(stream, "_merge_part_results") as merge:
            got = stream._reread_without_refused_columns(
                not_in_the_report("date_start", "objective_results"), PARTS, [mock.Mock()], COLUMNS, REPORT_DATE
            )

        assert got is None
        merge.assert_not_called()


class TestTheBatchNoLongerRecreatesTheReport:
    """The same sequence through `_process_report_batch`, where the creation path lives."""

    def read(self, parts, jobs, report_date, fields=None):
        if fields is None:
            raise refusal("adset_end")
        if "objective_results" in fields or "cost_per_objective_result" in fields:
            raise not_in_the_report("cost_per_objective_result", "objective_results")
        if "adset_end" in fields or "adset_start" in fields:
            raise refusal("adset_start" if "adset_start" in fields else "adset_end")
        return ROWS

    def run_batch(self, stream):
        report = {"report_run_id": "r1", "date": REPORT_DATE, "date_obj": DATE_OBJ, "next_date": DATE_OBJ.add(days=8)}
        with (
            mock.patch.object(stream, "_run_parts_to_completion", return_value=[mock.Mock()]),
            mock.patch.object(stream, "_merge_part_results", side_effect=self.read),
            mock.patch.object(stream, "_recreate_failed_parts") as recreate,
            mock.patch.object(stream, "_create_single_report") as create,
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
        ):
            got = list(stream._process_report_batch([report], COLUMNS, 1))
        return got, recreate, create

    def test_the_rows_of_the_built_report_are_emitted(self):
        got, _, _ = self.run_batch(make_stream())
        assert got == ROWS

    def test_nothing_is_handed_to_the_recreation_path(self):
        stream = make_stream()
        _, recreate, create = self.run_batch(stream)
        assert stream._rejected_columns == []
        assert stream._restart_from is None
        recreate.assert_not_called()
        create.assert_not_called()

    def test_both_refused_columns_are_shared_with_the_other_streams(self):
        self.run_batch(make_stream())
        assert AdsInsightStream._columns_refused_on_read == {"adset_end", "adset_start"}


class TestARefusalThatStillEndsInARecreationIsShared:
    """When the re-read cannot absorb it, the other streams should not pay it again."""

    def test_the_refused_column_is_recorded_for_the_run(self):
        stream = make_stream()
        acted = stream._record_columns_refused_while_reading(refusal("adset_end"), COLUMNS, REPORT_DATE, DATE_OBJ)

        assert acted is True
        assert AdsInsightStream._columns_refused_on_read == {"adset_end"}

    def test_a_later_stream_leaves_it_out_of_the_creation(self):
        make_stream()._record_columns_refused_while_reading(refusal("adset_end"), COLUMNS, REPORT_DATE, DATE_OBJ)

        columns = make_stream()._get_selected_columns()

        assert "adset_end" not in columns
        assert "adset_start" in columns


class TestAccountQuotaDuringARetryEndsTheStream:
    """The retry path used to skip the window and walk on, like the queue path before v1.80."""

    def run(self, code: int):
        stream = make_stream()
        report = {"report_run_id": "r1", "date": "2026-09-16", "date_obj": DATE_OBJ, "next_date": DATE_OBJ.add(days=1)}

        def throttled_while_retrying(*args, **kwargs):
            stream._throttled = True
            stream._last_throttle_code = code
            stream._throttled_from = DATE_OBJ
            return iter([])

        with (
            mock.patch.object(stream, "_initialize_client"),
            mock.patch.object(stream, "_create_report_batch", return_value=[report]) as create,
            mock.patch.object(stream, "_process_report_batch", side_effect=throttled_while_retrying),
            mock.patch.object(
                stream, "_advance_batch", side_effect=lambda *a, **k: pendulum.today().date().add(days=1)
            ) as advance,
            mock.patch.object(stream, "_fail_if_nothing_extracted") as floor,
            mock.patch("tap_facebook.streams.ad_insights.time.sleep"),
        ):
            list(stream.get_records(None))
        return stream, create, advance, floor

    def test_a_613_ends_the_stream_instead_of_skipping_the_window(self):
        _, create, advance, _ = self.run(613)
        assert create.call_count == 1
        advance.assert_not_called()

    def test_the_window_still_counts_as_failed(self):
        stream, _, _, floor = self.run(613)
        assert stream._dates_failed == 1
        floor.assert_called_once()

    def test_an_app_level_throttle_keeps_the_old_behaviour(self):
        """Codes 4 and 17 are the shared app limit, which clears within the run."""
        _, _, advance, _ = self.run(4)
        assert advance.called


if __name__ == "__main__":
    pytest.main([__file__])
