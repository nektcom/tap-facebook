"""Stream class for AdInsights."""

from __future__ import annotations

import re
import sys
import time
import typing as t
from functools import lru_cache
from hashlib import md5
from http import HTTPStatus

import pendulum
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsactionstats import AdsActionStats
from facebook_business.adobjects.adshistogramstats import AdsHistogramStats
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookRequest
from facebook_business.exceptions import FacebookRequestError
from nekt_singer_sdk import typing as th
from nekt_singer_sdk.custom_logger import internal_logger, user_logger
from nekt_singer_sdk.streams.core import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL

from tap_facebook.api_helper import (
    CALL_THRESHOLD_PERCENTAGE,
    get_suggested_sleep_time,
    has_reached_api_limit,
)
from tap_facebook.client import FacebookSDKStream

# The set of fields this tap requests is an explicit allow-list, NOT "whatever
# the installed facebook-business SDK happens to expose".
#
# AdsInsights.Field grows with every SDK release (137 entries in 19.x, 222 in
# 25.x). Deriving the schema from it means a routine dependency bump silently
# widens the `fields` param sent to the Graph API and rewrites the output
# schema for every downstream consumer. That is what broke pipelines when
# facebook-business went 19 -> 25 (NEKT-3931).
#
# Adding a field here is a deliberate, reviewable schema change. Bumping the
# SDK on its own is not. New SDK fields are reported by the drift check in
# `_log_schema_drift` so they can be adopted intentionally.
BASIC_FIELDS = [
    "account_id",
    "account_name",
    "action_values",
    "actions",
    "ad_id",
    "ad_name",
    "adset_id",
    "adset_name",
    "campaign_id",
    "campaign_name",
    "clicks",
    "conversion_rate_ranking",
    "conversion_values",
    "conversions",
    "cost_per_action_type",
    "cost_per_conversion",
    "cpc",
    "cpm",
    "cpp",
    "ctr",
    "date_start",
    "date_stop",
    "engagement_rate_ranking",
    "estimated_ad_recall_rate",
    "estimated_ad_recallers",
    "frequency",
    "impressions",
    "inline_link_click_ctr",
    "inline_link_clicks",
    "inline_post_engagement",
    "outbound_clicks",
    "outbound_clicks_ctr",
    "purchase_roas",
    "quality_ranking",
    "reach",
    "spend",
    "unique_actions",
    "unique_clicks",
    "unique_conversions",
    "unique_ctr",
    "unique_link_clicks_ctr",
    "video_15_sec_watched_actions",
    "video_30_sec_watched_actions",
    "video_avg_time_watched_actions",
    "video_p100_watched_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p95_watched_actions",
    "video_play_actions",
    "video_thruplay_watched_actions",
]

# Optional field groups, beyond BASIC_FIELDS. Each maps to a Facebook capability
# an account may or may not hold, so they are opted into independently -- a
# single "give me everything" switch would demand messaging ads AND a product
# catalog AND beta access all at once, which almost no account has.
#
# STANDARD needs no special permissions or product setup; the rest do.
#
# Membership in the installed SDK's catalog is NOT enough to be listed in a group:
# the SDK keeps names the Graph API no longer serves, and one unacceptable name
# makes the API reject the whole `fields` param -- so a single dead entry zeroes
# out the entire stream. See REJECTED_FIELDS below for the ones already ruled out.
STANDARD_FIELDS = [
    "account_currency",
    "ad_click_actions",
    "ad_impression_actions",
    "adset_end",
    "adset_start",
    "average_purchases_conversion_value",
    "buying_type",
    "canvas_avg_view_percent",
    "canvas_avg_view_time",
    "conversion_lead_rate",
    "conversion_leads",
    "cost_per_15_sec_video_view",
    "cost_per_2_sec_continuous_video_view",
    "cost_per_6_sec_video_view",
    "cost_per_ad_click",
    "cost_per_conversion_lead",
    "cost_per_dda_countby_convs",
    "cost_per_estimated_ad_recallers",
    "cost_per_inline_link_click",
    "cost_per_inline_post_engagement",
    "cost_per_one_thousand_ad_impression",
    "cost_per_outbound_click",
    "cost_per_thruplay",
    "cost_per_unique_action_type",
    "cost_per_unique_click",
    "cost_per_unique_conversion",
    "cost_per_unique_inline_link_click",
    "cost_per_unique_outbound_click",
    "created_time",
    "creative_media_type",
    "full_view_impressions",
    "full_view_reach",
    "instagram_profile_visits",
    "instagram_upcoming_event_reminders_set",
    "instant_experience_clicks_to_open",
    "instant_experience_clicks_to_start",
    "instant_experience_outbound_clicks",
    "interactive_component_tap",
    "landing_page_view_actions_per_link_click",
    "landing_page_view_per_link_click",
    "landing_page_view_per_purchase_rate",
    "mobile_app_purchase_roas",
    "objective",
    "onsite_conversion_messaging_detected_purchase_deduped",
    "optimization_goal",
    "place_page_name",
    "purchase_per_landing_page_view",
    "purchases_per_link_click",
    "qualifying_question_qualify_answer_rate",
    "social_spend",
    "total_card_view",
    "unique_inline_link_click_ctr",
    "unique_inline_link_clicks",
    "unique_outbound_clicks",
    "unique_outbound_clicks_ctr",
    "unique_video_continuous_2_sec_watched_actions",
    "unique_video_view_15_sec",
    "updated_time",
    "video_6_sec_watched_actions",
    "video_continuous_2_sec_watched_actions",
    "video_play_curve_actions",
    "video_play_retention_0_to_15s_actions",
    "video_play_retention_20_to_60s_actions",
    "video_play_retention_graph_actions",
    "video_time_watched_actions",
    "video_view_per_impression",
    "website_ctr",
    "website_purchase_roas",
    "wish_bid",
]

MESSAGING_FIELDS = [
    "cost_per_message_delivered",
    "marketing_messages_click_rate_benchmark",
    "marketing_messages_cost_per_delivered",
    "marketing_messages_cost_per_link_btn_click",
    "marketing_messages_delivered",
    "marketing_messages_delivery_rate",
    "marketing_messages_link_btn_click",
    "marketing_messages_link_btn_click_rate",
    "marketing_messages_media_view_rate",
    "marketing_messages_phone_call_btn_click_rate",
    "marketing_messages_quick_reply_btn_click",
    "marketing_messages_quick_reply_btn_click_rate",
    "marketing_messages_read",
    "marketing_messages_read_rate",
    "marketing_messages_read_rate_benchmark",
    "marketing_messages_sent",
    "marketing_messages_spend",
    "marketing_messages_spend_currency",
    "messages_delivered",
    "messages_delivered_ctr",
    "read_rate",
]

COMMERCE_FIELDS = [
    "catalog_segment_actions",
    "catalog_segment_value",
    "catalog_segment_value_mobile_purchase_roas",
    "catalog_segment_value_omni_purchase_roas",
    "catalog_segment_value_website_purchase_roas",
    "converted_product_app_custom_event_fb_mobile_purchase",
    "converted_product_app_custom_event_fb_mobile_purchase_value",
    "converted_product_offline_purchase",
    "converted_product_offline_purchase_value",
    "converted_product_omni_purchase",
    "converted_product_omni_purchase_values",
    "converted_product_quantity",
    "converted_product_value",
    "converted_product_website_pixel_purchase",
    "converted_product_website_pixel_purchase_value",
    "converted_promoted_product_app_custom_event_fb_mobile_purchase",
    "converted_promoted_product_app_custom_event_fb_mobile_purchase_value",
    "converted_promoted_product_offline_purchase",
    "converted_promoted_product_offline_purchase_value",
    "converted_promoted_product_omni_purchase",
    "converted_promoted_product_omni_purchase_values",
    "converted_promoted_product_quantity",
    "converted_promoted_product_value",
    "converted_promoted_product_website_pixel_purchase",
    "converted_promoted_product_website_pixel_purchase_value",
    "product_group_retailer_id",
    "product_retailer_id",
    "product_views",
    "shops_assisted_purchases",
]

BETA_FIELDS = [
    "advanced_actions_28d_view",
    "advanced_reach_1d_lookback",
    "advanced_reach_28d_lookback",
    "advanced_reach_7d_lookback",
    "anchor_event_attribution_setting",
    "anchor_events_performance_indicator",
    "auction_bid",
    "auction_competitiveness",
    "auction_max_competitor_bid",
    "creative_diversity_data",
    "creative_diversity_label",
    "creative_diversity_score",
    "creative_fatigue_summary",
    "creative_fatigued_ads",
    "dda_countby_convs",
    "dda_results",
    "multi_event_conversion_attribution_setting",
    "opportunity_score_l4",
    "result_values_performance_indicator",
]

RESULTS_FIELDS = [
    "cost_per_objective_result",
    "cost_per_result",
    "link_clicks_per_results",
    "objective_result_rate",
    "objective_results",
    "result_rate",
    "results",
]

ATTRIBUTION_FIELDS = [
    "attribution_setting",
]

# Fields the installed SDK exposes but the Graph API refuses, with the reason it
# gave when asked (checked against v25.0 for NEKT-4527).
#
# THE BAR FOR THIS LIST: only fields that NO account can ever get. A field that
# merely fails for some accounts, some dates or some campaign objectives stays in
# its group -- the sync drops it at runtime for the accounts that cannot serve it
# (see _resume_after_rejection), so nobody loses a metric that works for them.
# `adset_start` / `adset_end` were listed here at first and moved back out for
# exactly that reason: they are refused only while reading results, and only on
# some accounts.
#
# These are in no group on purpose: the drift check reports unclassified SDK
# fields as candidates to adopt, and without this list they would be offered up
# again on every run. Re-add one only after a live request proves the API accepts it.
REJECTED_FIELDS = {
    "age_targeting": "retired after Graph API v19.0",
    "gender_targeting": "retired after Graph API v19.0",
    "labels": "retired after Graph API v19.0",
    "location": "retired after Graph API v19.0",
    "estimated_ad_recall_rate_lower_bound": "retired after Graph API v19.0",
    "estimated_ad_recall_rate_upper_bound": "retired after Graph API v19.0",
    "estimated_ad_recallers_lower_bound": "retired after Graph API v19.0",
    "estimated_ad_recallers_upper_bound": "retired after Graph API v19.0",
    "marketing_messages_website_add_to_cart": "not a valid insights field",
    "marketing_messages_website_initiate_checkout": "not a valid insights field",
    "marketing_messages_website_purchase": "not a valid insights field",
    "marketing_messages_website_purchase_values": "not a valid insights field",
    "configurable_attribution_action": "requires a customization_name filter",
    "configurable_attribution_actionvalue": "requires a customization_name filter",
    "configurable_audience_overlap_reach": "requires a customization_name filter",
    "configurable_reachbyfrequency_action": "requires a customization_name filter",
    "configurable_reachbyfrequency_converters_count": "requires a customization_name filter",
    "configurable_reachbyfrequency_impressions_cost": "requires a customization_name filter",
    "configurable_reachbyfrequency_impressions_count": "requires a customization_name filter",
    "configurable_reachbyfrequency_reach": "requires a customization_name filter",
    "total_postbacks": "cannot be combined with other fields",
    "total_postbacks_detailed": "cannot be combined with other fields",
    "total_postbacks_detailed_v4": "cannot be combined with other fields",
}

# Sub-properties of the AdsActionStats / AdsHistogramStats nested objects.
# Same rule: pinned so an SDK bump cannot reshape nested records.
ACTION_STATS_FIELDS = [
    "1d_click",
    "1d_ev",
    "1d_view",
    "28d_click",
    "28d_view",
    "7d_click",
    "7d_view",
    "action_brand",
    "action_canvas_component_id",
    "action_canvas_component_name",
    "action_carousel_card_id",
    "action_carousel_card_name",
    "action_category",
    "action_converted_product_id",
    "action_destination",
    "action_device",
    "action_event_channel",
    "action_link_click_destination",
    "action_location_code",
    "action_reaction",
    "action_target_id",
    "action_type",
    "action_video_asset_id",
    "action_video_sound",
    "action_video_type",
    "dda",
    "inline",
    "interactive_component_sticker_id",
    "interactive_component_sticker_response",
    "skan_click",
    "skan_click_second_postback",
    "skan_click_third_postback",
    "skan_view",
    "skan_view_second_postback",
    "skan_view_third_postback",
    "value",
]

HISTOGRAM_STATS_FIELDS = [
    "1d_click",
    "1d_ev",
    "1d_view",
    "28d_click",
    "28d_view",
    "7d_click",
    "7d_view",
    "action_brand",
    "action_canvas_component_id",
    "action_canvas_component_name",
    "action_carousel_card_id",
    "action_carousel_card_name",
    "action_category",
    "action_converted_product_id",
    "action_destination",
    "action_device",
    "action_event_channel",
    "action_link_click_destination",
    "action_location_code",
    "action_reaction",
    "action_target_id",
    "action_type",
    "action_video_asset_id",
    "action_video_sound",
    "action_video_type",
    "dda",
    "inline",
    "interactive_component_sticker_id",
    "interactive_component_sticker_response",
    "skan_click",
    "skan_click_second_postback",
    "skan_click_third_postback",
    "skan_view",
    "skan_view_second_postback",
    "skan_view_third_postback",
    "value",
]

POLL_JOB_SLEEP_TIME = 5
AD_REPORT_RETRY_TIME = 2 * 60
AD_REPORT_INCREMENT_SLEEP_TIME = 1

# Meta occasionally answers a poll with a non-JSON body (e.g. an edge/CDN error
# page); the facebook-business SDK treats it as a success and crashes inside its
# parser instead of raising a FacebookRequestError. Give up on the job instance
# after this many consecutive unreadable polls and let the report-retry ladder
# in _process_report_batch recreate it.
MAX_CONSECUTIVE_POLL_FAILURES = 5
INSIGHTS_MAX_WAIT_TO_START_SECONDS = 5 * 60
DEFAULT_INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS = 30 * 60
JOB_STALE_ERROR_MESSAGE = (
    "This is an intermittent error and may resolve itself on "
    "subsequent queries to the Facebook API. "
    "You should deselect fields from the schema that are not necessary, "
    "as that may help improve the reliability of the Facebook API."
)


VALID_GRANULARITIES = {"daily", "monthly"}

# Graph API error code returned when the `fields` param is not acceptable.
FIELDS_PARAM_ERROR_CODE = 100

# A job that dies at 0% is usually transient, so only start hunting for a bad
# field once the same date has failed this many times in a row.
CONSECUTIVE_FAILURES_BEFORE_BISECT = 3

# Ceiling on how many fields a single run may drop on its own. Past this, the
# run falls back to BASIC_FIELDS rather than shrinking the schema field by field.
MAX_AUTO_FIELD_DROPS = 3

# Graph API codes that mean "you are out of quota", not "your request is wrong":
# 4 app-level, 17 user-level, and 613 the per-ad-account custom limit the async
# insights reports are metered against ("Custom Analytics metrics exceeded the
# rate limit of N calls per M hours for this ad account").
THROTTLE_ERROR_CODES = (4, 17, 613)

# How many slices a single degraded report may cover. The quota is spent per
# report CREATED, not per day of data, so one report spanning the whole pending
# window costs one call instead of one per day. Capped so that a first backfill
# does not turn into a single job Facebook cannot build.
THROTTLE_SPAN_MAX_SLICES = 31

# Facebook says how long until the quota frees up, but that can be hours --
# longer than a run should sit idle, since the next scheduled run would get
# there sooner. Wait a little (a throttle is often a burst) and then spend the
# one call anyway.
THROTTLE_DEFAULT_WAIT_SECONDS = 60
THROTTLE_MAX_WAIT_SECONDS = 300

# Field names are word tokens, so the rejected ones can be read straight out of
# the API's own message. Matching whole tokens matters: a substring search for
# `estimated_ad_recall_rate` also hits `estimated_ad_recall_rate_lower_bound`.
_WORD_TOKEN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _columns_named_in_error(message: str, columns: list[str]) -> list[str]:
    """Return the requested columns Facebook named in an error message.

    Every #100 phrasing seen so far enumerates the offending fields, whatever the
    reason -- retired after a version, unknown name, needs an extra filter, or not
    combinable with others. Reading the names back lets the sync drop exactly those
    and keep going, instead of losing the whole stream to one dead field.
    """
    tokens = set(_WORD_TOKEN.findall(message))
    return [column for column in columns if column in tokens]


class AdsInsightStream(FacebookSDKStream):
    name = "adsinsights"
    replication_key = "date_start"
    api_sleep_time = 60

    @property
    def effective_granularity(self) -> str:
        """Return the resolved granularity for this stream.

        Falls back to 'daily' if the configured value is not recognized.
        """
        requested = self.config.get("performance_granularity", "daily")
        if requested in VALID_GRANULARITIES:
            return requested
        user_logger.warning(
            f"[{self.name}] Granularity '{requested}' is not supported. Falling back to 'daily'."
        )
        return "daily"

    @property
    def _effective_time_increment(self) -> int | str:
        """Return the Facebook API time_increment value based on granularity.

        For 'daily': uses time_increment_days from report_definition (default 1).
        For 'monthly': returns the string "monthly" (accepted by Facebook API).
        """
        if self.effective_granularity == "monthly":
            return "monthly"
        return self.config.get("report_definition", {}).get("time_increment_days", 1)

    def _advance_date(self, current_date: pendulum.Date, time_increment: int | str) -> pendulum.Date:
        """Advance the date by the appropriate amount based on granularity."""
        if self.effective_granularity == "monthly":
            return current_date.add(months=1).start_of("month")
        return current_date.add(days=time_increment)

    def _reset_run_state(self) -> None:
        """Clear the per-run bookkeeping the degradation and the floor rely on."""
        self._rejected_columns: list[str] = []
        self._restart_from: pendulum.Date | None = None
        self._auto_drops = 0
        self._dates_failed = 0
        self._throttled = False
        self._throttle_wait = 0
        self._span_mode = False
        self._span_disabled = False
        self._span_failed_from: pendulum.Date | None = None
        # Set when report creation is refused for quota while a batch is being
        # processed, so the caller can resume that window as a single report.
        self._throttled_from: pendulum.Date | None = None

    def _note_throttled(self, fb_err: FacebookRequestError, current_date: pendulum.Date) -> None:
        """Record that Facebook refused a report because the quota is spent.

        The caller stops the batch here rather than trying the remaining dates:
        each one is another call against a budget that is already empty, and
        they would all be refused identically.
        """
        self._throttled = True
        suggested = get_suggested_sleep_time(
            headers=fb_err.http_headers() or {},
            account_id=self.config["account_id"],
        )
        self._throttle_wait = min(
            max(suggested, THROTTLE_DEFAULT_WAIT_SECONDS),
            THROTTLE_MAX_WAIT_SECONDS,
        )
        internal_logger.warning(
            f"[{self.name}] Report creation for {current_date.to_date_string()} was throttled "
            f"(code {fb_err.api_error_code()}, subcode {fb_err.api_error_subcode()}): "
            f"{fb_err.api_error_message()}. Stopping here; Facebook suggests waiting "
            f"{suggested}s, this run will wait {self._throttle_wait}s.",
            exc_info=True,
        )

    def _enter_span_mode(self) -> bool:
        """Switch to one report for the whole pending window. True if it flipped.

        Facebook meters these reports per report created, not per day of data,
        so asking for the window in one go costs a single call while returning
        the same daily rows (`time_increment` still slices the result set).
        That is the only lever left once the quota is gone.
        """
        if self._span_mode or self._span_disabled:
            return False

        self._span_mode = True
        user_logger.warning(
            f"[{self.name}] Facebook is limiting how many performance reports this ad account "
            "may request right now. Asking for the whole period in a single report instead of "
            "one per day, which costs a single request. The data extracted is the same."
        )
        internal_logger.warning(
            f"[{self.name}] Throttled on report creation; degrading to one span report of up to "
            f"{THROTTLE_SPAN_MAX_SLICES} slice(s) for the rest of the run."
        )
        return True

    def _warn_throttle_is_unrecoverable(self) -> None:
        """Tell the customer the quota is gone and one request is already the floor."""
        user_logger.warning(
            f"[{self.name}] Facebook is still refusing performance reports for this ad account: its "
            "request limit is spent. The extraction already asks for the whole period in a single "
            "request, so there is nothing left to reduce on our side. Running this source less "
            "often, or having fewer tools query the same ad account, keeps the limit from being hit."
        )
        internal_logger.warning(
            f"[{self.name}] Throttled while already in span mode (or with span mode disabled); "
            "no further degradation is available."
        )

    def _give_up_on_span(self, resume_from: pendulum.Date, report_label: str) -> None:
        """Abandon the degraded report and go back to one report per slice.

        Splitting the range is what Facebook itself recommends for a job that is
        too heavy to build, so the span is never retried again in this run: that
        would only ping-pong between the two shapes, spending a call each time.
        """
        self._span_mode = False
        self._span_disabled = True
        self._span_failed_from = resume_from
        user_logger.warning(
            f"[{self.name}] The single report covering {report_label} was too heavy for Facebook to build. "
            "Falling back to one report per day, which may run into the account's request limit."
        )
        internal_logger.warning(
            f"[{self.name}] Span report for {report_label} failed to complete; span mode disabled for "
            f"the rest of the run, resuming per-slice from {resume_from.to_date_string()}."
        )

    def _wait_out_throttle(self) -> None:
        """Idle for the backoff recorded when the throttle was seen, if any."""
        if not self._throttle_wait:
            return
        internal_logger.info(f"[{self.name}] Waiting {self._throttle_wait}s before the span report.")
        time.sleep(self._throttle_wait)
        self._throttle_wait = 0

    def _fail_if_nothing_extracted(
        self,
        batches_attempted: int,
        reports_queued: int,
        records_emitted: int,
    ) -> None:
        """Abort the run when nothing was extracted AND something went wrong.

        Both halves matter. Ending cleanly with no records is indistinguishable
        from "the account had no delivery in this period", and a full-refresh
        load reads that as an empty snapshot -- overwriting a populated table.
        But an account that genuinely did not spend anything must still finish
        green, so a run where every date built fine is never failed here.
        """
        if not batches_attempted or records_emitted:
            return
        if not self._dates_failed and reports_queued:
            # Every date built and returned nothing: the account really is empty.
            return

        user_logger.error(
            f"[{self.name}] No data could be extracted in this run and {self._dates_failed} date(s) failed, "
            "so the existing data was left untouched rather than replaced with an empty result. "
            "Please contact Nekt support."
        )
        internal_logger.error(
            f"[{self.name}] {batches_attempted} batch(es) attempted, {reports_queued} report(s) queued, "
            f"{self._dates_failed} date(s) failed, 0 records emitted; failing the run so the loader does "
            "not overwrite the table with an empty snapshot."
        )
        sys.exit(1)

    def _record_columns_refused_while_reading(
        self,
        fb_err: FacebookRequestError,
        columns: list[str],
        report_date: str,
        date_obj: pendulum.Date,
    ) -> bool:
        """Note columns the API refused while the report was being read back.

        A field can pass report creation and still be refused when the results
        are fetched (e.g. "nonexisting summary field"), so the same remedy
        applies here. Returns True when the caller should stop and let the sync
        resume from this date with a narrower field set -- retrying the exact
        same columns only burns ten minutes to fail identically.
        """
        if fb_err.api_error_code() != FIELDS_PARAM_ERROR_CODE:
            return False

        message = fb_err.api_error_message() or str(fb_err)
        rejected = _columns_named_in_error(message, columns)
        if not rejected:
            return False

        self._rejected_columns = rejected
        self._restart_from = date_obj
        internal_logger.warning(
            f"[{self.name}] Graph API refused {len(rejected)} column(s) while reading the report "
            f"for {report_date}: {message}. Restarting from this date without them."
        )
        return True

    def _resume_after_rejection(
        self,
        columns: list[str],
        report_date: pendulum.Date,
    ) -> tuple[list[str], pendulum.Date]:
        """Drop the columns Facebook refused and say where to pick the sync back up.

        Keeping them is not an option: the API rejects the request as a whole
        rather than ignoring the offending field, so one dead name means the
        stream yields nothing at all. Resuming from `_restart_from` (set when the
        rejection happened mid-batch) keeps already-yielded dates from repeating.
        """
        resume_from = self._restart_from or report_date
        self._restart_from = None

        dropped = self._rejected_columns
        self._rejected_columns = []
        remaining = [column for column in columns if column not in set(dropped)]

        if not remaining:
            user_logger.error(
                f"[{self.name}] Facebook rejected every metric requested for this stream, so no data "
                "could be extracted. Please contact Nekt support."
            )
            internal_logger.error(
                f"[{self.name}] Every column was rejected by the Graph API; nothing left to request."
            )
            sys.exit(1)

        user_logger.warning(
            f"[{self.name}] Facebook is no longer serving {len(dropped)} of the requested metrics "
            f"({', '.join(dropped)}). The extraction continues without them, so their columns will be "
            "empty. No action is needed on your side -- contact Nekt support if you rely on them."
        )
        internal_logger.warning(
            f"[{self.name}] Retrying {resume_from.to_date_string()} with {len(remaining)} column(s) "
            f"after dropping: {', '.join(dropped)}"
        )
        return remaining, resume_from

    def _advance_batch(
        self,
        current_date: pendulum.Date,
        time_increment: int | str,
        batch_size: int,
        end_date: pendulum.Date,
    ) -> pendulum.Date:
        """Advance past every date a batch starting here would have covered."""
        next_date = current_date
        for _ in range(max(batch_size, 1)):
            next_date = self._advance_date(next_date, time_increment)
            if next_date > end_date:
                break
        return next_date

    def _get_time_range(self, current_date: pendulum.Date, until: pendulum.Date | None = None) -> dict:
        """Return the time_range dict for the Facebook API request.

        For 'daily': since and until are the same day.
        For 'monthly': since is the first day, until is the last day of the month.
        When `until` is given the report spans that whole range instead: the
        result is still sliced by `time_increment`, so the rows are the same
        ones the one-report-per-slice path would have produced.
        """
        if until is not None:
            return {
                "since": current_date.to_date_string(),
                "until": until.to_date_string(),
            }
        if self.effective_granularity == "monthly":
            return {
                "since": current_date.start_of("month").to_date_string(),
                "until": current_date.end_of("month").to_date_string(),
            }
        return {
            "since": current_date.to_date_string(),
            "until": current_date.to_date_string(),
        }

    @property
    def report_level(self) -> str:
        """Return the aggregation level for the insights report."""
        return self.config.get("report_definition", {}).get("level", "ad")

    @property
    def report_breakdowns(self) -> list[str] | None:
        return self.config.get("report_definition", {}).get("breakdowns")

    @property
    def primary_keys(self) -> list[str] | None:
        return ["id"]

    @primary_keys.setter
    def primary_keys(self, new_value: list[str] | None) -> None:
        """Set primary key(s) for the stream.

        Args:
            new_value: TODO
        """
        self._primary_keys = new_value

    # config key -> field group. BASIC_FIELDS is always included.
    OPTIONAL_FIELD_GROUPS: t.ClassVar[dict[str, list[str]]] = {
        "include_insights_standard_fields": STANDARD_FIELDS,
        "include_insights_messaging_fields": MESSAGING_FIELDS,
        "include_insights_commerce_fields": COMMERCE_FIELDS,
        "include_insights_beta_fields": BETA_FIELDS,
        "include_insights_results_fields": RESULTS_FIELDS,
        "include_insights_attribution_fields": ATTRIBUTION_FIELDS,
    }

    @property
    def enabled_field_groups(self) -> list[str]:
        """Names of the optional field groups enabled for this source."""
        return [key for key in self.OPTIONAL_FIELD_GROUPS if self.config.get(key, False)]

    @property
    def insights_fields(self) -> list[str]:
        """Insights fields to request: BASIC_FIELDS plus any enabled group.

        Filtered against the installed SDK so a field retired upstream is
        skipped rather than raising, and de-duplicated while preserving order.
        """
        available = AdsInsights._field_types  # noqa: SLF001
        selected: list[str] = list(BASIC_FIELDS)
        for key in self.enabled_field_groups:
            selected.extend(self.OPTIONAL_FIELD_GROUPS[key])
        seen: set[str] = set()
        return [f for f in selected if f in available and not (f in seen or seen.add(f))]

    @property
    def action_stats_fields(self) -> list[str]:
        """AdsActionStats sub-properties present in the installed SDK."""
        return [f for f in ACTION_STATS_FIELDS if f in AdsActionStats._field_types]  # noqa: SLF001

    @property
    def histogram_stats_fields(self) -> list[str]:
        """AdsHistogramStats sub-properties present in the installed SDK."""
        return [
            f
            for f in HISTOGRAM_STATS_FIELDS
            if f in AdsHistogramStats._field_types  # noqa: SLF001
        ]

    def _get_datatype(self, field: str) -> th.Type | None:
        d_type = AdsInsights._field_types[field]  # noqa: SLF001
        if d_type == "string":
            return th.StringType()
        if d_type.startswith("list"):
            if "AdsActionStats" in d_type:
                sub_props = [
                    th.Property(clean_field, th.StringType())
                    for clean_field in self.action_stats_fields
                ]
                return th.ArrayType(th.ObjectType(*sub_props))
            if "AdsHistogramStats" in d_type:
                sub_props = []
                for clean_field in self.histogram_stats_fields:
                    if AdsHistogramStats._field_types[clean_field] == "string":  # noqa: SLF001
                        sub_props.append(th.Property(clean_field, th.StringType()))
                    else:
                        sub_props.append(
                            th.Property(
                                clean_field,
                                th.ArrayType(th.IntegerType()),
                            ),
                        )
                return th.ArrayType(th.ObjectType(*sub_props))
            return th.ArrayType(th.ObjectType())
        user_logger.error(f"Type not found for field: {field}")
        sys.exit(1)

    def _log_schema_drift(self) -> None:
        """Report divergence between the curated groups and the installed SDK.

        Neither case is fatal. The groups are the contract, so an SDK bump adds
        nothing until someone opts in; this only makes the delta visible and
        names the setting that unlocks each part of it.
        """
        available = set(AdsInsights._field_types)  # noqa: SLF001
        known = set(BASIC_FIELDS).union(*self.OPTIONAL_FIELD_GROUPS.values())

        gone = sorted(known - available)
        if gone:
            user_logger.warning(
                f"[{self.name}] {len(gone)} curated field(s) no longer exist in the "
                f"installed facebook-business SDK and will be skipped: {', '.join(gone)}"
            )

        requested = set(self.insights_fields)
        for key, fields in self.OPTIONAL_FIELD_GROUPS.items():
            skipped = sorted(set(fields) & available - requested)
            if skipped:
                internal_logger.info(
                    f"[{self.name}] {len(skipped)} field(s) not requested. Set "
                    f"{key} to true to include them: {', '.join(skipped)}"
                )

        unclassified = sorted(available - known - set(REJECTED_FIELDS))
        if unclassified:
            internal_logger.info(
                f"[{self.name}] {len(unclassified)} field(s) offered by the installed "
                f"facebook-business SDK belong to no group and are unreachable. Add "
                f"them to a group in ad_insights.py to expose them: "
                f"{', '.join(unclassified)}"
            )

        excluded = sorted(set(REJECTED_FIELDS) & available)
        if excluded:
            internal_logger.info(
                f"[{self.name}] {len(excluded)} field(s) are exposed by the SDK but "
                f"deliberately excluded because the Graph API refuses them (see "
                f"REJECTED_FIELDS); do not re-add without a live check: {', '.join(excluded)}"
            )

    @property
    @lru_cache  # noqa: B019
    def schema(self) -> dict:
        self._log_schema_drift()
        properties: th.List[th.Property] = []
        properties.append(th.Property("id", th.StringType()))
        for field in self.insights_fields:
            properties.append(th.Property(field, self._get_datatype(field)))
        for breakdown in self.report_breakdowns:
            properties.append(th.Property(breakdown, th.StringType()))
        return th.PropertiesList(*properties).to_dict()

    def _check_facebook_api_usage(self, headers: str) -> None:
        should_sleep = has_reached_api_limit(
            headers=headers,
            account_id=self.config.get("account_id"),
        )
        if should_sleep:
            user_logger.warning(
                f"[{self.name}]Call count limit nearing threshold of {CALL_THRESHOLD_PERCENTAGE}%, sleeping for {self.api_sleep_time} seconds..."
            )
            time.sleep(self.api_sleep_time)
            self.api_sleep_time = min(self.api_sleep_time * 2, 300)  # Double the sleep time, but cap it at 5min
        else:
            # Reset sleep time
            self.api_sleep_time = 60

    def _trigger_async_insight_report_creation(self, account_id: str, params: dict) -> th.Any:

        request = FacebookRequest(
            node_id=f"act_{account_id}",
            method="POST",
            endpoint="/insights",
            api_type="EDGE",
            include_summary=False,
            api=self.facebook_api,
        )

        request.add_params(params)

        return request.execute()

    def _create_report_batch(
        self,
        start_date: pendulum.Date,
        batch_size: int,
        end_date: pendulum.Date,
        columns: list[str],
        time_increment: int | str,
    ) -> list[dict]:
        """Create a batch of report requests without waiting for completion.

        Args:
            start_date: Starting date for the batch
            batch_size: Number of reports to create in this batch
            end_date: End date (to not exceed)
            columns: Report columns
            time_increment: Days per report (int) or "monthly" for monthly aggregation

        Returns:
            List of report metadata dicts with report_run_id and date info
        """
        batch_reports = []
        current_date = start_date
        self._throttled = False

        if self._span_mode:
            # One report for the whole window, so the batch is a single request.
            batch_size = 1
            self._wait_out_throttle()

        for _ in range(batch_size):
            if current_date > end_date:
                break

            next_date = self._advance_date(current_date, time_increment)
            span_until = None
            if self._span_mode:
                next_date = self._advance_batch(current_date, time_increment, THROTTLE_SPAN_MAX_SLICES, end_date)
                span_until = min(next_date.subtract(days=1), end_date)

            params = {
                "level": self.report_level,
                "action_breakdowns": self.config.get("report_definition", {}).get("action_breakdowns"),
                "action_report_time": self.config.get("report_definition", {}).get("action_report_time"),
                "breakdowns": self.report_breakdowns,
                "fields": columns,
                "time_increment": time_increment,
                "limit": 100,
                "action_attribution_windows": [
                    self.config.get("report_definition", {}).get("action_attribution_windows_view"),
                    self.config.get("report_definition", {}).get("action_attribution_windows_click"),
                ],
                "time_range": self._get_time_range(current_date, span_until),
            }
            report_label = (
                f"{current_date.to_date_string()} to {span_until.to_date_string()}"
                if span_until is not None
                else current_date.to_date_string()
            )

            try:
                response = self._trigger_async_insight_report_creation(
                    params=params, account_id=self.config["account_id"]
                )

                self._check_facebook_api_usage(headers=response._headers)
                if response.status() == HTTPStatus.OK:
                    report_run_id = response.json()["report_run_id"]
                    batch_reports.append(
                        {
                            "report_run_id": report_run_id,
                            "date": report_label,
                            "date_obj": current_date,
                            "until_obj": span_until,
                            "next_date": next_date,
                        }
                    )
                    user_logger.info(f"[{self.name}] Queued report for {report_label}")
                else:
                    user_logger.warning(f"[{self.name}] Failed to queue report for {report_label}")
                    internal_logger.warning(
                        f"[{self.name}] Report creation for {report_label} returned "
                        f"HTTP {response.status()} instead of 200; no report_run_id was issued."
                    )

            except FacebookRequestError as fb_err:
                message = fb_err.api_error_message() or str(fb_err)
                rejected = (
                    _columns_named_in_error(message, columns)
                    if fb_err.api_error_code() == FIELDS_PARAM_ERROR_CODE
                    else []
                )

                if rejected:
                    # The same `fields` param goes out for every date in the batch,
                    # so the remaining dates would fail identically -- and would keep
                    # failing on every future batch. Hand the names back to the caller,
                    # which retries this same date without them.
                    self._rejected_columns = rejected
                    internal_logger.warning(
                        f"[{self.name}] Graph API rejected the fields param for "
                        f"{current_date.to_date_string()} (code {fb_err.api_error_code()}): {message}. "
                        f"Dropping {len(rejected)} column(s) and retrying the batch: {', '.join(rejected)}"
                    )
                    break

                if fb_err.api_error_code() in THROTTLE_ERROR_CODES:
                    # Out of quota. Every remaining date in this batch is another
                    # call against an empty budget, and would be refused the same
                    # way, so stop here and let the caller ask for the window in
                    # one report instead.
                    self._note_throttled(fb_err, current_date)
                    break

                user_logger.warning(
                    f"[{self.name}] Error queueing report for {report_label}: {fb_err.api_error_message()}"
                )
                internal_logger.warning(
                    f"[{self.name}] Report creation failed for {report_label} "
                    f"(code {fb_err.api_error_code()}, subcode {fb_err.api_error_subcode()}, "
                    f"HTTP {fb_err.http_status()}): {message}",
                    exc_info=True,
                )

            current_date = next_date

        return batch_reports

    def _create_single_report(
        self,
        date: pendulum.Date,
        columns: list[str],
        time_increment: int | str,
        *,
        until: pendulum.Date | None = None,
        quiet: bool = False,
    ) -> str | None:
        """Create a single async report job. Returns report_run_id or None on failure.

        `quiet` keeps the customer-facing log clean while the sync is probing
        field subsets: those jobs are diagnostics, not work the customer asked
        for, so their failures belong on the internal channel only.

        `until` re-creates a report that spans a whole range rather than a single
        slice -- without it a degraded report would silently be retried as its
        first day only.
        """
        channel = internal_logger if quiet else user_logger
        params = {
            "level": self.report_level,
            "action_breakdowns": self.config.get("report_definition", {}).get("action_breakdowns"),
            "action_report_time": self.config.get("report_definition", {}).get("action_report_time"),
            "breakdowns": self.report_breakdowns,
            "fields": columns,
            "time_increment": time_increment,
            "limit": 100,
            "action_attribution_windows": [
                self.config.get("report_definition", {}).get("action_attribution_windows_view"),
                self.config.get("report_definition", {}).get("action_attribution_windows_click"),
            ],
            "time_range": self._get_time_range(date, until),
        }
        label = f"{date} to {until}" if until is not None else f"{date}"
        try:
            response = self._trigger_async_insight_report_creation(
                params=params, account_id=self.config["account_id"]
            )
            self._check_facebook_api_usage(headers=response._headers)
            if response.status() == HTTPStatus.OK:
                return response.json()["report_run_id"]
            channel.warning(f"[{self.name}] Failed to queue retry report for {label}")
        except FacebookRequestError as fb_err:
            if fb_err.api_error_code() in THROTTLE_ERROR_CODES:
                # Out of quota, not a bad request. Record it and stay quiet on the
                # customer channel: the caller decides what to do about it and
                # emits the single message that explains the whole run, instead of
                # one line per refused date.
                self._note_throttled(fb_err, date)
                return None
            channel.warning(f"[{self.name}] Error queueing retry report for {label}: {fb_err.api_error_message()}")
            internal_logger.warning(
                f"[{self.name}] Retry report creation failed for {label} "
                f"(code {fb_err.api_error_code()}, subcode {fb_err.api_error_subcode()}, "
                f"HTTP {fb_err.http_status()}): {fb_err.api_error_message()}",
                exc_info=True,
            )
        return None

    def _job_completes_with(
        self,
        date_obj: pendulum.Date,
        columns: list[str],
        time_increment: int | str,
    ) -> bool:
        """Ask Facebook to build one report with `columns` and say whether it survived."""
        report_run_id = self._create_single_report(date_obj, columns, time_increment, quiet=True)
        if not report_run_id:
            return False
        job = self._run_job_to_completion(
            report_instance=AdReportRun(report_run_id),
            report_date=date_obj.to_date_string(),
            quiet=True,
        )
        return isinstance(job, AdReportRun)

    def _bisect_failing_columns(
        self,
        date_obj: pendulum.Date,
        columns: list[str],
        time_increment: int | str,
    ) -> list[str]:
        """Find which optional column makes the report job die, by halving.

        Facebook says nothing useful when a job fails -- no field name, no
        reason, just 0%. So the only way to learn which column is at fault is to
        ask again with fewer of them. The search stays inside the optional
        groups: BASIC_FIELDS is the contract every source depends on, and if it
        alone cannot be built then no amount of dropping will help.

        Returns the offending column, or an empty list when the cause is not a
        single optional field (the caller then falls back to BASIC_FIELDS).
        """
        basic = [column for column in columns if column in set(BASIC_FIELDS)]
        suspects = [column for column in columns if column not in set(BASIC_FIELDS)]
        if not suspects:
            return []

        date_str = date_obj.to_date_string()
        internal_logger.info(
            f"[{self.name}] Bisecting {len(suspects)} optional column(s) on {date_str} to find what "
            "makes the report job fail."
        )

        if not self._job_completes_with(date_obj, basic, time_increment):
            internal_logger.warning(
                f"[{self.name}] BASIC_FIELDS alone also fails for {date_str}; the job failure is not "
                "caused by an optional field. Leaving the field set untouched."
            )
            return []

        probes = 1
        while len(suspects) > 1:
            half = suspects[: len(suspects) // 2]
            probes += 1
            # Only the first half needs a probe: with a single culprit, a half
            # that builds means the culprit is in the other one. Two culprits
            # simply cost a second bisect once the first has been dropped.
            suspects = suspects[len(half) :] if self._job_completes_with(date_obj, basic + half, time_increment) else half

        probes += 1
        if not self._job_completes_with(date_obj, [c for c in columns if c not in set(suspects)], time_increment):
            internal_logger.warning(
                f"[{self.name}] Dropping {suspects} did not make {date_str} build after {probes} probe(s); "
                "the failure is an interaction between fields, not one field."
            )
            return []

        internal_logger.info(f"[{self.name}] Bisect isolated '{suspects[0]}' on {date_str} after {probes} probe(s).")
        return suspects

    def _drop_columns_failing_the_job(
        self,
        date_obj: pendulum.Date,
        columns: list[str],
        time_increment: int | str,
    ) -> bool:
        """Take the column(s) that keep killing this date out of the request.

        Returns True when the caller should stop and let the sync resume from
        this date with a narrower set. A repeated job failure is otherwise a dead
        end: the tap re-sends the identical request ten times, a minute apart,
        and the date is lost anyway -- which is what stalled entire syncs before.
        """
        if self._auto_drops >= MAX_AUTO_FIELD_DROPS:
            return False

        if self._throttled:
            # A refused report says nothing about the columns. Probing now would
            # spend what is left of the quota and then blame the fields for it.
            internal_logger.warning(
                f"[{self.name}] Skipping the column bisect for {date_obj.to_date_string()}: the job "
                "failures are a spent ad account quota, not a rejected field."
            )
            return False

        dropped = self._bisect_failing_columns(date_obj, columns, time_increment)
        if self._throttled:
            # The quota ran out during the probes, so their failures are not
            # evidence against any column. Leave the field set untouched.
            internal_logger.warning(
                f"[{self.name}] Column bisect for {date_obj.to_date_string()} was cut short by the ad "
                "account quota; the field set is left as it is."
            )
            return False
        if not dropped:
            # Not one field, or not a field at all. Fall back to the contract set
            # once, so the run still delivers the core metrics for every date.
            optional = [column for column in columns if column not in set(BASIC_FIELDS)]
            if not optional:
                return False
            dropped = optional
            internal_logger.warning(
                f"[{self.name}] Could not isolate a single column for {date_obj.to_date_string()}; "
                f"falling back to BASIC_FIELDS by dropping {len(optional)} optional column(s)."
            )

        self._auto_drops += 1
        self._rejected_columns = dropped
        self._restart_from = date_obj
        return True

    def _process_report_batch(
        self,
        batch_reports: list[dict],
        columns: list[str],
        time_increment: int | str,
    ) -> t.Iterator[dict]:
        """Process a batch of reports, waiting for all to complete and yielding results.

        Args:
            batch_reports: List of report metadata from _create_report_batch
            columns: Report columns (used when retrying failed jobs)
            time_increment: Days per report (used when retrying failed jobs)

        Yields:
            Individual insight records
        """
        user_logger.info(f"[{self.name}] Processing batch of {len(batch_reports)} reports...")
        fail_on_error = self.config.get("fail_on_job_error", False)
        max_retries = 10

        for report_info in batch_reports:
            report_run_id = report_info["report_run_id"]
            report_date = report_info["date"]
            date_obj = report_info["date_obj"]
            span_until = report_info.get("until_obj")
            job_failures = 0

            for attempt in range(max_retries + 1):
                if attempt > 0:
                    user_logger.info(
                        f"[{self.name}] Retrying job for {report_date} (attempt {attempt}/{max_retries}), waiting 60s..."
                    )
                    time.sleep(60)
                    report_run_id = self._create_single_report(date_obj, columns, time_increment, until=span_until)
                    if not report_run_id:
                        if self._throttled:
                            # The ad account's budget is spent. The nine attempts
                            # left here, and every later date in this batch, are
                            # nine more refused calls that keep it spent: stop and
                            # let the caller ask for the rest in one report.
                            self._throttled_from = date_obj
                            return
                        continue

                job = self._run_job_to_completion(
                    report_instance=AdReportRun(report_run_id),
                    report_date=report_date,
                )
                if not isinstance(job, AdReportRun):
                    if span_until is not None:
                        # A span report is only ever used to survive a throttle.
                        # If Facebook cannot build it, splitting the range back
                        # into one report per slice is the documented remedy --
                        # and cheaper than retrying a job that is too heavy.
                        self._give_up_on_span(date_obj, report_date)
                        return
                    job_failures += 1
                    if job_failures >= CONSECUTIVE_FAILURES_BEFORE_BISECT and self._drop_columns_failing_the_job(
                        date_obj, columns, time_increment
                    ):
                        return
                    continue

                try:
                    records = []
                    for obj in job.get_result():
                        if isinstance(obj, AdsInsights):
                            obj["id"] = self._generate_hash_id(adinsight=obj, report_breakdowns=self.report_breakdowns)
                            records.append(obj.export_all_data())
                        else:
                            user_logger.warning(f"[{self.name}] Unexpected result type for {report_date}")
                    yield from records
                    break
                except FacebookRequestError as fb_err:
                    if self._record_columns_refused_while_reading(fb_err, columns, report_date, date_obj):
                        return

                    user_logger.warning(
                        f"[{self.name}] Error reading results for {report_date} (attempt {attempt}/{max_retries}): "
                        f"{fb_err.api_error_message()}. Retrying..."
                    )
                    internal_logger.warning(
                        f"[{self.name}] Reading report {report_run_id} for {report_date} failed "
                        f"(code {fb_err.api_error_code()}, HTTP {fb_err.http_status()}): "
                        f"{fb_err.api_error_message()}",
                        exc_info=True,
                    )
                except Exception as e:
                    user_logger.warning(
                        f"[{self.name}] Error reading results for {report_date} (attempt {attempt}/{max_retries}): {e}. Retrying..."
                    )
            else:
                # End of the ladder for this date:
                #   job fails -> retry
                #   -> CONSECUTIVE_FAILURES_BEFORE_BISECT in a row: bisect, drop the
                #      offending column, resume the date with a narrower set
                #   -> still failing after max_retries: give up on the date (here)
                #      -> fail_on_job_error=True: stop the run now (strict; the
                #         customer prefers no data over a gap in the history)
                #      -> default: skip the date and keep going
                #   -> end of run: _fail_if_nothing_extracted is the floor that
                #      keeps an all-failed run from overwriting the table with an
                #      empty snapshot.
                if span_until is not None:
                    # Same reasoning as a span job that never builds: go back to
                    # one report per slice rather than write the whole range off.
                    self._give_up_on_span(date_obj, report_date)
                    return
                self._dates_failed += 1
                msg = (
                    f"[{self.name}] Insights report job failed for {report_date} after {max_retries} retries. "
                    "Data for this date was not extracted. See logs above for the specific error."
                )
                user_logger.error(msg)
                if fail_on_error:
                    sys.exit(1)

    def _run_job_to_completion(
        self,
        report_instance: AdReportRun,
        report_date: str,
        *,
        quiet: bool = False,
    ) -> th.Any:
        status = None
        time_start = time.time()
        max_wait = self.config.get("insights_max_wait_to_finish_seconds", DEFAULT_INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS)
        channel = internal_logger if quiet else user_logger
        consecutive_poll_failures = 0

        while status != "Job Completed":
            duration = time.time() - time_start
            try:
                job = report_instance.api_get()
                status = job[AdReportRun.Field.async_status]
                percent_complete = job[AdReportRun.Field.async_percent_completion]
                job_id = job["id"]
            except FacebookRequestError:
                # Structured API errors (rate limits, auth) keep their existing
                # handling upstream in get_records.
                raise
            except Exception as poll_error:
                consecutive_poll_failures += 1
                if consecutive_poll_failures >= MAX_CONSECUTIVE_POLL_FAILURES:
                    channel.error(
                        f"[{self.name}] Could not check the insights report status for {report_date} "
                        "after several attempts. The report will be retried from scratch."
                    )
                    internal_logger.error(
                        f"[{self.name}] Polling api_get() failed {consecutive_poll_failures}x in a row "
                        f"for {report_date}; giving up on this job instance: {poll_error!r}",
                        exc_info=True,
                    )
                    return None
                internal_logger.warning(
                    f"[{self.name}] Unreadable response while polling insights job for {report_date} "
                    f"(attempt {consecutive_poll_failures}/{MAX_CONSECUTIVE_POLL_FAILURES}): {poll_error!r}",
                    exc_info=True,
                )
                time.sleep(min(POLL_JOB_SLEEP_TIME * consecutive_poll_failures, 60))
                continue
            consecutive_poll_failures = 0
            channel.info(f"[{self.name}] ID: {job_id} - {status} for {report_date} - {percent_complete}% done. ")

            if status == "Job Completed":
                return job
            if status == "Job Failed":
                channel.error(f"[{self.name}] Insights job {job_id} failed for {report_date}. " + JOB_STALE_ERROR_MESSAGE)
                return
            if duration > INSIGHTS_MAX_WAIT_TO_START_SECONDS and percent_complete == 0:
                channel.error(
                    f"[{self.name}] Insights job {job_id} did not start after {duration:.0f} seconds for {report_date}. "
                    + JOB_STALE_ERROR_MESSAGE
                )
                return
            if duration > max_wait:
                channel.error(
                    f"[{self.name}] Insights job {job_id} did not complete after {max_wait}s for {report_date}. "
                    f"To fix this, increase 'insights_max_wait_to_finish_seconds' in the tap config (current: {max_wait}s)."
                )
                return

            internal_logger.info(f"[{self.name}] Sleeping for {POLL_JOB_SLEEP_TIME} seconds until job is done")
            time.sleep(POLL_JOB_SLEEP_TIME)
        user_logger.error(f"[{self.name}] Job failed to complete for unknown reason")
        sys.exit(1)

    def _get_selected_columns(self) -> list[str]:
        columns = [keys[1] for keys, data in self.metadata.items() if data.selected and len(keys) > 0]
        if not columns:
            columns = list(self.schema["properties"])

        # pop ID, since it's auto-generated
        if "id" in columns:
            columns.remove("id")

        # Fields the source was told to stop asking for. They stay in the schema
        # so the column does not disappear from the warehouse -- it just arrives
        # empty. This is the manual counterpart of the automatic drop: once a
        # field is known to break an account, listing it here saves the sync from
        # rediscovering it on every run.
        excluded = set(self.config.get("insights_excluded_fields") or [])
        if excluded:
            internal_logger.info(
                f"[{self.name}] {len(excluded)} field(s) excluded by configuration: {', '.join(sorted(excluded))}"
            )
            columns = [column for column in columns if column not in excluded]

        # don't pass along columns that are part of breakdowns
        return [column for column in columns if column not in self.report_breakdowns]

    def _get_start_date(
        self,
        context: dict | None,
    ) -> pendulum.Date:
        lookback_window = self.config.get("report_definition", {}).get("lookback_window")
        config_start_date = pendulum.parse(self.config["start_date"]).date()
        if incremental_start_date := self.get_starting_replication_key_value(context):
            incremental_start_date = pendulum.parse(incremental_start_date).date()
        else:
            incremental_start_date = config_start_date

        if self.replication_method == REPLICATION_FULL_TABLE or config_start_date == incremental_start_date:
            report_start = config_start_date
            user_logger.info(f"[{self.name}] Using configured start date as report start filter {report_start}.")
        else:
            lookback_start_date = incremental_start_date.subtract(days=lookback_window)
            user_logger.info(
                f"[{self.name}] Incremental sync, applying lookback '{lookback_window}' to the "
                f"bookmark start date '{incremental_start_date}'. Syncing "
                f"reports starting on '{lookback_start_date}'."
            )
            report_start = lookback_start_date

        # Facebook store metrics maximum of 37 months old. Any time range that
        # older that 37 months from current date would result in 400 Bad request
        # HTTP response.
        # https://developers.facebook.com/docs/marketing-api/reference/ad-account/insights/#overview
        today = pendulum.today().date()
        oldest_allowed_start_date = today.subtract(months=37)
        if report_start < oldest_allowed_start_date:
            report_start = oldest_allowed_start_date
            user_logger.warning(
                f"[{self.name}] Report start date '{report_start}' is older than 37 months. "
                f"Using oldest allowed start date '{oldest_allowed_start_date}' instead."
            )
        return report_start

    def _generate_hash_id(self, adinsight: AdsInsights, report_breakdowns: list[str]):
        # Extract the relevant properties from the AdsInsights object
        date_start = adinsight.get("date_start", "")
        campaign_id = adinsight.get("campaign_id", "")
        adset_id = adinsight.get("adset_id", "")
        ad_id = adinsight.get("ad_id", "")

        # Get breakdown values for each breakdown field
        breakdown_values = []
        for breakdown in report_breakdowns:
            breakdown_values.append(str(adinsight.get(breakdown, "")))
        breakdown_string = "-".join(breakdown_values)

        hash_object = md5(f"{date_start}-{campaign_id}-{adset_id}-{ad_id}-{breakdown_string}".encode())
        return hash_object.hexdigest()

    def get_records(
        self,
        context: dict | None,
    ) -> t.Iterable[dict | tuple[dict, dict | None]]:
        self._initialize_client()
        time_increment = self._effective_time_increment

        if self.effective_granularity != "daily":
            user_logger.info(f"[{self.name}] Using '{self.effective_granularity}' granularity.")

        sync_end_date = pendulum.parse(
            self.config.get("end_date", pendulum.today().to_date_string()),
        ).date()

        report_date = self._get_start_date(context)

        # For monthly granularity, align start date to the first day of the month
        if self.effective_granularity == "monthly":
            report_date = report_date.start_of("month")

        columns = self._get_selected_columns()

        retry_count = 0
        batch_size = self.config.get("ad_insights_report_batch_size") or 30
        self._reset_run_state()
        batches_attempted = 0
        reports_queued = 0
        records_emitted = 0

        # Use batch processing for parallel report creation
        while report_date <= sync_end_date:
            if retry_count > 10:
                user_logger.error(f"[{self.name}] Failed to get insights after 10 retries. Stopping execution.")
                sys.exit(1)

            try:
                # Create a batch of reports in parallel
                batches_attempted += 1
                batch_reports = self._create_report_batch(
                    start_date=report_date,
                    batch_size=batch_size,
                    end_date=sync_end_date,
                    columns=columns,
                    time_increment=time_increment,
                )
                reports_queued += len(batch_reports)

                if self._rejected_columns:
                    # Same date, narrower field set. The rejected list only ever
                    # shrinks `columns`, so this cannot loop forever.
                    columns, report_date = self._resume_after_rejection(columns, report_date)
                    continue

                if not batch_reports:
                    if self._throttled and self._enter_span_mode():
                        # Same window, asked for as one report instead of one per
                        # slice. Nothing was queued, so no date is requested twice.
                        continue
                    if self._throttled:
                        self._warn_throttle_is_unrecoverable()
                    # Nothing queued: skip the whole span this batch just tried,
                    # not a single date -- otherwise every date is re-requested
                    # up to batch_size times before the window moves past it.
                    attempted = THROTTLE_SPAN_MAX_SLICES if self._span_mode else batch_size
                    report_date = self._advance_batch(report_date, time_increment, attempted, sync_end_date)
                    continue

                if self._throttled:
                    # Part of the batch was queued before the quota ran out. Take
                    # what it produced, then ask for the rest in one report.
                    self._enter_span_mode()

                # Process all reports in the batch
                for record in self._process_report_batch(batch_reports, columns, time_increment):
                    records_emitted += 1
                    yield record

                if self._rejected_columns:
                    # A column was refused while reading results: resume from the
                    # date that failed, so dates already yielded in this batch are
                    # not emitted twice.
                    columns, report_date = self._resume_after_rejection(columns, report_date)
                    continue

                if self._throttled_from is not None:
                    # Processing ran into the ad account's limit. Nothing was
                    # emitted for that date, so the window can be asked for again
                    # -- but as one report, the single call the account can still
                    # afford. When that is already the shape being used, there is
                    # nothing left to shrink, so the window is skipped instead of
                    # retried forever.
                    resume_from = self._throttled_from
                    self._throttled_from = None
                    if self._enter_span_mode():
                        report_date = resume_from
                        continue
                    self._warn_throttle_is_unrecoverable()
                    attempted = THROTTLE_SPAN_MAX_SLICES if self._span_mode else batch_size
                    report_date = self._advance_batch(resume_from, time_increment, attempted, sync_end_date)
                    continue

                if self._span_failed_from is not None:
                    # The degraded report could not be built. Pick the same range
                    # back up one slice at a time; it yielded nothing, so nothing
                    # is emitted twice.
                    report_date = self._span_failed_from
                    self._span_failed_from = None
                    continue

                # Successfully processed batch, advance to next batch
                report_date = batch_reports[-1]["next_date"]
                retry_count = 0  # Reset retry count on success

                # Brief pause between batches to avoid overwhelming API
                time.sleep(AD_REPORT_INCREMENT_SLEEP_TIME)

            except FacebookRequestError as fb_err:
                # Handle specific insights API errors first
                if fb_err.http_status() == HTTPStatus.BAD_REQUEST and "unsupported get request" in str(
                    fb_err.api_error_message().lower()
                ):
                    user_logger.warning(f"[{self.name}] API Error: {fb_err.api_error_message()}. Trying again..")
                    retry_count += 1
                    continue

                # Use base class error handling for common errors (rate limits, server errors)
                if self._handle_facebook_request_error(fb_err, retry_count, 10):
                    retry_count += 1
                    continue

                user_logger.error(f"[{self.name}] An unhandled error occurred: {fb_err}. Stopping execution.")
                user_logger.exception(f"[{self.name}] An unhandled error occurred: {fb_err}. Stopping execution.")
                sys.exit(1)

        self._fail_if_nothing_extracted(batches_attempted, reports_queued, records_emitted)


class AdsInsightHourlyAdvertiserTimezoneStream(AdsInsightStream):
    name = "adsinsights_hourly_advertiser_timezone"

    @property
    def report_breakdowns(self) -> list[str] | None:
        return ["hourly_stats_aggregated_by_advertiser_time_zone"]


class AdsInsightByAgeAndGenderStream(AdsInsightStream):
    name = "adsinsights_by_age_and_gender"

    @property
    def report_breakdowns(self) -> list[str] | None:
        return ["age", "gender"]


class AdsInsightByCountryStream(AdsInsightStream):
    name = "adsinsights_by_country"

    @property
    def report_breakdowns(self) -> list[str] | None:
        return ["country"]


class AdsInsightByDevicePlatformStream(AdsInsightStream):
    name = "adsinsights_by_device_platform"

    @property
    def report_breakdowns(self) -> list[str] | None:
        return ["publisher_platform", "device_platform", "impression_device", "platform_position"]


class AdsInsightByRegionStream(AdsInsightStream):
    name = "adsinsights_by_region"

    @property
    def report_breakdowns(self) -> list[str] | None:
        return ["region"]


class AdsInsightByHourStream(AdsInsightStream):
    name = "adsinsights_by_region"

    @property
    def report_breakdowns(self) -> list[str] | None:
        return ["region"]


class CampaignInsightsStream(AdsInsightStream):
    """Insights aggregated at the campaign level.

    Unlike the default AdsInsightStream (level=ad), this stream returns one row
    per campaign per time period, producing significantly fewer rows and faster
    extractions for accounts with many ads.
    """

    name = "campaign_insights"

    @property
    def report_level(self) -> str:
        return "campaign"

    def _generate_hash_id(self, adinsight: AdsInsights, report_breakdowns: list[str]):
        date_start = adinsight.get("date_start", "")
        campaign_id = adinsight.get("campaign_id", "")

        breakdown_values = []
        for breakdown in report_breakdowns:
            breakdown_values.append(str(adinsight.get(breakdown, "")))
        breakdown_string = "-".join(breakdown_values)

        hash_object = md5(f"{date_start}-{campaign_id}-{breakdown_string}".encode())
        return hash_object.hexdigest()
