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
import requests
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

# Fields the Graph API accepts but does not BUILD: the async job dies at 0%
# with 2/1504044 even when the field is requested alone, for a single day.
# Found by bisecting the STANDARD group in the Graph API Explorer on
# act_1049955961115823 (17/09/2026): the other 68 STANDARD fields build
# together, this one fails on its own. Unlike REJECTED_FIELDS these stay in the
# schema -- the column keeps existing in the warehouse, it arrives empty -- and
# are simply not requested. A source that needs one can put it back with the
# `insights_included_fields` config key, at its own risk.
# Verified on 2026-09-17 in the Graph API Explorer on five ad accounts (two of
# them syncing green): a report carrying any of these fails with 2/1504044 no
# matter the account size or the period, while the other ~190 metrics build in
# one report. Facebook stopped computing them around 2026-09-11 and answers with
# the generic "report too heavy" error instead of rejecting the field.
FIELDS_NOT_BUILT_BY_FACEBOOK = {
    "total_card_view": "async job fails with 2/1504044 even alone (Instant Experience metric)",
    "link_clicks_per_results": "async job fails with 2/1504044 even alone since 2026-09-11 (results group)",
    "objective_result_rate": "async job fails with 2/1504044 even alone since 2026-09-11 (results group)",
    "opportunity_score_l4": "async job fails with 2/1504044 even alone since 2026-09-11 (beta group)",
    "result_values_performance_indicator": "async job fails with 2/1504044 even alone since 2026-09-11 (beta group)",
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
# A job still at 0% is queued behind the ad account's own load, not broken:
# Facebook has simply not picked it up yet. Waiting costs nothing but time --
# polling is a read -- while giving up costs a report creation to ask for the
# same thing again, against the account's "5 calls per 6 hours" budget. So the
# wait to START is deliberately generous, and far longer than the five minutes
# that used to fail a whole split window (NEKT-5249: one part of nine sat at 0%
# for 301s on facebook-ads-TJaE, the window was given up and the run ended with
# no rows although the other eight parts had built).
DEFAULT_INSIGHTS_MAX_WAIT_TO_START_SECONDS = 20 * 60
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

# Graph API codes that mean "you are out of quota", not "your request is wrong".
# They are not the same kind of empty: 613 is the per-ad-account limit the async
# insights reports are metered against ("Custom Analytics metrics exceeded the
# rate limit of N calls per M hours for this ad account") and does not clear
# within a run, so the stream stops. 4 (app-level) and 17 (user-level) are
# shared by every account on the app and clear within minutes -- on 16/09/2026
# they refused first creations at the top of the hour and, treated like 613,
# painted four healthy runs red for nothing (NEKT-5249). Those are waited out.
ACCOUNT_THROTTLE_ERROR_CODES = (613,)
APP_THROTTLE_ERROR_CODES = (4, 17)
THROTTLE_ERROR_CODES = ACCOUNT_THROTTLE_ERROR_CODES + APP_THROTTLE_ERROR_CODES

# A creation refused by the app-level limit is retried this many times, waiting
# APP_THROTTLE_WAIT_SECONDS (or Facebook's own suggestion, when longer) between
# attempts, before the run gives the window up.
APP_THROTTLE_RETRIES = 2
APP_THROTTLE_WAIT_SECONDS = 90

# Creation failures that are Facebook's, not ours: HTTP 5xx and the generic
# code 1 ("An unknown error occurred", subcode 99) / code 2 ("Service
# temporarily unavailable"). One short wait and one more try; a healthy account
# lost a whole run to a single 500 on the first creation (16/09/2026).
TRANSIENT_CREATE_ERROR_CODES = (1, 2)
TRANSIENT_CREATE_RETRIES = 1
TRANSIENT_CREATE_WAIT_SECONDS = 30

# The span job failed every attempt; before the single-slice probe decides between "the
# window is too big" and "the account is not building reports", give Facebook
# time to recover from an outage. Without this the whole verdict was reached in
# ~2 minutes, and a blip that a sibling account rode out one minute later put
# a healthy account in the "not building" state for the run (uqda, 16/09/2026).
# Persistence over time, not the error code, is what tells the two apart.
PROBE_BACKOFF_SECONDS = 180

# How many slices one report may cover. The quota is spent per report CREATED,
# not per day of data, so asking for the window in one go costs a single call
# while `time_increment` still returns the same per-slice rows. This is the
# normal request shape, not a degradation (NEKT-5213). Capped so that a first
# backfill does not turn into a single job Facebook cannot build.
SPAN_MAX_SLICES = 31

# A span job that Facebook fails to build is recreated this many times before
# the shape is given up on. Most job failures are transient (~14% of jobs die
# at 0% and succeed on the next attempt), and the arithmetic favours insisting:
# one more attempt at the whole window costs ONE creation, while giving up and
# splitting costs eight or nine. Two attempts, the old value, made the tap pay
# the expensive answer to a question Facebook was about to answer for free --
# see the comment on SPLIT_MODE_TTL_DAYS.
SPAN_RETRIES = 3

# Once the period is already being asked for in parts, another attempt
# recreates every failed part, so the window keeps the single retry it had
# before: insisting there multiplies the cost instead of avoiding it.
SPLIT_SPAN_RETRIES = 1

# How many times a per-slice report is recreated after its job fails. Each
# retry is a new report, i.e. a new call against the account's budget. Ten
# recreations of a report Facebook cannot build never succeeded; they only kept
# the account throttled (NEKT-5249).
PER_SLICE_RETRIES = 2

# Split mode asks for the same period as several smaller reports (the core
# metrics, then one per enabled optional group), each carrying the join keys,
# and combines the rows before they are emitted -- same rows, same columns,
# same table. It exists because "Service temporarily unavailable" (2/1504044)
# was read as "the report is too heavy to build" (Meta support, bug
# 1490232869529657, 16/09/2026), backed by an Explorer test where the full
# field set failed in seconds while the 51 BASIC_FIELDS built.
#
# That test is now known to have been contaminated: the field set it called
# "too heavy" still carried the four metrics Facebook had stopped building, and
# any report containing one of them fails whatever its size. Re-run on
# 2026-09-18 without them, on two accounts that had just split their period
# into eight and nine parts -- act_1053762787247772 (facebook-ads-WhFu, the very
# account of the original test) and act_1750548592846476 (facebook-ads-TJaE) --
# all 192 requested fields built in a single report, in under a minute, at the
# first attempt.
#
# So split mode is mostly answering transient failures, and it is the most
# expensive answer there is: eight or nine creations against the account's
# "5 calls per 6 hours" budget where one would do. Hence a short memory -- the
# next day's run tries the whole report again instead of carrying a verdict
# taken on a bad night for a week.
SPLIT_MODE_TTL_DAYS = 1
SPLIT_MODE_STATE_KEY = "insights_split_mode_since"

# A part with more columns than this is itself cut in halves. STANDARD has 69
# fields and on the first account tested (TJaE, 17/09/2026) it never left 0%
# in 5 minutes, for 14 days or for 1 -- the only group that behaved like the
# full 197-field report rather than failing outright.
SPLIT_PART_MAX_COLUMNS = 40

# Split mode never falls back to one report per day. On TJaE that fallback
# meant 7 creations per day x 14 days = 98 creations in one go, which spent the
# ad account's #613 budget before the second day was polled -- and the parts
# that had failed for the 14-day span failed the same way for a single day.
# A window whose parts do not all build is left for the next run instead.

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


def _columns_never_dropped(stream: AdsInsightStream) -> set[str]:
    """Columns that must survive any narrowing of a request or a read.

    They are the replication key and the inputs of `_generate_hash_id`: without
    them a row cannot be bookmarked or joined to the other parts of the period.
    Facebook's #100 message sometimes echoes the whole field list of the part
    instead of naming the single offending column (seen on facebook-ads-WhFu on
    2026-09-18, where 13 names came back, keys included). Dropping what the
    message names is only safe while these stay.
    """
    return {stream.replication_key, *stream._split_key_fields(), *(stream.report_breakdowns or [])}  # noqa: SLF001


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

    # Set on the class -- i.e. for the whole tap process -- the moment one
    # insights stream finds that Facebook will not build even a single-slice
    # report for the ad account. Every other insights stream of the same run
    # checks it before spending calls of its own on the same refusal.
    _account_not_building: bool = False

    # Same scope: once one insights stream learns that this ad account only
    # builds the report in parts, the other insights streams of the run start
    # in parts too instead of failing the full report twice each.
    _account_split_mode: bool = False

    # Names of the parts this process has already had to cut in halves
    # because Facebook would not build them whole (see _halve_part). The
    # next windows, and the other insights streams of the run, ask for the
    # halves directly instead of paying the failed whole again.
    _account_halved_parts: frozenset[str] = frozenset()

    # Columns this ad account takes at report creation and then refuses when the
    # result is read back ("(#100) Tried accessing nonexisting summary field").
    # Shared across the whole process: the account either serves a column or it
    # does not, so the first stream that discovers a refusal spares every other
    # stream of the run the same round trip. Deliberately not persisted between
    # runs -- Facebook has published no rule for which account refuses what, so
    # each run rediscovers it and an account that starts serving a column again
    # gets it back on its own.
    _columns_refused_on_read: set[str] = set()  # noqa: RUF012

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
        self._last_throttle_code: int | None = None
        # Facebook's own reason for the last job that ended as "Job Failed".
        self._last_job_error: dict = {}
        self._throttle_headers_logged = False
        # One report per window is the DEFAULT shape, not a reaction to being
        # throttled: it returns the same rows for a fraction of the quota. The
        # per-slice shape is the fallback, entered only when Facebook cannot
        # build the span job (see _give_up_on_span).
        self._span_mode = True
        self._span_disabled = False
        self._span_failed_from: pendulum.Date | None = None
        # Set when report creation is refused for quota while a batch is being
        # processed, so the caller can resume that window as a single report.
        self._throttled_from: pendulum.Date | None = None
        # Split mode: the period is requested as several smaller reports (see
        # SPLIT_MODE_TTL_DAYS). Off until the full report has failed and the
        # core metrics alone have built, or until a previous run's verdict is
        # read back from the state. `_split_from` says where to pick the sync
        # back up once the mode was entered mid-batch.
        self._split_mode = False
        self._split_from: pendulum.Date | None = None
        self._sync_context: dict | None = None

    def _note_throttled(self, fb_err: FacebookRequestError, current_date: pendulum.Date) -> None:
        """Record that Facebook refused a report because the quota is spent.

        The caller stops the batch here rather than trying the remaining dates:
        each one is another call against a budget that is already empty, and
        they would all be refused identically.
        """
        self._throttled = True
        self._last_throttle_code = fb_err.api_error_code()
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
        self._log_throttle_headers(fb_err.http_headers() or {}, when="creation refused")

    def _log_throttle_headers(self, headers: dict, *, when: str) -> None:
        """Ship Facebook's own view of the account's budget, verbatim.

        `x-fb-ads-insights-throttle` is the insights-specific header (access
        tier plus per-app and per-account utilisation) and nothing else in the
        tap reads it; the generic call-count headers sat at 0% seconds before
        an account was refused. Logged raw, so a new field from Facebook shows
        up without a parser change.
        """

        def pick(name: str):
            return headers.get(name) or headers.get(name.lower()) or headers.get(name.title())

        internal_logger.info(
            f"[{self.name}] Throttle headers ({when}) for act_{self.config.get('account_id')}: "
            f"x-fb-ads-insights-throttle={pick('X-FB-Ads-Insights-Throttle')!r} "
            f"x-business-use-case-usage={pick('X-Business-Use-Case-Usage')!r} "
            f"x-ad-account-usage={pick('X-Ad-Account-Usage')!r}"
        )

    def _warn_throttle_is_unrecoverable(self) -> None:
        """Tell the customer the quota is gone and one request is already the floor."""
        if self._last_throttle_code in APP_THROTTLE_ERROR_CODES:
            user_logger.warning(
                f"[{self.name}] Facebook's request limit for the Nekt application (shared by every "
                "connected ad account) was still reached after waiting and retrying. This is not "
                "specific to your account; the next scheduled run will pick the period up again."
            )
            internal_logger.warning(
                f"[{self.name}] App-level throttle (code {self._last_throttle_code}) persisted through "
                f"{APP_THROTTLE_RETRIES} retries; giving the window up for this run."
            )
            return
        if self._split_mode:
            user_logger.warning(
                f"[{self.name}] Facebook is refusing further performance reports for this ad account: its "
                "request limit is spent. The extraction was already requesting the metrics in smaller reports; "
                "the rest of the period is left for the next scheduled run."
            )
            internal_logger.warning(
                f"[{self.name}] Throttled in split mode; the remaining window is skipped for this run."
            )
            return
        user_logger.warning(
            f"[{self.name}] Facebook is still refusing performance reports for this ad account: its "
            "request limit is spent. The extraction already asks for the whole period in a single "
            "request, so there is nothing left to reduce on our side. Running this source less "
            "often, or having fewer tools query the same ad account, keeps the limit from being hit."
        )
        internal_logger.warning(
            f"[{self.name}] Throttled while already asking for the window in one report (or with "
            "the span shape disabled); no further reduction is available."
        )

    def _give_up_on_span(self, resume_from: pendulum.Date, report_label: str) -> None:
        """Fall back from the single-window report to one report per slice.

        Splitting the range is what Facebook itself recommends for a job that is
        too heavy to build, so the span is never retried again in this run: that
        would only ping-pong between the two shapes, spending a call each time.
        """
        self._span_mode = False
        self._span_disabled = True
        self._span_failed_from = resume_from
        user_logger.warning(
            f"[{self.name}] The single report covering {report_label} could not be built by Facebook. "
            "Falling back to one report per day, which may run into the account's request limit."
        )
        internal_logger.warning(
            f"[{self.name}] Span report for {report_label} failed to complete; span mode disabled for "
            f"the rest of the run, resuming per-slice from {resume_from.to_date_string()}."
        )

    # ---- split mode: the same period as several smaller reports ------------

    def _split_key_fields(self) -> list[str]:
        """Columns every part of a split report carries so its rows can be joined.

        They are the inputs of `_generate_hash_id` for this level (the breakdown
        columns come back with every report on their own). Requesting a key the
        level does not have -- ad_id on a campaign report -- is a rejected
        request, so the list follows the level.
        """
        keys = ["date_start", "date_stop", "campaign_id"]
        if self.report_level == "adset":
            keys.append("adset_id")
        elif self.report_level == "ad":
            keys.extend(["adset_id", "ad_id"])
        return keys

    def _partition_columns(self, columns: list[str]) -> tuple[list[str], dict[str, list[str]]]:
        """Split the requested columns into the core set and the optional groups.

        Returns (core, {group label: columns}). A column that belongs to no
        group stays with the core: it is not what makes a report heavy, and a
        part of one unknown column is not worth a creation of its own.
        """
        basic_set = set(BASIC_FIELDS)
        core: list[str] = []
        grouped: dict[str, list[str]] = {}
        for column in columns:
            if column in basic_set:
                core.append(column)
                continue
            owner = next((key for key, group in self.OPTIONAL_FIELD_GROUPS.items() if column in group), None)
            if owner is None:
                core.append(column)
                continue
            label = owner.removeprefix("include_insights_").removesuffix("_fields")
            grouped.setdefault(label, []).append(column)
        return core, grouped

    def _core_columns(self, columns: list[str]) -> list[str]:
        """The core (BASIC_FIELDS) subset of `columns`, plus the join keys."""
        core, _ = self._partition_columns(columns)
        keys = [key for key in self._split_key_fields() if key not in core]
        return keys + core

    def _report_parts(self, columns: list[str]) -> list[tuple[str, list[str]]]:
        """How the columns are spread over reports for one period.

        Outside split mode this is one report with every column. In split
        mode the core metrics come first (their rows are the base every other
        part is merged into) and each enabled optional group is its own
        report, all sharing the join keys.
        """
        core, grouped = self._partition_columns(columns)
        if not self._split_mode or not grouped:
            return [("all", list(columns))]
        keys = self._split_key_fields()
        parts = [("core", [key for key in keys if key not in core] + core)]
        for label, group_columns in grouped.items():
            pieces = -(-len(group_columns) // SPLIT_PART_MAX_COLUMNS)  # ceil
            size = -(-len(group_columns) // pieces)
            for number in range(pieces):
                chunk = group_columns[number * size : (number + 1) * size]
                if not chunk:
                    continue
                name = label if pieces == 1 else f"{label}-{number + 1}"
                parts.append((name, [key for key in keys if key not in chunk] + chunk))
        # Parts Facebook already refused to build whole in this process are
        # asked for as halves from the start.
        expanded: list[tuple[str, list[str]]] = []
        for name, part_columns in parts:
            halves = self._halve_part(name, part_columns) if name in AdsInsightStream._account_halved_parts else None
            expanded.extend(halves or [(name, part_columns)])
        return expanded

    def _halve_part(self, name: str, columns: list[str]) -> list[tuple[str, list[str]]] | None:
        """Cut one part in two, each half carrying the join keys.

        The weight Facebook refuses is per field type, not per count: on
        act_1049955961115823 (17/09/2026) 35 STANDARD fields built in one
        report while 10 BETA fields did not -- and every half of the refused
        groups built. Returns None when there is nothing left to cut.
        """
        keys = self._split_key_fields()
        metrics = [column for column in columns if column not in keys]
        if len(metrics) < 2:
            return None
        middle = -(-len(metrics) // 2)
        return [
            (f"{name}-a", [key for key in keys if key not in metrics[:middle]] + metrics[:middle]),
            (f"{name}-b", [key for key in keys if key not in metrics[middle:]] + metrics[middle:]),
        ]

    def _has_optional_columns(self, columns: list[str]) -> bool:
        _, grouped = self._partition_columns(columns)
        return bool(grouped)

    def _enter_split_mode(self, why: str, *, resume_from: pendulum.Date) -> None:
        """Switch this run -- and the next ones -- to requesting the period in parts.

        Recorded on the class for the other insights streams of this process
        and in the stream state for the next runs (see SPLIT_MODE_TTL_DAYS).
        """
        self._split_mode = True
        self._split_from = resume_from
        AdsInsightStream._account_split_mode = True
        try:
            state = self.get_context_state(self._sync_context)
        except Exception:  # noqa: BLE001 -- a state hiccup must not stop the extraction
            internal_logger.warning(f"[{self.name}] Could not record the split-mode verdict in the state.", exc_info=True)
        else:
            state[SPLIT_MODE_STATE_KEY] = pendulum.today().to_date_string()

        core, grouped = self._partition_columns(self._get_selected_columns())
        user_logger.warning(
            f"[{self.name}] Facebook could not build the performance report with all "
            f"{len(core) + sum(len(g) for g in grouped.values())} metrics at once for this ad account, but it "
            "builds the core metrics. The extraction continues by requesting the metrics in "
            f"{len(self._report_parts(self._get_selected_columns()))} smaller reports per period and combining "
            "them: same rows, same columns, it only takes a few more requests. Rows of a period are written once "
            "all of its reports have built, so the row counter may stay at 0 for a while. No action is needed on "
            "your side."
        )
        internal_logger.warning(
            f"[{self.name}] act_{self.config.get('account_id')}: entering split mode ({why}); parts = core "
            f"({len(core)}) + {', '.join(f'{label} ({len(cols)})' for label, cols in grouped.items())}; "
            f"resuming from {resume_from.to_date_string()}; remembered for {SPLIT_MODE_TTL_DAYS} days."
        )

    def _restore_split_mode(self, context: dict | None) -> None:
        """Start in split mode when this process or a recent run already found it necessary."""
        if AdsInsightStream._account_split_mode:
            self._split_mode = True
            internal_logger.info(
                f"[{self.name}] Starting in split mode: an earlier stream of this run found the account "
                "only builds the report in parts."
            )
            return
        try:
            state = self.get_context_state(context)
        except Exception:  # noqa: BLE001
            internal_logger.warning(f"[{self.name}] Could not read the split-mode marker from the state.", exc_info=True)
            return
        since = state.get(SPLIT_MODE_STATE_KEY)
        if not since:
            return
        try:
            since_date = pendulum.parse(str(since)).date()
        except Exception:  # noqa: BLE001
            state.pop(SPLIT_MODE_STATE_KEY, None)
            return
        if since_date.add(days=SPLIT_MODE_TTL_DAYS) < pendulum.today().date():
            state.pop(SPLIT_MODE_STATE_KEY, None)
            internal_logger.info(
                f"[{self.name}] Split-mode marker from {since_date.to_date_string()} expired after "
                f"{SPLIT_MODE_TTL_DAYS} days; trying the full report again."
            )
            return
        self._split_mode = True
        AdsInsightStream._account_split_mode = True
        user_logger.info(
            f"[{self.name}] Requesting the metrics in smaller reports per period, as in the previous runs "
            f"(since {since_date.to_date_string()})."
        )
        internal_logger.info(
            f"[{self.name}] Starting in split mode from the state marker ({since_date.to_date_string()})."
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

        A run that extracted some dates and lost others is neither case: the
        table did move forward, so nothing is failed, but the customer would
        otherwise read a green run as a complete one. It is told instead.
        """
        if not batches_attempted:
            return
        if records_emitted:
            if self._dates_failed:
                user_logger.warning(
                    f"[{self.name}] This stream was only partially updated: Facebook refused the "
                    f"performance reports for {self._dates_failed} date(s), most often because the ad "
                    "account's request limit was already spent. The dates that were extracted are in "
                    "the table; the refused ones are not, and the next run picks them up."
                )
                internal_logger.warning(
                    f"[{self.name}] Partial extraction: {self._dates_failed} date(s) failed while "
                    f"{records_emitted} record(s) were emitted over {batches_attempted} batch(es) and "
                    f"{reports_queued} queued report(s). The run stays green; the bookmark is not capped."
                )
            return
        if not self._dates_failed and reports_queued:
            # Every date built and returned nothing: the account really is empty.
            return

        if self.get_starting_replication_key_value(self._sync_context):
            # The table already holds this stream's history and the loader is
            # adding to it, not replacing it, so emitting nothing cannot erase
            # anything -- the only cost is that the stream did not advance.
            # Failing here would be worse than the gap it guards against: three
            # failed runs in a row disable the pipeline, which stops every other
            # stream too. So the customer is told plainly and the run ends.
            user_logger.warning(
                f"[{self.name}] This stream was not updated in this run: Facebook refused the performance "
                f"reports for {self._dates_failed} date(s), most often because the ad account's request "
                "limit was already spent. The data extracted previously is untouched and the next run "
                "picks up from where it stopped."
            )
            internal_logger.warning(
                f"[{self.name}] {batches_attempted} batch(es) attempted, {reports_queued} report(s) queued, "
                f"{self._dates_failed} date(s) failed, 0 records emitted; the stream has a bookmark, so the "
                "run is not failed -- nothing would be overwritten and a failed run would count towards "
                "disabling the pipeline."
            )
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

    def _refused_columns_safe_to_drop(
        self,
        message: str,
        columns: list[str],
        report_date: str,
    ) -> list[str] | None:
        """The columns Facebook named in `message`, or None when it names a key.

        A message that names the replication key or one of the join keys is
        echoing the request instead of pointing at a single dead field (seen on
        facebook-ads-WhFu on 2026-09-18, where 13 names came back, keys
        included). Dropping those leaves rows that cannot be bookmarked or
        joined, and the run then dies far from here with `KeyError: 'date_start'`
        inside the SDK's state handling.

        This is the one place that decides, and it is asked again on EVERY pass
        of the re-read loop -- not only the first. Facebook names a few columns
        at a time, so the pass that echoes the keys is usually a later one; that
        is exactly how facebook-ads-TJaE and facebook-ads-WhFu were disabled on
        2026-09-19/20 despite the guard that only ran on the first refusal.
        """
        refused = _columns_named_in_error(message, columns)
        if not refused:
            return None
        if protected := set(refused) & _columns_never_dropped(self):
            internal_logger.warning(
                f"[{self.name}] Refusal for {report_date} names key column(s) "
                f"({', '.join(sorted(protected))}), so the message is not identifying a single field; "
                f"no column is dropped. Original error: {message}"
            )
            return None
        return refused

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
        rejected = self._refused_columns_safe_to_drop(message, columns, report_date)
        if not rejected:
            return False

        self._rejected_columns = rejected
        self._restart_from = date_obj
        internal_logger.warning(
            f"[{self.name}] Graph API refused {len(rejected)} column(s) while reading the report "
            f"for {report_date}: {message}. Restarting from this date without them."
        )
        return True

    def _reread_without_refused_columns(
        self,
        fb_err: FacebookRequestError,
        parts: list[dict],
        jobs: list[AdReportRun],
        columns: list[str],
        report_date: str,
    ) -> list[dict] | None:
        """Read the report that is already built again, without the refused columns.

        A column can pass report creation and still be refused when the result
        is fetched ("(#100) Tried accessing nonexisting summary field"). The
        report itself is fine -- the job completed -- so recreating it buys
        nothing and costs a creation against the account's budget, which is the
        scarce resource (three creations per stream instead of one, and the run
        dies on the rate limit before the last stream is served). Naming the
        columns on the read instead makes Facebook serve exactly those, verified
        against a live report that answers #100 when read with no field list.

        Facebook names one column at a time, so this keeps narrowing and
        re-reading until the read succeeds. Returns the rows on success, or None
        when this is not a refusal it can absorb -- the caller then falls back to
        the slower path that recreates the report.
        """
        if fb_err.api_error_code() != FIELDS_PARAM_ERROR_CODE:
            return None

        message = fb_err.api_error_message() or str(fb_err)
        refused = self._refused_columns_safe_to_drop(message, columns, report_date)
        if not refused:
            return None

        remaining = list(columns)
        dropped: list[str] = []
        # One pass per column is the worst case; the guard is the loop's bound,
        # not a retry budget -- every pass strictly shrinks `remaining`.
        for _ in range(len(columns)):
            dropped.extend(refused)
            refused_set = set(refused)
            remaining = [column for column in remaining if column not in refused_set]
            if not remaining:
                # Nothing left to ask for: let the caller's path report it.
                return None
            core_columns = set(parts[0].get("columns") or [])
            if core_columns and not (core_columns & set(remaining)):
                # The core report defines which rows exist; with none of its
                # columns readable there is no base to join the rest onto, and
                # returning the optional parts alone would emit half a row.
                return None
            try:
                rows = self._merge_part_results(parts, jobs, report_date, fields=remaining)
            except FacebookRequestError as retry_err:
                if retry_err.api_error_code() != FIELDS_PARAM_ERROR_CODE:
                    return None
                retry_message = retry_err.api_error_message() or str(retry_err)
                # Asked again, keys included: a later pass is just as likely to
                # come back with the whole field list as the first one.
                refused = self._refused_columns_safe_to_drop(retry_message, remaining, report_date)
                if not refused:
                    return None
                continue

            AdsInsightStream._columns_refused_on_read.update(dropped)
            user_logger.warning(
                f"[{self.name}] Facebook accepted {len(dropped)} metric(s) when the report was requested "
                f"and then refused to return them for this ad account: {', '.join(sorted(dropped))}. "
                "The remaining metrics were extracted normally and these columns arrive empty; the next "
                "run checks them again."
            )
            internal_logger.warning(
                f"[{self.name}] Re-read report(s) {[part['report_run_id'] for part in parts]} for "
                f"{report_date} without {len(dropped)} refused column(s) ({', '.join(sorted(dropped))}) "
                f"instead of recreating them; {len(remaining)} column(s) served. Original error: {message}"
            )
            return rows

        return None

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

    # The titles these settings carry in nekt.config.json, so the customer reads
    # the name they see in the source form rather than a config key.
    OPTIONAL_FIELD_GROUP_TITLES: t.ClassVar[dict[str, str]] = {
        "include_insights_standard_fields": "Ads Insights: Include additional standard metrics",
        "include_insights_messaging_fields": "Ads Insights: Include messaging ads metrics",
        "include_insights_commerce_fields": "Ads Insights: Include commerce metrics",
        "include_insights_beta_fields": "Ads Insights: Include beta metrics",
        "include_insights_results_fields": "Ads Insights: Include objective-based results metrics",
        "include_insights_attribution_fields": "Ads Insights: Include SKAN and attribution metrics",
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

        # Which optional groups are off is the customer's call and something
        # they can act on, so it goes to them -- once per stream, and one line
        # for all groups instead of one per group. The internal line keeps the
        # field names.
        requested = set(self.insights_fields)
        skipped_by_group = {
            key: sorted(set(fields) & available - requested) for key, fields in self.OPTIONAL_FIELD_GROUPS.items()
        }
        skipped_by_group = {key: fields for key, fields in skipped_by_group.items() if fields}
        if skipped_by_group and not getattr(self, "_optional_groups_logged", False):
            self._optional_groups_logged = True
            titles = [self.OPTIONAL_FIELD_GROUP_TITLES.get(key, key) for key in skipped_by_group]
            user_logger.info(
                f"[{self.name}] Some optional metric groups are not included in this source: "
                f"{'; '.join(titles)}. They can be enabled in the source's advanced settings."
            )
            internal_logger.info(
                f"[{self.name}] {sum(len(v) for v in skipped_by_group.values())} field(s) not requested, by setting: "
                + " | ".join(f"{key}={','.join(fields)}" for key, fields in skipped_by_group.items())
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
        if not getattr(self, "_throttle_headers_logged", False):
            self._throttle_headers_logged = True
            self._log_throttle_headers(dict(headers or {}), when="first accepted creation")
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

    def _request_report_creation(self, params: dict, label: str) -> th.Any:
        """POST the report request, riding out what is Facebook's to fix.

        Three failure classes are retried here because none of them says
        anything about the ad account or the request: the app-level limit
        (codes 4/17, shared by every account, clears in minutes), Facebook's own
        5xx / code 1-2 hiccups, and a dropped connection. Anything else -- the
        per-account 613, a bad field, a permission error -- is raised unchanged
        for the caller to classify. The retries are bounded so that a real
        outage still ends the run instead of hanging it.
        """
        app_attempts = 0
        transient_attempts = 0
        while True:
            try:
                return self._trigger_async_insight_report_creation(
                    params=params, account_id=self.config["account_id"]
                )
            except FacebookRequestError as fb_err:
                code = fb_err.api_error_code()
                if code in APP_THROTTLE_ERROR_CODES and app_attempts < APP_THROTTLE_RETRIES:
                    app_attempts += 1
                    suggested = get_suggested_sleep_time(
                        headers=fb_err.http_headers() or {},
                        account_id=self.config["account_id"],
                    )
                    wait = min(max(suggested, APP_THROTTLE_WAIT_SECONDS), THROTTLE_MAX_WAIT_SECONDS)
                    user_logger.info(
                        f"[{self.name}] Facebook's application-wide request limit was reached while "
                        f"requesting the report for {label}; waiting {wait}s before trying again "
                        f"({app_attempts}/{APP_THROTTLE_RETRIES})."
                    )
                    internal_logger.warning(
                        f"[{self.name}] App-level throttle on creation for {label} (code {code}, "
                        f"subcode {fb_err.api_error_subcode()}): {fb_err.api_error_message()}. "
                        f"Suggested {suggested}s, waiting {wait}s (attempt {app_attempts}/{APP_THROTTLE_RETRIES})."
                    )
                    self._log_throttle_headers(fb_err.http_headers() or {}, when="app limit on creation")
                    time.sleep(wait)
                    continue
                is_transient = (fb_err.http_status() or 0) >= HTTPStatus.INTERNAL_SERVER_ERROR or (
                    code in TRANSIENT_CREATE_ERROR_CODES
                )
                if is_transient and transient_attempts < TRANSIENT_CREATE_RETRIES:
                    transient_attempts += 1
                    internal_logger.warning(
                        f"[{self.name}] Transient Facebook error creating the report for {label} "
                        f"(code {code}, subcode {fb_err.api_error_subcode()}, HTTP {fb_err.http_status()}): "
                        f"{fb_err.api_error_message()}. Retrying once in {TRANSIENT_CREATE_WAIT_SECONDS}s."
                    )
                    time.sleep(TRANSIENT_CREATE_WAIT_SECONDS)
                    continue
                raise
            except requests.exceptions.RequestException as net_err:
                if transient_attempts < TRANSIENT_CREATE_RETRIES:
                    transient_attempts += 1
                    internal_logger.warning(
                        f"[{self.name}] Connection error creating the report for {label}: {net_err!r}. "
                        f"Retrying once in {TRANSIENT_CREATE_WAIT_SECONDS}s."
                    )
                    time.sleep(TRANSIENT_CREATE_WAIT_SECONDS)
                    continue
                raise

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
            # The default: one report for the whole window, so the batch is a
            # single request instead of one per slice.
            batch_size = 1
            self._wait_out_throttle()

        for _ in range(batch_size):
            if current_date > end_date:
                break

            next_date = self._advance_date(current_date, time_increment)
            span_until = None
            if self._span_mode:
                next_date = self._advance_batch(current_date, time_increment, SPAN_MAX_SLICES, end_date)
                span_until = min(next_date.subtract(days=1), end_date)

            report_label = (
                f"{current_date.to_date_string()} to {span_until.to_date_string()}"
                if span_until is not None
                else current_date.to_date_string()
            )

            parts = self._queue_report_parts(current_date, span_until, report_label, columns, time_increment)
            if self._rejected_columns or self._throttled:
                # Either would refuse every remaining date of the batch the same
                # way; the caller decides how to resume.
                break
            if parts:
                batch_reports.append(
                    {
                        "report_run_id": parts[0]["report_run_id"],
                        "parts": parts,
                        "date": report_label,
                        "date_obj": current_date,
                        "until_obj": span_until,
                        "next_date": next_date,
                    }
                )

            current_date = next_date

        return batch_reports

    def _report_params(self, columns: list[str], time_increment: int | str, time_range: dict) -> dict:
        return {
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
            "time_range": time_range,
        }

    def _queue_report_parts(
        self,
        current_date: pendulum.Date,
        span_until: pendulum.Date | None,
        report_label: str,
        columns: list[str],
        time_increment: int | str,
    ) -> list[dict]:
        """Create the report(s) that cover one period: one, or one per part in split mode.

        Returns the created parts, or an empty list when the period could not
        be queued. A rejected `fields` param or a spent quota is recorded on
        the stream (`_rejected_columns`, `_throttled`) for the caller to act on.
        A part that fails to be created for any other reason makes the whole
        period unavailable for this batch: half a period is not emitted.
        """
        parts = self._report_parts(columns)
        time_range = self._get_time_range(current_date, span_until)
        created: list[dict] = []

        for part_name, part_columns in parts:
            part_label = report_label if len(parts) == 1 else f"{report_label} [{part_name}]"
            try:
                response = self._request_report_creation(
                    self._report_params(part_columns, time_increment, time_range), part_label
                )
                self._check_facebook_api_usage(headers=response._headers)
                if response.status() != HTTPStatus.OK:
                    user_logger.warning(f"[{self.name}] Failed to queue report for {part_label}")
                    internal_logger.warning(
                        f"[{self.name}] Report creation for {part_label} returned "
                        f"HTTP {response.status()} instead of 200; no report_run_id was issued."
                    )
                    return []
                created.append(
                    {"name": part_name, "columns": part_columns, "report_run_id": response.json()["report_run_id"]}
                )

            except FacebookRequestError as fb_err:
                message = fb_err.api_error_message() or str(fb_err)
                rejected = (
                    _columns_named_in_error(message, part_columns)
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
                        f"{part_label} (code {fb_err.api_error_code()}): {message}. "
                        f"Dropping {len(rejected)} column(s) and retrying the batch: {', '.join(rejected)}"
                    )
                    return []

                if fb_err.api_error_code() in THROTTLE_ERROR_CODES:
                    # Out of quota. Every remaining date in this batch is another
                    # call against an empty budget, and would be refused the same
                    # way, so stop here and let the caller ask for the window in
                    # one report instead.
                    self._note_throttled(fb_err, current_date)
                    return []

                user_logger.warning(f"[{self.name}] Error queueing report for {part_label}: {fb_err.api_error_message()}")
                internal_logger.warning(
                    f"[{self.name}] Report creation failed for {part_label} "
                    f"(code {fb_err.api_error_code()}, subcode {fb_err.api_error_subcode()}, "
                    f"HTTP {fb_err.http_status()}): {message}",
                    exc_info=True,
                )
                return []
            except requests.exceptions.RequestException as net_err:
                # The retry inside _request_report_creation did not get through.
                # One unreachable date is not a reason to kill the run: the floor
                # at the end decides whether nothing at all was extracted.
                user_logger.warning(
                    f"[{self.name}] Could not reach Facebook to request the report for {part_label}; "
                    "the date was skipped for this run."
                )
                internal_logger.error(
                    f"[{self.name}] Report creation for {part_label} failed on the connection after "
                    f"retrying: {net_err!r}",
                    exc_info=True,
                )
                self._dates_failed += 1
                return []

        if len(created) == 1:
            user_logger.info(f"[{self.name}] Queued report for {report_label}")
        else:
            user_logger.info(
                f"[{self.name}] Queued {len(created)} reports for {report_label} "
                f"({', '.join(part['name'] for part in created)})"
            )
        return created

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
            response = self._request_report_creation(params, label)
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
        except requests.exceptions.RequestException as net_err:
            channel.warning(f"[{self.name}] Could not reach Facebook to request the report for {label}.")
            internal_logger.error(
                f"[{self.name}] Retry report creation for {label} failed on the connection after "
                f"retrying: {net_err!r}",
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
            # Not one field, or not a field at all.
            optional = [column for column in columns if column not in set(BASIC_FIELDS)]
            if not optional:
                return False
            if self._split_mode:
                # The date is already being asked for in parts and one part still
                # does not build for no single field. Emitting the row without
                # that part's columns would write a silently incomplete row; let
                # the date fail instead, the next run tries again.
                internal_logger.warning(
                    f"[{self.name}] Could not isolate a single column for {date_obj.to_date_string()} while "
                    "already in split mode; the date is given up rather than emitted with empty columns."
                )
                return False
            # Fall back to the contract set once, so the run still delivers the
            # core metrics for every date.
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
        max_retries = PER_SLICE_RETRIES

        for report_info in batch_reports:
            report_date = report_info["date"]
            date_obj = report_info["date_obj"]
            span_until = report_info.get("until_obj")
            # One report per period by default; several in split mode. A part
            # whose job fails has its report_run_id cleared and is the only one
            # recreated on the next attempt.
            parts: list[dict] = report_info.get("parts") or [
                {"name": "all", "columns": columns, "report_run_id": report_info["report_run_id"]}
            ]
            job_failures = 0

            # One whole-period report is worth insisting on: another attempt
            # costs one creation, while giving the shape up leads to the probe
            # and then to eight or nine. In split mode it is not: every attempt
            # recreates each failed part, so insisting would cost parts x
            # attempts -- exactly the spending this is meant to avoid. There the
            # window keeps the single retry it always had.
            span_attempts = SPLIT_SPAN_RETRIES if self._split_mode else SPAN_RETRIES
            attempts_allowed = span_attempts if span_until is not None else max_retries

            for attempt in range(attempts_allowed + 1):
                if attempt > 0:
                    user_logger.info(
                        f"[{self.name}] Retrying job for {report_date} "
                        f"(attempt {attempt}/{attempts_allowed}), waiting 60s..."
                    )
                    time.sleep(60)
                    if not self._recreate_failed_parts(parts, date_obj, time_increment, until=span_until):
                        if self._throttled:
                            # The ad account's budget is spent. The nine attempts
                            # left here, and every later date in this batch, are
                            # nine more refused calls that keep it spent: stop and
                            # let the caller ask for the rest in one report.
                            self._throttled_from = date_obj
                            return
                        continue

                jobs = self._run_parts_to_completion(parts, report_date)
                if jobs is None:
                    if span_until is not None:
                        if attempt < span_attempts and not self._throttled:
                            # One more try before giving the shape up: most job
                            # failures on a healthy account are transient.
                            continue
                        # Enough. Whether the window is too big or the
                        # account is refusing every report is decided by one
                        # single-slice probe, not by a 13-30 report burst.
                        self._leave_span(date_obj, report_date, columns, time_increment, until=span_until, parts=parts)
                        return
                    job_failures += 1
                    if job_failures >= CONSECUTIVE_FAILURES_BEFORE_BISECT:
                        # The same date failed three times in a row: ask for it in
                        # parts (the usual cause is a report too heavy to build).
                        # Already in parts, no column hunt: each probe is another
                        # creation against the account's budget, and a part that
                        # does not build for a day did not build for the window
                        # either -- the date is given up below.
                        if self._split_columns_failing_the_job(date_obj, columns):
                            return
                        if not self._split_mode and self._drop_columns_failing_the_job(
                            date_obj, columns, time_increment
                        ):
                            return
                    continue

                try:
                    # Columns this account already refused earlier in the run are
                    # named up front, so the read succeeds first time instead of
                    # paying a refusal and a second read on every window.
                    known_refused = AdsInsightStream._columns_refused_on_read & set(columns)
                    first_read_fields = (
                        [column for column in columns if column not in known_refused] if known_refused else None
                    )
                    yield from self._merge_part_results(parts, jobs, report_date, fields=first_read_fields)
                    break
                except FacebookRequestError as fb_err:
                    # The report is built; a refused column is answered by
                    # reading it again with a narrower field list, which costs
                    # nothing, before falling back to recreating it.
                    rows = self._reread_without_refused_columns(fb_err, parts, jobs, columns, report_date)
                    if rows is not None:
                        yield from rows
                        break
                    if self._record_columns_refused_while_reading(fb_err, columns, report_date, date_obj):
                        return

                    user_logger.warning(
                        f"[{self.name}] Error reading results for {report_date} (attempt {attempt}/{attempts_allowed}): "
                        f"{fb_err.api_error_message()}. Retrying..."
                    )
                    internal_logger.warning(
                        f"[{self.name}] Reading report(s) {[part['report_run_id'] for part in parts]} for "
                        f"{report_date} failed (code {fb_err.api_error_code()}, HTTP {fb_err.http_status()}): "
                        f"{fb_err.api_error_message()}",
                        exc_info=True,
                    )
                except Exception as e:
                    user_logger.warning(
                        f"[{self.name}] Error reading results for {report_date} (attempt {attempt}/{attempts_allowed}): {e}. Retrying..."
                    )
            else:
                # End of the ladder for this date:
                #   job fails -> retry
                #   -> CONSECUTIVE_FAILURES_BEFORE_BISECT in a row: ask for the date
                #      in parts (split mode); already in parts: bisect, drop the
                #      offending column, resume the date with a narrower set
                #   -> still failing after max_retries: give up on the date (here)
                #      -> fail_on_job_error=True: stop the run now (strict; the
                #         customer prefers no data over a gap in the history)
                #      -> default: skip the date and keep going
                #   -> end of run: _fail_if_nothing_extracted is the floor that
                #      keeps an all-failed run from overwriting the table with an
                #      empty snapshot.
                if span_until is not None:
                    # Same reasoning as a span job that never builds: probe one
                    # slice rather than write the whole range off or burst.
                    self._leave_span(date_obj, report_date, columns, time_increment, until=span_until, parts=parts)
                    return
                self._dates_failed += 1
                msg = (
                    f"[{self.name}] Insights report job failed for {report_date} after {attempts_allowed} retries. "
                    "Data for this date was not extracted. See logs above for the specific error."
                )
                user_logger.error(msg)
                if fail_on_error:
                    sys.exit(1)

    def _recreate_failed_parts(
        self,
        parts: list[dict],
        date_obj: pendulum.Date,
        time_increment: int | str,
        *,
        until: pendulum.Date | None,
    ) -> bool:
        """Recreate the report of every part whose job did not build.

        A part that did not build whole is recreated as two halves (once: a
        half that fails again is recreated as it is). The cut is remembered
        on the class so the next windows and the other insights streams ask
        for the halves directly. Returns False when a report could not be
        created; `_throttled` then says whether that was the quota.
        """
        rebuilt: list[dict] = []
        for part in parts:
            if part.get("report_run_id") or part.get("halved") or len(parts) == 1:
                rebuilt.append(part)
                continue
            halves = self._halve_part(part["name"], part["columns"])
            if not halves:
                rebuilt.append(part)
                continue
            AdsInsightStream._account_halved_parts = AdsInsightStream._account_halved_parts | {part["name"]}
            rebuilt.extend({"name": name, "columns": columns, "report_run_id": None, "halved": True} for name, columns in halves)
            user_logger.info(
                f"[{self.name}] Facebook did not build the '{part['name']}' report for {date_obj.to_date_string()}"
                f"{' to ' + until.to_date_string() if until else ''}; asking for its metrics in two smaller reports."
            )
            internal_logger.info(
                f"[{self.name}] Part '{part['name']}' ({len(part['columns'])} columns) halved into "
                f"{' + '.join(f'{name} ({len(columns)})' for name, columns in halves)}; remembered for the process."
            )
        parts[:] = rebuilt

        for part in parts:
            if part.get("report_run_id"):
                continue
            report_run_id = self._create_single_report(date_obj, part["columns"], time_increment, until=until)
            if not report_run_id:
                return False
            part["report_run_id"] = report_run_id
        return True

    def _run_parts_to_completion(self, parts: list[dict], report_date: str) -> list[AdReportRun] | None:
        """Wait for every part of a period to build.

        The parts are checked in turn, one status call each per round, so a
        part that sits at 0% for five minutes does not hold up the others (on
        TJaE, 17/09/2026, the parts were polled one after the other and each
        attempt took 5+ minutes for the same verdict). Every part is polled
        even after one has failed: the reports exist already, so finding out
        which built is free, and only the failed ones are recreated. Returns
        the built jobs in part order, or None when at least one did not build
        (its report_run_id is cleared).
        """
        if len(parts) == 1:
            job = self._run_job_to_completion(
                report_instance=AdReportRun(parts[0]["report_run_id"]), report_date=report_date
            )
            if isinstance(job, AdReportRun):
                return [job]
            parts[0]["report_run_id"] = None
            return None

        pending = {
            index: self._new_poll_state(
                AdReportRun(part["report_run_id"]), f"{report_date} [{part['name']}]", announce=False
            )
            for index, part in enumerate(parts)
        }
        jobs: dict[int, AdReportRun] = {}
        failed: list[str] = []
        while pending:
            for index in list(pending):
                outcome, job = self._poll_job_once(pending[index])
                if outcome == "completed":
                    jobs[index] = job
                    del pending[index]
                elif outcome == "failed":
                    parts[index]["report_run_id"] = None
                    failed.append(parts[index]["name"])
                    del pending[index]
            if pending:
                time.sleep(max(state["sleep"] for state in pending.values()))
        if failed:
            if len(parts) > 1:
                internal_logger.warning(
                    f"[{self.name}] {len(failed)} of {len(parts)} part(s) did not build for {report_date}: "
                    f"{', '.join(failed)}. The period is not emitted until every part has built."
                )
            return None
        # One line for the period, not one per part: the customer asked for a
        # report, and how it was cut up is ours.
        user_logger.info(f"[{self.name}] Report for {report_date} is ready.")
        internal_logger.info(f"[{self.name}] All {len(parts)} part(s) built for {report_date}.")
        return [jobs[index] for index in range(len(parts))]

    def _merge_part_results(
        self,
        parts: list[dict],
        jobs: list[AdReportRun],
        report_date: str,
        fields: list[str] | None = None,
    ) -> list[dict]:
        """Read the built report(s) of a period and combine them into one row per key.

        The first part (the core metrics) is the base: every row of the period
        is one of its rows. Each further part adds its columns to the base row
        with the same hash id -- the id is built from the join keys every part
        carries, so a row means the same thing in all of them. A row that only
        an optional part returned has no base to join and is dropped (counted
        internally); the core report is the one that defines which rows exist.

        With a single part this is exactly what the stream always emitted.

        `fields` narrows what the read asks for. Left out, Facebook serves the
        list the report was created with; passed, it serves exactly these -- the
        escape hatch for a column the account accepted at creation and refuses
        at read (see _reread_without_refused_columns).
        """
        merged: dict[str, dict] = {}
        order: list[str] = []
        orphans = 0
        for index, (part, job) in enumerate(zip(parts, jobs)):
            part_fields = None
            if fields is not None:
                # A part only holds the columns it was created with; asking it
                # for another part's columns is what the API would refuse.
                part_columns = set(part.get("columns") or [])
                part_fields = [column for column in fields if not part_columns or column in part_columns]
                if not part_fields:
                    # Every column of this part was refused. Reading it with an
                    # empty list would just bring the created field set back, so
                    # the part is skipped -- its columns arrive empty, the rows
                    # of the other parts are kept.
                    internal_logger.warning(
                        f"[{self.name}] Part '{part.get('name')}' of {report_date} has no readable column "
                        "left and was skipped."
                    )
                    continue
            for obj in job.get_result(fields=part_fields):
                if not isinstance(obj, AdsInsights):
                    user_logger.warning(f"[{self.name}] Unexpected result type for {report_date}")
                    continue
                key = self._generate_hash_id(adinsight=obj, report_breakdowns=self.report_breakdowns)
                if index == 0:
                    obj["id"] = key
                    merged[key] = obj.export_all_data()
                    order.append(key)
                    continue
                base = merged.get(key)
                if base is None:
                    orphans += 1
                    continue
                for column, value in obj.export_all_data().items():
                    if column not in base:
                        base[column] = value
        if orphans:
            internal_logger.warning(
                f"[{self.name}] {orphans} row(s) of the optional parts for {report_date} had no matching row in "
                "the core report and were dropped."
            )
        if len(parts) > 1:
            internal_logger.info(
                f"[{self.name}] Combined {len(parts)} parts for {report_date} into {len(order)} row(s)."
            )
        return [merged[key] for key in order]

    def _split_columns_failing_the_job(self, date_obj: pendulum.Date, columns: list[str]) -> bool:
        """Answer a date that keeps failing by asking for it in parts.

        Returns True when the caller should stop and let the sync resume from
        this date in split mode. Nothing to split (already in parts, or only
        core metrics requested) returns False so the column hunt can run.
        """
        if self._split_mode or self._throttled or not self._has_optional_columns(columns):
            return False
        self._enter_split_mode(
            f"the report for {date_obj.to_date_string()} failed {CONSECUTIVE_FAILURES_BEFORE_BISECT}x in a row",
            resume_from=date_obj,
        )
        return True

    def _new_poll_state(
        self,
        report_instance: AdReportRun,
        report_date: str,
        *,
        quiet: bool = False,
        announce: bool = True,
    ) -> dict:
        """Everything one async job needs between two status checks.

        `announce` says whether this job tells the customer it is ready. It is
        off for the parts of a split period, which announce once as a whole in
        `_run_parts_to_completion`, and always off when `quiet`.
        """
        return {
            "instance": report_instance,
            "report_date": report_date,
            "channel": internal_logger if quiet else user_logger,
            "announce": announce and not quiet,
            "start": time.time(),
            "poll_failures": 0,
            "sleep": POLL_JOB_SLEEP_TIME,
        }

    def _poll_job_once(self, state: dict) -> tuple[str, th.Any]:
        """One status check of an async job.

        Returns ("completed", job), ("failed", None) or ("pending", None);
        `state["sleep"]` says how long to wait before the next check. Kept as
        a single step so several jobs can be checked in turn (split mode)
        instead of each one holding the line for up to 5 minutes.
        """
        channel = state["channel"]
        report_date = state["report_date"]
        max_wait = self.config.get("insights_max_wait_to_finish_seconds", DEFAULT_INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS)
        max_wait_to_start = self.config.get(
            "insights_max_wait_to_start_seconds", DEFAULT_INSIGHTS_MAX_WAIT_TO_START_SECONDS
        )
        duration = time.time() - state["start"]
        try:
            job = state["instance"].api_get()
            status = job[AdReportRun.Field.async_status]
            percent_complete = job[AdReportRun.Field.async_percent_completion]
            job_id = job["id"]
        except FacebookRequestError:
            # Structured API errors (rate limits, auth) keep their existing
            # handling upstream in get_records.
            raise
        except Exception as poll_error:
            state["poll_failures"] += 1
            if state["poll_failures"] >= MAX_CONSECUTIVE_POLL_FAILURES:
                channel.error(
                    f"[{self.name}] Could not check the insights report status for {report_date} "
                    "after several attempts. The report will be retried from scratch."
                )
                internal_logger.error(
                    f"[{self.name}] Polling api_get() failed {state['poll_failures']}x in a row "
                    f"for {report_date}; giving up on this job instance: {poll_error!r}",
                    exc_info=True,
                )
                return "failed", None
            internal_logger.warning(
                f"[{self.name}] Unreadable response while polling insights job for {report_date} "
                f"(attempt {state['poll_failures']}/{MAX_CONSECUTIVE_POLL_FAILURES}): {poll_error!r}",
                exc_info=True,
            )
            state["sleep"] = min(POLL_JOB_SLEEP_TIME * state["poll_failures"], 60)
            return "pending", None
        state["poll_failures"] = 0
        state["sleep"] = POLL_JOB_SLEEP_TIME
        # One line per status check, so internal debug only: at info on the
        # customer channel this put the progress of every poll in their log.
        internal_logger.debug(f"[{self.name}] ID: {job_id} - {status} for {report_date} - {percent_complete}% done.")

        if status == "Job Completed":
            done = f"[{self.name}] Insights job {job_id} completed for {report_date} after {duration:.0f}s."
            if state["announce"]:
                # The internal pair of the customer line, at the same cadence.
                user_logger.info(f"[{self.name}] Report for {report_date} is ready.")
                internal_logger.info(done)
            else:
                # A part of a split period, or a probe: the period announces
                # itself once, so each part stays at debug.
                internal_logger.debug(done)
            return "completed", job
        if status == "Job Failed":
            self._record_job_failure(job, job_id, report_date, channel)
            return "failed", None
        if duration > max_wait_to_start and percent_complete == 0:
            channel.error(
                f"[{self.name}] Insights job {job_id} did not start after {duration:.0f} seconds for {report_date}. "
                f"To give Facebook longer, increase 'insights_max_wait_to_start_seconds' in the tap config "
                f"(current: {max_wait_to_start}s). " + JOB_STALE_ERROR_MESSAGE
            )
            if channel is not internal_logger:
                internal_logger.error(
                    f"[{self.name}] AdReportRun {job_id} for {report_date} still at 0% ({status}) after "
                    f"{duration:.0f}s; budget insights_max_wait_to_start_seconds={max_wait_to_start}s."
                )
            return "failed", None
        if duration > max_wait:
            channel.error(
                f"[{self.name}] Insights job {job_id} did not complete after {max_wait}s for {report_date}. "
                f"To fix this, increase 'insights_max_wait_to_finish_seconds' in the tap config (current: {max_wait}s)."
            )
            if channel is not internal_logger:
                internal_logger.error(
                    f"[{self.name}] AdReportRun {job_id} for {report_date} at {percent_complete}% ({status}) after "
                    f"{duration:.0f}s; budget insights_max_wait_to_finish_seconds={max_wait}s."
                )
            return "failed", None
        return "pending", None

    def _run_job_to_completion(
        self,
        report_instance: AdReportRun,
        report_date: str,
        *,
        quiet: bool = False,
    ) -> th.Any:
        """Wait for one async job; returns the built job, or None when it did not build."""
        state = self._new_poll_state(report_instance, report_date, quiet=quiet)
        while True:
            outcome, job = self._poll_job_once(state)
            if outcome == "completed":
                return job
            if outcome == "failed":
                return None
            internal_logger.debug(f"[{self.name}] Sleeping for {state['sleep']} seconds until job is done")
            time.sleep(state["sleep"])

    def _record_job_failure(self, job: th.Any, job_id: str, report_date: str, channel: th.Any) -> None:
        """Log why Facebook failed the job, in Facebook's own words.

        Since Graph API v25 a failed AdReportRun carries error_code,
        error_subcode, error_message, error_user_title and error_user_msg. They
        were being discarded, which left the customer with a generic
        "intermittent error" and the engineer with a guess. A quota code here
        is a throttle: the report was created, but the account had no budget
        left to build it.
        """
        fields: dict = {}
        for name in ("error_code", "error_subcode", "error_message", "error_user_title", "error_user_msg"):
            try:
                value = job[name]
            except (KeyError, TypeError, IndexError):
                value = None
            if value not in (None, ""):
                fields[name] = value
        self._last_job_error = fields

        line = f"[{self.name}] Insights job {job_id} failed for {report_date}."
        if fields.get("error_user_title"):
            line += f" Facebook says: {fields['error_user_title']}."
        reason = fields.get("error_user_msg") or fields.get("error_message")
        line += f" {reason}" if reason else " " + JOB_STALE_ERROR_MESSAGE
        channel.error(line)
        internal_logger.error(
            f"[{self.name}] AdReportRun {job_id} for {report_date} ended as Job Failed on "
            f"act_{self.config.get('account_id')}: "
            + (" ".join(f"{k}={v!r}" for k, v in fields.items()) if fields else "no error fields on the job object")
        )

        try:
            code = int(fields["error_code"])
        except (KeyError, TypeError, ValueError):
            code = None
        if code in ACCOUNT_THROTTLE_ERROR_CODES:
            self._throttled = True
        elif code in APP_THROTTLE_ERROR_CODES:
            # Shared, short-lived: the normal retry (with its wait) is the right
            # answer, not giving the window up as if the account were spent.
            internal_logger.info(
                f"[{self.name}] Job {job_id} failed on the app-level limit (code {code}); "
                "treating it as transient, the retry ladder will recreate it."
            )

    def _leave_span(
        self,
        date_obj: pendulum.Date,
        report_label: str,
        columns: list[str],
        time_increment: int | str,
        *,
        until: pendulum.Date | None = None,
        parts: list[dict] | None = None,
    ) -> None:
        """Decide, with one single-slice probe, whether per-slice reports are worth it.

        The span job failed every attempt. On a healthy account that means the window
        is too big for one job, and one report per slice is Facebook's own
        remedy. On an account Facebook is currently not building reports for,
        that same fallback is 13-30 creations that all fail and leave the
        account throttled -- which is what kept those pipelines down (NEKT-5249).
        The two cases look identical from the span job alone, so spend exactly
        one more creation to tell them apart -- after a pause, because a short
        Facebook outage looks identical too and only time separates it.

        The probe asks for the most recent slice of the window, not the first:
        a backfill whose window starts at the edge of Facebook's retention would
        fail the probe on age alone and be misread as an account verdict. The
        probe is a diagnostic only; its rows are not emitted, so the per-slice
        pass that follows covers the whole window and nothing is emitted twice.
        """
        if self._throttled:
            self._throttled_from = date_obj
            return

        if self._split_mode:
            # Already in parts and some of them did not build twice. No probe
            # and no per-day fallback: see SPLIT_PART_MAX_COLUMNS above.
            self._give_up_on_split_window(report_label, parts or [])
            return

        probe_date = until or date_obj
        probe_label = probe_date.to_date_string()
        # The probe asks for the core metrics only. If even those do not build
        # for one day, the account is not building reports, full stop. If they
        # do, the probe has also just shown the way out: request the period in
        # parts (split mode) -- the same call with every column is what failed.
        probe_columns = self._core_columns(columns)
        user_logger.info(
            f"[{self.name}] Waiting {PROBE_BACKOFF_SECONDS}s and then checking whether Facebook can build a "
            f"single-day report of the core metrics for {probe_label} before deciding how to ask for the period."
        )
        internal_logger.info(
            f"[{self.name}] Span {report_label} failed {SPAN_RETRIES + 1}x; backing off {PROBE_BACKOFF_SECONDS}s "
            f"before the single-slice probe on {probe_label} ({len(probe_columns)} core columns) so a transient "
            "outage is not read as a verdict."
        )
        time.sleep(PROBE_BACKOFF_SECONDS)

        probe_id = self._create_single_report(probe_date, probe_columns, time_increment, quiet=True)
        if not probe_id:
            if self._throttled:
                self._throttled_from = date_obj
            else:
                self._mark_account_not_building(report_label, "the single-day report could not be created either")
            return

        job = self._run_job_to_completion(report_instance=AdReportRun(probe_id), report_date=probe_label, quiet=True)
        if not isinstance(job, AdReportRun):
            self._mark_account_not_building(
                report_label,
                f"the single-day report of the core metrics for {probe_label} failed the same way "
                f"{PROBE_BACKOFF_SECONDS}s later",
            )
            return

        if not self._split_mode and self._has_optional_columns(columns):
            # The core metrics build; the full set did not. Ask for the same
            # window again in parts before giving the span shape up: a span of
            # the core metrics is still one creation instead of 13-30.
            self._enter_split_mode(
                f"the span {report_label} failed {SPAN_RETRIES + 1}x with every column and the core-metrics probe "
                f"for {probe_label} built",
                resume_from=date_obj,
            )
            return

        # The account builds single-slice reports: the span really was too big.
        self._give_up_on_span(date_obj, report_label)

    def _give_up_on_split_window(self, report_label: str, parts: list[dict]) -> None:
        """Leave a window whose parts did not all build for the next run.

        The rows of the parts that did build are not emitted: the rule is that
        a row is written whole or not at all. The window counts as a failed
        date so the floor at the end of the run can tell this apart from an
        account that had nothing to report.
        """
        self._dates_failed += 1
        built = [part["name"] for part in parts if part.get("report_run_id")]
        missing = [part["name"] for part in parts if not part.get("report_run_id")]
        user_logger.error(
            f"[{self.name}] Facebook built {len(built)} of {len(parts)} smaller reports covering {report_label}, "
            f"but not: {', '.join(missing) or 'unknown'}. The period was left for the next scheduled run rather "
            "than written with those columns empty. Asking for it one day at a time would only spend the "
            "account's request limit on reports that fail the same way."
        )
        internal_logger.error(
            f"[{self.name}] act_{self.config.get('account_id')}: split window {report_label} given up after "
            f"{SPAN_RETRIES + 1} attempts; built={built} missing={missing}; last job error: {self._last_job_error!r}."
        )

    def _mark_account_not_building(self, report_label: str, why: str) -> None:
        """Stop this stream, and every later insights stream of the run, cheaply.

        Recorded on the class so the other insights streams of the same process
        read it before spending their own span + retry + probe on the same
        account. It is an observation about the ad account, not the stream.
        """
        AdsInsightStream._account_not_building = True
        self._dates_failed += 1
        user_logger.error(
            f"[{self.name}] Facebook is not building performance reports for this ad account right now: "
            f"the report covering {report_label} failed {SPAN_RETRIES + 1} times, and {why}. The extraction "
            "stopped here rather than requesting the period one day at a time, which would only spend the account's request "
            "limit on reports that fail the same way. Existing data was left untouched. This is a limit on "
            "Facebook's side; it has cleared on its own within a few days for other accounts, and the next "
            "scheduled run will try again."
        )
        internal_logger.error(
            f"[{self.name}] act_{self.config.get('account_id')}: span job failed {SPAN_RETRIES + 1}x and {why}; "
            f"last job error: {self._last_job_error!r}. Marked the account as not building for the rest of "
            "this process so the other insights streams do not repeat the cost."
        )

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
        # Fields Facebook does not build (see FIELDS_NOT_BUILT_BY_FACEBOOK) are
        # left out unless the source asks for them back explicitly.
        forced_back = set(self.config.get("insights_included_fields") or [])
        not_built = {f for f in FIELDS_NOT_BUILT_BY_FACEBOOK if f in columns and f not in forced_back}
        if not_built:
            internal_logger.info(
                f"[{self.name}] {len(not_built)} field(s) not requested because Facebook does not build them: "
                + ", ".join(f"{f} ({FIELDS_NOT_BUILT_BY_FACEBOOK[f]})" for f in sorted(not_built))
            )
        # Columns an earlier stream of this same run already saw this account
        # accept at creation and refuse at read. Leaving them out of the request
        # keeps the later streams from paying the same discovery round trip.
        refused_earlier = {f for f in AdsInsightStream._columns_refused_on_read if f in columns}
        if refused_earlier:
            internal_logger.info(
                f"[{self.name}] {len(refused_earlier)} field(s) not requested because this ad account "
                f"refused to return them earlier in this run: {', '.join(sorted(refused_earlier))}"
            )
        excluded |= not_built
        excluded |= refused_earlier
        if excluded:
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
        account_id = self.config.get("account_id")
        internal_logger.info(f"[{self.name}] Syncing insights for ad account act_{account_id}")
        if AdsInsightStream._account_not_building:
            user_logger.error(
                f"[{self.name}] Skipped: an earlier stream of this run found that Facebook is not building "
                "performance reports for this ad account right now. Existing data was left untouched; the "
                "next scheduled run will try again."
            )
            internal_logger.error(
                f"[{self.name}] act_{account_id}: skipping the stream, the account was marked as not "
                "building earlier in this process."
            )
            sys.exit(1)
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
        self._sync_context = context
        self._restore_split_mode(context)
        batches_attempted = 0
        reports_queued = 0
        records_emitted = 0

        # Use batch processing for parallel report creation
        while report_date <= sync_end_date:
            if retry_count > 10:
                user_logger.error(f"[{self.name}] Failed to get insights after 10 retries. Stopping execution.")
                sys.exit(1)

            # Once this account has refused a column, the next reports are not
            # created with it either: the refusal is a property of the account,
            # not of the window that happened to discover it.
            if refused := AdsInsightStream._columns_refused_on_read & set(columns):
                columns = [column for column in columns if column not in refused]

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
                    if self._throttled:
                        # The window is already one request; there is no smaller
                        # shape left to ask for. A refused window is a failed
                        # window, not an empty one.
                        self._dates_failed += 1
                        self._warn_throttle_is_unrecoverable()
                        if self._last_throttle_code in ACCOUNT_THROTTLE_ERROR_CODES:
                            # The spent budget belongs to the ad account and does
                            # not come back within the run. Walking on to the next
                            # window only collects more refusals -- fourteen of
                            # them on facebook-ads-69sh, the first backfill of a
                            # source starting in 2025 -- and if the quota does
                            # free up mid-run, a later window builds while the
                            # skipped ones stay behind the bookmark and become a
                            # hole no lookback reaches. Stop the stream here: the
                            # next run picks up where the data stopped.
                            internal_logger.warning(
                                f"[{self.name}] Account budget spent at {report_date}; ending the stream here "
                                f"instead of asking for the remaining windows. The {records_emitted} record(s) "
                                "already extracted are kept and the next run resumes from them."
                            )
                            break
                    # Nothing queued: skip the whole span this batch just tried,
                    # not a single date -- otherwise every date is re-requested
                    # up to batch_size times before the window moves past it.
                    attempted = SPAN_MAX_SLICES if self._span_mode else batch_size
                    report_date = self._advance_batch(report_date, time_increment, attempted, sync_end_date)
                    continue

                # Process all reports in the batch
                for record in self._process_report_batch(batch_reports, columns, time_increment):
                    records_emitted += 1
                    yield record

                if AdsInsightStream._account_not_building:
                    # Nothing this run can do for the rest of the period; the
                    # floor below decides whether it ends red.
                    break

                if self._rejected_columns:
                    # A column was refused while reading results: resume from the
                    # date that failed, so dates already yielded in this batch are
                    # not emitted twice.
                    columns, report_date = self._resume_after_rejection(columns, report_date)
                    continue

                if self._split_from is not None:
                    # The period is now asked for in parts. Nothing was emitted
                    # for the date that failed, so pick the window back up there
                    # (dates before it in the batch were already yielded).
                    report_date = self._split_from
                    self._split_from = None
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
                    # Reports had been queued for this window, so without this
                    # the floor would read "queued, nothing failed, no rows" as an
                    # account with nothing to report and end the run green
                    # (TJaE, 17/09/2026).
                    self._dates_failed += 1
                    self._warn_throttle_is_unrecoverable()
                    attempted = SPAN_MAX_SLICES if self._span_mode else batch_size
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
