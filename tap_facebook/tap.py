"""facebook tap class."""

from __future__ import annotations

import sys
import typing as t
from datetime import datetime, timezone

from nekt_singer_sdk import Tap
from nekt_singer_sdk.custom_logger import internal_logger, user_logger
from nekt_singer_sdk import typing as th

if t.TYPE_CHECKING:
    from tap_facebook.client import FacebookStream

from tap_facebook.streams import (
    ActivitiesStream,
    AdAccountsStream,
    AdImages,
    AdLabelsStream,
    AdsetsStream,
    AdsInsightByAgeAndGenderStream,
    AdsInsightByCountryStream,
    AdsInsightByDevicePlatformStream,
    AdsInsightByRegionStream,
    AdsInsightHourlyAdvertiserTimezoneStream,
    AdsInsightStream,
    AdsStream,
    AdVideos,
    CampaignInsightsStream,
    CampaignStream,
    CreativeFilesStream,
    CreativeStream,
    CustomAudiences,
    CustomConversions,
)
from tap_facebook.streams.creative import creative_files_enabled

STREAM_TYPES = [
    AdsInsightStream,
    AdsetsStream,
    AdsStream,
    CampaignStream,
    CreativeStream,
    AdLabelsStream,
    AdAccountsStream,
    CustomConversions,
    CustomAudiences,
    AdImages,
    AdVideos,
    ActivitiesStream,
]

ADVANCED_STREAM_TYPES = [
    AdsInsightByAgeAndGenderStream,
    AdsInsightByCountryStream,
    AdsInsightByDevicePlatformStream,
    AdsInsightByRegionStream,
    AdsInsightHourlyAdvertiserTimezoneStream,
]


class TapFacebook(Tap):
    """Singer tap for extracting data from the Facebook Marketing API."""

    name = "tap-facebook"

    # add parameters you have in config.json
    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            description="The token to authenticate against the API service",
            required=True,
        ),
        # NOTE: there is deliberately no `api_version` setting. The Graph API
        # version is tied to the pinned facebook-business release and is owned
        # by the connector, not the user -- see API_VERSION in client.py.
        th.Property(
            "account_id",
            th.StringType,
            description="Your Facebook Account ID.",
            required=True,
        ),
        th.Property(
            "report_definition",
            th.ObjectType(
                th.Property(
                    "level",
                    th.StringType,
                    description="Represents the level of result aggregation.",
                    default="ad",
                ),
                th.Property(
                    "action_breakdowns",
                    th.ArrayType(th.StringType),
                    description=("How to break down action results. " "Supports more than one breakdowns.",),
                    default=[],
                ),
                th.Property(
                    "breakdowns",
                    th.ArrayType(th.StringType),
                    description=(
                        "How to break down the result. "
                        "For more than one breakdown, only certain combinations are available: "
                        "See 'Combining Breakdowns' in the "
                        "[Breakdowns page](https://developers.facebook.com/docs/marketing-api/insights/breakdowns). "  # noqa: E501
                        "The option impression_device cannot be used by itself"
                    ),
                    default=[],
                ),
                th.Property(
                    "time_increment_days",
                    th.IntegerType,
                    description=(
                        "The amount of days to aggregate your stats by, in days. "
                        "A value of 1 will return a daily aggregation of your stats."
                    ),
                    default=1,
                ),
                th.Property(
                    "action_attribution_windows_view",
                    th.StringType,
                    description=(
                        "The attribution window for the actions. For example, "
                        "28d_view means the API returns all actions that happened "
                        "28 days after someone viewed the ad."
                    ),
                    default="1d_view",
                ),
                th.Property(
                    "action_attribution_windows_click",
                    th.StringType,
                    description=(
                        "The attribution window for the actions. "
                        "For example, 28d_click means the API returns "
                        "all actions that happened 28 days after someone clicked on the ad."
                    ),
                    default="7d_click",
                ),
                th.Property(
                    "action_report_time",
                    th.StringType,
                    description=(
                        "Determines the report time of action stats. "
                        "For example, if a person saw the ad on Jan 1st but converted on Jan "
                        "2nd, when you query the API with action_report_time=impression, you "
                        "see a conversion on Jan 1st. When you query the API with "
                        "action_report_time=conversion, you see a conversion on Jan 2nd."
                    ),
                    default="mixed",
                ),
                th.Property(
                    "lookback_window",
                    th.IntegerType,
                    description=(
                        "Facebook freezes insight data 28 days after it was generated, which "
                        "means that all data from the past 28 days may have changed since we "
                        "last emitted it, so we attempt to retrieve it again."
                    ),
                    default=28,
                ),
            ),
            description=(
                "A list of insight report definitions. See the "
                "[Ad Insights docs](https://developers.facebook.com/docs/marketing-api/reference/adgroup/insights) "  # noqa: E501
                "for more details."
            ),
            default={},
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="The latest record date to sync",
        ),
        th.Property(
            "enable_advanced_reports",
            th.BooleanType,
            default=False,
            description="Define whether the user should have access to advanced report streams or not. Should be used with caution since the extraction time can increase significantly.",
        ),
        th.Property(
            "ad_insights_report_batch_size",
            th.IntegerType,
            description="The number of reports to request before checking the state and processing them.",
            default=30,
        ),
        # Ad insights field groups. BASIC_FIELDS is always requested; each flag
        # adds one group. They are separate settings because each maps to a
        # different Facebook capability an account may or may not hold.
        th.Property(
            "include_insights_standard_fields",
            th.BooleanType,
            description=(
                "Adds extra cost-per, unique, video retention and landing-page "
                "metrics to Ads Insights. No special permissions are required, "
                "but each report becomes heavier and slower to extract."
            ),
            default=False,
        ),
        th.Property(
            "include_insights_messaging_fields",
            th.BooleanType,
            description=(
                "Adds marketing message metrics (sent, delivered, read, button "
                "clicks) to Ads Insights. Only enable if the account runs "
                "WhatsApp or Messenger message campaigns -- Facebook may reject "
                "the request otherwise."
            ),
            default=False,
        ),
        th.Property(
            "include_insights_commerce_fields",
            th.BooleanType,
            description=(
                "Adds catalog segment and converted product metrics to Ads Insights. "
                "Only enable if the account has a product catalog with purchase "
                "tracking configured -- Facebook may reject the request otherwise."
            ),
            default=False,
        ),
        th.Property(
            "include_insights_beta_fields",
            th.BooleanType,
            description=(
                "Adds limited-availability metrics such as creative diversity, "
                "creative fatigue, advanced reach and auction insights to Ads "
                "Insights. Only enable if the account has elevated product access "
                "from Facebook -- Facebook may reject the request otherwise."
            ),
            default=False,
        ),
        th.Property(
            "include_insights_results_fields",
            th.BooleanType,
            description=(
                "Adds results, cost per result and objective result metrics to Ads "
                "Insights. Only enable if your campaign objectives report these "
                "metrics -- Facebook may reject the request otherwise."
            ),
            default=False,
        ),
        th.Property(
            "include_insights_attribution_fields",
            th.BooleanType,
            description=(
                "Adds SKAdNetwork and attribution setting metrics to Ads Insights. "
                "Only enable if attribution is configured on the account -- "
                "Facebook may reject the request otherwise."
            ),
            default=False,
        ),
        th.Property(
            "creative_fields_mode",
            th.StringType,
            description=(
                "Controls which fields to extract from creatives. "
                "Options: 'basic' (common fields without complex processing), "
                "'advanced' (requires more computation from Facebook). "
                "Use 'basic' for faster extraction with lower rate limits."
            ),
            default="advanced",
        ),
        th.Property(
            "ad_accounts_fields_mode",
            th.StringType,
            description=(
                "Controls which fields to extract from ad accounts. "
                "Options: 'basic' (core fields that work with limited permissions), "
                "'extended' (all fields including sensitive data like funding_source_details, "
                "owner, tax_id - requires elevated permissions on all ad accounts). "
                "Use 'basic' if you encounter permission errors on /me/adaccounts."
            ),
            default="extended",
        ),
        th.Property(
            "performance_granularity",
            th.StringType,
            description=(
                "Time granularity for insight streams (adsinsights and all breakdown variants). "
                "Accepted values: daily, monthly. When set to 'monthly', the Facebook API aggregates "
                "metrics by calendar month. Defaults to 'daily', which preserves the existing behavior "
                "using the time_increment_days setting from report_definition."
            ),
            default="daily",
        ),
        th.Property(
            "enable_campaign_insights",
            th.BooleanType,
            default=False,
            description=(
                "Enable the campaign_insights stream, which provides insights aggregated "
                "at the campaign level instead of the ad level. Produces significantly fewer "
                "rows and faster extractions for accounts with many ads."
            ),
        ),
        th.Property(
            "creative_thumbnail_width",
            th.IntegerType,
            description="The width for creative thumbnails.",
            default=1024,
        ),
        th.Property(
            "creative_thumbnail_height",
            th.IntegerType,
            description="The height for creative thumbnails.",
            default=1024,
        ),
        th.Property(
            "enable_creative_files_stream",
            th.BooleanType,
            default=False,
            description=(
                "When enabled, the tap adds a creative_files stream that downloads each "
                "ad creative's image and/or thumbnail from Facebook's CDN and uploads it "
                "to a Nekt volume. This makes extractions significantly slower and "
                "heavier: every creative's file is downloaded and re-uploaded. Only "
                "enable this if you need the creative files stored in a Nekt volume."
            ),
        ),
        th.Property(
            "nekt_volume_to_upload_creative_files",
            th.StringType,
            description=(
                "Nekt volume to upload creative image files to. Required when the "
                "creative files stream is enabled."
            ),
        ),
        th.Property(
            "creative_files_to_upload",
            th.StringType,
            default="image,thumbnail",
            description=(
                "Comma-separated list of which creative files to upload: 'image' "
                "(image_url, the full-resolution asset) and/or 'thumbnail' "
                "(thumbnail_url, Facebook's downscaled preview). Defaults to both. "
                "Only applies when the creative files stream is enabled."
            ),
        ),
        th.Property(
            "ads_page_size",
            th.StringType,
            description=(
                "Number of ads to fetch per API request. "
                "Reduce to 50 if you hit 'Please reduce the amount of data' errors on the ads stream. "
                "Values below 50 are not supported due to Facebook pagination constraints."
            ),
            default="100",
        ),
        th.Property(
            "include_ads_tracking_fields",
            th.BooleanType,
            default=True,
            description=(
                "Include tracking_specs, conversion_specs and recommendations in the ads stream. "
                "Disable for large accounts that hit Facebook error code 1 "
                "('Please reduce the amount of data you\\'re asking for')."
            ),
        ),
        th.Property(
            "split_creative_on_error",
            th.BooleanType,
            default=True,
            description=(
                "If the ads stream still hits Facebook error code 1 after tracking fields "
                "are excluded, fetch creative fields in a separate batched request instead "
                "of inline. Disable to fall back to the previous behavior."
            ),
        ),
        th.Property(
            "ads_auto_reduce_page_size",
            th.BooleanType,
            default=True,
            description=(
                "Automatically reduce the ads page size to 50 when Facebook error code 1 "
                "('Please reduce the amount of data you\\'re asking for') persists after "
                "tracking and creative fields are already split out. Never goes below 50 "
                "due to Facebook pagination constraints."
            ),
        ),
        th.Property(
            "ads_two_phase_on_error",
            th.BooleanType,
            default=True,
            description=(
                "Last resort for Facebook error code 1 on the ads stream: list ads with "
                "id and updated_time only, then batch-fetch the remaining fields by id in "
                "small chunks. Activated automatically only when every other mitigation "
                "step was insufficient."
            ),
        ),
        th.Property(
            "include_ad_preview_link",
            th.BooleanType,
            default=False,
            description=(
                "Include a shareable preview link (preview_shareable_link) in the ads stream. "
                "Fetched inline via the previews edge — no extra API calls. "
                "Disabled by default."
            ),
        ),
        th.Property(
            "preview_ad_format",
            th.StringType,
            default="DESKTOP_FEED_STANDARD",
            description=(
                "Ad placement format used to generate the preview link. "
                "Only applies when include_ad_preview_link is enabled."
            ),
        ),
        th.Property(
            "insights_max_wait_to_finish_seconds",
            th.IntegerType,
            default=1800,
            description=(
                "Maximum time in seconds to wait for a Facebook async insights job to complete. "
                "Increase for large accounts where jobs take longer to process. "
                "If a job exceeds this limit, the tap raises an error instead of silently skipping the data."
            ),
        ),
        th.Property(
            "insights_max_wait_to_start_seconds",
            th.IntegerType,
            default=1200,
            description=(
                "Maximum time in seconds to wait for a Facebook async insights job to leave 0%. A job that "
                "has not started yet is queued behind the ad account's own load, so waiting only costs time, "
                "while giving up costs a report creation against the account's rate limit. Increase it for "
                "busy accounts whose reports take long to be picked up."
            ),
        ),
        th.Property(
            "insights_excluded_fields",
            th.ArrayType(th.StringType),
            default=[],
            description=(
                "Ads Insights metrics to stop requesting for this account. Facebook refuses some "
                "metrics depending on the account's campaign objectives or product access, and it "
                "rejects the whole report rather than the single metric. The tap detects and drops "
                "those automatically; listing them here makes the exclusion permanent and saves the "
                "detection on every run. The columns stay in the schema and arrive empty."
            ),
        ),
        th.Property(
            "insights_included_fields",
            th.ArrayType(th.StringType),
            default=[],
            description=(
                "Ads Insights metrics the tap leaves out by default because Facebook does not build "
                "reports that contain them (for example total_card_view) and that this account wants "
                "requested anyway. Use only after checking in the Graph API Explorer that the metric "
                "builds for the ad account; otherwise every insights report of the source fails."
            ),
        ),
        th.Property(
            "fail_on_job_error",
            th.BooleanType,
            default=False,
            description=(
                "If true, raises an error when an insights job fails after all retries, stopping the pipeline. "
                "If false (default), logs the error and skips the date, allowing the pipeline to continue."
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> list[FacebookStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]

        if self.config.get("enable_campaign_insights", False):
            streams.append(CampaignInsightsStream(tap=self))

        # Child stream of `creatives`; only offered when the user opted in, since
        # downloading every creative's files is a deliberate, much heavier sync.
        if creative_files_enabled(self.config):
            streams.append(CreativeFilesStream(tap=self))

        advanced_streams = []
        if self.config.get("enable_advanced_reports", False):
            advanced_streams = [stream_class(tap=self) for stream_class in ADVANCED_STREAM_TYPES]

        return [*streams, *advanced_streams]

    def sync_all(self, *args, **kwargs) -> None:
        """Run every stream, then fail the run if an insights stream was partial with no history.

        Such a stream -- a full sync, or a first sync, that extracted some dates
        and lost others -- ends normally so its bookmark is saved: it is an
        unsorted stream, so exiting inside it would discard the progress and
        the next run would start over from the configured start date. The
        failure is raised here instead, once every stream has run, so a load
        the destination treated as a full replacement is never reported as a
        success (see AdsInsightStream._fail_if_nothing_extracted).
        """
        AdsInsightStream._incomplete_without_history = []
        super().sync_all(*args, **kwargs)
        incomplete = list(AdsInsightStream._incomplete_without_history)
        if not incomplete:
            return
        user_logger.error(
            f"The extraction finished, but {len(incomplete)} performance report stream(s) were only partly "
            f"loaded: {', '.join(incomplete)}. The run is marked as failed so the missing dates are not "
            "mistaken for a complete load; the next runs continue from where these streams stopped."
        )
        internal_logger.error(
            f"Failing the run after sync_all: insights stream(s) {incomplete} extracted part of the period "
            "with no bookmark in the state (full sync or first sync); their bookmarks were finalized."
        )
        sys.exit(1)

    def load_streams(self) -> list[FacebookStream]:
        """Order the streams for the sync.

        The SDK sorts by name and syncs in that order, which is where the
        rotation has to be applied: `discover_streams` only decides which
        streams exist.
        """
        return self._insights_last_in_a_rotating_order(super().load_streams())

    @staticmethod
    def _insights_last_in_a_rotating_order(streams: list[FacebookStream]) -> list[FacebookStream]:
        """Keep the insights streams last and start from a different one each hour.

        Only the insights streams spend the ad account's report budget, and
        Facebook refuses new reports once it is gone. With a fixed order the
        same stream is always served last, so on an account that runs out it
        never advances at all: on facebook-ads-UQfB (2026-09-18) two streams
        reached the current day while `adsinsights_by_country` sat nine days
        behind, and would have stayed behind every run.

        Rotating gives each of them the front of the queue in turn, so the delay
        is shared instead of falling on one stream forever. The turn counts the
        hour as well as the day, because a pipeline is scheduled several times a
        day: on facebook-ads-WYkS (2026-09-20) the three runs of the day all
        took the same order, so `adsinsights` lost its turn three times in a row
        and stayed on the same bookmark.

        Day and hour are added rather than combined into a single count of
        hours: a pipeline that runs once a day at a fixed hour would advance by
        24 turns each day, and 24 is a multiple of six, so with six insights
        streams that pipeline would take the same order forever -- the very
        starvation this is meant to end. Added, it advances one turn a day
        there, and by the gap between runs on a pipeline scheduled more often.
        Two runs within the same hour still take the same order: a retry is not
        a reshuffle.
        """
        insights = [stream for stream in streams if isinstance(stream, AdsInsightStream)]
        if len(insights) < 2:  # noqa: PLR2004
            return streams

        others = [stream for stream in streams if not isinstance(stream, AdsInsightStream)]
        now = datetime.now(tz=timezone.utc)
        turn = now.date().toordinal() + now.hour
        offset = turn % len(insights)
        return [*others, *insights[offset:], *insights[:offset]]


if __name__ == "__main__":
    TapFacebook.cli()
