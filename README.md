# `tap-facebook`

Singer tap for extracting data from the Facebook Marketing API.

Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Capabilities

- `catalog`
- `state`
- `discover`
- `about`
- `stream-maps`
- `schema-flattening`

## Settings

| Setting              | Required | Default | Description                                                                                                                                 |
| :------------------- | :------: | :-----: | :------------------------------------------------------------------------------------------------------------------------------------------ |
| access_token         |   True   |  None   | The token to authenticate against the API service                                                                                           |
| api_version          |  False   |  v16.0  | The API version to request data from.                                                                                                       |
| account_id           |   True   |  None   | Your Facebook Account ID.                                                                                                                   |
| start_date           |  False   |  None   | The earliest record date to sync                                                                                                            |
| end_date             |  False   |  None   | The latest record date to sync                                                                                                              |
| stream_maps          |  False   |  None   | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config    |  False   |  None   | User-defined config values to be used within map expressions.                                                                               |
| flattening_enabled   |  False   |  None   | 'True' to enable schema flattening and automatically expand nested properties.                                                              |
| flattening_max_depth |  False   |  None   | The max depth to flatten schemas.                                                                                                           |

A full list of supported settings and capabilities is available by running: `tap-facebook --about`

## Installation

```bash
pipx install git+https://github.com/MeltanoLabs/tap-facebook.git
```

## Configuration

### Meltano Variables

The following config values need to be set in order to use with Meltano. These can be set in `meltano.yml`, via
`meltano config tap-facebook set --interactive`, or via the env var mappings shown above.

- `access_token:` access token from TAP_FACEBOOK_ACCESS_TOKEN variable
- `start_date:` start date
- `end_date:` end_date
- `account_id:` account ID from TAP_FACEBOOK_ACCOUNT_ID variable
- `api_version:` api version

```bash
tap-facebook --about
```

### Elastic License 2.0

The licensor grants you a non-exclusive, royalty-free, worldwide, non-sublicensable, non-transferable license to use, copy, distribute, make available, and prepare derivative works of the software.

### Attribution Window

Attribution Window is time period during which conversions might be credited to ads, we can have this time period between 1 day to 7 days for clicks and views

- `action_attribution_windows:` We can add these variable to params, it will have a list type value which takes in 1d-7d clicks and 1d-7d views values. We have added
  this variable in get_url_params function of ads insights stream

### Authentication

A Facebook access token is required to make API requests. (See [Facebook API](https://developers.facebook.com/docs/facebook-login/guides/access-tokens/) docs for more info)

## Usage

### API Limitation - Rate Limits

Hitting the rate limit for the Facebook API while making requests will return the following error:

```
400 Client Error: b'{"error":{"message":"(#80004) There have been too many calls to this ad-account. Wait a bit and try again
```

This error is handled using the [Backoff Library](https://github.com/litl/backoff), and the program will cease for a random amount of time before
attempting to call the API again

### Executing the Tap Directly

```bash
tap-facebook --version
tap-facebook --help
tap-facebook --config CONFIG --discover > ./catalog.json
```

## API quirks

- **`creative_media_type` understates aggregated reports (NEKT-5768, 2026-10-08).** In an async insights report
  above the ad level (`level=campaign`, as `campaign_insights` uses), requesting `creative_media_type` makes Facebook
  sum only part of the ads of each row, silently: spend, impressions and clicks of 3 of 34 ads for a campaign-day on
  a customer's account, and spend 0 for campaigns whose ads were all left out (`social_spend` alone kept its full
  value). Periods up to ~2024-09 came back right, later ones wrong. The same report without the field, the field at
  `level=ad`, and the synchronous endpoint return the full numbers, so a quick check in the Graph API Explorer does
  not reproduce it: it needs an async report (POST `act_<id>/insights`, then read the report). Since v1.89 the field
  is not requested above the ad level and its column arrives empty there (`FIELDS_THAT_SKEW_AGGREGATED_REPORTS`).
  Data loaded by v1.55-v1.88 with the standard metrics group on is wrong and is only fixed by reading the period
  again (reset the stream's bookmark; the merge overwrites the rows in place).

## Contributing

This project uses parent-child streams. Learn more about them [here](https://gitlab.com/meltano/sdk/-/blob/main/docs/parent_streams.md).

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_facebook/tests` subfolder and
then run:

```bash
poetry run pytest
```

You can also test the `tap-facebook` CLI interface directly using `poetry run`:

```bash
poetry run tap-facebook --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-facebook
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-facebook --version
# OR run a test `elt` pipeline:
meltano elt tap-facebook target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.

### Ads Insights fields

- account_currency: string
- account_id: string
- account_name: string
- action_values: array
- actions: array
- ad_click_actions: array
- ad_id: string
- ad_impression_actions: array
- ad_name: string
- adset_id: string
- adset_name: string
- attribution_setting: string
- auction_bid: string
- auction_competitiveness: string
- auction_max_competitor_bid: string
- buying_type: string
- campaign_id: string
- campaign_name: string
- canvas_avg_view_percent: string
- canvas_avg_view_time: string
- catalog_segment_actions: array
- catalog_segment_value: array
- catalog_segment_value_mobile_purchase_roas: array
- catalog_segment_value_omni_purchase_roas: array
- catalog_segment_value_website_purchase_roas: array
- clicks: string
- conversion_rate_ranking: string
- conversion_values: array
- conversions: array
- converted_product_quantity: array
- converted_product_value: array
- cost_per_15_sec_video_view: array
- cost_per_2_sec_continuous_video_view: array
- cost_per_action_type: array
- cost_per_ad_click: array
- cost_per_conversion: array
- cost_per_estimated_ad_recallers: string
- cost_per_inline_link_click: string
- cost_per_inline_post_engagement: string
- cost_per_outbound_click: array
- cost_per_thruplay: array
- cost_per_unique_action_type: array
- cost_per_unique_click: string
- cost_per_unique_inline_link_click: string
- cost_per_unique_outbound_click: array
- cpc: string
- cpm: string
- cpp: string
- created_time: string
- ctr: string
- date_start: string
- date_stop: string
- engagement_rate_ranking: string
- estimated_ad_recall_rate: string
- estimated_ad_recallers: string
- frequency: string
- full_view_impressions: string
- full_view_reach: string
- impressions: string
- inline_link_click_ctr: string
- inline_link_clicks: string
- inline_post_engagement: string
- instant_experience_clicks_to_open: string
- instant_experience_clicks_to_start: string
- instant_experience_outbound_clicks: array
- labels: string
- location: string
- marketing_messages_website_purchase_values: string
- mobile_app_purchase_roas: array
- objective: string
- optimization_goal: string
- outbound_clicks: array
- outbound_clicks_ctr: array
- purchase_roas: array
- qualifying_question_qualify_answer_rate: string
- quality_ranking: string
- reach: string
- social_spend: string
- spend: string
- unique_actions: array
- unique_clicks: string
- unique_ctr: string
- unique_inline_link_click_ctr: string
- unique_inline_link_clicks: string
- unique_link_clicks_ctr: string
- unique_outbound_clicks: array
- unique_outbound_clicks_ctr: array
- updated_time: string
- video_15_sec_watched_actions: array
- video_30_sec_watched_actions: array
- video_avg_time_watched_actions: array
- video_continuous_2_sec_watched_actions: array
- video_p100_watched_actions: array
- video_p25_watched_actions: array
- video_p50_watched_actions: array
- video_p75_watched_actions: array
- video_p95_watched_actions: array
- video_play_actions: array
- video_play_curve_actions: array
- video_play_retention_0_to_15s_actions: array
- video_play_retention_0_to_15s_actions: array
- video_play_retention_20_to_60s_actions: array
- video_play_retention_graph_actions: array
- video_time_watched_actions: array
- website_ctr: array
- website_purchase_roas: array
- wish_bid: string
