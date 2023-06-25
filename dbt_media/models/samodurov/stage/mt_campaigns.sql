with source as (
      select * from {{ source('TEST', 'my_target_ad_groups_all') }}
),
renamed as (
    select
        {{ adapter.quote("id") }},
        {{ adapter.quote("date") }},
        {{ adapter.quote("ad_offers") }},
        {{ adapter.quote("upload_receipt") }},
        {{ adapter.quote("earn_offer_rewards") }},
        {{ adapter.quote("video_started") }},
        {{ adapter.quote("paused") }},
        {{ adapter.quote("resumed_after_pause") }},
        {{ adapter.quote("fullscreen_on") }},
        {{ adapter.quote("fullscreen_off") }},
        {{ adapter.quote("sound_turned_off") }},
        {{ adapter.quote("sound_turned_on") }},
        {{ adapter.quote("video_viewed_10_seconds") }},
        {{ adapter.quote("video_viewed_25_percent") }},
        {{ adapter.quote("video_viewed_50_percent") }},
        {{ adapter.quote("video_viewed_75_percent") }},
        {{ adapter.quote("video_viewed_100_percent") }},
        {{ adapter.quote("video_viewed_10_seconds_rate") }},
        {{ adapter.quote("video_viewed_25_percent_rate") }},
        {{ adapter.quote("video_viewed_50_percent_rate") }},
        {{ adapter.quote("video_viewed_75_percent_rate") }},
        {{ adapter.quote("video_viewed_100_percent_rate") }},
        {{ adapter.quote("video_depth_of_view") }},
        {{ adapter.quote("started_cost") }},
        {{ adapter.quote("video_viewed_10_seconds_cost") }},
        {{ adapter.quote("video_viewed_25_percent_cost") }},
        {{ adapter.quote("video_viewed_50_percent_cost") }},
        {{ adapter.quote("video_viewed_75_percent_cost") }},
        {{ adapter.quote("video_viewed_100_percent_cost") }},
        {{ adapter.quote("uniques_video_started") }},
        {{ adapter.quote("uniques_video_viewed_10_seconds") }},
        {{ adapter.quote("uniques_video_viewed_25_percent") }},
        {{ adapter.quote("uniques_video_viewed_50_percent") }},
        {{ adapter.quote("uniques_video_viewed_75_percent") }},
        {{ adapter.quote("uniques_video_viewed_100_percent") }},
        {{ adapter.quote("uniques_video_viewed_10_seconds_rate") }},
        {{ adapter.quote("uniques_video_viewed_25_percent_rate") }},
        {{ adapter.quote("uniques_video_viewed_50_percent_rate") }},
        {{ adapter.quote("uniques_video_viewed_75_percent_rate") }},
        {{ adapter.quote("uniques_video_viewed_100_percent_rate") }},
        {{ adapter.quote("uniques_video_depth_of_view") }},
        {{ adapter.quote("reach") }},
        {{ adapter.quote("total") }},
        {{ adapter.quote("increment") }},
        {{ adapter.quote("frequency") }},
        {{ adapter.quote("tps") }},
        {{ adapter.quote("tpd") }},
        {{ adapter.quote("value") }},
        {{ adapter.quote("romi") }},
        {{ adapter.quote("adv_cost_share") }},
        {{ adapter.quote("playable_game_open") }},
        {{ adapter.quote("playable_game_close") }},
        {{ adapter.quote("playable_call_to_action") }},
        {{ adapter.quote("impressions") }},
        {{ adapter.quote("in_view") }},
        {{ adapter.quote("never_focused") }},
        {{ adapter.quote("never_visible") }},
        {{ adapter.quote("never_50_perc_visible") }},
        {{ adapter.quote("never_1_sec_visible") }},
        {{ adapter.quote("human_impressions") }},
        {{ adapter.quote("impressions_analyzed") }},
        {{ adapter.quote("in_view_percent") }},
        {{ adapter.quote("human_and_viewable_perc") }},
        {{ adapter.quote("never_focused_percent") }},
        {{ adapter.quote("never_visible_percent") }},
        {{ adapter.quote("never_50_perc_visible_percent") }},
        {{ adapter.quote("never_1_sec_visible_percent") }},
        {{ adapter.quote("in_view_diff_percent") }},
        {{ adapter.quote("active_in_view_time") }},
        {{ adapter.quote("attention_quality") }},
        {{ adapter.quote("opening_app") }},
        {{ adapter.quote("opening_post") }},
        {{ adapter.quote("moving_into_group") }},
        {{ adapter.quote("clicks_on_external_url") }},
        {{ adapter.quote("launching_video") }},
        {{ adapter.quote("comments") }},
        {{ adapter.quote("joinings") }},
        {{ adapter.quote("likes") }},
        {{ adapter.quote("shares") }},
        {{ adapter.quote("votings") }},
        {{ adapter.quote("sending_form") }},
        {{ adapter.quote("slide_1_clicks") }},
        {{ adapter.quote("slide_1_shows") }},
        {{ adapter.quote("slide_2_clicks") }},
        {{ adapter.quote("slide_2_shows") }},
        {{ adapter.quote("slide_3_clicks") }},
        {{ adapter.quote("slide_3_shows") }},
        {{ adapter.quote("slide_4_clicks") }},
        {{ adapter.quote("slide_4_shows") }},
        {{ adapter.quote("slide_5_clicks") }},
        {{ adapter.quote("slide_5_shows") }},
        {{ adapter.quote("slide_6_clicks") }},
        {{ adapter.quote("slide_6_shows") }},
        {{ adapter.quote("slide_1_ctr") }},
        {{ adapter.quote("slide_2_ctr") }},
        {{ adapter.quote("slide_3_ctr") }},
        {{ adapter.quote("slide_4_ctr") }},
        {{ adapter.quote("slide_5_ctr") }},
        {{ adapter.quote("slide_6_ctr") }},
        {{ adapter.quote("shows") }},
        {{ adapter.quote("clicks") }},
        {{ adapter.quote("goals") }},
        {{ adapter.quote("spent") }},
        {{ adapter.quote("cpm") }},
        {{ adapter.quote("cpc") }},
        {{ adapter.quote("cpa") }},
        {{ adapter.quote("ctr") }},
        {{ adapter.quote("cr") }},
        {{ adapter.quote("partition") }},
        {{ adapter.quote("create_datetime") }}

    from source
)
select * from renamed
  