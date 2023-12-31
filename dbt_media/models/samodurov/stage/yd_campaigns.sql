with source as (
      select * from {{ source('TEST', 'yandex_direct_custom_report') }}
),
renamed as (
    select
        {{ adapter.quote("AdFormat") }},
        {{ adapter.quote("AdGroupId") }},
        {{ adapter.quote("AdGroupName") }},
        {{ adapter.quote("AdId") }},
        {{ adapter.quote("AdNetworkType") }},
        {{ adapter.quote("Age") }},
        {{ adapter.quote("AvgClickPosition") }},
        {{ adapter.quote("AvgCpc") }},
        {{ adapter.quote("AvgEffectiveBid") }},
        {{ adapter.quote("AvgImpressionPosition") }},
        {{ adapter.quote("AvgPageviews") }},
        {{ adapter.quote("AvgTrafficVolume") }},
        {{ adapter.quote("BounceRate") }},
        {{ adapter.quote("Bounces") }},
        {{ adapter.quote("CampaignId") }},
        {{ adapter.quote("CampaignName") }},
        {{ adapter.quote("CampaignUrlPath") }},
        {{ adapter.quote("CampaignType") }},
        {{ adapter.quote("CarrierType") }},
        {{ adapter.quote("Clicks") }},
        {{ adapter.quote("ClientLogin") }},
        {{ adapter.quote("ConversionRate") }},
        {{ adapter.quote("Conversions") }},
        {{ adapter.quote("Cost") }},
        {{ adapter.quote("CostPerConversion") }},
        {{ adapter.quote("Criteria") }},
        {{ adapter.quote("CriteriaId") }},
        {{ adapter.quote("CriteriaType") }},
        {{ adapter.quote("Ctr") }},
        {{ adapter.quote("Date") }},
        {{ adapter.quote("Device") }},
        {{ adapter.quote("ExternalNetworkName") }},
        {{ adapter.quote("Gender") }},
        {{ adapter.quote("GoalsRoi") }},
        {{ adapter.quote("Impressions") }},
        {{ adapter.quote("IncomeGrade") }},
        {{ adapter.quote("LocationOfPresenceId") }},
        {{ adapter.quote("LocationOfPresenceName") }},
        {{ adapter.quote("MatchType") }},
        {{ adapter.quote("MobilePlatform") }},
        {{ adapter.quote("Profit") }},
        {{ adapter.quote("Revenue") }},
        {{ adapter.quote("RlAdjustmentId") }},
        {{ adapter.quote("Sessions") }},
        {{ adapter.quote("Slot") }},
        {{ adapter.quote("TargetingCategory") }},
        {{ adapter.quote("TargetingLocationId") }},
        {{ adapter.quote("TargetingLocationName") }},
        {{ adapter.quote("WeightedCtr") }},
        {{ adapter.quote("WeightedImpressions") }},
        {{ adapter.quote("partition") }},
        {{ adapter.quote("create_datetime") }}

    from source
)
select * from renamed
  