{{
    config (
      materialized='view',
      order_by='date'
    )
}}

select
    toDate(Date) as date,
    Clicks as clicks,
    Cost as cost,
    ifNull(CostPerConversion, 0) as cpc,
    Ctr as ctr,
    'yandex_direct' as source
from {{ ref('yd_campaigns') }}
union all
select
    toDate(date) as date,
    clicks,
    spent as cost,
    cpc,
    ctr,
    'my_target' as source
from {{ ref('mt_campaigns') }} mt
