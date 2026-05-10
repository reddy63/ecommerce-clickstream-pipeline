/*
    Fact table: daily revenue summary across all categories
*/

with staging as (
    select * from {{ ref('stg_clickstream') }}
)

select
    event_date,
    count(distinct category)  as unique_categories,
    sum(total_events)         as total_events,
    sum(total_revenue)        as total_revenue,
    round(avg(avg_price_per_event), 2) as avg_price_per_event,
    max(total_revenue)        as top_category_revenue,
    current_timestamp         as dbt_updated_at
from staging
group by event_date
order by event_date desc
