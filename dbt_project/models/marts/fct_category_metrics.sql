/*
    Fact table: category-level metrics per day
    Aggregates from staging with additional computed columns
*/

with staging as (
    select * from {{ ref('stg_clickstream') }}
)

select
    category,
    event_date,
    total_events,
    total_revenue,
    avg_price_per_event,
    round(
        total_revenue * 100.0 / nullif(sum(total_revenue) over (partition by event_date), 0),
        2
    ) as revenue_pct_of_day,
    rank() over (
        partition by event_date
        order by total_revenue desc
    ) as revenue_rank,
    current_timestamp as dbt_updated_at
from staging
