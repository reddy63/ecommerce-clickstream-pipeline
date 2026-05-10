/*
    Staging model: cleans and standardizes raw clickstream metrics
    Source: Spark gold_to_postgres.py → clickstream_metrics table
*/

with source as (
    select * from {{ source('raw', 'clickstream_metrics') }}
),

cleaned as (
    select
        category,
        event_date,
        coalesce(total_events, 0)  as total_events,
        coalesce(total_revenue, 0) as total_revenue,
        round(
            case
                when total_events > 0
                then total_revenue / total_events
                else 0
            end, 2
        ) as avg_price_per_event,
        current_timestamp as dbt_loaded_at
    from source
    where category is not null
      and event_date is not null
)

select * from cleaned
