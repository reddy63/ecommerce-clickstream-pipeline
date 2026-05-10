/*
    Custom test: ensure no revenue values are negative
*/

select
    category,
    event_date,
    total_revenue
from {{ ref('stg_clickstream') }}
where total_revenue < 0
