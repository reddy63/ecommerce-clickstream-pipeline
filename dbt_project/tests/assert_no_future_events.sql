/*
    Custom test: ensure no events have future dates
*/

select
    category,
    event_date
from {{ ref('stg_clickstream') }}
where event_date > current_date
