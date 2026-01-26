select
    vendor_id,
    count(*) as trip_count,
    round(avg(total_amount)::numeric, 2) as avg_total_amount,
    round(sum(total_amount)::numeric, 2) as total_revenue
from {{ ref('fact_trips') }}
group by vendor_id
order by vendor_id
