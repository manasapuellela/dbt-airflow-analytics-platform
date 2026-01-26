select
    pickup_date,
    count(*) as trip_count,
    round(avg(trip_distance)::numeric, 2) as avg_trip_distance,
    round(avg(total_amount)::numeric, 2) as avg_total_amount,
    round(sum(total_amount)::numeric, 2) as total_revenue
from {{ ref('fact_trips') }}
group by pickup_date
order by pickup_date
