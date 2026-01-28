{{ config(materialized='table') }}

with trips as (
    select *
    from {{ ref('ods_yellow_taxi_trips') }}
)

select
    pickup_date,
    count(*) as events_count,
    count(distinct vendor_id) as unique_entities_count,
    avg(case when vendor_id is null then 1 else 0 end)::numeric as pct_missing_vendor_id,
    avg(case when passenger_count is null then 1 else 0 end)::numeric as pct_missing_passenger_count,
    avg(case when trip_distance <= 0 then 1 else 0 end)::numeric as pct_invalid_trip_distance,
    avg(case when fare_amount < 0 then 1 else 0 end)::numeric as pct_invalid_fare_amount,
    percentile_cont(0.95) within group (order by trip_distance)
        filter (where trip_distance is not null) as p95_trip_distance,
    percentile_cont(0.95) within group (order by total_amount)
        filter (where total_amount is not null) as p95_total_amount,
    sum(case when trip_distance > 50 then 1 else 0 end) as trips_very_long_count
from trips
group by pickup_date
order by pickup_date
