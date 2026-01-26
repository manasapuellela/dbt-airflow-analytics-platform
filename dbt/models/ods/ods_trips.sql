with trips as (
    select *
    from {{ ref('stg_yellow_taxi') }}
)

select
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_date,
    passenger_count,
    trip_distance,
    pickup_location_id,
    dropoff_location_id,
    rate_code_id,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount
from trips
where pickup_datetime is not null
