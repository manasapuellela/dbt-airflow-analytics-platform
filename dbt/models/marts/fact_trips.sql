with trips as (
    select *
    from {{ ref('ods_trips') }}
)

select
    trips.vendor_id,
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.pickup_date,
    trips.passenger_count,
    trips.trip_distance,
    trips.pickup_location_id,
    trips.dropoff_location_id,
    trips.rate_code_id,
    trips.payment_type,
    trips.fare_amount,
    trips.tip_amount,
    trips.total_amount
from trips
