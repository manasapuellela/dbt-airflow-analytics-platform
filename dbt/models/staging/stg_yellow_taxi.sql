with source as (
    select *
    from {{ source('raw', 'yellow_taxi_trips') }}
)

select
    cast(vendorid as integer) as vendor_id,
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric(10, 2)) as trip_distance,
    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,
    cast(ratecodeid as integer) as rate_code_id,
    cast(payment_type as integer) as payment_type,
    cast(fare_amount as numeric(10, 2)) as fare_amount,
    cast(tip_amount as numeric(10, 2)) as tip_amount,
    cast(total_amount as numeric(10, 2)) as total_amount,
    cast(pickup_date as date) as pickup_date
from source
