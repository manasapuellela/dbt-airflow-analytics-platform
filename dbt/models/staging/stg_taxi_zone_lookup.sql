with source as (
    select *
    from {{ source('raw', 'taxi_zone_lookup') }}
)

select
    cast(locationid as integer) as location_id,
    borough,
    zone,
    service_zone
from source
