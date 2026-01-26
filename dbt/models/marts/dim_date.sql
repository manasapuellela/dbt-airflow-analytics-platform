with dates as (
    select distinct pickup_date as date_day
    from {{ ref('ods_trips') }}
)

select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day
from dates
