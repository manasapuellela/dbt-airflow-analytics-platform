# Data Dictionary

## raw.yellow_taxi_trips
| Column | Type | Description |
| --- | --- | --- |
| vendorid | int | TLC vendor identifier. |
| tpep_pickup_datetime | timestamp | Pickup timestamp. |
| tpep_dropoff_datetime | timestamp | Dropoff timestamp. |
| passenger_count | int | Passenger count. |
| trip_distance | numeric | Trip distance in miles. |
| pulocationid | int | Pickup location zone id. |
| dolocationid | int | Dropoff location zone id. |
| ratecodeid | int | Rate code id. |
| payment_type | int | Payment type code. |
| fare_amount | numeric | Fare amount. |
| tip_amount | numeric | Tip amount. |
| total_amount | numeric | Total fare + extras. |
| pickup_date | date | Derived pickup date partition. |

## marts.fact_trips
| Column | Type | Description |
| --- | --- | --- |
| pickup_date | date | Pickup date. |
| pickup_location_id | int | Pickup location id. |
| dropoff_location_id | int | Dropoff location id. |
| total_amount | numeric | Total amount. |

## marts.mart_daily_metrics
| Column | Type | Description |
| --- | --- | --- |
| pickup_date | date | Day bucket. |
| trip_count | int | Number of trips. |
| avg_trip_distance | numeric | Average trip distance. |
| avg_total_amount | numeric | Average total amount. |
| total_revenue | numeric | Total revenue. |
