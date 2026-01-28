{{ config(
    materialized='incremental',
    unique_key='key_trip',
    incremental_strategy='append',
    on_schema_change='fail'
) }}

{% set initialize %}
    -- Create a sequence to generate surrogate keys for trips
    CREATE SEQUENCE IF NOT EXISTS trip_seq;
{% endset %}

{% do run_query(initialize) %}

WITH from_ods AS (
    SELECT
        o.id_trip,
        o.pickup_datetime,
        o.dropoff_datetime,
        o.passenger_count,
        o.trip_distance,
        o.fare_amount,
        o.extra,
        o.mta_tax,
        o.tip_amount,
        o.tolls_amount,
        o.improvement_surcharge,
        o.congestion_surcharge,
        o.Airport_fee,
        o.total_amount,
        o.last_update AS ods_update_time,
        o.pickup_neighborhood_fk,
        o.dropoff_neighborhood_fk,
        o.rate_code_FK,
        o.vendor_FK,
        o.store_and_fwd_flag,
        o.payment_type_fk
    FROM {{ ref('ods_taxi_trip') }} as o
    {% if is_incremental() %}
    WHERE ods_update_time > (
                SELECT COALESCE(MAX(time), '1900-01-01 00:00:00')
                FROM last_execution_times
                WHERE target_table = '{{this.identifier}}')

    {% endif %}
),

---- lookup dimension tables ----
vendor_lookup as (
    SELECT
        id_vendor,
        key_vendor
    from {{ref('dm_vendor')}}
    where is_current = true
),

zone_pickup_lookup as (
SELECT
    id_neighborhood,
    key_zone,
    borough_name
FROM {{ref('dm_zone')}}
WHERE is_current = TRUE
),

zone_dropoff_lookup as (
SELECT
    id_neighborhood,
    key_zone,
    borough_name
FROM {{ref('dm_zone')}}
WHERE is_current = TRUE
),

date_pickup_lookup AS (
SELECT
        date,
        key_date
    FROM {{ ref('dm_date') }}
),


date_dropoff_lookup AS (
SELECT
        date,
        key_date
    FROM {{ ref('dm_date') }}
),

weather_lookup AS (
    SELECT
        id_weather,
        key_weather,
        borough_name,
        weather_date
    FROM {{ref('dm_weather')}}
),

payment_type_lookup AS (
    SELECT
        id_payment_type,
        payment_type
    FROM {{ ref('ods_payment_type')}}
),

ratecode_lookup AS (
    SELECT
        id_ratecode,
        ratecode_name
    FROM {{ ref('ods_ratecode')}}
),

----------------------------

fact_data AS (
SELECT
NEXTVAL('trip_seq') AS key_taxi_trip,
o.id_trip,
---- FOREIGN KEYS ----
COALESCE(v.key_vendor, -1) AS key_vendor,
COALESCE(zp.key_zone, -1) AS key_zone_pickup,
COALESCE(zd.key_zone, -1) AS key_zone_dropoff,
COALESCE(dp.key_date, -1) AS key_date_pickup,
COALESCE(dd.key_date, -1) AS key_date_dropoff,
COALESCE(wl.key_weather, -1) AS key_weather,
---- TEMPORAL ATTRIBUTES ----
CAST(o.pickup_datetime AS TIME) AS pickup_time,
CAST(o.dropoff_datetime AS TIME) AS dropoff_time,
EXTRACT(HOUR FROM o.pickup_datetime)::SMALLINT AS pickup_hour,
EXTRACT(HOUR FROM o.dropoff_datetime)::SMALLINT AS dropoff_hour,

---- DEGENERATE DIMENSIONS ----
COALESCE(pt.payment_type, 'Unknown') AS payment_type,
COALESCE(rl.ratecode_name, 'Unknown') AS ratecode_name,
o.store_and_fwd_flag,

---- MEASURES ----
o.passenger_count,
o.trip_distance,
o.fare_amount,
o.extra,
o.mta_tax,
o.tip_amount,
o.tolls_amount,
o.improvement_surcharge,
o.congestion_surcharge,
o.airport_fee,
o.total_amount,
---- CALCULATED MEASURE ----
CAST(EXTRACT(EPOCH FROM (o.dropoff_datetime - o.pickup_datetime)) / 60 AS DECIMAL(10,2)) AS trip_duration_minutes,
CASE
    WHEN o.fare_amount > 0 THEN
        CAST((o.tip_amount / o.fare_amount * 100) AS DECIMAL(10,2))
    ELSE 0.00
END AS tip_percentage,
CASE
    WHEN zp.borough_name != zd.borough_name THEN TRUE
    ELSE FALSE
END AS is_cross_borough,
CASE
    WHEN EXTRACT(HOUR FROM o.pickup_datetime) BETWEEN 5 AND 11 THEN 'Morning Rush'
    WHEN EXTRACT(HOUR FROM o.pickup_datetime) BETWEEN 12 AND 15 THEN 'Midday'
    WHEN EXTRACT(HOUR FROM o.pickup_datetime) BETWEEN 16 AND 19 THEN 'Evening Rush'
    WHEN EXTRACT(HOUR FROM o.pickup_datetime) BETWEEN 20 AND 23 THEN 'Night'
    ELSE 'Late Night'
END AS time_of_day_category

FROM from_ods as o
LEFT JOIN vendor_lookup as v ON o.vendor_fk = v.id_vendor
LEFT JOIN zone_pickup_lookup AS zp ON o.pickup_neighborhood_fk = zp.id_neighborhood
LEFT JOIN zone_dropoff_lookup AS zd ON o.dropoff_neighborhood_fk = zd.id_neighborhood
LEFT JOIN date_pickup_lookup AS dp ON CAST(o.pickup_datetime AS DATE) = dp.date
LEFT JOIN date_dropoff_lookup AS dd ON CAST(o.dropoff_datetime AS DATE) = dd.date
LEFT JOIN weather_lookup AS wl ON zp.borough_name = wl.borough_name AND CAST(o.pickup_datetime AS DATE) = wl.weather_date
LEFT JOIN payment_type_lookup AS pt ON pt.id_payment_type = o.payment_type_fk
LEFT JOIN ratecode_lookup AS rl ON rl.id_ratecode = o.rate_code_fk

)

SELECT * FROM fact_data