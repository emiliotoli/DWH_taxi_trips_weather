{{ config(
    materialized='incremental',
    unique_key='id_trip',
    on_schema_change='sync_all_columns'
) }}

with src as (
    select *
    from {{ source('raw', 'taxi_trip') }}

),

filtered as (
    select *
    from src
        WHERE tpep_pickup_datetime IS NOT NULL
        AND tpep_dropoff_datetime IS NOT NULL
        AND tpep_dropoff_datetime > tpep_pickup_datetime
        AND tpep_pickup_datetime >= '2000-01-01'
        {% if is_incremental() %}
        AND tpep_pickup_datetime > (
            select max(pickup_datetime) from {{this}}
        )
        {% endif %}
),

neighborhood as (
    select
        id_neighborhood,
        borough_fk
    from {{ ref('ods_neighborhood') }}
),

transformed as (
    select
        concat_ws(
            '_',
            cast(VendorID as varchar),
            cast(tpep_pickup_datetime as varchar),
            cast(tpep_dropoff_datetime as varchar)
        ) as trip_id,
        ---- TIMESTAMPS
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        ---- IDs
        cast(VendorID as integer) as id_vendor,
        cast(PULocationID as integer) as pickup_neighborhood_fk,
        cast(DOLocationID as integer) as dropoff_neighborhood_fk,
        cast(RatecodeID as integer) as rate_code_id,
        ---- FLAG
        store_and_fwd_flag,
        cast(passenger_count as integer) as passenger_count,
        trip_distance,
        cast(payment_type as integer)    as payment_type,
        GREATEST(fare_amount, 0) AS fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        total_amount,
        current_timestamp as last_update
    from filtered f
)

select * from transformed
