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
        and tpep_pickup_datetime >= (
          coalesce(
              (select max(pickup_datetime) from {{ this }}),
              timestamp '2000-01-01'
          ) - interval '7 days'
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
        cast(VendorID as integer) as vendor_fk,
        coalesce(CAST(pu.id_neighborhood AS INTEGER), -1) as pickup_neighborhood_fk,
        coalesce(CAST(do.id_neighborhood AS INTEGER), -1) as dropoff_neighborhood_fk,
        cast(RatecodeID as integer) as rate_code_fk,
        ---- FLAG
        store_and_fwd_flag,
        cast(passenger_count as integer) as passenger_count,
        trip_distance,
        cast(payment_type as integer)    as payment_type_fk,
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
    left join neighborhood pu
        on cast(f.PULocationID as integer) = pu.id_neighborhood
    left join neighborhood do
        on cast(f.DOLocationID as integer) = do.id_neighborhood
)

select * from transformed
