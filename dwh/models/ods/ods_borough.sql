{{ config(
    materialized= 'table'
)}}

with src as (
    select * from {{ source('raw' , 'borough')}}
),

clean as (
    select
        trim(boroughname) as borough_name
    from src
    where trim(boroughname) is not null
),

dedup as (
    select distinct borough_name
    from clean
),

with_columns as (
    select
        row_number() over (order by borough_name) as id_borough,
        borough_name,
        CURRENT_TIMESTAMP as last_update
    from dedup
)

select * from with_columns