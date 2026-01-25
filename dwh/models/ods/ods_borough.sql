{{ config(
    materialized= 'table'
)}}


with src as (
    select
        nullif(
            regexp_replace(lower(trim(boroughname)), '\s+', ' '),
            ''
        ) as borough_name
    from {{ source('raw', 'borough') }}
),

clean as (
    select
        borough_name
    from src
    where trim(borough_name) is not null
),

dedup as (
    select distinct borough_name
    from clean
    where borough_name <> 'unknown'
),

final_table as (
    select
        -1 as id_borough,
        'unknown' as borough_name,
        CURRENT_TIMESTAMP as last_update

    union all

    select
        row_number() over (order by borough_name) as id_borough,
        borough_name,
        CURRENT_TIMESTAMP AS last_update
    from dedup
)

select * from final_table