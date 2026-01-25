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
),

final_table as (
    select
        -1 as id_borough,
        'unknown' as borough_name,
        {{ dbt.current_timestamp() }} as last_update

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['borough_name']) }},
        borough_name,
        {{ dbt.current_timestamp() }}
    from dedup
)

select * from final_table