{{ config(
    materialized='table'
) }}

with src as (

    select
        cast(id as integer) as id_ratecode,
        trim(ratecode) as ratecode_name
    from {{ source('raw', 'ratecode_id')}}
),
    deduplicated as (
        select distinct
            id_ratecode,
            ratecode_name
        from src
    ),
    clean as (
        select *
        from deduplicated
        where ratecode_name is not null
    ),

    final as (
        select * ,
               current_timestamp as last_update
        from clean
    )

select * from final
