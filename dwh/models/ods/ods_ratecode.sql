{{ config(
    materialized='table'
) }}

with src as (

    select
        cast(id as integer) as id_ratecode,
        nullif(
            regexp_replace(trim(ratecode), '\s+', ' '),
            ''
        ) as ratecode_name
    from {{ source('raw', 'ratecode_id')}}
),

    clean as (
        select
            id_ratecode,
            ratecode_name
        from src
        where id_ratecode is not null
          and ratecode_name is not null
    ),

    deduplicated as (
        select
            id_ratecode,
            min(ratecode_name) as ratecode_name
        from clean
        group by id_ratecode
        ),

    final_table as (
        select * ,
               current_timestamp as last_update
        from clean
    )

select * from final_table
