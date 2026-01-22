{{ config(
    materialized='table',
)}}

with src AS (
    select
        cast(id as integer) as id_payment_type,
        lower(trim(type)) as payment_type
    FROM
        {{ source('raw','payment_type')}}

),
    clean as (

        select
            id_payment_type,
            payment_type
        from src
        where payment_type is not null
    ),

    deduplicated as (

        select distinct
            id_payment_type,
            payment_type
        from clean
    ),
    final as (select id_payment_type,
                     payment_type,
                     current_timestamp as last_update
              from deduplicated
              )

select * from final