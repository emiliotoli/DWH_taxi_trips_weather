{{ config(
    materialized='table',
)}}

with src as (
    select
        cast(id as integer) as id_payment_type,

        -- normalizzazione: trim + lower + spazi multipli → singolo spazio
        nullif(
            regexp_replace(lower(trim(type)), '\s+', ' '),
            ''
        ) as payment_type
    from {{ source('raw','payment_type') }}
    ),

    clean as (

        select
            id_payment_type,
            payment_type
        from src
        where payment_type is not null
        and payment_type is not null
    ),

    deduplicated as (

        select
            id_payment_type,
            min(payment_type) as payment_type
        from clean
        group by id_payment_type
        ),

    final_table as (select id_payment_type,
                     payment_type,
                     current_timestamp as last_update
              from deduplicated
              )

select * from final_table