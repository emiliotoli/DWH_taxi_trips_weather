{{ config(
    materialized = 'table'
)}}

with src as (
    select
        cast(id as integer) as id_vendor,
        trim(vendor) as vendor_name
    from
        {{ source('raw', 'vendor_id')}}
),
    deduplicated as
        (select distinct
            id_vendor,
            vendor_name
        from src),
    clean as (
        select
            id_vendor,
            vendor_name
        from deduplicated
        where vendor_name is not null
    ),
    final as (
        select
            id_vendor,
            vendor_name,
            current_timestamp as last_update
        from clean
    )

select * from final