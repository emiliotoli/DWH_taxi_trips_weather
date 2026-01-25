{{ config(
    materialized = 'table'
)}}

with src as (
    select
        cast(id as integer) as id_vendor,
        nullif(
            regexp_replace(trim(vendor), '\s+', ' '),
            ''
        ) as vendor_name
    from
        {{ source('raw', 'vendor_id')}}
),
    clean as (
    select
        id_vendor,
        vendor_name
    from src
    where id_vendor is not null
      and vendor_name is not null
    ),

    deduplicated as (
    select
        id_vendor,
        min(vendor_name) as vendor_name
    from clean
    group by id_vendor
    ),

    final_table as (
        select
            id_vendor,
            vendor_name,
            current_timestamp as last_update
        from deduplicated
    )

select * from final_table