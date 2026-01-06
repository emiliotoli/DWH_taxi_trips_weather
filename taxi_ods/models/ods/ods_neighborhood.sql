{{ config(
    materialized= 'table'
)}}

with src as (
    select
        cast(LocationID as Integer) as id_neighborhood,
        trim(Borough) as borough_name,
        trim(Zone) as neighborhood_name,
        trim(service_zone) as service_zone
    from {{source('raw', 'neighborhood')}}
),

boroughs as (
    select
        id_borough,
        borough_name
    from {{ ref('ods_borough')}}
),

joined as (
    select s.id_neighborhood,
           b.id_borough as borough_fk,
           s.neighborhood_name,
           s.service_zone,
           current_timestamp as last_update
    from src s
    left join boroughs b on s.borough_name = b.borough_name
)

select * from joined
