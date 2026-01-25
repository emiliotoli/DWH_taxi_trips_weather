{{ config(
    materialized= 'table'
)}}

with src as (
    select
        cast(LocationID as Integer) as id_neighborhood,
        trim(Borough) as borough_name,
        trim(Zone) as neighborhood_name,
        trim(service_zone) as service_zone,
        lower(
          nullif(regexp_replace(trim(Borough), '\s+', ' '), '')
        ) as borough_key
    from {{source('raw', 'neighborhood')}}
),

src_dedup as (
    select *
    from (
        select
            s.*,
            row_number() over (
                partition by id_neighborhood
                order by borough_name, neighborhood_name, service_zone
            ) as rn
        from src s
    ) x
    where rn = 1
),

boroughs as (
    select
        id_borough,
        borough_name,
        lower(nullif(regexp_replace(trim(borough_name), '\s+', ' '), '')) as borough_key
    from {{ ref('ods_borough')}}
),

joined as (
    select s.id_neighborhood,
           coalesce(b.id_borough, -1) as borough_fk,
           s.neighborhood_name,
           s.service_zone,
           current_timestamp as last_update
    from src_dedup s
    left join boroughs b on s.borough_key=b.borough_key
)

select
    id_neighborhood,
    borough_fk,
    neighborhood_name,
    service_zone,
    last_update
from joined
