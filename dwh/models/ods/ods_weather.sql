{{ config(
    materialized= 'incremental',
    on_schema_change= 'sync_all_columns',
    unique_key= 'id_weather'
)}}

with src as (
    SELECT
        lower(trim(borough_name)) as borough_name,
        cast(time as date) as weather_date,
        cast("temperature_2m_mean (°C)" as double) as temperature_mean,
        cast("temperature_2m_min (°C)" as double) as temperature_min,
        cast("temperature_2m_max (°C)" as double) as temperature_max,

        cast("apparent_temperature_mean (°C)" as double) as apparent_temperature_mean,
        cast("apparent_temperature_min (°C)" as double) as apparent_temperature_min,
        cast("apparent_temperature_max (°C)" as double) as apparent_temperature_max,

        cast("precipitation_sum (mm)" as double) as precipitation_sum,
        cast("rain_sum (mm)"          as double) as rain_sum,
        cast("snowfall_sum (cm)"      as double) as snowfall_sum,

        cast("wind_speed_10m_max (km/h)"  as double) as wind_speed_max,
        cast("wind_speed_10m_mean (km/h)" as double) as wind_speed_mean,
        cast("wind_speed_10m_min (km/h)"  as double) as wind_speed_min,

        cast("weather_code (wmo code)" as integer) as weather_code

    FROM {{ source('raw', 'weather') }}
    WHERE time is not null
),

    filtered as (
        select *
        from src
        {% if is_incremental() %}
            where weather_date > (
                select max(weather_date) from {{ this }}
                )
        {% endif %}
    ),

    dedup as (
        select *
        from (
            select
                f.*,
                row_number() over (
                partition by borough_name, weather_date
                order by temperature_mean desc nulls last
            ) as rn
            from filtered f
        ) x
        where rn = 1
    ),

    borough as (
         select *
         from {{ ref('ods_borough') }}
    ),

    joined as (

        select
        b.id_borough       as borough_fk,
        f.weather_date,
        f.temperature_mean,
        f.temperature_max,
        f.temperature_min,
        f.apparent_temperature_mean,
        f.apparent_temperature_max,
        f.apparent_temperature_min,
        f.precipitation_sum,
        f.rain_sum,
        f.snowfall_sum,
        f.wind_speed_max,
        f.wind_speed_mean,
        f.wind_speed_min,
        f.weather_code,
        current_timestamp  as last_update

        from dedup f left join borough b on f.borough_name = b.borough_name

    ),

    final_table as (SELECT concat_ws('_',
                    cast(coalesce(borough_fk, sha256('unknown')) as varchar),
                    cast(weather_date as varchar)
                    ) as id_weather,
                     borough_fk,
                     weather_date,
                     temperature_mean,
                     temperature_max,
                     temperature_min,
                     apparent_temperature_mean,
                     apparent_temperature_max,
                     apparent_temperature_min,
                     precipitation_sum,
                     rain_sum,
                     snowfall_sum,
                     wind_speed_max,
                     wind_speed_mean,
                     wind_speed_min,
                     weather_code,
                     last_update
              from joined)

select *
from final_table