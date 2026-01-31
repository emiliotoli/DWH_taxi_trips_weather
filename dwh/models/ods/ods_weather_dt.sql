{{ config(
    materialized= 'incremental',
    on_schema_change= 'sync_all_columns',
    unique_key= 'id_weather'
)}}

with src as (
    SELECT
        lower(trim(borough_name)) as borough_name,
        cast(time as datetime) as weather_datetime,
        cast("temperature_2m (°C)" as double) as temperature,

        cast("apparent_temperature (°C)" as double) as apparent_temperature,
        cast("rain (mm)" as double) as rain,
        cast("snowfall (cm)" as double) as snowfall,
        cast("wind_speed_10m (km/h)"  as double) as wind_speed,
        cast("relative_humidity_2m (%)" as double) as humidity

    FROM {{ source('raw', 'weather') }}
    WHERE time is not null
),

    filtered as (
        select *
        from src
        {% if is_incremental() %}
            where weather_datetime > (
                select max(weather_datetime) from {{ this }}
                )
        {% endif %}
    ),

    dedup as (
        select *
        from (
            select
                f.*,
                row_number() over (
                partition by borough_name, weather_datetime
                order by temperature desc nulls last
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
        f.weather_datetime,
        f.temperature,
        f.apparent_temperature,
        f.rain,
        f.snowfall,
        f.wind_speed,
        f.humidity,
        current_timestamp  as last_update

        from dedup f left join borough b on f.borough_name = b.borough_name

    ),

    final_table as (SELECT concat_ws('_',
                    cast(coalesce(borough_fk, sha256('unknown')) as varchar),
                    cast(weather_datetime as varchar)
                    ) as id_weather,
                     borough_fk,
                     CAST(weather_datetime AS DATE) AS weather_date,
                     CAST(weather_datetime AS TIME) AS weather_time,
                     temperature,
                     apparent_temperature,
                     rain,
                     snowfall,
                     wind_speed,
                     humidity,
                     last_update
              from joined)

select *
from final_table