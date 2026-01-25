{{ config(materialized='incremental', unique_key='key_weather') }}
{% set initialize %}
    -- Create a sequence to generate incremental surrogate keys
    CREATE SEQUENCE IF NOT EXISTS weather_seq;
{% endset %}

{% do run_query(initialize) %}

WITH from_ods AS (
    SELECT
        o.id_weather,
        o.weather_date,
        o.temperature_mean,
        o.temperature_max,
        o.temperature_min,
        o.apparent_temperature_mean,
        o.apparent_temperature_min,
        o.apparent_temperature_max,
        o.precipitation_sum,
        o.rain_sum,
        o.snowfall_sum,
        o.wind_speed_max,
        o.wind_speed_mean,
        o.wind_speed_min,
        o.weather_code,
        o.borough_fk,
        o.last_update as ods_update_time
    FROM {{ source("ods", "ods_weather")}} as o
    {% if is_incremental()%}
    WHERE o.last_update > (
        SELECT COALESCE(MAX(time), '1900-01-01 00:00:00')
        FROM last_execution_times
        WHERE target_table = '{{ this.identifier }}')

    {% endif %}
),

with_borough_name as (
    SELECT
        o.id_weather,
        o.weather_date,
        o.temperature_mean,
        o.temperature_max,
        o.temperature_min,
        o.apparent_temperature_mean,
        o.apparent_temperature_min,
        o.apparent_temperature_max,
        o.precipitation_sum,
        o.rain_sum,
        o.snowfall_sum,
        o.wind_speed_max,
        o.wind_speed_mean,
        o.wind_speed_min,
        o.weather_code,
        coalesce(b.borough_name, 'Unknown') AS borough_name
    FROM from_ods as o LEFT JOIN {{source("ods", "ods_borough")}} as b
    ON o.borough_fk = b.id_borough

)

SELECT
    NEXTVAL('weather_seq') as key_weather,
    w.id_weather,
    w.weather_date,
    w.temperature_mean,
    w.temperature_max,
    w.temperature_min,
    w.apparent_temperature_mean,
    w.apparent_temperature_min,
    w.apparent_temperature_max,
    w.precipitation_sum,
    w.rain_sum,
    w.snowfall_sum,
    w.wind_speed_max,
    w.wind_speed_mean,
    w.wind_speed_min,
    w.weather_code,
    w.borough_name
FROM with_borough_name w

