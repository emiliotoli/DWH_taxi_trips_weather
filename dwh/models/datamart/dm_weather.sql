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
    FROM {{ ref('ods_weather')}} as o
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
        coalesce(b.borough_name, 'unknown') AS borough_name,
    FROM from_ods as o LEFT JOIN {{source("ods", "ods_borough")}} as b
    ON o.borough_fk = b.id_borough,
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
    w.borough_name,
    --- calculated attributes
    CASE
        WHEN w.temperature_mean < -10 THEN 'Extreme Cold'
        WHEN w.temperature_mean < 0 THEN 'Freezing'
        WHEN w.temperature_mean < 10 THEN 'Cold'
        WHEN w.temperature_mean < 20 THEN 'Mild'
        WHEN w.temperature_mean < 30 THEN 'Warm'
        WHEN w.temperature_mean < 35 THEN 'Hot'
        ELSE 'Extreme Heat'
    END AS temperature_category,
    CASE
        WHEN w.apparent_temperature_mean < 0 THEN 'Feels Freezing'
        WHEN w.apparent_temperature_mean < 10 THEN 'Feels Cold'
        WHEN w.apparent_temperature_mean < 20 THEN 'Feels Mild'
        WHEN w.apparent_temperature_mean < 30 THEN 'Feels Warm'
        ELSE 'Feels Hot'
    END AS apparent_temperature_category,
    CAST(
        COALESCE(w.rain_sum, 0) + COALESCE(w.snowfall_sum * 10, 0)  -- 1cm snow ≈ 10mm rain
        AS DECIMAL(10,2)
    ) AS total_precipitation_mm,
    CASE
        WHEN w.rain_sum = 0 THEN 'No Rain'
        WHEN w.rain_sum < 2.5 THEN 'Light Rain'
        WHEN w.rain_sum < 7.6 THEN 'Moderate Rain'
        WHEN w.rain_sum < 50 THEN 'Heavy Rain'
        ELSE 'Violent Rain'
    END AS rain_intensity,
    CASE
        WHEN w.snowfall_sum = 0 THEN 'No Snow'
        WHEN w.snowfall_sum < 2.5 THEN 'Light Snow'
        WHEN w.snowfall_sum < 10 THEN 'Moderate Snow'
        WHEN w.snowfall_sum < 25 THEN 'Heavy Snow'
        ELSE 'Blizzard'
    END AS snow_intensity,
            -- Is rainy
    CASE WHEN COALESCE(w.rain_sum, 0) > 0 THEN TRUE ELSE FALSE END AS is_rainy,

    -- Is snowy
    CASE WHEN COALESCE(w.snowfall_sum, 0) > 0 THEN TRUE ELSE FALSE END AS is_snowy

FROM with_borough_name w

