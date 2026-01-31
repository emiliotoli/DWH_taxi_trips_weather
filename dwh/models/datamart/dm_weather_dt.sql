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
        o.weather_time,
        o.temperature,
        o.apparent_temperature,
        o.rain,
        o.snowfall,
        o.wind_speed,
        o.humidity,
        o.borough_fk,
        o.last_update as ods_update_time
    FROM {{ ref('ods_weather_dt')}} as o
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
        o.weather_time,
        o.temperature,
        o.apparent_temperature,
        o.rain,
        o.snowfall,
        o.wind_speed,
        o.humidity,
        coalesce(b.borough_name, 'unknown') AS borough_name,
    FROM from_ods as o LEFT JOIN {{source("ods", "ods_borough")}} as b
    ON o.borough_fk = b.id_borough
)

SELECT
    NEXTVAL('weather_seq') as key_weather,
    w.id_weather,
    w.weather_date,
    w.weather_time,
    w.temperature,
    w.apparent_temperature,
    w.rain,
    w.snowfall,
    w.wind_speed,
    w.humidity,
    w.borough_name,
    --- calculated attributes
    CASE
        WHEN w.temperature < -10 THEN 'Extreme Cold'
        WHEN w.temperature < 0 THEN 'Freezing'
        WHEN w.temperature < 10 THEN 'Cold'
        WHEN w.temperature < 20 THEN 'Mild'
        WHEN w.temperature < 30 THEN 'Warm'
        WHEN w.temperature < 35 THEN 'Hot'
        ELSE 'Extreme Heat'
    END AS temperature_category,
    CASE
        WHEN w.apparent_temperature < 0 THEN 'Feels Freezing'
        WHEN w.apparent_temperature < 10 THEN 'Feels Cold'
        WHEN w.apparent_temperature < 20 THEN 'Feels Mild'
        WHEN w.apparent_temperature < 30 THEN 'Feels Warm'
        ELSE 'Feels Hot'
    END AS apparent_temperature_category,
    CASE
        WHEN w.rain = 0 THEN 'No Rain'
        WHEN w.rain < 2.5 THEN 'Light Rain'
        WHEN w.rain < 10 THEN 'Moderate Rain'
        WHEN w.rain < 50 THEN 'Heavy Rain'
        ELSE 'Violent Rain'
    END AS rain_intensity,
    CASE
        WHEN w.snowfall = 0 THEN 'No Snow'
        WHEN w.snowfall < 0.5 THEN 'Light Snow'
        WHEN w.snowfall < 4 THEN 'Moderate Snow'
        ELSE 'Heavy Snow'
    END AS snow_intensity,
            -- Is rainy
    CASE WHEN COALESCE(w.rain, 0) > 0 THEN TRUE ELSE FALSE END AS is_rainy,

    -- Is snowy
    CASE WHEN COALESCE(w.snowfall, 0) > 0 THEN TRUE ELSE FALSE END AS is_snowy,

    -- Is windy
    CASE
        WHEN w.wind_speed = 0 THEN 'No Wind'
        WHEN w.wind_speed < 19 THEN 'Light Wind'
        WHEN w.wind_speed < 38 THEN 'Moderate Wind'
        WHEN w.wind_speed < 61 THEN 'Strong Wind'
        ELSE 'Very Strong Wind'
    END AS wind_intensity,

FROM with_borough_name w

