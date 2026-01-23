{{ config(
    materialized='table',
    schema='datamart'
) }}

WITH date_spine AS (
    -- Genera una riga per ogni giorno
    SELECT
        d::DATE AS date_value
    FROM generate_series(
        DATE '2024-01-01',
        DATE '2050-12-31',
        INTERVAL 1 DAY
    ) AS t(d)
)

SELECT
    -- Chiave surrogata (formato YYYYMMDD, es: 20240615)
    CAST(strftime(date_value, '%Y%m%d') AS INTEGER) AS key_date,

    -- Data completa
    date_value AS date,

    -- Nome giorno
    lower(strftime(date_value, '%A')) AS day_name,

    -- Nome mese
    lower(strftime(date_value, '%B')) AS month_name,

    -- Anno
    EXTRACT(year FROM date_value)::SMALLINT AS year,

    -- Settimana ISO
    EXTRACT(week FROM date_value)::INT AS week,

    -- ========== WEEKEND ==========
    CASE
        WHEN EXTRACT(DOW FROM date_value) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend,

    -- ========== STAGIONI (emisfero nord) ==========
    CASE
        WHEN EXTRACT(MONTH FROM date_value) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM date_value) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM date_value) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM date_value) IN (9, 10, 11) THEN 'Fall'
    END AS season,

-- ========== FESTIVITÀ USA ==========
CASE
    -- New Year's Day
    WHEN strftime(date_value, '%m-%d') = '01-01' THEN TRUE
    -- Martin Luther King Jr. Day (3° lunedì di gennaio)
    -- Independence Day
    WHEN strftime(date_value, '%m-%d') = '07-04' THEN TRUE
    -- Veterans Day
    WHEN strftime(date_value, '%m-%d') = '11-11' THEN TRUE
    -- Christmas Day
    WHEN strftime(date_value, '%m-%d') = '12-25' THEN TRUE
    ELSE FALSE
END AS is_holiday,

CASE
    WHEN strftime(date_value, '%m-%d') = '01-01' THEN 'New Year''s Day'
    WHEN strftime(date_value, '%m-%d') = '07-04' THEN 'Independence Day'
    WHEN strftime(date_value, '%m-%d') = '11-11' THEN 'Veterans Day'
    WHEN strftime(date_value, '%m-%d') = '12-25' THEN 'Christmas Day'
    ELSE NULL
END AS holiday_name

FROM date_spine
ORDER BY date_value