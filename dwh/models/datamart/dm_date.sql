{{ config(materialized='table') }}

WITH date_spine AS (

    -- genera una riga per ogni giorno
    SELECT
        d::date AS date
    FROM generate_series(
        DATE '2000-01-01',
        DATE '2035-12-31',
        INTERVAL 1 DAY
    ) AS t(d)

)

SELECT
    date,

    -- nome giorno
    lower(strftime(date, '%A')) AS day_name,

    -- nome mese
    lower(strftime(date, '%B')) AS month_name,

    -- anno
    EXTRACT(year FROM date)::SMALLINT AS year,

    -- settimana ISO
    EXTRACT(week FROM date)::INT AS week

FROM date_spine
ORDER BY date
