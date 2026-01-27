{{ config(
    materialized='incremental',
    unique_key='key_zone',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

{% set initialize %}
    -- Create a sequence to generate incremental surrogate keys
    CREATE SEQUENCE IF NOT EXISTS zone_seq;
{% endset %}

{% do run_query(initialize) %}

WITH from_ods AS (
    SELECT
        n.id_neighborhood,
        n.neighborhood_name,
        n.service_zone,
        b.borough_name,
        n.last_update AS ods_update_time
    FROM {{ ref('ods_borough') }} AS b
    INNER JOIN {{ ref('ods_neighborhood') }} AS n
        ON b.id_borough = n.borough_fk
    {% if is_incremental() %}
    WHERE n.last_update > (
        SELECT COALESCE(MAX(time), '1900-01-01 00:00:00')
        FROM last_execution_times
        WHERE target_table = '{{ this.identifier }}'
    )
    {% endif %}
)
{% if is_incremental() %}

,changed_records AS (
    SELECT t.key_zone
    FROM {{ this }} AS t
    INNER JOIN from_ods AS o
        ON t.neighborhood_name = o.neighborhood_name
        AND t.borough_name = o.borough_name
        AND t.is_current = TRUE
        AND (
            t.id_neighborhood <> o.id_neighborhood  -- ID cambiato
            OR t.service_zone <> o.service_zone     -- service_zone cambiato
        )
),

-- Chiudi i record esistenti che sono cambiati
records_to_close AS (
    SELECT
        t.key_zone,
        t.id_neighborhood,
        t.neighborhood_name,
        coalesce(t.borough_name, 'unknown') as borough_name,
        t.service_zone,
        t.valid_from,
        o.ods_update_time AS valid_to,
        FALSE AS is_current
    FROM {{ this }} AS t
    INNER JOIN from_ods AS o
        ON t.neighborhood_name = o.neighborhood_name
        AND t.borough_name = o.borough_name
        AND is_current = true
    WHERE t.key_zone IN (SELECT key_zone FROM changed_records)
),

new_records AS (
    SELECT
        NEXTVAL('zone_seq') AS key_zone,
        o.id_neighborhood,
        o.neighborhood_name,
        o.borough_name,
        o.service_zone,
        o.ods_update_time AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current
    FROM from_ods AS o
    LEFT JOIN {{ this }} AS t
        ON t.neighborhood_name = o.neighborhood_name
        AND t.borough_name = o.borough_name
        AND t.is_current = TRUE
    WHERE t.key_zone IS NULL  -- Nuovi record (quartiere mai visto)
       OR t.id_neighborhood <> o.id_neighborhood  -- ID cambiato
       OR t.service_zone <> o.service_zone        -- service_zone cambiato
)
SELECT * FROM records_to_close
UNION ALL
SELECT * FROM new_records

{% else %}

SELECT
    NEXTVAL('zone_seq') AS key_zone,
    id_neighborhood,
    neighborhood_name,
    borough_name,
    service_zone,
    ods_update_time AS valid_from,
    CAST(NULL AS TIMESTAMP) AS valid_to,
    TRUE AS is_current
FROM from_ods

{% endif %}