{{ config(materialized='incremental',
unique_key='key_vendor',
incremental_strategy='delete+insert',
on_schema_change='fail')
}}

{% set initialize %}
    -- Create a sequence to generate incremental surrogate keys
    CREATE SEQUENCE IF NOT EXISTS vendor_seq;
{% endset %}

{% do run_query(initialize) %}

WITH from_ods AS (
    SELECT o.id_vendor,
            o.vendor_name,
            o.last_update as ods_update_time
    FROM {{ source("ods","ods_vendor") }} AS o
    {% if is_incremental() %}
    WHERE o.last_update > (
        SELECT COALESCE(MAX(time), '1900-01-01 00:00:00')
        FROM last_execution_times
        WHERE target_table = '{{ this.identifier }}'
    )
    {% endif %}
)
{% if is_incremental() %}
,changed_records as (
    SELECT t.key_vendor
    FROM {{this}} as t
    INNER JOIN from_ods o on t.id_vendor = o.id_vendor
    AND is_current=true
    AND t.vendor_name <> o.vendor_name
),

records_to_close as (
    SELECT
        t.key_vendor,
        t.id_vendor,
        t.vendor_name,
        t.valid_from,
        o.ods_update_time AS valid_to,
        FALSE AS is_current
    FROM {{this}} as t INNER JOIN from_ods as o
    ON t.id_vendor = o.id_vendor
    WHERE t.key_vendor IN (SELECT key_vendor FROM changed_records)
),
new_records as (
    SELECT
        NEXTVAL('vendor_seq') AS key_vendor,
        o.id_vendor,
        o.vendor_name,
        o.ods_update_time as valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current
    FROM from_ods as o LEFT JOIN {{this}} as t
    ON o.id_vendor = t.id_vendor
    AND t.is_current = TRUE
    WHERE t.key_vendor IS NULL OR o.vendor_name <> t.vendor_name
)

SELECT * FROM records_to_close
UNION ALL
SELECT * FROM new_records

    {% else %}

         SELECT
             NEXTVAL('vendor_seq') as key_vendor,
             o.id_vendor,
             o.vendor_name,
             o.ods_update_time as valid_from,
             CAST(NULL AS TIMESTAMP) AS valid_to,
             TRUE AS is_current
         FROM from_ods as o

    {% endif %}
