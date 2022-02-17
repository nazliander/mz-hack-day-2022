{{
    config(
        materialized ='materializedview',
        tags=['crypto']
    )
}}

{# 20 mins #}
{% set slide_threshold = '1200000' %}

WITH with_casted_insertion AS (

    SELECT *, extract(epoch from timestamp) * 1000 AS inserted_at
    FROM {{ ref('stg_crypto') }}

)

SELECT * FROM with_casted_insertion
WHERE TRUE
    AND mz_logical_timestamp() >= inserted_at
    AND mz_logical_timestamp() < inserted_at + {{ slide_threshold }}
