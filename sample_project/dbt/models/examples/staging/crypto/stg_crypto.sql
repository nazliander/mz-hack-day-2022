{{ config(
    materialized='view',
    tags=['crypto']
) }}

WITH converted_casted AS (
    
    SELECT 
        CAST(CONVERT_FROM(data, 'utf8') AS jsonb) AS data
    FROM {{ ref('rc_coins') }}

)

SELECT

    (data->>'uuid')::string as uuid,
    (data->>'name')::string as name,
    (data->>'symbol')::string as symbol,
    (data->>'btc_price')::numeric as btc_price,
    (data->>'last_24h_volume')::numeric as last_24h_volume,
    (data->>'marketcap')::numeric as marketcap,
    (data->>'price')::double as price,
    (data->>'timestamp')::timestamp as timestamp

FROM converted_casted
