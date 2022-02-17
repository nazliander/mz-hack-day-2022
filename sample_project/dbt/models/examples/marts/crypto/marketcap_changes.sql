
{{
    config(
        materialized ='materializedview',
        tags=['crypto']
    )
}}


WITH prepare_data AS (

    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) = 1 AS latest_record_in_batch,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp ASC) = 1 AS first_record_in_batch
    FROM {{ ref('fct_crypto_sliding_window') }}
    
),

latest_records AS (

    SELECT * FROM prepare_data WHERE latest_record_in_batch
    
),

first_records AS (

    SELECT * FROM prepare_data WHERE first_record_in_batch
    
),

combined_comparison AS (

    SELECT
        latest_records.uuid,
        latest_records.symbol,
        latest_records.marketcap AS last_marketcap,
        first_records.marketcap AS first_marketcap
    FROM latest_records
    INNER JOIN first_records ON first_records.symbol = latest_records.symbol
    
)

SELECT
    symbol,
    ((last_marketcap - first_marketcap) / first_marketcap) * 100 AS marketcap_changes
FROM combined_comparison
ORDER BY marketcap_changes DESC
