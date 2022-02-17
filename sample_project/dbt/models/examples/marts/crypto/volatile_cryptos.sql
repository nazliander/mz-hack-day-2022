{{
    config(
        materialized ='materializedview',
        tags=['crypto']
    )
}}


WITH prices AS (

    SELECT
        symbol,
        MAX(price) AS max_price,
        MIN(price) AS min_price,
        AVG(price) AS mean_price
    FROM {{ ref('fct_crypto_sliding_window') }}
    GROUP BY
        symbol

)

SELECT
    symbol,
    ((max_price - min_price) / mean_price) * 100 AS deviation_price
FROM prices
ORDER BY deviation_price DESC
LIMIT 10
