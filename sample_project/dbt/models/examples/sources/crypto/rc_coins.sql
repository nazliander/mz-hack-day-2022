{{ config(materialized='source', tags=['crypto']) }}

{% set source_name %}
    {{ mz_generate_name('rc_coins') }}
{% endset %}

CREATE SOURCE {{ source_name }}
FROM KAFKA BROKER 'redpanda:9092' TOPIC 'crypto'
  KEY FORMAT BYTES
  VALUE FORMAT BYTES
ENVELOPE UPSERT;
