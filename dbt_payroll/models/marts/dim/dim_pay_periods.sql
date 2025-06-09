{{
    config(
        materialized='incremental',
        unique_key='pay_period_pk',
    )
}}
WITH pay_periods AS (
    SELECT
        pay_period_id AS pay_period_pk,
        period_start_date,
        period_end_date,
        payday,
        frequency,
        pay_period_label
    FROM {{ ref('stg_pay_periods') }}
)
SELECT *
FROM pay_periods