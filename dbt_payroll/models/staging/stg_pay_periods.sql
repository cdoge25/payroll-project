WITH source AS (
    SELECT *
    FROM {{ source('landing', 'dim_pay_period') }}
),
pay_periods_transformed AS (
    SELECT
        pay_period_id,
        period_start_date,
        period_end_date,
        payday,
        frequency,
        pay_period_label
    FROM source
    ORDER BY pay_period_id
)
SELECT *
FROM pay_periods_transformed