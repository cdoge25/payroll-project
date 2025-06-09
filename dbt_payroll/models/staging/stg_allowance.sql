WITH source AS (
    SELECT *
    FROM {{ source('landing', 'allowance') }}
),
allowance_transformed AS (
    SELECT
        allowance_id,
        employee_id,
        allowance_type,
        allowance_amount,
        allowance_start_date,
        allowance_end_date
    FROM source
)
SELECT *
FROM allowance_transformed