WITH source AS (
    SELECT *
    FROM {{ source('landing', 'bonus') }}
)
bonus_transformed AS (
    SELECT
        bonus_id,
        employee_id,
        bonus_type,
        bonus_amount,
        bonus_date
    FROM source
)
SELECT *
FROM bonus_transformed