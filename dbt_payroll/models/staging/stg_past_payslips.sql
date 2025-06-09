WITH source AS (
    SELECT *
    FROM {{ source('landing', 'past_payslips') }}
),