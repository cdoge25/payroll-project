{{
    config(
        materialized='incremental',
        unique_key='allowance_pk',
        incremental_strategy='merge',
    )
}}
WITH allowances AS (
    SELECT *
    FROM {{ ref('stg_allowances') }}
),
employees AS (
    SELECT *
    FROM {{ ref('dim_employees') }}
),
allowances_joined AS (
    SELECT
        a.allowance_id AS allowance_pk,
        e.employee_pk AS employee_fk,
        a.allowance_type,
        a.allowance_amount,
        a.allowance_start_date,
        a.allowance_end_date,
        TO_NUMBER(TO_CHAR(a.allowance_start_date, 'YYYYMMDD')) AS allowance_start_date_fk,
        TO_NUMBER(TO_CHAR(a.allowance_end_date, 'YYYYMMDD')) AS allowance_end_date_fk
    FROM allowances a
    LEFT JOIN employees e
        ON a.employee_id = e.employee_id
    WHERE e.dbt_valid_to IS NULL
)
SELECT *
FROM allowances_joined