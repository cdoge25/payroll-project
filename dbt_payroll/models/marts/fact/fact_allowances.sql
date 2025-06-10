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
contracts AS (
    SELECT *
    FROM {{ ref('dim_contracts') }}
),
allowances_joined AS (
    SELECT
        a.allowance_id AS allowance_pk,
        e.employee_pk AS employee_fk,
        c.contract_pk AS contract_fk,
        a.allowance_type,
        a.allowance_amount,
        a.allowance_start_date,
        a.allowance_end_date,
        TO_NUMBER(TO_CHAR(a.allowance_start_date, 'YYYYMMDD')) AS allowance_start_date_fk,
        TO_NUMBER(TO_CHAR(a.allowance_end_date, 'YYYYMMDD')) AS allowance_end_date_fk
    FROM allowances a
    LEFT JOIN employees e
        ON a.employee_id = e.employee_id
    LEFT JOIN contracts c
        ON a.employee_id = c.employee_id
        AND a.allowance_start_date BETWEEN c.start_date AND COALESCE(c.end_date, '9999-12-31')
    WHERE e.dbt_valid_to IS NULL
    AND c.dbt_valid_to IS NULL
)
SELECT *
FROM allowances_joined