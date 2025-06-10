{{
    config(
        materialized='incremental',
        unique_key='timesheet_pk',
        incremental_strategy='merge',
    )
}}
WITH timesheet AS (
    SELECT *
    FROM {{ ref('stg_timesheet') }}
),
employees AS (
    SELECT *
    FROM {{ ref('dim_employees') }}
),
contracts AS (
    SELECT *
    FROM {{ ref('dim_contracts') }}
),
timesheet_joined AS (
    SELECT
        t.timesheet_id AS timesheet_pk,
        e.employee_pk AS employee_fk,
        c.contract_pk AS contract_fk,
        t.timesheet_transaction_date,
        TO_NUMBER(TO_CHAR(t.timesheet_transaction_date, 'YYYYMMDD')) AS timesheet_transaction_date_fk,
        t.start_time,
        t.end_time,
        t.timesheet_transaction_hours
    FROM timesheet t
    LEFT JOIN employees e
        ON t.employee_id = e.employee_id
    LEFT JOIN contracts c
        ON t.employee_id = c.employee_id
        AND t.timesheet_transaction_date BETWEEN c.start_date AND COALESCE(c.end_date, '9999-12-31')
    WHERE e.dbt_valid_to IS NULL
    AND c.dbt_valid_to IS NULL
)
SELECT *
FROM timesheet_joined