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
timesheet_joined AS (
    SELECT
        t.timesheet_id AS timesheet_pk,
        e.employee_pk AS employee_fk,
        t.timesheet_transaction_date,
        TO_NUMBER(TO_CHAR(t.timesheet_transaction_date, 'YYYYMMDD')) AS timesheet_transaction_date_fk,
        t.start_time,
        t.end_time,
        t.timesheet_transaction_hours,
        t.pay_period_id AS pay_period_fk
    FROM timesheet t
    LEFT JOIN employees e
        ON t.employee_id = e.employee_id
    WHERE e.dbt_valid_to IS NULL
)
SELECT *
FROM timesheet_joined