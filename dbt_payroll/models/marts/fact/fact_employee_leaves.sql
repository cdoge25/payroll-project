{{
    config(
        materialized='incremental',
        unique_key='leave_pk',
        incremental_strategy='merge',
    )
}}
WITH employee_leaves AS (
    SELECT *
    FROM {{ ref('stg_employee_leaves') }}
),
employees AS (
    SELECT *
    FROM {{ ref('dim_employees') }}
),
employee_leaves_joined AS (
    SELECT
        l.leave_id AS leave_pk,
        e.employee_pk AS employee_fk,
        l.leave_type,
        l.leave_start_date,
        l.leave_end_date,
        TO_NUMBER(TO_CHAR(l.leave_start_date, 'YYYYMMDD')) AS leave_start_date_fk,
        TO_NUMBER(TO_CHAR(l.leave_end_date, 'YYYYMMDD')) AS leave_end_date_fk,
        leave_hours
    FROM employee_leaves l
    LEFT JOIN employees e
        ON l.employee_id = e.employee_id
    WHERE e.dbt_valid_to IS NULL
)
SELECT *
FROM employee_leaves_joined