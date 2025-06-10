{{
    config(
        materialized='incremental',
        unique_key='toil_pk',
        incremental_strategy='merge',
    )
}}
WITH time_off_in_lieu AS (
    SELECT *
    FROM {{ ref('stg_time_off_in_lieu') }}
),
employees AS (
    SELECT *
    FROM {{ ref('dim_employees') }}
),
time_off_in_lieu_joined AS (
    SELECT
        t.toil_id AS toil_pk,
        e.employee_pk AS employee_fk,
        t.overtime_date,
        TO_NUMBER(TO_CHAR(t.overtime_date, 'YYYYMMDD')) AS overtime_date_fk,
        t.toil_hours_accrued,
        t.toil_usage_date,
        TO_NUMBER(TO_CHAR(t.toil_usage_date, 'YYYYMMDD')) AS toil_usage_date_fk
    FROM time_off_in_lieu t
    LEFT JOIN employees e
        ON t.employee_id = e.employee_id
    WHERE e.dbt_valid_to IS NULL
)
SELECT *
FROM time_off_in_lieu_joined