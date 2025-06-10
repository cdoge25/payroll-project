{{
    config(
        materialized='incremental',
        unique_key='roster_pk',
        incremental_strategy='merge',
    )
}}
WITH roster AS (
    SELECT *
    FROM {{ ref('stg_roster') }}
),
employees AS (
    SELECT *
    FROM {{ ref('dim_employees') }}
),
roster_joined AS (
    SELECT
        r.roster_id AS roster_pk,
        e.employee_pk AS employee_fk,
        r.shift,
        r.hours,
        r.work_date,
        TO_NUMBER(TO_CHAR(r.work_date, 'YYYYMMDD')) AS work_date_fk,
        r.services,
        r.location,
        r.pay_period_id AS pay_period_fk
    FROM roster r
    LEFT JOIN employees e
        ON r.employee_id = e.employee_id
    WHERE e.dbt_valid_to IS NULL
)
SELECT *
FROM roster_joined