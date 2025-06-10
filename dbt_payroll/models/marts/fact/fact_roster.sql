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
contracts AS (
    SELECT *
    FROM {{ ref('dim_contracts') }}
),
roster_joined AS (
    SELECT
        r.roster_id AS roster_pk,
        e.employee_pk AS employee_fk,
        c.contract_pk AS contract_fk,
        r.shift,
        r.hours,
        r.work_date,
        TO_NUMBER(TO_CHAR(r.work_date, 'YYYYMMDD')) AS work_date_fk,
        r.services,
        r.location
    FROM roster r
    LEFT JOIN employees e
        ON r.employee_id = e.employee_id
    LEFT JOIN contracts c
        ON r.employee_id = c.employee_id
        AND r.work_date BETWEEN c.start_date AND COALESCE(c.end_date, '9999-12-31')
    WHERE e.dbt_valid_to IS NULL
    AND c.dbt_valid_to IS NULL
)
SELECT *
FROM roster_joined