{{
    config(
        materialized='incremental',
        unique_key='bonus_pk',
        incremental_strategy='merge',
    )
}}
WITH bonuses AS (
    SELECT *
    FROM {{ ref('stg_bonuses') }}
),
employees AS (
    SELECT *
    FROM {{ ref('dim_employees') }}
),
bonuses_joined AS (
    SELECT
        b.bonus_id AS bonus_pk,
        e.employee_pk AS employee_fk,
        b.bonus_type,
        b.bonus_amount,
        b.bonus_date,
        TO_NUMBER(TO_CHAR(b.bonus_date, 'YYYYMMDD')) AS bonus_date_fk
    FROM bonuses b
    LEFT JOIN employees e
        ON b.employee_id = e.employee_id
    WHERE e.dbt_valid_to IS NULL
)
SELECT *
FROM bonuses_joined