{{
    config(
        materialized='view',
    )
}}
WITH base AS (
    SELECT
        t.employee_fk,
        t.contract_fk,
        t.timesheet_transaction_date_fk,
        t.start_time,
        t.end_time,
        t.timesheet_transaction_hours,
        EXTRACT(DOW FROM t.timesheet_transaction_date) AS day_of_week,
        d.pay_period_week,
        d.pay_period_fortnight,
        d.pay_period_month
    FROM {{ ref('fact_timesheet') }} t
    LEFT JOIN {{ ref('dim_dates') }} d
        ON t.timesheet_transaction_date_fk = d.date_pk
),
classified AS (
    SELECT
        employee_fk,
        contract_fk,
        timesheet_transaction_hours,
        pay_period_week,
        pay_period_fortnight,
        pay_period_month,

        -- Night shift condition: starts after 22:00 and ends before 06:00
        CASE 
            WHEN start_time >= TIME '22:00:00' OR end_time <= TIME '06:00:00' THEN timesheet_transaction_hours
            ELSE 0
        END AS night_shift_hours,

        -- Sunday condition: day_of_week = 0 (Sunday)
        CASE 
            WHEN day_of_week = 0 THEN timesheet_transaction_hours
            ELSE 0
        END AS sunday_hours,

        -- Ordinary hours (initially equal to total hours)
        timesheet_transaction_hours AS raw_ordinary_hours
    FROM base
),
aggregated AS (
    SELECT
        employee_fk,
        contract_fk,
        pay_period_week,
        pay_period_fortnight,
        pay_period_month,
        SUM(timesheet_transaction_hours) AS total_hours,
        SUM(night_shift_hours) AS night_shift_hours,
        SUM(sunday_hours) AS sunday_hours,
        SUM(raw_ordinary_hours) AS ordinary_hours
    FROM classified
    GROUP BY employee_fk, contract_fk, pay_period_week, pay_period_fortnight, pay_period_month
),
resolved AS (
    SELECT
        *,
        -- Determine overtime based on weekly threshold of 38 hours
        CASE 
            WHEN total_hours > 38 THEN LEAST(total_hours - 38, 2)
            ELSE 0
        END AS overtime_first_2_hours,

        CASE 
            WHEN total_hours > 40 THEN total_hours - 40
            ELSE 0
        END AS overtime_after_2_hours
    FROM aggregated
),
final_adjustment AS (
    SELECT
        employee_fk,
        contract_fk,
        total_hours,
        pay_period_week,
        pay_period_fortnight,
        pay_period_month,

        -- Overtime is deducted from ordinary hours first
        GREATEST(ordinary_hours - overtime_first_2_hours - overtime_after_2_hours - night_shift_hours - sunday_hours, 0) AS ordinary_hours,

        night_shift_hours,
        sunday_hours,
        0 AS sunday_overtime_hours,  -- All Sunday hours treated as penalty not overtime as per business rule
        overtime_first_2_hours,
        overtime_after_2_hours
    FROM resolved
)
SELECT *
FROM final_adjustment
