{{
    config(
        materialized='view',
    )
}}
with weekly_hours as (
    select
        iwh.*,
        dc.pay_rate,
        dc.payment_frequency
    from {{ ref('int_weekly_hours') }} iwh
    left join {{ ref('dim_contracts') }} dc
        on iwh.contract_fk = dc.contract_pk
),

rate_multipliers as (
    select *
    from (values
        ('ORDINARY', 1.0),
        ('NIGHT_SHIFT', 1.25),
        ('SUNDAY', 2.0),
        ('SUNDAY_OVERTIME', 2.5),
        ('OVERTIME_FIRST_2', 1.5),
        ('OVERTIME_AFTER_2', 2.0)
    ) as r(rate_type, multiplier)
),

hours_extended as (
    select
        employee_fk,
        contract_fk,
        payment_frequency,
        pay_period_week,
        pay_period_fortnight,
        pay_period_month,
        case
            when payment_frequency = 'Weekly' then pay_period_week
            when payment_frequency = 'Fortnightly' then pay_period_fortnight
            when payment_frequency = 'Monthly' then pay_period_month
        end as pay_period,
        pay_rate,
        sum(total_hours) as total_hours,
        sum(ordinary_hours) as ordinary_hours,
        sum(night_shift_hours) as night_shift_hours,
        sum(sunday_hours) as sunday_hours,
        sum(sunday_overtime_hours) as sunday_overtime_hours,
        sum(overtime_first_2_hours) as overtime_first_2_hours,
        sum(overtime_after_2_hours) as overtime_after_2_hours
    from weekly_hours
    group by employee_fk, contract_fk, payment_frequency, pay_period_week, pay_period_fortnight, pay_period_month,
             case
                 when payment_frequency = 'Weekly' then pay_period_week
                 when payment_frequency = 'Fortnightly' then pay_period_fortnight
                 when payment_frequency = 'Monthly' then pay_period_month
             end,
             pay_rate
),

salaries as (
    select
        employee_fk,
        contract_fk,
        payment_frequency,
        pay_period,
        pay_period_week,
        pay_period_fortnight,
        pay_period_month,
        pay_rate,
        total_hours,
        ordinary_hours,
        night_shift_hours,
        sunday_hours,
        sunday_overtime_hours,
        overtime_first_2_hours,
        overtime_after_2_hours,
        -- Calculated pay components
        ordinary_hours * pay_rate as ordinary_pay,
        night_shift_hours * pay_rate * 1.25 as night_shift_pay,
        sunday_hours * pay_rate * 2.0 as sunday_pay,
        sunday_overtime_hours * pay_rate * 2.5 as sunday_overtime_pay,
        overtime_first_2_hours * pay_rate * 1.5 as overtime_first_2_pay,
        overtime_after_2_hours * pay_rate * 2.0 as overtime_after_2_pay,
        -- Total pay
        (
            ordinary_hours * pay_rate +
            night_shift_hours * pay_rate * 1.25 +
            sunday_hours * pay_rate * 2.0 +
            sunday_overtime_hours * pay_rate * 2.5 +
            overtime_first_2_hours * pay_rate * 1.5 +
            overtime_after_2_hours * pay_rate * 2.0
        ) as salary
    from hours_extended
)

select *
from salaries
