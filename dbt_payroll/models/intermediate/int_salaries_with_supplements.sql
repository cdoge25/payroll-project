{{
    config(
        materialized='view',
    )
}}

with all_supplements as (
    select
        employee_fk,
        contract_fk,
        bonus_date_fk as supplement_date_fk,
        bonus_amount as supplement_amount,
        'bonus' as supplement_type
    from {{ ref('fact_bonuses') }}

    union all

    select
        employee_fk,
        contract_fk,
        allowance_start_date_fk as supplement_date_fk,
        allowance_amount as supplement_amount,
        'allowance' as supplement_type
    from {{ ref('fact_allowances') }}
),

supplement_periods as (
    select
        s.employee_fk,
        dc.payment_frequency,
        case dc.payment_frequency
            when 'Weekly' then dd.pay_period_week
            when 'Fortnightly' then dd.pay_period_fortnight
            when 'Monthly' then dd.pay_period_month
        end as pay_period,
        sum(case when s.supplement_type = 'bonus' then s.supplement_amount else 0 end) as total_bonus_amount,
        sum(case when s.supplement_type = 'allowance' then s.supplement_amount else 0 end) as total_allowance_amount
    from all_supplements s
    join {{ ref('dim_dates') }} dd
        on s.supplement_date_fk = dd.date_pk
    join {{ ref('dim_contracts') }} dc
        on s.contract_fk = dc.contract_pk
    group by 1, 2, 3
),

salary_with_supplements as (
    select
        s.*,
        coalesce(sp.total_bonus_amount, 0) as bonus_amount,
        coalesce(sp.total_allowance_amount, 0) as allowance_amount,
        s.salary
            + coalesce(sp.total_bonus_amount, 0)
            + coalesce(sp.total_allowance_amount, 0) as salary_with_bonus_and_allowance

    from {{ ref('int_salaries') }} s
    left join supplement_periods sp
      on s.employee_fk = sp.employee_fk
     and s.payment_frequency = sp.payment_frequency
     and (
         (sp.payment_frequency = 'Weekly'     and sp.pay_period = s.pay_period_week) or
         (sp.payment_frequency = 'Fortnightly' and sp.pay_period = s.pay_period_fortnight) or
         (sp.payment_frequency = 'Monthly'    and sp.pay_period = s.pay_period_month)
     )
)

select *
from salary_with_supplements
