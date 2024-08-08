with revenue_data as (

    select

        dpd."year" as revenue_year,
        dpd."month" as revenue_month,
        dt.taxi_type,
        sum(t.total_amount) as monthly_revenue

    from {{ ref('fct__trips') }} as t

    inner join {{ ref('dim__date') }} as dpd on dpd.date_key = t.pickup_date_key

    inner join {{ ref('dim__taxi_type') }} as dt on dt.taxi_type_key = t.taxi_type_key

    group by dpd."year", dpd."month", dt.taxi_type
)

select

    revenue_year,
    revenue_month,
    sum(case when taxi_type = 'yellow' then monthly_revenue else 0 end) as yellow_taxi_revenue,
    sum(case when taxi_type = 'green' then monthly_revenue else 0 end) as green_taxi_revenue

from revenue_data

group by revenue_year, revenue_month

order by revenue_year, revenue_month
