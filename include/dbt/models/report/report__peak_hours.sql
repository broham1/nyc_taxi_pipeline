with trips_data as (
    
    select

        dpt."hour" as hour_of_day,
        dt.taxi_type,
        count(*) as total_trips

    from {{ ref('fct__trips') }} as t

    inner join {{ ref('dim__taxi_type') }} as dt on dt.taxi_type_key = t.taxi_type_key

    inner join {{ ref('dim__time') }} as dpt on dpt.time_key = t.pickup_time_key

    group by dpt."hour", dt.taxi_type
)

select

    hour_of_day,
    sum(case when taxi_type = 'yellow' then total_trips else 0 end) as yellow_taxi_trips,
    sum(case when taxi_type = 'green' then total_trips else 0 end) as green_taxi_trips

from trips_data

group by hour_of_day

order by hour_of_day  