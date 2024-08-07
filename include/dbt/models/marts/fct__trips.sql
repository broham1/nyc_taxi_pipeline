with 

trips as (

    select * from {{ ref('int__filtered_trips') }}

)

select 

        row_number() over (order by (select null)) as trip_key,
        dt.taxi_type_key,
        dv.vendor_key,
        dpt.time_key as pickup_time_key,
        ddt.time_key as dropoff_time_key,
        dpd.date_key as pickup_date_key,
        ddd.date_key as dropoff_date_key,
        dpl.location_key as pickup_location_key,
        ddl.location_key as dropoff_location_key,
        dr.ratecode_key,
        dtt.trip_type_key,
        dp.payment_key,
        t.trip_duration_seconds,
        t.trip_distance_in_miles,
        t.passenger_count,
        t.fare_amount,
        t.misc_charges,
        t.mta_tax,
        t.improvement_surcharge,
        t.tip_amount,
        t.tolls_amount,
        t.total_amount,
        t.congestion_surcharge,
        t.airport_fee

    from trips as t

    inner join {{ ref('dim__taxi_type') }} as dt on dt.taxi_type = t.taxi_type

    inner join {{ ref('dim__vendors') }} as dv on dv.vendor_ID = t.vendor_ID

    inner join {{ ref('dim__time') }} as dpt on dpt."time" = time(t.pu_timestamp)

    inner join {{ ref('dim__time') }} as ddt on ddt."time" = time(t.do_timestamp)

    inner join {{ ref('dim__date') }} as dpd on dpd."date" = date(t.pu_timestamp)

    inner join {{ ref('dim__date') }} as ddd on ddd."date" = date(t.do_timestamp)

    inner join {{ ref('dim__location') }} as dpl on dpl.location_key = t.pu_location_ID

    inner join {{ ref('dim__location') }} as ddl on ddl.location_key = t.do_location_ID

    inner join {{ ref('dim__ratecode') }} as dr on dr.ratecode_ID = t.ratecode_ID

    inner join {{ ref('dim__payment') }} as dp on dp.payment_type = t.payment_type

    left join {{ ref('dim__trip_type') }} as dtt on dtt.trip_type_ID = t.trip_type