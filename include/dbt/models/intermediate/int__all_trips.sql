with 

green_trips as (

    select * from {{ ref('stg__green_tripdata') }}

),

yellow_trips as (

    select * from {{ ref('stg__yellow_tripdata') }}

)

select
        'green' as taxi_type,
        vendor_ID,
        pu_timestamp,
        do_timestamp,
        datediff('second', pu_timestamp, do_timestamp) as trip_duration_seconds,
        trip_distance_in_miles,
        pu_location_ID,
        do_location_ID,
        passenger_count,
        ratecode_ID,
        payment_type,
        fare_amount,
        misc_charges,
        mta_tax,
        improvement_surcharge,
        tip_amount,
        tolls_amount,
        total_amount,
        congestion_surcharge,
        airport_fee,
        trip_type

from green_trips

union 

select
        'yellow' as taxi_type,
        vendor_ID,
        pu_timestamp,
        do_timestamp,
        datediff('second', pu_timestamp, do_timestamp) as trip_duration_seconds,
        trip_distance_in_miles,
        pu_location_ID,
        do_location_ID,
        passenger_count,
        ratecode_ID,
        payment_type,
        fare_amount,
        misc_charges,
        mta_tax,
        improvement_surcharge,
        tip_amount,
        tolls_amount,
        total_amount,
        congestion_surcharge,
        airport_fee,
        trip_type

from yellow_trips