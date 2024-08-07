with

source as (

    select * from {{ source('trip_data','green_tripdata_ext') }}

),

renamed as (

    select
 
        vendorID as vendor_ID,
        lpep_pickup_datetime as pu_timestamp,
        lpep_dropoff_datetime as do_timestamp,
        trip_distance as trip_distance_in_miles,
        PULocationID as pu_location_ID,
        DOLocationID as do_location_ID,
        passenger_count as passenger_count,
        ratecodeID as ratecode_ID,
        payment_type as payment_type,
        fare_amount as fare_amount,
        extra as misc_charges,
        mta_tax as mta_tax,
        improvement_surcharge as improvement_surcharge,
        tip_amount as tip_amount,
        tolls_amount as tolls_amount,
        total_amount as total_amount,
        null as congestion_surcharge,
        null as airport_fee,
        trip_type as trip_type

    from source

)

select * from renamed