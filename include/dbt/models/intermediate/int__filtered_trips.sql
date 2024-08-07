with

all_trips as (

    select * from {{ ref('int__all_trips') }}

)

select 

    *
    
from all_trips

where 

    vendor_ID in (1, 2) 

    and trip_duration_seconds > 0

    and trip_distance_in_miles > 0

    and ratecode_ID in (1, 2, 3, 4, 5, 6)

    and payment_type in (1, 2, 3, 4, 5, 6)

    and passenger_count > 0

    and fare_amount > 0

    and misc_charges >= 0

    and mta_tax >= 0

    and improvement_surcharge >= 0

    and tip_amount >= 0

    and tolls_amount >= 0

    and total_amount > 0

    and (congestion_surcharge >= 0 or congestion_surcharge is null)

    and (airport_fee >= 0 or airport_fee is null)

