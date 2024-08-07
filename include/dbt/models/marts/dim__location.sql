with 

locations as (

    select * from {{ source('location','taxi_zone_lookup') }}

)

select

    locationID as location_key,
    borough,
    zone, 
    service_zone

from locations
