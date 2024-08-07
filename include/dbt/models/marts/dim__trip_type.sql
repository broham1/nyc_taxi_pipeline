with 

types as (

    select distinct trip_type as tt from {{ ref('int__filtered_trips') }}

)

select 

    row_number() over (order by tt asc) as trip_type_key,
    tt as trip_type_ID,
    case
        when tt = 1 then 'Street-hail'
        when tt = 2 then 'Dispatch'
    end as trip_type

from types
