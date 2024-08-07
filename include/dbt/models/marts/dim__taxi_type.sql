with 

types as (

    select 'green' as taxi_type

    union

    select 'yellow' as taxi_type

)

select

    row_number() over (order by taxi_type) as taxi_type_key,
    taxi_type

from types