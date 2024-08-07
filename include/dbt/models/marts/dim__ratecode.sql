with 

ratecodes as (

    select distinct ratecode_ID as rid from {{ ref('int__filtered_trips') }}

)

select

    row_number() over (order by rid asc) as ratecode_key,
    rid as ratecode_ID, 
    case
        when rid = 1 then 'Standard Rate'
        when rid = 2 then 'JFK'
        when rid = 3 then 'Newark'
        when rid = 4 then 'Nassau or Westchester'
        when rid = 5 then 'Negotiated Fare'
        when rid = 6 then 'Group Ride'
    end as ratecode

from ratecodes