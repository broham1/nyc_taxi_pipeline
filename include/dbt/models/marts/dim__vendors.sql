with

vendors as (

    select distinct vendor_ID as vid from {{ ref('int__filtered_trips') }}

)

select

    row_number() over (order by vid asc) as vendor_key,
    vid as vendor_ID,
    case
        when vid = 1 then 'Creative Mobile Technologies, LLC'
        when vid = 2 then 'VeriFone Inc.'
        else null
    end as vendor

from vendors