with 

all_dates as (
    
    select 

        date_ts as "date",
        year(date_ts) as "year",
        quarter(date_ts) as "quarter",
        month(date_ts) as "month",
        monthname(date_ts) as monthname,
        week(date_ts) as "week",
        day(date_ts) as "day",
        dayname(date_ts) as day_of_week
    
    from {{ ref('int__combined_dates') }}

)

select distinct

    row_number() over (order by "date") as date_key, -- surrogate key
    *

from all_dates

order by "date" asc