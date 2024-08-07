with 

all_times as (
    
    select

        time_ts as "time",
        hour(time_ts) as "hour",
        minute(time_ts) as "minute",
        second(time_ts) as "second"

    from {{ ref('int__combined_times') }}

)


select

    row_number() over (order by "time") as time_key, 
    "time",
    "hour",
    "minute",
    "second",
    case
        when "hour" >= 0 and "hour" < 6 then 'night'
        when "hour" >= 6 and "hour" < 12 then 'morning'
        when "hour" >= 12 and "hour" < 18 then 'afternoon'
        when "hour" >= 18 and "hour" < 24 then 'evening'
        else null
    end as time_of_day

from all_times
