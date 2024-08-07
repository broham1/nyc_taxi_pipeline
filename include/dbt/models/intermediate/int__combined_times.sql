with 

all_times as (

  select distinct time(pu_timestamp) as time_ts from {{ ref('int__filtered_trips') }}

  union
  
  select distinct time(do_timestamp) as time_ts from {{ ref('int__filtered_trips') }}

)

select * from all_times