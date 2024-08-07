with 

all_dates as (

  select distinct date(pu_timestamp) as date_ts from {{ ref('int__filtered_trips') }}

  union
  
  select distinct date(do_timestamp) as date_ts from {{ ref('int__filtered_trips') }}

)

select * from all_dates