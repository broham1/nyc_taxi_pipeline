with

all_payments as (

    select distinct payment_type as pt from {{ ref('int__filtered_trips') }}

)

select

    row_number() over (order by pt asc) as payment_key,
    pt as payment_type,
    case
        when pt = 1 then 'Credit'
        when pt = 2 then 'Cash'
        when pt = 3 then 'No Charge'
        when pt = 4 then 'Dispute'
        when pt = 5 then 'Unknown'
        when pt = 6 then 'Voided Trip'
    end as payment_desc

from all_payments