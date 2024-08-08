select

    dpl.zone,
    dt.taxi_type,
    count(*) as total_trips,

from {{ ref('fct__trips') }} as t

inner join {{ ref('dim__taxi_type') }} as dt on dt.taxi_type_key = t.taxi_type_key

inner join {{ ref('dim__location') }} as dpl on dpl.location_key = t.pickup_location_key

group by dpl.zone, dt.taxi_type

having dt.taxi_type = 'yellow'

order by total_trips desc

limit 10
