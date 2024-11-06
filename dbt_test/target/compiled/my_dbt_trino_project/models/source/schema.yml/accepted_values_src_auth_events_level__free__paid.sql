
    
    

with all_values as (

    select
        level as value_field,
        count(*) as n_records

    from "datalake"."analytics_source"."src_auth_events"
    group by level

)

select *
from all_values
where value_field not in (
    'free','paid'
)


