
    
    

with all_values as (

    select
        success as value_field,
        count(*) as n_records

    from "datalake"."analytics_source"."src_auth_events"
    group by success

)

select *
from all_values
where value_field not in (
    'True','False'
)


