select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

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



      
    ) dbt_internal_test