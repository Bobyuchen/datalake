select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ts
from "datalake"."analytics_source"."src_auth_events"
where ts is null



      
    ) dbt_internal_test