select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select sessionId
from "datalake"."analytics_source"."src_auth_events"
where sessionId is null



      
    ) dbt_internal_test