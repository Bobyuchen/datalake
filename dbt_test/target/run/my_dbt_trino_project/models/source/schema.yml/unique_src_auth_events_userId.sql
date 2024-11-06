select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    userId as unique_field,
    count(*) as n_records

from "datalake"."analytics_source"."src_auth_events"
where userId is not null
group by userId
having count(*) > 1



      
    ) dbt_internal_test