
    
    

select
    userId as unique_field,
    count(*) as n_records

from "datalake"."analytics_source"."src_auth_events"
where userId is not null
group by userId
having count(*) > 1


