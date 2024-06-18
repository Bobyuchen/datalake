
    
    

select
    sessionId as unique_field,
    count(*) as n_records

from "datalake"."analytics_source"."src_auth_events"
where sessionId is not null
group by sessionId
having count(*) > 1


