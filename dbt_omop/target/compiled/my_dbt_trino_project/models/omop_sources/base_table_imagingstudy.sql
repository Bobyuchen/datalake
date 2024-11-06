




with source as (
    select 
        
            request_method as "request_method",
        
            request_url as "request_url",
        
            resource_id as "resource_id",
        
            resource_type as "resource_type",
        
            resource_status as "resource_status",
        
            full_url as "full_url",
        
            identifier as "identifier",
        
            meta as "meta",
        
            series as "series",
        
            subject as "subject",
        
            id as "id",
        
            resourcetype as "resourcetype",
        
            status as "status"
        
    from "iceberg"."ifhir"."imagingstudy"
)

select * from source