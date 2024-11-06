




with source as (
    select 
        
            request_method as "request_method",
        
            request_url as "request_url",
        
            resource_id as "resource_id",
        
            resource_type as "resource_type",
        
            resource_status as "resource_status",
        
            full_url as "full_url",
        
            code as "code",
        
            ingredient as "ingredient",
        
            meta as "meta",
        
            id as "id",
        
            resourcetype as "resourcetype"
        
    from "iceberg"."ifhir"."substance"
)

select * from source