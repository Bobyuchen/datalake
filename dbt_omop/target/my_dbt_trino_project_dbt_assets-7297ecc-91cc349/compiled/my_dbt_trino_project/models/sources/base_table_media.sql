




with source as (
    select 
        
            request_method as "request_method",
        
            request_url as "request_url",
        
            resource_id as "resource_id",
        
            resource_type as "resource_type",
        
            resource_status as "resource_status",
        
            full_url as "full_url",
        
            bodysite as "bodysite",
        
            meta as "meta",
        
            subject as "subject",
        
            id as "id",
        
            content as "content",
        
            resourcetype as "resourcetype",
        
            status as "status"
        
    from "postgresql"."dbt"."fhir_data_media"
)

select * from source