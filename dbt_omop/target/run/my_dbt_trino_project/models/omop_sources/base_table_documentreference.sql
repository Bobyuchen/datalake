
  create or replace view
    "iceberg"."omop"."base_table_documentreference"
  security definer
  as
    




with source as (
    select 
        
            request_method as "request_method",
        
            request_url as "request_url",
        
            resource_id as "resource_id",
        
            resource_type as "resource_type",
        
            resource_status as "resource_status",
        
            full_url as "full_url",
        
            meta as "meta",
        
            subject as "subject",
        
            id as "id",
        
            category as "category",
        
            content as "content",
        
            resourcetype as "resourcetype",
        
            status as "status"
        
    from "iceberg"."ifhir"."documentreference"
)

select * from source
  ;
