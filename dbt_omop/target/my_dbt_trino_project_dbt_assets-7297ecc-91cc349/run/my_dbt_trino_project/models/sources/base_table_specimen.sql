
  create or replace view
    "iceberg"."omop"."base_table_specimen"
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
        
            receivedtime as "receivedtime",
        
            id as "id",
        
            type as "type",
        
            resourcetype as "resourcetype"
        
    from "postgresql"."dbt"."fhir_data_specimen"
)

select * from source
  ;
