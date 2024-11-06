
  create or replace view
    "iceberg"."omop"."base_table_encounter"
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
        
            servicetype as "servicetype",
        
            meta as "meta",
        
            id as "id",
        
            class as "class",
        
            resourcetype as "resourcetype",
        
            status as "status"
        
    from "postgresql"."dbt"."fhir_data_encounter"
)

select * from source
  ;
