
  create or replace view
    "iceberg"."omop"."base_table_coverage"
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
        
            payor as "payor",
        
            beneficiary as "beneficiary",
        
            meta as "meta",
        
            id as "id",
        
            resourcetype as "resourcetype",
        
            status as "status"
        
    from "postgresql"."dbt"."fhir_data_coverage"
)

select * from source
  ;
