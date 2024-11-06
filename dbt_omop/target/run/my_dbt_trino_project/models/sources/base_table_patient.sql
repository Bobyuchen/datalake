
  create or replace view
    "iceberg"."omop"."base_table_patient"
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
        
            identifier as "identifier",
        
            gender as "gender",
        
            meta as "meta",
        
            name as "name",
        
            id as "id",
        
            birthdate as "birthdate",
        
            resourcetype as "resourcetype"
        
    from "iceberg"."ifhir"."patient"
)

select * from source
  ;
