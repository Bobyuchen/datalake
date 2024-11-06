
  create or replace view
    "iceberg"."omop"."base_table_diagnosticreport"
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
        
            conclusion as "conclusion",
        
            code as "code",
        
            performer as "performer",
        
            effectivedatetime as "effectivedatetime",
        
            meta as "meta",
        
            subject as "subject",
        
            id as "id",
        
            media as "media",
        
            imagingstudy as "imagingstudy",
        
            resourcetype as "resourcetype",
        
            status as "status",
        
            presentedform as "presentedform"
        
    from "postgresql"."dbt"."fhir_data_diagnosticreport"
)

select * from source
  ;
