
  create or replace view
    "iceberg"."omop"."base_table_claim"
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
        
            insurance as "insurance",
        
            identifier as "identifier",
        
            extension as "extension",
        
            item as "item",
        
            created as "created",
        
            use as "use",
        
            diagnosis as "diagnosis",
        
            procedure as "procedure",
        
            priority as "priority",
        
            type as "type",
        
            supportinginfo as "supportinginfo",
        
            provider as "provider",
        
            meta as "meta",
        
            patient as "patient",
        
            enterer as "enterer",
        
            subtype as "subtype",
        
            id as "id",
        
            resourcetype as "resourcetype",
        
            status as "status"
        
    from "postgresql"."dbt"."fhir_data_claim"
)

select * from source
  ;
