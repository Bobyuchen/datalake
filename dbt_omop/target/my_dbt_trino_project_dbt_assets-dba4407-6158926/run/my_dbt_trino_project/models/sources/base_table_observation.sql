
  create or replace view
    "iceberg"."omop"."base_table_observation"
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
        
            valuecodeableconcept as "valuecodeableconcept",
        
            performer as "performer",
        
            code as "code",
        
            effectivedatetime as "effectivedatetime",
        
            meta as "meta",
        
            subject as "subject",
        
            id as "id",
        
            resourcetype as "resourcetype",
        
            status as "status",
        
            valuestring as "valuestring",
        
            method as "method",
        
            component as "component",
        
            interpretation as "interpretation",
        
            specimen as "specimen",
        
            derivedfrom as "derivedfrom",
        
            category as "category",
        
            referencerange as "referencerange",
        
            valuequantity as "valuequantity"
        
    from "postgresql"."dbt"."fhir_data_observation"
)

select * from source
  ;
