
  create or replace view
    "iceberg"."omop"."base_table_medicationrequest"
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
        
            dosageinstruction as "dosageinstruction",
        
            dispenserequest as "dispenserequest",
        
            meta as "meta",
        
            subject as "subject",
        
            id as "id",
        
            category as "category",
        
            medicationcodeableconcept as "medicationcodeableconcept",
        
            intent as "intent",
        
            resourcetype as "resourcetype",
        
            status as "status",
        
            statusreason as "statusreason"
        
    from "iceberg"."ifhir"."medicationrequest"
)

select * from source
  ;
