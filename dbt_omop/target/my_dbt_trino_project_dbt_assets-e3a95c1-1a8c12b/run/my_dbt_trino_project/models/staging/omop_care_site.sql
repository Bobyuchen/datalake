
  
    

    create table "iceberg"."omop"."omop_care_site__dbt_tmp"
      
      
    as (
      --OK


select 
  json_extract_scalar(json_extract(identifier, '$[0]'), '$.value') as "care_site_id",
  name as "care_site_name",
  json_extract_scalar(json_extract(type, '$[0]'), '$.coding[0].code') as "place_of_service_concept_id",
  current_timestamp as create_time

FROM "iceberg"."omop"."base_table_organization"
    );

  