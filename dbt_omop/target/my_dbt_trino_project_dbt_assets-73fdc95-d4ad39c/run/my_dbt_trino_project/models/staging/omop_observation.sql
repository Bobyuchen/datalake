
  
    

    create table "iceberg"."omop"."omop_observation__dbt_tmp"
      
      
    as (
      --OK


SELECT

  substring(resource_id, -36) as "observation_id",  -- 醫療觀察紀錄id
  json_extract_scalar(json_extract(valueQuantity, '$.unit'), '$') AS "unit_concept_id",  --觀察結果的單位（unit）應換成代號id
  json_extract_scalar(json_array_get(performer, 0), '$.reference') AS "provider_id",--醫療提供者id
  resource_id AS "visit_occurrence_id",--就診id
  json_extract_scalar(subject, '$.reference') AS "person_id",
  json_extract_scalar(json_array_get(json_extract(code, '$.coding'), 0), '$.code') AS "observation_concept_id",
  "effectiveDateTime" as "observation_date",
  "effectiveDateTime" as "observation_datetime",
  json_extract_scalar(json_extract(valueQuantity, '$.value'), '$') AS "value_as_number",--量測結果
  valueString AS "value_as_string",--觀察結果
  json_extract_scalar(json_array_get(json_extract(valuecodeableconcept, '$.coding'), 0), '$.code') AS "value_as_concept_id",
  current_timestamp as create_time

FROM "iceberg"."omop"."base_table_observation"
    );

  