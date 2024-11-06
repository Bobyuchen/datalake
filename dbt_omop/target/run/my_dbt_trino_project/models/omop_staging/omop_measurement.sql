
  
    

    create table "iceberg"."omop"."omop_measurement"
      
      
    as (
      

SELECT
  substring(resource_id, -36) as "observation_id",  -- 醫療觀察紀錄id
  json_extract_scalar(json_extract(valueQuantity, '$.unit'), '$') AS "unit_concept_id",  --觀察結果的單位（unit）應換成代號id
  json_extract_scalar(json_array_get(referenceRange, 0), '$.low.value') AS "range_low",
  json_extract_scalar(json_array_get(referenceRange, 0), '$.high.value') AS "range_high",
  json_extract_scalar(json_array_get(performer, 0), '$.reference') AS "provider_id",--醫療提供者id
  resource_id AS "visit_occurrence_id",--就診id
  --substring(code from '"code": "([^"]+)"') as "measurement_source_value", --不知道取哪個 
  json_extract_scalar(subject, '$.reference') AS "person_id",
  --substring(code from '"code": "([^"]+)"') as "measurement_concept_id",  --不知道取哪個 
  "effectiveDateTime" as "observation_date",
  "effectiveDateTime" as "observation_datetime",
  json_extract_scalar(json_extract(valueQuantity, '$.value'), '$') AS "value_as_number",--量測結果
  json_extract_scalar(json_array_get(json_extract(valuecodeableconcept, '$.coding'), 0), '$.code') AS "value_as_concept_id",
  current_timestamp as create_time
FROM 
  "iceberg"."omop"."base_table_observation"
    );

  