

SELECT

substring(resource_id, -36) as "observation_id",  -- 醫療觀察紀錄id
json_extract_scalar(json_extract(valueQuantity, '$.unit'), '$') AS "unit_concept_id",  --觀察結果的單位（unit）應換成代號id
json_extract_scalar(json_array_get(performer, 0), '$.reference') AS "provider_id",--醫療提供者id
-- visit_occurrence_id 來自 request_url 中的最後一部分
resource_id AS "visit_occurrence_id",--就診id
-- person_id 來自 subject 中的 reference，這裡將 subject_reference 改為 subject
json_extract_scalar(subject, '$.reference') AS "person_id",
-- observation_concept_id 來自 code 中的 code
-- 提取 code_coding 中第一個物件的 code
-- 提取 code_coding 中的所有 code，並將其轉換為字符串顯示
json_extract_scalar(json_array_get(json_extract(code, '$.coding'), 0), '$.code') AS "observation_concept_id",
-- observation_date 和 observation_datetime 來自 effectiveDateTime
"effectiveDateTime" as "observation_date",
"effectiveDateTime" as "observation_datetime",
-- value_as_number 來自 valueQuantity_value，這裡假設 valueQuantity 是個 JSON 結構
json_extract_scalar(json_extract(valueQuantity, '$.value'), '$') AS "value_as_number",
-- value_as_string 來自 valueString
valueString AS "value_as_string",
-- value_as_concept_id 來自 valueCodeableConcept 中的 code
json_extract_scalar(json_array_get(json_extract(valuecodeableconcept, '$.coding'), 0), '$.code') AS "value_as_concept_id",-- create_time 保持不變
current_timestamp as create_time

FROM "iceberg"."omop"."base_table_observation"