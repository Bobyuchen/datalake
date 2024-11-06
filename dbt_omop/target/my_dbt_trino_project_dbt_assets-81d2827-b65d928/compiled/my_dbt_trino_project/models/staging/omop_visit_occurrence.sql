


SELECT
  'resource_id' as "visit_occurence_id", --取value
  serviceType as "visit_source_value", --訪視原始數據
-- jsonb_array_elements(location::jsonb) -> location -> managingOrganization ->> identifier as "care_site_id", --缺漏
--jsonb_array_elements(location::jsonb) -> 'location' ->> 'display'  as "care_site_id", --缺漏
--'unknown' as "admitting_from_concept_id", --入院來源id
--'unknown' as "admitting_from_source_value", --入院來源的 原始描述值
--'unknown' as "admitting_from_source_concept_id", --入院來源的標準化概念的進一步分類
--'unknown' as "discharge_to_concept_id", --出院去向識別碼
--'unknown' as "discharge_to_source_value", --出院去向描述
--'partOf' as "preceding_visit_occurence", --前一次訪視紀錄
--'subject.identifier' as "person_id",
-- jsonb_array_elements(subject::jsonb) ->> 'identifier' as "person_id", --缺漏
--jsonb_array_elements(type::jsonb) ->> 'id' as "visit_concept_id", -- 缺漏
--jsonb_array_elements(type::jsonb) ->> 'text' as "visit_source_value2", -- 缺漏

--period::jsonb ->> 'start' as "visit_start_date", -- 缺漏
--period::jsonb ->> 'start' as "visit_start_datetime", -- 缺漏
--period::jsonb ->> 'start' as "visit_end_date", -- 缺漏
--period::jsonb ->> 'start' as "visit_end_datetime", -- 缺漏
--'unknown' as "visit_type_concept_id", --不明  訪視類型代碼
  json_extract_scalar(class, '$.code') as "visit_type_source_value", --訪視類型原始 急診 住院 門診
--'performer.identifier' as "provider_id"
  'identifier' as "provider_id", -- 缺漏 performer
  current_timestamp as create_time



FROM 
  "iceberg"."omop"."base_table_encounter"