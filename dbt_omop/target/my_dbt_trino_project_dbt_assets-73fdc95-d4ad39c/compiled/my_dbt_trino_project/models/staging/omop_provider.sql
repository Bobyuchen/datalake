
  
select 
  json_extract_scalar(json_array_get(identifier, 0), '$.value') as "provider_id",--醫療服務提供者id
  json_extract_scalar(json_array_get(identifier, 0), '$.value') as "provider_name",--醫療服務提供者name缺漏
  json_extract_scalar(json_array_get(identifier, 0), '$.value') as "npi",--全國醫療服務提供者識別碼。唯一10位數字識別碼
  'qualification' as "dea", --缺漏，有權開具受管制藥物的醫療提供者的唯一識別碼。
  regexp_extract(identifier, '"code":\s*"([^"]+)"', 1) as "specialty_concept_id",--醫療提供者的專業(內外科等對應一個id)
  'location' as "care_site_id", --缺漏 醫療機構Care Site唯一識別碼
  'birthDate' as "year_of_birth", --缺漏
  'gender' as "gender_concept_id", --缺漏
  current_timestamp as create_time
FROM 
  "iceberg"."omop"."base_table_practitioner"