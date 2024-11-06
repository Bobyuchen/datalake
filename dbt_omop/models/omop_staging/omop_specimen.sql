{{ config(materialized='table',
  depends_on=['ref("base_table_specimen")']
  ,unique_key='resource_id'
  ) }}

select 
  id as "specimen_id",--檢體識別碼
  subject as "person_id", --值名與官網不同
  json_extract_scalar(type, '$.coding[0].code') as "specimen_concept_id",--檢體類型id
  receivedTime as "specimen_date", --重複時間
  receivedTime as "specimen_datetime" , --重複時間
  'quantity' as "quantity", --缺漏 quantity
  'unit' as "unit_concept_id", --缺漏 quantity.unit
  'text' as "anatomic_site_concept_id", --位置與官網不同 
  current_timestamp as create_time

FROM 
  {{   ref('base_table_specimen')  }}
