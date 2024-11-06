{{ config(materialized='table',
  depends_on=['ref("base_table_procedure")']
  ,unique_key='resource_id') }}


SELECT
  resource_id as "procedure_occurrence_id", --缺漏
  json_extract_scalar(subject, '$.reference') AS "visit_occurrence_id", --缺漏
  current_timestamp as create_time

FROM 
  {{   ref('base_table_procedure')  }}