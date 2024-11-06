
  
    

    create table "iceberg"."omop"."omop_procedure_occurrence__dbt_tmp"
      
      
    as (
      


SELECT
  resource_id as "procedure_occurrence_id", --缺漏
  json_extract_scalar(subject, '$.reference') AS "visit_occurrence_id", --缺漏
  current_timestamp as create_time

FROM 
  "iceberg"."omop"."base_table_procedure"
    );

  