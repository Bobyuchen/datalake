
  
    

    create table "iceberg"."omop"."observaton__dbt_tmp"
      
      
    as (
      
SELECT
'identifier' as "observation_id",  --缺漏
'valueQuantity_unit' as "unit_concept_id",  --部分缺漏 放單位並可能要換成代碼
substring(performer from '"reference": "([^"]+)"')  as "provider_id",
substring(encounter from '"reference": "([^"]+)"') as "visit_occurrence_id", --缺漏
substring(subject_reference from 'urn:uuid:([a-f0-9-]+)') as "person_id",
substring(meta_profile from '"code": "([^"]+)"')  as "observation_concept_id", --observation_concept_id 應該對應於一個標準化的醫療概念 ID，醫療觀察類型。應需轉換成id。
"effectiveDateTime" as "observation_date",
"effectiveDateTime" as "observation_datetime",
'valueInt' as "value_as_number",  --缺漏
'valueString' as "value_as_string",  --部分缺漏
'valueCodeableConcept_coding' as "value_as_concept_id"  --缺漏
,create_time
FROM  
 "iceberg"."omop"."base_table_observation"
    );

  