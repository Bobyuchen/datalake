
  
    

    create table "iceberg"."omop"."omop_person"
      
      
    as (
      
  
SELECT
  id as person_id,
  CASE
    WHEN gender = 'male' THEN 8507
    WHEN gender = 'female' THEN 8532
    ELSE 8551
  END as gender_concept_id,
  
  EXTRACT(YEAR FROM date_parse("birthDate", '%Y-%m-%d')) AS year_of_birth,
  EXTRACT(MONTH FROM date_parse("birthDate", '%Y-%m-%d')) AS month_of_birth,
  EXTRACT(DAY FROM date_parse("birthDate", '%Y-%m-%d')) AS day_of_birth,
  "birthDate" as birth_datetime,
  json_extract_scalar(json_array_get(identifier, 0), '$.value') AS person_source_value,
  gender as gender_source_value,
  current_timestamp as create_time

FROM 
  "iceberg"."omop"."base_table_patient"
    );

  