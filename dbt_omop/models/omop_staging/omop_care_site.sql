--OK
{{ config(
    materialized='table',
    depends_on=['ref("base_table_organization")'],
    unique_key='resource_id'
) }}

select 
  json_extract_scalar(json_extract(identifier, '$[0]'), '$.value') as "care_site_id",
  name as "care_site_name",
  json_extract_scalar(json_extract(type, '$[0]'), '$.coding[0].code') as "place_of_service_concept_id",
  current_timestamp as create_time

FROM {{ ref('base_table_organization') }}


