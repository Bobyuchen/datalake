
  create view "postgres"."dbt"."base_table_observation__dbt_tmp"
    
    
  as (
    
 




    
    
    SELECT
       
    (SELECT create_time FROM "postgres"."dbt"."row_data" WHERE  row = main.row and  column_name = 'resourceType' AND column_value = 'Observation')::timestamp AS "create_time"
    FROM "postgres"."dbt"."row_data" main
    WHERE column_name = 'resourceType' AND column_value = 'Observation'
    

  );