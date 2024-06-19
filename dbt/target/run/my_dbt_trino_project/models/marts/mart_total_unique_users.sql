
  
    

    create table "datalake"."analytics"."mart_total_unique_users__dbt_tmp"
      
      
    as (
      

SELECT
    total_unique_users
FROM "datalake"."analytics_stage"."stg_unique_users"
    );

  