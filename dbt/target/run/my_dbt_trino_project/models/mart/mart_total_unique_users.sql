
  
    

    create table "iceberg"."analytics"."mart_total_unique_users__dbt_tmp"
      
      
    as (
      

SELECT
    total_unique_users
FROM "iceberg"."analytics_stage"."stg_unique_users"
    );

  