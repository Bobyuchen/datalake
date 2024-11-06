
  
    

    create table "iceberg"."analytics"."mart_user_activity_per_state__dbt_tmp"
      
      
    as (
      

SELECT
  state,
  city,
  SUM(activity_count) AS total_activity
FROM "iceberg"."analytics_stage"."stg_user_activity_per_location"
GROUP BY state, city
ORDER BY state, SUM(activity_count) DESC
    );

  