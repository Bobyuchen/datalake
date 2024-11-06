
  create or replace view
    "iceberg"."analytics_stage"."stg_unique_users"
  security definer
  as
    

SELECT
    COUNT(DISTINCT userid) AS total_unique_users
FROM "iceberg"."analytics_source"."src_listen_events"
WHERE userid IS NOT NULL
  ;
