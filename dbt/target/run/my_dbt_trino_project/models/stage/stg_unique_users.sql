
  create or replace view
    "datalake"."analytics_stage"."stg_unique_users"
  security definer
  as
    

SELECT
    COUNT(DISTINCT userid) AS total_unique_users
FROM "datalake"."analytics_source"."src_listen_events"
WHERE userid IS NOT NULL
  ;
