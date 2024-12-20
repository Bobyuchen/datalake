
  create or replace view
    "iceberg"."analytics_stage"."stg_user_level_by_gender"
  security definer
  as
    

WITH user_subscriptions AS (
    SELECT
        gender,
        level, 
        COUNT(DISTINCT userId) AS user_count
    FROM "iceberg"."analytics_source"."src_auth_events"
    WHERE gender IS NOT NULL AND level IS NOT NULL
    GROUP BY gender, level
)

SELECT
    gender,
    level,
    user_count
FROM user_subscriptions
  ;
