

SELECT
  state,
  city,
  SUM(activity_count) AS total_activity
FROM "datalake"."analytics_stage"."stg_user_activity_per_location"
GROUP BY state, city
ORDER BY state, SUM(activity_count) DESC