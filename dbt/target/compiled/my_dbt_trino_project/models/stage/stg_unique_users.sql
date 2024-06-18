

SELECT
    COUNT(DISTINCT userid) AS total_unique_users
FROM "datalake"."analytics_source"."src_listen_events"
WHERE userid IS NOT NULL