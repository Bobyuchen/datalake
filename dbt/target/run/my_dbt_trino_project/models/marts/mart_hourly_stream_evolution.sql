
  
    

    create table "datalake"."analytics"."mart_hourly_stream_evolution__dbt_tmp"
      
      
    as (
      

SELECT
    stream_date,
    stream_hour,
    total_streams,
    SUM(total_streams) OVER (PARTITION BY stream_date ORDER BY stream_hour) AS cumulative_streams
FROM "datalake"."analytics_stage"."stg_streams_hourly"
ORDER BY
    stream_date,
    stream_hour
    );

  