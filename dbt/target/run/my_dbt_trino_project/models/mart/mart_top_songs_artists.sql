
  
    

    create table "iceberg"."analytics"."mart_top_songs_artists__dbt_tmp"
      
      
    as (
      

WITH top_songs AS (
    SELECT
        artist,
        song,
        play_count
    FROM "iceberg"."analytics_stage"."stg_top_songs_artists"
)

SELECT
    artist,
    song,
    play_count
FROM top_songs
    );

  