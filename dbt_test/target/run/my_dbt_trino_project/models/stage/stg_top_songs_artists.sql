
  create or replace view
    "iceberg"."analytics_stage"."stg_top_songs_artists"
  security definer
  as
    

WITH song_plays AS (
    SELECT
        artist,
        song,
        COUNT(*) AS play_count
    FROM "iceberg"."analytics_source"."src_listen_events"
    GROUP BY artist, song
)

SELECT
    artist,
    song,
    play_count
FROM song_plays
ORDER BY play_count DESC
  ;
