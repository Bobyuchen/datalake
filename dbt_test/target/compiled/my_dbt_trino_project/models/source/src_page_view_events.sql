

with source as (
    select
        CAST(_id AS VARCHAR) as id,
        ts,
        CAST(sessionId AS VARCHAR) as sessionId,
        page,
        auth,
        level,
        CAST(CAST(NULLIF(itemInSession, '') AS DOUBLE) AS INTEGER) as itemInSession, 
        city,
        zip,
        state,
        userAgent,
        CAST(NULLIF(lon, '') AS double) as lon,
        CAST(NULLIF(lat, '') AS double) as lat,
        CAST(CAST(NULLIF(userId, '') AS DOUBLE) AS INTEGER) as userId, 
        lastName,
        firstName,
        gender,
        CAST(CAST(NULLIF(registration, '') AS DOUBLE) AS BIGINT) as registration, 
        artist,
        song,
        CAST(NULLIF(duration, '') AS double) as duration
    from "mongo"."demo"."page_view_events"
)

select * from source


    where id NOT IN (SELECT id FROM "iceberg"."analytics_source"."src_page_view_events")
