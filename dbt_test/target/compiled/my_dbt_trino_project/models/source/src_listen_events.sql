

with source as (

    select
        CAST(_id AS VARCHAR) as id,
        artist,
        song,
        CAST(NULLIF(duration, '') AS DOUBLE) as duration,
        ts,
        sessionid,
        auth,
        level,
        CAST(NULLIF(itemInSession, '') AS INTEGER) as itemInSession,
        city,
        zip,
        state,
        country,
        userAgent,
        CAST(NULLIF(lon, '') AS DOUBLE) as lon,
        CAST(NULLIF(lat, '') AS DOUBLE) as lat,
        CAST(CAST(NULLIF(userId, '') AS DOUBLE) AS INTEGER) as userId,
 -- Assurez-vous que userId peut aussi être converti de cette manière.
        lastName,
        firstName,
        gender,
        CAST(CAST(NULLIF(registration, '') AS DOUBLE) AS BIGINT) as registration
    from "mongo"."demo"."listen_events"

)

select * from source


    where id NOT IN (SELECT id FROM "iceberg"."analytics_source"."src_listen_events")
