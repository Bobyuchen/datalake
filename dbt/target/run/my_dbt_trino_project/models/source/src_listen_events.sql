insert into "iceberg"."analytics_source"."src_listen_events" ("id", "artist", "song", "duration", "ts", "sessionid", "auth", "level", "iteminsession", "city", "zip", "state", "country", "useragent", "lon", "lat", "userid", "lastname", "firstname", "gender", "registration")
    (
        select "id", "artist", "song", "duration", "ts", "sessionid", "auth", "level", "iteminsession", "city", "zip", "state", "country", "useragent", "lon", "lat", "userid", "lastname", "firstname", "gender", "registration"
        from "iceberg"."analytics_source"."src_listen_events__dbt_tmp"
    )

