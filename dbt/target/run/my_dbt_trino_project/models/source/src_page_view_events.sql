insert into "datalake"."analytics_source"."src_page_view_events" ("id", "ts", "sessionid", "page", "auth", "level", "iteminsession", "city", "zip", "state", "useragent", "lon", "lat", "userid", "lastname", "firstname", "gender", "registration", "artist", "song", "duration")
    (
        select "id", "ts", "sessionid", "page", "auth", "level", "iteminsession", "city", "zip", "state", "useragent", "lon", "lat", "userid", "lastname", "firstname", "gender", "registration", "artist", "song", "duration"
        from "datalake"."analytics_source"."src_page_view_events__dbt_tmp"
    )

