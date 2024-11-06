insert into "iceberg"."analytics_source"."src_auth_events" ("id", "ts", "sessionid", "level", "iteminsession", "city", "zip", "state", "useragent", "lon", "lat", "userid", "lastname", "firstname", "gender", "registration", "success_boolean")
    (
        select "id", "ts", "sessionid", "level", "iteminsession", "city", "zip", "state", "useragent", "lon", "lat", "userid", "lastname", "firstname", "gender", "registration", "success_boolean"
        from "iceberg"."analytics_source"."src_auth_events__dbt_tmp"
    )

