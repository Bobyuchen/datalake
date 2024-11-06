





with jobs as (
  select
    job_name,
    job_id,
    job_run_id,
    
min(coalesce(
        try_cast(run_started_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_started_at AS varchar)) AS 
    timestamp(6)
)
    ))
 as job_run_started_at,
    
max(coalesce(
        try_cast(run_completed_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_completed_at AS varchar)) AS 
    timestamp(6)
)
    ))
 as job_run_completed_at,
    
    (((((to_milliseconds((CAST(CAST(coalesce(
        try_cast(
max(coalesce(
        try_cast(run_completed_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_completed_at AS varchar)) AS 
    timestamp(6)
)
    ))
 as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(
max(coalesce(
        try_cast(run_completed_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_completed_at AS varchar)) AS 
    timestamp(6)
)
    ))
 AS varchar)) AS 
    timestamp(6)
)
    ) AS TIMESTAMP) AS DATE) - CAST(CAST(coalesce(
        try_cast(
min(coalesce(
        try_cast(run_started_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_started_at AS varchar)) AS 
    timestamp(6)
)
    ))
 as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(
min(coalesce(
        try_cast(run_started_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_started_at AS varchar)) AS 
    timestamp(6)
)
    ))
 AS varchar)) AS 
    timestamp(6)
)
    ) AS TIMESTAMP) AS DATE)))) / 86400000) * 24 + hour(CAST(coalesce(
        try_cast(
max(coalesce(
        try_cast(run_completed_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_completed_at AS varchar)) AS 
    timestamp(6)
)
    ))
 as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(
max(coalesce(
        try_cast(run_completed_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_completed_at AS varchar)) AS 
    timestamp(6)
)
    ))
 AS varchar)) AS 
    timestamp(6)
)
    ) AS TIMESTAMP)) - hour(CAST(coalesce(
        try_cast(
min(coalesce(
        try_cast(run_started_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_started_at AS varchar)) AS 
    timestamp(6)
)
    ))
 as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(
min(coalesce(
        try_cast(run_started_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_started_at AS varchar)) AS 
    timestamp(6)
)
    ))
 AS varchar)) AS 
    timestamp(6)
)
    ) AS TIMESTAMP))) * 60 + minute(CAST(coalesce(
        try_cast(
max(coalesce(
        try_cast(run_completed_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_completed_at AS varchar)) AS 
    timestamp(6)
)
    ))
 as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(
max(coalesce(
        try_cast(run_completed_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_completed_at AS varchar)) AS 
    timestamp(6)
)
    ))
 AS varchar)) AS 
    timestamp(6)
)
    ) AS TIMESTAMP)) - minute(CAST(coalesce(
        try_cast(
min(coalesce(
        try_cast(run_started_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_started_at AS varchar)) AS 
    timestamp(6)
)
    ))
 as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(
min(coalesce(
        try_cast(run_started_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_started_at AS varchar)) AS 
    timestamp(6)
)
    ))
 AS varchar)) AS 
    timestamp(6)
)
    ) AS TIMESTAMP))) * 60 + second(CAST(coalesce(
        try_cast(
max(coalesce(
        try_cast(run_completed_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_completed_at AS varchar)) AS 
    timestamp(6)
)
    ))
 as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(
max(coalesce(
        try_cast(run_completed_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_completed_at AS varchar)) AS 
    timestamp(6)
)
    ))
 AS varchar)) AS 
    timestamp(6)
)
    ) AS TIMESTAMP)) - second(CAST(coalesce(
        try_cast(
min(coalesce(
        try_cast(run_started_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_started_at AS varchar)) AS 
    timestamp(6)
)
    ))
 as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(
min(coalesce(
        try_cast(run_started_at as 
    timestamp(6)
),
        cast(from_iso8601_timestamp(cast(run_started_at AS varchar)) AS 
    timestamp(6)
)
    ))
 AS varchar)) AS 
    timestamp(6)
)
    ) AS TIMESTAMP)))
 as job_run_execution_time
  from "iceberg"."omop"."dbt_invocations"
  where job_id is not null
  group by job_name, job_id, job_run_id
)

select
  job_name as name,
  job_id as id,
  job_run_id as run_id,
  job_run_started_at as run_started_at,
  job_run_completed_at as run_completed_at,
  job_run_execution_time as run_execution_time
from jobs