/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (
    select 1 as foo
)

select
    cast(null as varchar) as command_invocation_id,
    cast(null as varchar) as dbt_version,
    cast(null as varchar) as project_name,
    cast(null as timestamp) as run_started_at,
    cast(null as varchar) as dbt_command,
    cast(null as boolean) as full_refresh_flag,
    cast(null as varchar) as target_profile_name,
    cast(null as varchar) as target_name,
    cast(null as varchar) as target_schema,
    cast(null as integer) as target_threads,
    cast(null as varchar) as dbt_cloud_project_id,
    cast(null as varchar) as dbt_cloud_job_id,
    cast(null as varchar) as dbt_cloud_run_id,
    cast(null as varchar) as dbt_cloud_run_reason_category,
    cast(null as varchar) as dbt_cloud_run_reason,
    cast(null as VARCHAR) as env_vars,
    cast(null as VARCHAR) as dbt_vars,
    cast(null as VARCHAR) as invocation_args,
    cast(null as VARCHAR) as dbt_custom_envs
from dummy_cte
where 1 = 0