/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (
    select 1 as foo
)

select
    cast(null as varchar) as command_invocation_id,
    cast(null as varchar) as node_id,
    cast(null as timestamp) as run_started_at,
    cast(null as boolean) as was_full_refresh,
    cast(null as varchar) as thread_id,
    cast(null as varchar) as status,
    cast(null as timestamp) as compile_started_at,
    cast(null as timestamp) as query_completed_at,
    cast(null as double) as total_node_runtime,
    cast(null as integer) as rows_affected,
    
    cast(null as varchar) as materialization,
    cast(null as varchar) as schema,
    cast(null as varchar) as name,
    cast(null as varchar) as alias,
    cast(null as varchar) as message,
    cast(null as VARCHAR) as adapter_response
from dummy_cte
where 1 = 0