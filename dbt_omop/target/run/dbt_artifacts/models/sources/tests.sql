
  
    

    create table "iceberg"."omop"."tests"
      
      WITH (format = delta)
    as (
      /* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (
    select 1 as foo
)

select
    cast(null as varchar) as command_invocation_id,
    cast(null as varchar) as node_id,
    cast(null as timestamp) as run_started_at,
    cast(null as varchar) as name,
    cast(null as VARCHAR) as depends_on_nodes,
    cast(null as varchar) as package_name,
    cast(null as varchar) as test_path,
    cast(null as VARCHAR) as tags,
    cast(null as VARCHAR) as all_results
from dummy_cte
where 1 = 0
    );

  