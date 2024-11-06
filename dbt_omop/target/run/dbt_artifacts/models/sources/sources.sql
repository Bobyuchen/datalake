
  
    

    create table "iceberg"."omop"."sources"
      
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
    cast(null as varchar) as database,
    cast(null as varchar) as schema,
    cast(null as varchar) as source_name,
    cast(null as varchar) as loader,
    cast(null as varchar) as name,
    cast(null as varchar) as identifier,
    cast(null as varchar) as loaded_at_field,
    
        cast(null as VARCHAR) as freshness,
    
    cast(null as VARCHAR) as all_results
from dummy_cte
where 1 = 0
    );

  