
select  *
from (
  SELECT 
			row ,
            (je.key::text) AS column_name,

            jsonb_typeof(je.value) as "types",

            case when jsonb_typeof(je.value)='object' or jsonb_typeof(je.value)='array' 
            then je.value::text  
            else replace(je.value::text, '"', '') end as column_value
            ,create_time
        FROM (
            SELECT
              json_data.resource as "data",
              ROW_NUMBER() OVER (ORDER BY data asc)  as "row"
              ,create_time
            FROM dbt.p_origin
            LEFT JOIN LATERAL (                                        --因資料為空 為保留全部欄位 要將空欄loop join
              select 
              (entry)::jsonb ->'resource'	as resource
              from
                jsonb_array_elements("data"->'RETURN_DATA') as "entry"
  	) as json_data  ON true
        where create_time = (SELECT MAX(create_time) FROM  dbt.p_origin )
        ) all_data,
        LATERAL jsonb_each(data) je ) as base