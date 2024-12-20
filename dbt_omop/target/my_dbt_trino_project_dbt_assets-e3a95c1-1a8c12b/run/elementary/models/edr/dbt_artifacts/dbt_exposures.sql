
  
    

    create table "iceberg"."omop"."dbt_exposures"
      
      
    as (
      

select * from (
            select
            
                
        cast('dummy_string' as varchar) as unique_id

,
                
        cast('dummy_string' as varchar) as name

,
                
        cast('dummy_string' as varchar) as maturity

,
                
        cast('dummy_string' as varchar) as type

,
                
        cast('dummy_string' as varchar) as owner_email

,
                
        cast('dummy_string' as varchar) as owner_name

,
                
        cast('this_is_just_a_long_dummy_string' as varchar) as url

,
                
        cast('this_is_just_a_long_dummy_string' as varchar) as depends_on_macros

,
                
        cast('this_is_just_a_long_dummy_string' as varchar) as depends_on_nodes

,
                
        cast('this_is_just_a_long_dummy_string' as varchar) as depends_on_columns

,
                
        cast('this_is_just_a_long_dummy_string' as varchar) as description

,
                
        cast('this_is_just_a_long_dummy_string' as varchar) as tags

,
                
        cast('this_is_just_a_long_dummy_string' as varchar) as meta

,
                
        cast('dummy_string' as varchar) as package_name

,
                
        cast('this_is_just_a_long_dummy_string' as varchar) as original_path

,
                
        cast('dummy_string' as varchar) as path

,
                
        cast('dummy_string' as varchar) as generated_at

,
                
        cast('dummy_string' as varchar) as metadata_hash

,
                
        cast('dummy_string' as varchar) as label

,
                
        cast('this_is_just_a_long_dummy_string' as varchar) as raw_queries


        ) as empty_table
        where 1 = 0
    );

  