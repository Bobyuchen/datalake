
  
    

    create table "iceberg"."omop"."test_result_rows"
      
      
    as (
      -- indexes are not supported in all warehouses, relevant to postgres only


-- depends_on: "iceberg"."omop"."elementary_test_results"
select * from (
            select
            
                
        cast('this_is_just_a_long_dummy_string' as varchar) as elementary_test_results_id

,
                
        cast('this_is_just_a_long_dummy_string' as varchar) as result_row

,
                cast('2091-02-17' as 
    timestamp(6)
) as detected_at

,
                cast('2091-02-17' as 
    timestamp(6)
) as created_at


        ) as empty_table
        where 1 = 0
    );

  