
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select 1
from (
    select count(*) as row_count
    from BOA_DB.dbt_dev.stg_web_analytics
) counts
where counts.row_count < 50
  
  
      
    ) dbt_internal_test