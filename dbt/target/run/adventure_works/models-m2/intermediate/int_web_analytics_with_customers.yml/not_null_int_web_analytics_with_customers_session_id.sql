
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select session_id
from BOA_DB.dbt_dev.int_web_analytics_with_customers
where session_id is null



  
  
      
    ) dbt_internal_test