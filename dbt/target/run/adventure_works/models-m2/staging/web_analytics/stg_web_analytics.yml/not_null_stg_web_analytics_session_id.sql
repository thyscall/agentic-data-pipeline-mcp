
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select session_id
from BOA_DB.dbt_dev.stg_web_analytics
where session_id is null



  
  
      
    ) dbt_internal_test