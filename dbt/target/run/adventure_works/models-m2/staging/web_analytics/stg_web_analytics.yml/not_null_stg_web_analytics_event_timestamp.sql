
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_timestamp
from BOA_DB.dbt_dev.stg_web_analytics
where event_timestamp is null



  
  
      
    ) dbt_internal_test