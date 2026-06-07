
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_type
from BOA_DB.raw_ext.web_analytics_raw
where event_type is null



  
  
      
    ) dbt_internal_test