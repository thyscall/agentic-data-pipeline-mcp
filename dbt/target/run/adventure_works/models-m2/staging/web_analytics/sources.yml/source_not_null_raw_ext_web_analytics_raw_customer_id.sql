
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_id
from BOA_DB.raw_ext.web_analytics_raw
where customer_id is null



  
  
      
    ) dbt_internal_test