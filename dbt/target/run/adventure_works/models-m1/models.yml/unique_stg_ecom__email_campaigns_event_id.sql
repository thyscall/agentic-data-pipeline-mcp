
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    event_id as unique_field,
    count(*) as n_records

from BOA_DB.dbt_dev.stg_ecom__email_campaigns
where event_id is not null
group by event_id
having count(*) > 1



  
  
      
    ) dbt_internal_test