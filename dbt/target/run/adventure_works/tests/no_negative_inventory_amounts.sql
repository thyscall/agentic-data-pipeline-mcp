
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: ensure inventory quantities are never negative
--
-- Returns rows where quantity < 0 (test fails if any rows returned)

select
    quantity
from
    BOA_DB.dbt_dev.stg_adventure_db__inventory
where
    quantity < 0
  
  
      
    ) dbt_internal_test