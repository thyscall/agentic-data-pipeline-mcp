
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: ensure preferred vendors have credit ratings >= 1
--
-- Returns rows where a preferred vendor has insufficient credit rating
-- (test fails if any rows returned)

select
    *
from
    BOA_DB.dbt_dev.stg_adventure_db__vendors
where
    preferred_vendor_status = true
and
    credit_rating < 1
  
  
      
    ) dbt_internal_test