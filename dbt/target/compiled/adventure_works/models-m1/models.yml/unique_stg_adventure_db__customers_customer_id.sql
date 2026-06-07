
    
    

select
    customer_id as unique_field,
    count(*) as n_records

from BOA_DB.dbt_dev.stg_adventure_db__customers
where customer_id is not null
group by customer_id
having count(*) > 1


