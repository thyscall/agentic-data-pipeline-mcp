
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from BOA_DB.dbt_dev.stg_ecom__email_campaigns
    group by event_type

)

select *
from all_values
where value_field not in (
    'email_opened','add_to_cart','conversion','email_clicked'
)


