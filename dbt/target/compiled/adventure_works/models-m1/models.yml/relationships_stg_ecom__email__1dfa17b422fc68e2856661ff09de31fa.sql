
    
    

with child as (
    select order_id as from_field
    from BOA_DB.dbt_dev.stg_ecom__email_campaigns
    where order_id is not null
),

parent as (
    select sales_order_id as to_field
    from BOA_DB.dbt_dev.stg_ecom__sales_orders
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


