
  create or replace   view BOA_DB.dbt_dev.int_external__orders_formatted
  
  
  
  
  as (
    with orders as (
    select * from BOA_DB.dbt_dev.base_external__orders
),

details as (
    select * from BOA_DB.dbt_dev.base_external__order_details
),

/* combine the items into JSON objects and then into an Array */
nested_details as (
    select 
        sales_order_id,
        array_agg(
            object_construct(
                'product_id', product_id,
                'quantity', quantity,
                'price', unit_price
            )
        ) as order_detail
    from details
    group by 1
),

final as (
    select 
        o.sales_order_id,
        o.customer_id,
        o.order_date,
        o.delivery_estimate, 
        o.order_status,
        d.order_detail
    from orders o
    left join nested_details d 
        on o.sales_order_id = d.sales_order_id
)

select * from final
  );

