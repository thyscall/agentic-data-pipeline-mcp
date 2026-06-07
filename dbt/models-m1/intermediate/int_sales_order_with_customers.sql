with orders as (
    select * from {{ ref('stg_ecom__sales_orders') }}
),

customers as (
    select * from {{ ref('stg_adventure_db__customers') }}
),

joined as (
    select
        o.sales_order_id,
        o.order_date,
        o.total_due,
        c.customer_id,
        c.country_region
    from orders o
    left join customers c on o.customer_id = c.customer_id
)

select * from joined