with source as (
    select * from {{ source('external', 'order_details_raw') }}
),

renamed as (
    select
        sales_order_id,
        product_id,
        order_qty as quantity,
        unit_price
    from source
)

select * from renamed