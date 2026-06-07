with source as (
    select * from {{ source('external', 'orders_raw') }}
),

renamed as (
    select
        sales_order_id,
        customer_id,
        last_modified::timestamp_ntz as order_date,
        null as delivery_estimate,
        status as order_status
    from source
)

select * from renamed