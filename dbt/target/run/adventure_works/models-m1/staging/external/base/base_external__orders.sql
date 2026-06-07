
  create or replace   view BOA_DB.dbt_dev.base_external__orders
  
  
  
  
  as (
    with source as (
    select * from BOA_DB.RAW_EXT.orders_raw
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
  );

