
  create or replace   view BOA_DB.dbt_dev.base_external__order_details
  
  
  
  
  as (
    with source as (
    select * from BOA_DB.RAW_EXT.order_details_raw
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
  );

