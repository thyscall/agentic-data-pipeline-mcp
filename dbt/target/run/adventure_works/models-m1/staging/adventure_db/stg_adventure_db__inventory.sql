
  create or replace   view BOA_DB.dbt_dev.stg_adventure_db__inventory
  
  
  
  
  as (
    with

source as (

    select * from is566.adventure_prod_db.inventory_prod_db

),

renamed as (

    select
        productid::string   as product_id,
        locationid          as location_id,
        shelf               as shelf,
        bin                 as bin,
        quantity            as quantity
    from source

)

select * from renamed
  );

