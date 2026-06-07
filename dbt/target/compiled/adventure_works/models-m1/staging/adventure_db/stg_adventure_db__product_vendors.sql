with

source as (

    select * from is566.adventure_prod_db.product_vendor_prod_db

),
measures as (
    select * from BOA_DB.dbt_dev.measures
),

renamed as (

    select
        s.productid::string         as product_id,
        s.vendorid::string          as vendor_id,
        s.averageleadtime           as average_lead_time,
        s.standardprice             as standard_price,
        s.lastreceiptcost           as last_receipt_cost,
        s.lastreceiptdate           as last_receipt_date,
        s.minorderqty               as min_order_qty,
        s.maxorderqty               as max_order_qty,
        s.onorderqty                as on_order_qty,
        m.measure_name              as measurement,
        s.modifieddate              as modified_date                      
    from source s
    left join measures m
    on s.unitmeasurecode = m.measure_code

)

select * from renamed