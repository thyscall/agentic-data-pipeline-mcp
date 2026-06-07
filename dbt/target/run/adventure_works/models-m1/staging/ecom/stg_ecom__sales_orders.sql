
  create or replace   view BOA_DB.dbt_dev.stg_ecom__sales_orders
  
  
  
  
  as (
    with

base as (

    select * from BOA_DB.dbt_dev.base_ecom__sales_orders

),

ship_methods as (
    select * from BOA_DB.dbt_dev.ship_method
),

renamed as (

    select
        b.sales_order_id::string as sales_order_id,
        b.customer_id::string as customer_id,
        b.account_number,
        b.bill_to_address_id,
        b.comment,
        b.credit_card_approval_code,
        b.credit_card_id,
        b.currency_rate_id,
        
        -- Handling the delivery days noise:
        CASE 
            WHEN b.delivery_estimate ILIKE '%week%' 
                THEN REGEXP_SUBSTR(b.delivery_estimate, '[0-9]+')::INT * 7
            WHEN b.delivery_estimate ILIKE '%day%' 
                THEN REGEXP_SUBSTR(b.delivery_estimate, '[0-9]+')::INT
            ELSE NULL  
        END     
            as delivery_estimate_days,

        b.due_date,
        b.freight,
        b.modified_date,
        b.online_order_flag,
        b.order_date,
        b.order_details,
        b.purchase_order_number,
        b.revision_number,
        b.sales_order_number,
        b.sales_person_id,
        b.ship_date,
        s.name as shipping_method,
        b.ship_to_address_id,
        b.status,
        b.sub_total,
        b.tax_amt,
        b.territory_id,
        b.total_due
    from base b
    left join ship_methods s
    on b.ship_method_id = s.ship_method_id

), 

external_data as (
    select

        sales_order_id,
        customer_id, 
        null as account_number,
        null as bill_to_address_id, 
        null as comment, 
        null as credit_card_approval_code, 
        null as credit_card_id, 
        null as currency_rate_id, 
        null::int as delivery_estimate_days,
        null as due_date,
        null as freight, 
        null as modified_date,
        null as online_order_flag,
        order_date,
        order_detail as order_details, 
        null as purchase_order_number, 
        null as revision_number, 
        null as sales_order_number,
        null as sales_person_id,
        null as ship_date, 
        null as shipping_method, 
        null as ship_to_address_id, 
        order_status as status,
        null as sub_total, 
        null as tax_amt, 
        null as territory_id, 
        null as total_due

    from BOA_DB.dbt_dev.int_external__orders_formatted


),

final as (
    select * from renamed
    UNION ALL
    select * from external_data
)

select * from final
  );

