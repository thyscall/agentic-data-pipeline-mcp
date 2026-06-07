
  create or replace   view BOA_DB.dbt_dev.stg_real_time__chat_logs
  
  
  
  
  as (
    with source as (
    select * from BOA_DB.RAW_EXT.chat_logs_raw
),

renamed as (
    select
        raw:"_id"::string as ticket_id,
        raw:customer_id::string as customer_id,
        raw:sales_order_id::string as sales_order_id,
        raw:ticket_channel::string as ticket_channel,
        raw:ticket_subject::string as ticket_subject,
        raw:ticket_description::string as ticket_description,
        raw:resolution::string as resolution,
        raw:chat_start_time::timestamp_ntz as chat_start_time
    from source
)

select * from renamed
  );

