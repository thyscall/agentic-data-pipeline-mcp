with source as (
    select * from BOA_DB.RAW_EXT.chat_logs_raw
),

renamed as (
    select
        raw:chat_id::string as chat_id,
        raw:customer_id::string as customer_id,
        raw:last_modified::timestamp_ntz as chat_timestamp,
        raw:message_log as message_log
    from source
)

select * from renamed