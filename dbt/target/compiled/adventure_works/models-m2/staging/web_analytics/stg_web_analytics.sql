with

source as (

    select * from BOA_DB.raw_ext.web_analytics_raw

),

renamed as (

    select
        customer_id::string                    as customer_id,
        product_id::string                     as product_id,
        session_id::string                     as session_id,
        page_url::string                       as page_url,
        event_type::string                     as event_type,
        cast(event_timestamp as timestamp_ntz) as event_timestamp,
        current_timestamp()                    as dbt_loaded_at
    from source

)

select * from renamed