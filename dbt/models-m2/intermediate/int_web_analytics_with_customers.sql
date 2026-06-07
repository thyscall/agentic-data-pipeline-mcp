with events as (
    select * from {{ ref('stg_web_analytics') }}
),

customers as (
    select * from {{ ref('stg_adventure_db__customers') }}
),

joined as (
    select

        e.session_id,
        e.customer_id,
        e.product_id,
        e.page_url,
        e.event_type,
        e.event_timestamp,
        
        c.first_name,
        c.last_name,
        c.full_name,
        c.email_address,
        c.city,
        c.state_province,
        c.country_region,
        c.account_number
        
    from events e
    left join customers c
        on e.customer_id = c.customer_id
)

select * from joined