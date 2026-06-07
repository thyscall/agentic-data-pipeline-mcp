
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from BOA_DB.raw_ext.web_analytics_raw
    group by event_type

)

select *
from all_values
where value_field not in (
    'page_view','click','add_to_cart','purchase'
)


