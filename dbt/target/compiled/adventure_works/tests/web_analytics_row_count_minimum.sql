

select 1
from (
    select count(*) as row_count
    from BOA_DB.dbt_dev.stg_web_analytics
) counts
where counts.row_count < 50