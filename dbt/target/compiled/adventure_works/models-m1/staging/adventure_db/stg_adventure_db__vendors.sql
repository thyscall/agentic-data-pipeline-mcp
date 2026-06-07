with

source as (

    select * from is566.adventure_prod_db.vendor_prod_db

),

renamed as (

    select
        vendorid::string                 as vendor_id,
        accountnumber                    as account_number,
        name                             as name,
        creditrating                     as credit_rating,
        preferredvendorstatus            as preferred_vendor_status,
        activeflag                       as active_flag,
        purchasingwebserviceurl          as purchasing_webservice_url,
        modifieddate                     as modified_date
    from source

)

select * from renamed