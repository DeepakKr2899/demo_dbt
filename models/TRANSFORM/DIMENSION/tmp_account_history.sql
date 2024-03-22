with
    source as (select * from {{ source("sources_recurly", "account_history") }}),
    renamed as (
        select
            id,
            {{ convert_timezone_utc_to_pdt("updated_at") }} as updated_at,
            code,
            null as tenant_id,
            bill_to,
            state,
            username,
            first_name,
            last_name,
            email,
            cc_emails,
            preferred_locale,
            company,
            vat_number,
            tax_exempt,
            exemption_certificate,
            hosted_login_token,
            {{ convert_timezone_utc_to_pdt("created_at") }} as created_at,
            {{ convert_timezone_utc_to_pdt("deleted_at") }} as deleted_at,
            account_first_name,
            account_last_name,
            account_phone,
            account_street_1,
            account_street_2,
            account_city,
            account_region,
            account_postal_code,
            account_country,
            {{ convert_timezone_utc_to_pdt("_fivetran_synced") }} as _fivetran_synced,
            row_number() over (
                partition by id order by updated_at desc
            ) as account_history_row_number,
            case
                when account_history_row_number = 1
                then 'Y'
                when account_history_row_number <> 1
                then 'N'
                else 'ERROR'
            end as active_flag,
            custom_anwdstore_id,
            custom_classic_website_id,
            parent_account_id,
            'RECURLY' as source_system
        from source
        qualify active_flag = 'Y'
    )
select *
from renamed
