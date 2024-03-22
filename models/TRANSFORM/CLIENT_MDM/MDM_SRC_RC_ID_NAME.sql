with
    source as (select * from {{ ref("pstg_account_history") }}),
    renamed as (
        select
            id,
            parent_account_id,
            code,
            company,
            tenant_id,
            source_system,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to
        from source
        where dbt_valid_to is null and company is not null and trim(company, ' ') <> ''
    )
select *
from renamed
