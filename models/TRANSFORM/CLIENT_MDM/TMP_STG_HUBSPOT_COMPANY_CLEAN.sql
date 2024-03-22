with
    source as (select * from {{ ref("TMP_STG_HUBSPOT_COMPANY_RAW") }}),
    renamed as (
        select distinct
            (id) as id,
            company_name,
            entity_type,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to
        from source
        where max_id = id
    )
select *
from renamed
