with
    source as (select * from {{ ref("pstg_company") }}),
    renamed as (
        select distinct
            trim(max(id) over (partition by upper(property_name)), ' ') as max_id,
            concat(id, '|', property_name) as platform_key,
            id,
            'Hubspot' as platform_name,
            property_name as company_name,
            'Hubspot Lead' as entity_type,
            property_address as address,
            property_city as city,
            property_state as state,
            property_zip as postal_code,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to
        from source
        where
            dbt_valid_to is null
            and property_address is not null
            and property_city is not null
            and property_state is not null
            and property_zip is not null
            and property_hs_analytics_source_data_1 <> 'IMPORT'
    )
select *
from renamed
