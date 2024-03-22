with
    source as (select * from {{ ref("pstg_account") }}),
    renamed as (
        select distinct
            (id) as id,
            name,
            parent_id,
            account_number,
            source_system,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to

        from source
        where
            name is not null
            and date_cancelled_c is null
            and dbt_valid_to is null
            and account_number <> 'A791F92C-F50C-820F-6B9E-67D3CE6C200F'
    )
select *
from renamed
