with source as (
      select * from {{ source('sources_ecomm_cwd_smd_dbo', 'websites_full') }}
),
renamed as (
    select *, 'E-COMMERCE' as source_system     

    from source
)
select * from renamed