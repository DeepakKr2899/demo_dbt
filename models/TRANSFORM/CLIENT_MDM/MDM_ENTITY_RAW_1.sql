select
    coalesce(
        mdm_override.override_value,
        coalesce(
            pstg_companies.companyname,
            coalesce(
                mdm_src_rc_id_name.company,
                coalesce(
                    mdm_dim_fulfill_clients.customer_name,
                    coalesce(
                        mdm_src_sf_id_name.name,
                        coalesce(
                            stg_ecommerce_website_list.website_name,
                            coalesce(
                                pstg_companies.full_name,
                                finance_customers_list.customer_name_raw
                            )
                        )
                    )
                )
            )
        )
    ) as company_name,
    finance_customers_list.finance_row_number as fin_row_num,
    pstg_companies.company_id as ns_id,
    pstg_companies.parent_id as ns_par_id,
    pstg_companies2.parent_id as ns_gpar_id,
    stg_ecommerce_website_list.website_group_id as ecomm_web_group_id,
    mdm_src_rc_id_name.id as rc_id,
    upper(mdm_src_rc_id_name.code) as rc_code,
    mdm_src_rc_id_name.tenant_id as rc_tenant_id,
    pstg_companies.company_extid as ns_ext_id,
    mdm_src_rc_id_name.parent_account_id as rc_par_id,
    cast(mdm_dim_fulfill_clients.customer_id as varchar(50)) as oracle_cust_id,
    upper(stg_ecommerce_website_list.website_id) as ecomm_web_id,
    mdm_src_sf_id_name.id as sf_id,
    mdm_src_sf_id_name.parent_id as sf_par_id,
    mdm_src_sf_id_name.account_number as sf_acct_num
from {{ ref("pstg_companies") }} as pstg_companies

left outer join
    {{ ref("pstg_companies") }} as pstg_companies2
    on pstg_companies.parent_id = pstg_companies2.company_id
    and pstg_companies2.dbt_valid_to is null

full outer join
    {{ ref("MDM_SRC_RC_ID_NAME") }} as mdm_src_rc_id_name
    on upper(pstg_companies.recurly_website_id) = upper(mdm_src_rc_id_name.code)

full outer join
    {{ ref("MDM_DIM_FULFILL_CLIENTS") }} as mdm_dim_fulfill_clients
    on cast(pstg_companies.company_extid as varchar(50))
    = cast(mdm_dim_fulfill_clients.customer_id as varchar(50))

left outer join
    {{ ref("MDM_SRC_SF_ID_NAME") }} as mdm_src_sf_id_name
    on (
        pstg_companies.recurly_website_id is not null
        and mdm_src_sf_id_name.account_number is not null
        and pstg_companies.recurly_website_id = mdm_src_sf_id_name.account_number
    )
    or (
        mdm_src_rc_id_name.code is not null
        and mdm_src_sf_id_name.account_number is not null
        and upper(mdm_src_rc_id_name.code) = upper(mdm_src_sf_id_name.account_number)
    )

left outer join
    {{ ref("stg_ecommerce_website_list") }} as stg_ecommerce_website_list
    on (
        upper(pstg_companies.recurly_website_id)
        = upper(stg_ecommerce_website_list.website_id)
        or upper(mdm_src_rc_id_name.code) = upper(stg_ecommerce_website_list.website_id)
        or upper(mdm_src_sf_id_name.account_number)
        = upper(stg_ecommerce_website_list.website_id)
    )
    and upper(stg_ecommerce_website_list.website_name) not like '%ZZ%'

left outer join
    {{ source("reference_excel_sources", "finance_customers_list") }}
    as finance_customers_list
    on finance_customers_list.ns_comp_id = pstg_companies.company_id
    and cast(finance_customers_list.customer_id as varchar(50))
    = cast(mdm_dim_fulfill_clients.customer_id as varchar(50))
    and finance_customers_list.customer_name_raw = pstg_companies.full_name

left join
    {{ source("reference_overrides", "mdm_override") }} as mdm_override
    on (
        mdm_override.original_value = pstg_companies.companyname
        or mdm_override.original_value = mdm_src_rc_id_name.company
        or mdm_override.original_value = mdm_dim_fulfill_clients.customer_name
        or mdm_override.original_value = mdm_src_sf_id_name.name
        or mdm_override.original_value = stg_ecommerce_website_list.website_name
        or mdm_override.original_value = finance_customers_list.customer_name_raw
    )
    and mdm_override.value_type = 'Name'
where pstg_companies.dbt_valid_to is null
