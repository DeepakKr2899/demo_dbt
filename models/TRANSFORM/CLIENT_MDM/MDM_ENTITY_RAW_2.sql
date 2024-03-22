select
    coalesce(
        mdm_override.override_value,
        coalesce(
            mdm_entity_raw_1.company_name,
            coalesce(
                tmp_stg_hubspot_company_clean.company_name,
                coalesce(ecommerce_website_list.website_name, mdm_src_sf_id_name.name)
            )
        )
    ) as company_name,
    mdm_entity_raw_1.ns_id as ns_id,
    mdm_entity_raw_1.ns_par_id as ns_par_id,
    mdm_entity_raw_1.ns_gpar_id as ns_gpar_id,
    mdm_entity_raw_1.rc_code as rc_code,
    mdm_entity_raw_1.rc_tenant_id as rc_tenant_id,
    mdm_entity_raw_1.ns_ext_id as ns_ext_id,
    mdm_entity_raw_1.rc_id as rc_id,
    mdm_entity_raw_1.rc_par_id as rc_par_id,
    mdm_entity_raw_1.oracle_cust_id as oracle_cust_id,
    upper(
        coalesce(mdm_entity_raw_1.ecomm_web_id, ecommerce_website_list.website_id)
    ) as ecomm_web_id,
    coalesce(
        mdm_entity_raw_1.ecomm_web_group_id, ecommerce_website_list.website_group_id
    ) as ecomm_web_group_id,
    coalesce(mdm_entity_raw_1.sf_id, mdm_src_sf_id_name.id) as sf_id,
    coalesce(mdm_entity_raw_1.sf_par_id, mdm_src_sf_id_name.parent_id) as sf_par_id,
    coalesce(
        mdm_entity_raw_1.sf_acct_num, mdm_src_sf_id_name.account_number
    ) as sf_acct_num,
    mdm_entity_raw_1.fin_row_num as fin_row_num,
    tmp_stg_hubspot_company_clean.id as hs_id
from {{ ref("MDM_ENTITY_RAW_1") }} as mdm_entity_raw_1

full outer join
    {{ ref("TMP_STG_HUBSPOT_COMPANY_CLEAN") }} as tmp_stg_hubspot_company_clean
    on tmp_stg_hubspot_company_clean.company_name = mdm_entity_raw_1.company_name
    and tmp_stg_hubspot_company_clean.company_name is not null

full outer join
    {{ ref("MDM_SRC_SF_ID_NAME") }} mdm_src_sf_id_name
    on (
        mdm_entity_raw_1.company_name = mdm_src_sf_id_name.name
        and mdm_entity_raw_1.company_name is not null
    )
    or (
        tmp_stg_hubspot_company_clean.company_name = mdm_src_sf_id_name.name
        and tmp_stg_hubspot_company_clean.company_name is not null
    )

full outer join
    {{ ref("stg_ecommerce_website_list") }} ecommerce_website_list
    on mdm_entity_raw_1.company_name = ecommerce_website_list.website_name

left join
    {{ source("reference_overrides", "mdm_override") }} mdm_override
    on (
        mdm_override.original_value = tmp_stg_hubspot_company_clean.company_name
        or mdm_override.original_value = mdm_src_sf_id_name.name
    )
    and mdm_override.value_type = 'Name'

where
    (
        lower(mdm_entity_raw_1.company_name) not like '%redesign%'
        and lower(mdm_entity_raw_1.company_name) not like '%staging%'
        and lower(mdm_entity_raw_1.company_name) not like '% uat %'
        and lower(mdm_entity_raw_1.company_name) not like '%template%'
        and lower(mdm_entity_raw_1.company_name) not like '%testing%'
        and lower(mdm_entity_raw_1.company_name) not like '%test%'
        and lower(mdm_entity_raw_1.company_name) not like 'test'
        and lower(mdm_entity_raw_1.company_name) not like '%demo%'
        and lower(mdm_entity_raw_1.company_name) not like '%sandbox%'
        and lower(mdm_entity_raw_1.company_name) not like 'zz %'
        and lower(mdm_entity_raw_1.company_name) not like 'zz%'
        and lower(mdm_entity_raw_1.company_name) not like 'zzz%'
        and lower(mdm_entity_raw_1.company_name) not like 'zz -%'
        and lower(mdm_entity_raw_1.company_name) not like '%training%'
        and lower(mdm_entity_raw_1.company_name) not like '%vin65%'
        and lower(mdm_entity_raw_1.company_name) not like '%delton%'
        and lower(mdm_entity_raw_1.company_name) not like '%asfasdff%'
        and lower(mdm_entity_raw_1.company_name) not like '%warehouse%'
        and lower(mdm_entity_raw_1.company_name) not like '%fundrais%'
        and lower(mdm_entity_raw_1.company_name) <> 'united parcel service'
    )
    and (
        (
            lower(mdm_entity_raw_1.company_name) not like 'winedirect%'
            and lower(mdm_entity_raw_1.company_name) not like '%winedirect%'
        )
        or lower(mdm_entity_raw_1.company_name) like '% - winedirect%'
    )
    and (mdm_entity_raw_1.ns_id > 0 or mdm_entity_raw_1.ns_id is null)
