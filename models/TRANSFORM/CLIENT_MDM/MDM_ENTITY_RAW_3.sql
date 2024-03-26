select
    coalesce(
        mdm_tmp_entity_hist.entity_id,
        coalesce(
            mdm_tmp_entity_hist2.entity_id,
            coalesce(
                mdm_tmp_entity_hist3.entity_id,
                coalesce(
                    mdm_tmp_entity_hist4.entity_id,
                    coalesce(
                        mdm_tmp_entity_hist5.entity_id,
                        coalesce(
                            mdm_tmp_entity_hist6.entity_id,
                            mdm_tmp_entity_hist7.entity_id
                        )
                    )
                )
            )
        )
    ) as entity_id,
    mdm_entity_raw_2.company_name as company_name,
    mdm_entity_raw_2.ns_id as ns_id,
    mdm_entity_raw_2.ns_par_id as ns_par_id,
    mdm_entity_raw_2.ns_gpar_id as ns_gpar_id,
    mdm_entity_raw_2.rc_code as rc_code,
    mdm_entity_raw_2.rc_tenant_id as rc_tenant_id,
    mdm_entity_raw_2.ns_ext_id as ns_ext_id,
    mdm_entity_raw_2.rc_id as rc_id,
    mdm_entity_raw_2.rc_par_id as rc_par_id,
    mdm_entity_raw_2.oracle_cust_id as oracle_cust_id,
    mdm_entity_raw_2.ecomm_web_id as ecomm_web_id,
    mdm_entity_raw_2.ecomm_web_group_id as ecomm_web_group_id,
    mdm_entity_raw_2.sf_par_id as sf_par_id,
    mdm_entity_raw_2.sf_acct_num as sf_acct_num,
    mdm_entity_raw_2.sf_id as sf_id,
    mdm_entity_raw_2.fin_row_num as fin_row_num,
    mdm_entity_raw_2.hs_id as hs_id
from {{ ref('MDM_ENTITY_RAW_2') }} mdm_entity_raw_2

left join
    {{ source("reference_overrides", "mdm_tmp_entity_hist") }} mdm_tmp_entity_hist
    on (
        mdm_entity_raw_2.ns_id = mdm_tmp_entity_hist.ns_id
        and mdm_tmp_entity_hist.ns_id is not null
        and mdm_entity_raw_2.ns_ext_id = mdm_tmp_entity_hist.ns_ext_id
        and mdm_tmp_entity_hist.ns_ext_id is not null
        and mdm_tmp_entity_hist.company_name = mdm_entity_raw_2.company_name
    )

left join
    {{ source("reference_overrides", "mdm_tmp_entity_hist") }} mdm_tmp_entity_hist2
    on (
        mdm_entity_raw_2.rc_id = mdm_tmp_entity_hist2.rc_id
        and mdm_tmp_entity_hist2.rc_id is not null
        and mdm_tmp_entity_hist2.company_name = mdm_entity_raw_2.company_name
    )

left join
    {{ source("reference_overrides", "mdm_tmp_entity_hist") }} mdm_tmp_entity_hist3
    on (
        mdm_entity_raw_2.oracle_cust_id = mdm_tmp_entity_hist3.oracle_cust_id
        and mdm_tmp_entity_hist3.oracle_cust_id is not null
    )

left join
    {{ source("reference_overrides", "mdm_tmp_entity_hist") }} mdm_tmp_entity_hist4
    on (
        mdm_entity_raw_2.rc_code = mdm_tmp_entity_hist4.rc_code
        and mdm_tmp_entity_hist4.rc_code is not null
    )

left join
    {{ source("reference_overrides", "mdm_tmp_entity_hist") }} mdm_tmp_entity_hist5
    on (
        mdm_entity_raw_2.ecomm_web_id = mdm_tmp_entity_hist5.rc_code
        and mdm_tmp_entity_hist5.rc_code is not null
        and mdm_entity_raw_2.ns_id = mdm_tmp_entity_hist5.ns_id
    )
    or (
        mdm_entity_raw_2.company_name = mdm_tmp_entity_hist5.company_name
        and (
            mdm_entity_raw_2.ns_id = mdm_tmp_entity_hist5.ns_id
            or mdm_entity_raw_2.rc_id = mdm_tmp_entity_hist5.rc_id
            or mdm_entity_raw_2.fin_row_num = mdm_tmp_entity_hist5.fin_row_num
        )
    )

left join
    {{ source("reference_overrides", "mdm_tmp_entity_hist") }} mdm_tmp_entity_hist6
    on (
        mdm_entity_raw_2.sf_id = mdm_tmp_entity_hist6.sf_id
        and mdm_tmp_entity_hist6.sf_id is not null
        and mdm_entity_raw_2.ns_id = mdm_tmp_entity_hist6.ns_id
        and mdm_entity_raw_2.ns_id is not null
        and mdm_entity_raw_2.rc_id = mdm_tmp_entity_hist6.rc_id
    )

left join
    {{ source("reference_overrides", "mdm_tmp_entity_hist") }} mdm_tmp_entity_hist7
    on (
        mdm_entity_raw_2.hs_id = mdm_tmp_entity_hist7.hs_id
        and mdm_tmp_entity_hist7.hs_id is not null
        and mdm_entity_raw_2.rc_id is null
        and mdm_entity_raw_2.ns_id is null
        and mdm_entity_raw_2.rc_code is null
        and mdm_entity_raw_2.ecomm_web_id is null
        and mdm_entity_raw_2.sf_id is null
        and mdm_entity_raw_2.oracle_cust_id is null
    )
