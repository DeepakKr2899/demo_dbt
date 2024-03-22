{{
    config(
        pre_hook=[
            "DROP TABLE IF EXISTS {{target.name}}_TRANSFORM.CLIENT_MDM.MDM_TMP_ENTTY_RAW_NEW"
        ]
    )
}}
select
    row_number() over (order by company_name, ns_id) as increment_id,
    mdm_entity_raw_3.company_name as company_name,
    mdm_entity_raw_3.ns_id as ns_id,
    mdm_entity_raw_3.ns_par_id as ns_par_id,
    mdm_entity_raw_3.ns_gpar_id as ns_gpar_id,
    mdm_entity_raw_3.rc_code as rc_code,
    mdm_entity_raw_3.rc_par_id as rc_par_id,
    mdm_entity_raw_3.rc_tenant_id as rc_tenant_id,
    mdm_entity_raw_3.ns_ext_id as ns_ext_id,
    mdm_entity_raw_3.rc_id as rc_id,
    mdm_entity_raw_3.oracle_cust_id as oracle_cust_id,
    mdm_entity_raw_3.ecomm_web_id as ecomm_web_id,
    mdm_entity_raw_3.ecomm_web_group_id as ecomm_web_group_id,
    mdm_entity_raw_3.sf_id as sf_id,
    mdm_entity_raw_3.sf_par_id as sf_par_id,
    mdm_entity_raw_3.sf_acct_num as sf_acct_num,
    mdm_entity_raw_3.hs_id as hs_id,
    mdm_entity_raw_3.fin_row_num as fin_row_num
from {{ ref("MDM_ENTITY_RAW_3") }} mdm_entity_raw_3

where mdm_entity_raw_3.entity_id is null
