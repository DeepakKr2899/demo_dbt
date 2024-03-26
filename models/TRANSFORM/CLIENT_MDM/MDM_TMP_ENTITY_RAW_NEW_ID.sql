select
    (mdm_max_entity_id.max_id + mdm_tmp_entity_raw_new.increment_id) as entity_id,
    mdm_tmp_entity_raw_new.company_name as company_name,
    mdm_tmp_entity_raw_new.ns_id as ns_id,
    mdm_tmp_entity_raw_new.ns_par_id as ns_par_id,
    mdm_tmp_entity_raw_new.ns_gpar_id as ns_gpar_id,
    mdm_tmp_entity_raw_new.rc_code as rc_code,
    mdm_tmp_entity_raw_new.rc_tenant_id as rc_tenant_id,
    mdm_tmp_entity_raw_new.rc_par_id as rc_par_id,
    mdm_tmp_entity_raw_new.ns_ext_id as ns_ext_id,
    mdm_tmp_entity_raw_new.rc_id as rc_id,
    mdm_tmp_entity_raw_new.oracle_cust_id as oracle_cust_id,
    mdm_tmp_entity_raw_new.ecomm_web_id as ecomm_web_id,
    mdm_tmp_entity_raw_new.ecomm_web_group_id as ecomm_web_group_id,
    mdm_tmp_entity_raw_new.sf_id as sf_id,
    mdm_tmp_entity_raw_new.sf_par_id as sf_par_id,
    mdm_tmp_entity_raw_new.sf_acct_num as sf_acct_num,
    mdm_tmp_entity_raw_new.hs_id as hs_id,
    mdm_tmp_entity_raw_new.fin_row_num as fin_row_num
from {{ ref("MDM_TMP_ENTITY_RAW_NEW") }} mdm_tmp_entity_raw_new

left join
    {{ source("reference_overrides", "mdm_max_entity_id") }} mdm_max_entity_id on 1 = 1
