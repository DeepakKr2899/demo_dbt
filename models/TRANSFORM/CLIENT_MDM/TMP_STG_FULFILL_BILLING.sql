select
    coalesce(mdm.override_value, erp.customer_name) as customer_name,
    erp.billing_code as billing_code,
    erp.invoice_type as invoice_type,
    substring(erp.invoice_date, 1, 7) as invoice_month,
    erp.invoice_date as invoice_date,
    erp.customer_id as customer_id,
    erp.description as description,
    count(*) as invoice_count,
    sum(erp.invoice_amount) as sum_invoice_amount,
    substring(max(erp.invoice_date), 1, 7) as max_invoice_month,
    substring(current_date(), 1, 7) as load_yr_mo
from {{ ref("pstg_erp_ar_invoices_to_ns") }} as erp
left join
    {{ source("reference_overrides", "mdm_override") }} as mdm
    on mdm.original_value = erp.customer_name
    and mdm.value_type = 'Name'
group by
    customer_name,
    billing_code,
    invoice_type,
    invoice_date,
    customer_id,
    description,
    mdm.override_value
order by customer_name, invoice_date, invoice_type
