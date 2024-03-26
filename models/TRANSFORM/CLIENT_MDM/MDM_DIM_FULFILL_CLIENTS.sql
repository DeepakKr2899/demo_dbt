select
    customer_name,
    customer_id,
    load_yr_mo,
    max(invoice_month) as max_invoice_month,
    max(invoice_date) as max_invoice_date
from {{ ref("TMP_STG_FULFILL_BILLING") }}
where invoice_type = 'Direct Shipping Invoice'
group by customer_name, customer_id, load_yr_mo
