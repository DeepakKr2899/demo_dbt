{% snapshot pstg_erp_ar_invoices_to_ns %}

    {{
        config(
            unique_key='ar_invoice_id',
            strategy='check',
            check_cols=['carrier', 'quantity_invoice'],
        )
    }}

    select * from {{ source('sources_oracle_xws', 'erp_ar_invoices_to_ns') }}

{% endsnapshot %}