{% snapshot pstg_companies %}

    {{
        config(
            unique_key='company_id',
            strategy='check',
            check_cols=['company_name', 'country'],
        )
    }}

    select * from {{ source('sources_netsuite', 'companies') }}

{% endsnapshot %}