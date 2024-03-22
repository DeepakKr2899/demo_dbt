{% snapshot pstg_account_history %}

    {{
        config(
            unique_key='id',
            strategy='check',
            check_cols=['code', 'bill_to'],
        )
    }}

    select * from {{ ref('tmp_account_history') }}

{% endsnapshot %}