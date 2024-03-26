{% snapshot pstg_account %}

    {{
        config(
            unique_key="id",
            strategy="check",
            check_cols=["name", "type"],
        )
    }}

    select *, 'SALESFORCE' as source_system
    from {{ source("sources_salesforce", "account") }}

{% endsnapshot %}
