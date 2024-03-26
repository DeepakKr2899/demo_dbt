{% snapshot pstg_websites_full %}

    {{
        config(
            unique_key='websiteid',
            strategy='check',
            check_cols=['modifiedby', 'datasource'],
        )
    }}

    select * from {{ ref('stg_websites_full') }}

{% endsnapshot %}