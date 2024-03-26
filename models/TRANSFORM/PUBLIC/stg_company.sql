{{
    config(
        materialized="incremental",
        unique_key="id",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

select *
from {{ source("sources_hubspot", "company") }}
