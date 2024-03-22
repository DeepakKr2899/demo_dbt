select
    websiteid as website_id,
    websitegroupid as website_group_id,
    websitename as website_name,
    dateadded as date_added,
    datemodified as date_modified,
    row_number() over (
        partition by websiteid order by datemodified
    ) as pstg_websites_full_row_number
from {{ ref("pstg_websites_full") }}
where dbt_valid_to is null
qualify pstg_websites_full_row_number = 1
