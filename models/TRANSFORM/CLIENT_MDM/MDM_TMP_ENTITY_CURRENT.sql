select * from {{ ref("MDM_ENTITY_RAW_3") }} where entity_id is not null
