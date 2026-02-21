with apps as (
    select * from {{ ref('stg_apps') }}
)

select
    row_number() over () as app_key,
    app_id,
    title as app_name,
    developer_id,
    category,
    price,
    case when free then false else true end as is_paid,
    installs,
    rating as catalog_rating,
    ratings_count
from apps
