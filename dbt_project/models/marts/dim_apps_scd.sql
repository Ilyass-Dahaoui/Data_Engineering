-- dimension built from snapshot to support SCD Type 2
with snaps as (
    select * from {{ ref('dim_apps_snapshot') }}
)

select
    row_number() over (partition by app_id order by dbt_valid_from) as app_sk,
    app_id,
    title as app_name,
    developer_id,
    category,
    price,
    case when free then false else true end as is_paid,
    installs,
    rating as catalog_rating,
    ratings_count,
    dbt_valid_from,
    dbt_valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from snaps
