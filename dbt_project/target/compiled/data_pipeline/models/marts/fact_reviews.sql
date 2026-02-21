select
    r.review_id,
    a.app_key,
    d.developer_key,
    dt.date_key,
    r.score as rating,
    r.thumbs_up_count,
    r.content as review_text,
    r.review_created_version as review_version
from "dbt"."main"."stg_reviews" r
left join "dbt"."main"."dim_apps" a on r.app_id = a.app_id
left join "dbt"."main"."dim_developers" d on r.app_id = a.app_id
left join "dbt"."main"."dim_date" dt on cast(substr(r.review_timestamp,1,10) as date) = dt.date