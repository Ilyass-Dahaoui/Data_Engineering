select
    r.review_id,
    a.app_key,
    d.developer_key,
    dt.date_key,
    r.score as rating,
    r.thumbs_up_count,
    r.content as review_text,
    r.review_created_version as review_version
from {{ ref('stg_reviews') }} r
left join {{ ref('dim_apps') }} a on r.app_id = a.app_id
left join {{ ref('dim_developers') }} d on r.app_id = a.app_id
left join {{ ref('dim_date') }} dt on cast(substr(r.review_timestamp,1,10) as date) = dt.date
