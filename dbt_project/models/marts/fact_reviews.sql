
{{ config(materialized='incremental', unique_key='review_id') }}

select
        r.review_id,
        a.app_sk as app_key,
        d.developer_key,
        dt.date_key,
        r.score as rating,
        r.thumbs_up_count,
        r.content as review_text,
        r.review_created_version as review_version
from {{ ref('stg_reviews') }} r
left join {{ ref('dim_apps_scd') }} a
    on r.app_id = a.app_id
    and cast(substr(r.review_timestamp,1,10) as timestamp) >= a.dbt_valid_from
    and (a.dbt_valid_to is null or cast(substr(r.review_timestamp,1,10) as timestamp) < a.dbt_valid_to)
left join {{ ref('dim_developers') }} d on r.app_id = a.app_id
left join {{ ref('dim_date') }} dt on cast(strftime(cast(substr(r.review_timestamp,1,10) as date), '%Y%m%d') as integer) = dt.date_key

{% if is_incremental() %}
where r.review_id not in (select review_id from {{ this }})
{% endif %}
