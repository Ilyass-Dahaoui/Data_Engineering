with cats as (
    select distinct category from "dbt"."main"."stg_apps"
)

select
    row_number() over () as category_key,
    category as category_name
from cats