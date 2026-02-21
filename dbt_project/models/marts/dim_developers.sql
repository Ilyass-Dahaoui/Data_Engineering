with devs as (
    select distinct developer from {{ ref('stg_apps') }}
)

select
    row_number() over () as developer_key,
    developer as developer_name,
    null as developer_website,
    null as developer_email
from devs
