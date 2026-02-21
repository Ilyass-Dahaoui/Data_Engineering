-- staging model for apps metadata
-- reads raw json file using duckdb's read_json_auto

select
    appId                         as app_id,
    title,
    developer,
    developerId                   as developer_id,
    genre                         as category,
    cast(score as double)        as rating,
    cast(ratings as integer)      as ratings_count,
    installs,
    cast(price as double)         as price,
    free,
    contentRating                 as content_rating,
    released,
    updated,
    version,
    description,
    summary
from read_json_auto('C:\Users\ka903\Desktop\Data_Engineering-main\DATA\raw\apps_metadata.json')