-- staging model for reviews

select
    reviewId                      as review_id,
    app_id,
    app_name,
    userName                      as user_name,
    content,
    cast(score as integer)        as score,
    cast(thumbsUpCount as integer) as thumbs_up_count,
    reviewCreatedVersion          as review_created_version,
    "at"                           as review_timestamp,
    replyContent                  as reply_content,
    repliedAt                     as replied_at
from read_json_auto('C:\Users\ka903\Desktop\Data_Engineering-main\DATA\raw\apps_reviews.json')