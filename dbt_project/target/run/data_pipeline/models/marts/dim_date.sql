
  
    
    

    create  table
      "dbt"."main"."dim_date__dbt_tmp"
  
    as (
      with dates as (
    select distinct cast(substr(review_timestamp,1,10) as date) as dt
    from "dbt"."main"."stg_reviews"
    where review_timestamp is not null
)

select
    row_number() over () as date_key,
    dt as date,
    extract(year from dt) as year,
    extract(month from dt) as month,
    extract(quarter from dt) as quarter,
    extract(dow from dt) as day_of_week,
    case when extract(dow from dt) in (0,6) then true else false end as is_weekend
from dates
    );
  
  