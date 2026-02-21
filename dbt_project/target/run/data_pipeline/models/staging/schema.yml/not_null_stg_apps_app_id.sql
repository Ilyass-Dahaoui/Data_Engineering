
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select app_id
from "dbt"."main"."stg_apps"
where app_id is null



  
  
      
    ) dbt_internal_test