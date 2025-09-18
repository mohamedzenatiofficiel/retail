
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sales_item_id
from "retail"."gold"."sales_item"
where sales_item_id is null



  
  
      
    ) dbt_internal_test