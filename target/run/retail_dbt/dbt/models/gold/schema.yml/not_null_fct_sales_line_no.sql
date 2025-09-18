
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select line_no
from "retail"."gold"."fct_sales"
where line_no is null



  
  
      
    ) dbt_internal_test