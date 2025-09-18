
    
    

select
    sales_item_id as unique_field,
    count(*) as n_records

from "retail"."gold"."sales_item"
where sales_item_id is not null
group by sales_item_id
having count(*) > 1


