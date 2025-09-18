
  
    

  create  table "retail"."gold"."dim_customer__dbt_tmp"
  
  
    as
  
  (
    -- Dimension client (on garde les colonnes telles quelles)
select
  c.customer_id,
  c.emails,
  c.phone_numbers,
  max(c._ingestion_ts) as _last_ingestion_ts
from "retail"."silver"."customers" c
group by 1,2,3
  );
  