-- models/gold/mart_daily_kpis.sql


select
  date_trunc('day', sales_datetime) as day,
  sum(item_quantity)                as total_units,
  sum(item_amount)                  as total_revenue,
  avg(discount_perc)                as avg_discount
from "retail"."gold"."sales_item"
group by 1
order by 1