-- models/gold/mart_daily_kpis.sql
{{ config(materialized='table') }}

select
  EXTRACT(day FROM sales_datetime) as day,
  sum(item_quantity)                as total_units,
  sum(item_amount)                  as total_revenue,
  avg(discount_perc)                as avg_discount
from {{ ref('sales_item') }}
group by 1
order by 1
