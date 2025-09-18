-- Dimension produit (dédoublée au besoin)
select
  p.product_sku      as product_id,
  p.description,
  p.supplier,
  p.unit_amount,
  max(p._ingestion_ts) as _last_ingestion_ts
from "retail"."silver"."products" p
group by 1,2,3,4