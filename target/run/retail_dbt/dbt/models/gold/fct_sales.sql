
  
    

  create  table "retail"."gold"."fct_sales__dbt_tmp"
  
  
    as
  
  (
    -- models/gold/sales_item.sql
-- Fait ligne de vente (grain = sale_id + line_no)

SELECT
    -- PK simple = concaténation sale_id + line_no
    (sp.sale_id::text || '-' || sp.line_no::text)     AS sales_item_id,

    sc.datetime::timestamp                           AS sales_datetime,
    sp.amount::numeric                               AS item_amount,
    sp.product_sku,
    sp.quantity::numeric                             AS item_quantity,
    p.description                                   AS product_description,

    -- % remise calculée sur la base du prix unitaire
    CASE
        WHEN p.unit_amount IS NOT NULL AND sp.quantity > 0
        THEN ROUND(1 - (sp.amount / (p.unit_amount * sp.quantity)), 4)
        ELSE NULL
    END                                             AS discount_perc

FROM "retail"."silver"."sales_product" sp
JOIN "retail"."silver"."sales_customer" sc
  ON sc.id = sp.sale_id
LEFT JOIN "retail"."silver"."products" p
  ON p.product_sku = sp.product_sku
  );
  