-- Schémas
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;

-- Silver : products
CREATE TABLE IF NOT EXISTS silver.products (
  product_sku    TEXT PRIMARY KEY,
  description    TEXT,
  unit_amount    NUMERIC,
  supplier       TEXT,
  _ingestion_ts  TIMESTAMPTZ,
  _batch_id      UUID
);

-- Silver : customers
CREATE TABLE IF NOT EXISTS silver.customers (
  customer_id    TEXT PRIMARY KEY,
  emails         TEXT[],              -- on stocke la liste telle quelle pour l'instant
  phone_numbers  TEXT[],
  _ingestion_ts  TIMESTAMPTZ,
  _batch_id      UUID
);

-- Silver : sales_customer (niveau vente)
CREATE TABLE IF NOT EXISTS silver.sales_customer (
  id             BIGINT PRIMARY KEY,  -- id de vente (auto-incr côté source)
  datetime       TIMESTAMPTZ,
  total_amount   NUMERIC,
  customer_id    TEXT,
  _ingestion_ts  TIMESTAMPTZ,
  _batch_id      UUID
);

-- Silver : sales_product (niveau ligne)
CREATE TABLE IF NOT EXISTS silver.sales_product (
  sale_id        BIGINT,
  line_no        INT,
  product_sku    TEXT,
  quantity       INT,
  amount         NUMERIC,
  _ingestion_ts  TIMESTAMPTZ,
  _batch_id      UUID,
  PRIMARY KEY (sale_id, line_no)
);

-- META : état d'ingestion / checkpoints
CREATE TABLE IF NOT EXISTS meta.ingestion_state (
  source_name    TEXT PRIMARY KEY,              
  last_sales_id  BIGINT,                        
  last_run_ts    TIMESTAMPTZ,
  note           TEXT
);

