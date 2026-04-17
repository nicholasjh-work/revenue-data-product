CREATE TABLE IF NOT EXISTS revenue.silver.fact_sales_completed (
  transaction_id     STRING  NOT NULL,
  transaction_date   DATE    NOT NULL,
  fiscal_year        INT     NOT NULL,
  fiscal_quarter     INT     NOT NULL,
  calendar_year      INT,
  quarter_name       STRING,
  month_name         STRING,
  customer_key       INT     NOT NULL,
  customer_name      STRING,
  customer_segment   STRING,
  product_key        INT     NOT NULL,
  product_name       STRING,
  product_category   STRING,
  region_key         INT,
  quantity           INT,
  unit_price         DECIMAL(12,2),
  unit_cost          DECIMAL(12,2),
  discount_pct       DECIMAL(5,4),
  gross_amount       DECIMAL(14,2),
  discount_amount    DECIMAL(14,2),
  net_revenue        DECIMAL(14,2) NOT NULL,
  total_cost         DECIMAL(14,2),
  gross_margin       DECIMAL(14,2),
  gross_margin_pct   DECIMAL(6,4),
  order_status       STRING  NOT NULL,
  _ingested_at       TIMESTAMP NOT NULL,
  _source_system     STRING    NOT NULL
)
USING DELTA
PARTITIONED BY (fiscal_year, fiscal_quarter)
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true,
  delta.columnMapping.mode         = 'name'
);

OPTIMIZE revenue.silver.fact_sales_completed
ZORDER BY (customer_key, product_key, transaction_date);
