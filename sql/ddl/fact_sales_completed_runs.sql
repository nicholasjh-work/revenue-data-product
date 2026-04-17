CREATE SCHEMA IF NOT EXISTS revenue.ops;

CREATE TABLE IF NOT EXISTS revenue.ops.fact_sales_completed_runs (
  run_id            STRING     NOT NULL,
  run_timestamp     TIMESTAMP  NOT NULL,
  row_count         BIGINT     NOT NULL,
  source_row_count  BIGINT,
  drift_pct         DOUBLE,
  lag_days          INT,
  sla_status        STRING     NOT NULL,
  checks_passed     BOOLEAN    NOT NULL,
  failure_message   STRING
)
USING DELTA
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true,
  delta.columnMapping.mode         = 'name'
);
