# Changelog : `revenue.silver.fact_sales_completed`

All notable changes to this data product are documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and semantic versioning.

---

## [1.0.0] - 2026-04-16

### Added
- Initial publication of `revenue.silver.fact_sales_completed` governed revenue data product
- Published data contract (`DATA_CONTRACT.md`) defining ownership, SLA, grain, semantics, quality gates, and exclusion policy
- JDBC-based Azure SQL to Delta pipeline on Databricks with Unity Catalog namespace
- Encoded "realized revenue" business policy: only `order_status = 'Completed'` included; Pending, Cancelled, Shipped excluded by design
- Blocking data quality gates (`dq/checks.py`):
  - Null checks on required fields
  - Grain uniqueness on `transaction_id`
  - `order_status = 'Completed'` enforcement
  - Freshness SLA (2-day lag threshold)
  - Source-to-curated row count reconciliation against Azure SQL (guards against silent row loss from inner-join dimension mismatches)
- Lightweight run log table (`revenue.ops.fact_sales_completed_runs`) writing one row per pipeline run with SLA status, drift, and failure detail
- Lineage columns on curated table: `_ingested_at`, `_source_system`
- Architecture diagram (`docs/architecture.png`)

### Design decisions
- Bronze layer intentionally out of scope for v1 to keep the contract surface narrow
- Dataset-level SLA status captured in the runs table, not on individual transaction rows (SLA is a run-level property, not a row-level attribute)
- Quality check status is not materialized on the fact table; jobs that fail DQ gates do not write, so a consumer reading the table always reads a passing state

### Known limitations
- Full-overwrite load; incremental MERGE planned for 1.1.0
- Power BI semantic model not yet connected (planned consumer layer)
- No auto-alerting beyond job failure; webhook or GitHub Issues integration planned for 1.1.0
- Corporate actions and return adjustments out of scope for this product

### Planned (1.1.0)
- Incremental MERGE load pattern on `transaction_date`
- Power BI semantic model with health tile powered by the runs table
- Alerting webhook on DQ failure
