# Data Contract : `revenue.silver.fact_sales_completed`

## Ownership
- **Product owner:** Nick Hidalgo (Data Product Owner / Analytics Operations)
- **Producer system:** Azure SQL : `dbo.fact_sales` and conformed dimensions
- **Intended consumers:** Finance reporting, revenue analytics, executive KPIs

## Purpose
A single governed source of **realized revenue** transactions. Only
records with `order_status = 'Completed'` are included. Pending,
Cancelled, and Shipped are excluded by policy to keep realized-revenue
logic conservative.

## Grain
One row per completed sales transaction. `transaction_id` is unique.

## Refresh & SLA
- **Cadence:** nightly batch
- **Freshness SLA:** `max(transaction_date) >= current_date - 2`
- **Change policy:** additive schema changes are non-breaking;
  breaking changes require a new table version.

## Semantics
| Field | Meaning |
|---|---|
| `net_revenue` | Recognized revenue after discounts. Metric of record. |
| `gross_margin` | `net_revenue - total_cost`. Do not recompute downstream. |
| `order_status` | Always `Completed` in this table : enforced at ingest. |
| `fiscal_year` / `fiscal_quarter` | From `dim_date`. Source of truth for fiscal calendar. |

## Quality gates (blocking)
- Zero nulls in: `transaction_id`, `transaction_date`, `customer_key`,
  `product_key`, `net_revenue`, `order_status`.
- Zero rows where `order_status <> 'Completed'`.
- `transaction_id` unique.
- Freshness SLA met.
- Referential integrity to `dim_customer`, `dim_product`, `dim_date` is
  enforced at build time via inner joins.

## Exclusions (by design)
Records with `order_status IN ('Pending','Cancelled','Shipped')` are
intentionally excluded. Consumers needing in-flight revenue should use
a separate pipeline product, not this table.
