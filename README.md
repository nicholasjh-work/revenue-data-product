# Revenue Data Product

Governed, finance-grade revenue dataset built on the Databricks
Lakehouse. Productizes the "realized revenue" view of sales so Finance
and Analytics consume one contracted source instead of reconciling
divergent filters across dashboards.

## Architecture

![Architecture](docs/architecture.png)

The silver job reads Azure SQL via JDBC, applies the Completed-only
business rule, joins conformed dimensions, and writes a partitioned
Delta table. The DQ task runs reconciliation and null/grain/SLA checks,
then logs one row per run to the `fact_sales_completed_runs` operations
table , which is where dataset-level SLA status lives. Bronze is out of
scope for v1 to keep the contract surface narrow; a gold aggregate
layer is a future iteration.

### Stack
- **Source:** Azure SQL Database
- **Transform & store:** Databricks (PySpark, Unity Catalog, Delta Lake)
- **Orchestration:** Databricks Workflows
- **Quality:** assertion-based checks run as the final job task
- **Governance:** published data contract + Unity Catalog lineage

## Operational visibility

Dataset-level health lives in `revenue.ops.fact_sales_completed_runs`:

| Column | Purpose |
|---|---|
| `run_id` | Unique identifier for the pipeline run |
| `run_timestamp` | UTC timestamp of the run |
| `row_count` | Curated row count produced by the run |
| `source_row_count` | Source Completed row count at run time |
| `drift_pct` | Absolute drift between source and curated |
| `lag_days` | Freshness lag at run time |
| `sla_status` | `GREEN` / `AMBER` / `RED` |
| `checks_passed` | Overall DQ outcome |
| `failure_message` | Populated when `checks_passed = false` |

This is the audit trail and the source of the health tile a Power BI
semantic model will read.

## Repo layout
```
.
├── notebooks/
│   ├── silver/
│   │   └── fact_sales_completed.py          # JDBC extract + Completed-only transform + Delta write
│   └── dq/
│       └── run_checks_task.py               # Final workflow task wrapping run_checks()
├── sql/
│   └── ddl/
│       ├── fact_sales_completed.sql         # Silver Delta DDL + ZORDER
│       └── fact_sales_completed_runs.sql    # Ops runs table DDL
├── dq/
│   └── checks.py                            # Blocking DQ + reconciliation + run logging
├── docs/
│   ├── architecture.mmd                     # Mermaid source
│   └── architecture.png                     # Rendered diagram
├── DATA_CONTRACT.md                         # Ownership, SLA, schema, exclusions
├── CHANGELOG.md                             # Versioned changes
└── README.md
```

## Running
The Databricks Workflow has two tasks:

1. `notebooks/silver/fact_sales_completed.py`: extract, transform, write silver
2. `notebooks/dq/run_checks_task.py`: blocking DQ gates and run log

Both tasks read secrets from the `kv-revenue` scope; do not hardcode
connection strings. Before the first run, create both tables from
`sql/ddl/fact_sales_completed.sql` and
`sql/ddl/fact_sales_completed_runs.sql`.

The DQ task wraps `dq.checks.run_checks(spark, jdbc_url, jdbc_props)`
so source-to-curated reconciliation runs against the same source system
the silver task reads from. One row is appended to
`revenue.ops.fact_sales_completed_runs` on every execution, pass or fail.

## Ownership
Data Product Owner: Nick Hidalgo. See `DATA_CONTRACT.md` for SLA,
schema, and exclusion policy.
