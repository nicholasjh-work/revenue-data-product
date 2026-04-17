# dq/checks.py
# Nicholas Hidalgo
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession, functions as F


FRESHNESS_SLA_DAYS = 2
DRIFT_TOLERANCE_PCT = 0.01

TABLE = "revenue.silver.fact_sales_completed"
RUNS_TABLE = "revenue.ops.fact_sales_completed_runs"
SOURCE_SQL = (
    "(SELECT COUNT(*) AS c FROM dbo.fact_sales "
    "WHERE order_status = 'Completed') src"
)


def _sla_status(lag_days: int | None) -> str:
    if lag_days is None:
        return "RED"
    if lag_days <= 1:
        return "GREEN"
    if lag_days <= FRESHNESS_SLA_DAYS:
        return "AMBER"
    return "RED"


def _log_run(
    spark: SparkSession,
    run_id: str,
    row_count: int,
    source_row_count: int | None,
    drift_pct: float | None,
    lag_days: int | None,
    sla_status: str,
    checks_passed: bool,
    failure_message: str | None,
    runs_table: str,
) -> None:
    row = Row(
        run_id=run_id,
        run_timestamp=datetime.now(timezone.utc),
        row_count=row_count,
        source_row_count=source_row_count,
        drift_pct=drift_pct,
        lag_days=lag_days,
        sla_status=sla_status,
        checks_passed=checks_passed,
        failure_message=failure_message,
    )
    df = spark.createDataFrame([row])
    df.write.format("delta").mode("append").saveAsTable(runs_table)


def run_checks(
    spark: SparkSession,
    table: str = TABLE,
    jdbc_url: str | None = None,
    jdbc_props: dict | None = None,
    runs_table: str | None = RUNS_TABLE,
) -> dict:
    """Blocking DQ gates for the governed revenue data product.

    Parameters
    ----------
    spark : SparkSession
    table : fully qualified name of the curated table
    jdbc_url, jdbc_props : optional. If both provided, source-vs-curated
        reconciliation runs. If either is missing, reconciliation is skipped.
    runs_table : optional. If provided, every run logs one row regardless of
        outcome. Pass None to disable the run log.

    Raises
    ------
    AssertionError on any blocking failure. A run-log row is always written
    before the exception propagates when runs_table is set.
    """
    run_id = str(uuid.uuid4())
    failures: list[str] = []
    df = spark.table(table)

    row_count = df.count()
    source_row_count: int | None = None
    drift_pct: float | None = None
    lag_days: int | None = None

    if row_count == 0:
        failures.append("Row count is zero")

    required = [
        "transaction_id",
        "transaction_date",
        "customer_key",
        "product_key",
        "net_revenue",
        "order_status",
    ]
    for col in required:
        n_null = df.filter(F.col(col).isNull()).count()
        if n_null > 0:
            failures.append(f"Nulls in {col}: {n_null}")

    bad_status = df.filter(F.col("order_status") != "Completed").count()
    if bad_status > 0:
        failures.append(f"order_status != 'Completed': {bad_status}")

    duplicates = (
        df.groupBy("transaction_id").count().filter("count > 1").count()
    )
    if duplicates > 0:
        failures.append(f"Duplicate transaction_id: {duplicates}")

    lag_row = spark.sql(
        f"SELECT datediff(current_date(), max(transaction_date)) AS lag_days "
        f"FROM {table}"
    ).first()
    lag_days = lag_row["lag_days"] if lag_row else None

    if lag_days is None:
        failures.append("Freshness check failed: table is empty or no transaction_date")
    elif lag_days > FRESHNESS_SLA_DAYS:
        failures.append(
            f"Freshness SLA miss: {lag_days} days stale "
            f"(SLA = {FRESHNESS_SLA_DAYS})"
        )

    reconciliation_skipped_reason: str | None = None
    if jdbc_url is None or jdbc_props is None:
        reconciliation_skipped_reason = "jdbc config not provided"
    else:
        try:
            source_row_count = (
                spark.read.jdbc(jdbc_url, SOURCE_SQL, properties=jdbc_props)
                .first()["c"]
            )
            if source_row_count and source_row_count > 0:
                drift_pct = abs(row_count - source_row_count) / source_row_count
                if drift_pct > DRIFT_TOLERANCE_PCT:
                    failures.append(
                        f"Row count drift vs source: {drift_pct:.2%} "
                        f"(source={source_row_count}, curated={row_count}, "
                        f"tolerance={DRIFT_TOLERANCE_PCT:.0%})"
                    )
            elif source_row_count == 0:
                failures.append("Source row count is zero for Completed orders")
        except Exception as exc:
            reconciliation_skipped_reason = f"reconciliation error: {exc}"
            failures.append(f"Reconciliation check failed to run: {exc}")

    sla_status = _sla_status(lag_days)
    checks_passed = not failures
    failure_message = "\n - ".join(failures) if failures else None

    if reconciliation_skipped_reason and not failures:
        print(f"Note: reconciliation skipped: {reconciliation_skipped_reason}")

    if runs_table:
        try:
            _log_run(
                spark=spark,
                run_id=run_id,
                row_count=row_count,
                source_row_count=source_row_count,
                drift_pct=drift_pct,
                lag_days=lag_days,
                sla_status=sla_status,
                checks_passed=checks_passed,
                failure_message=failure_message,
                runs_table=runs_table,
            )
        except Exception as exc:
            print(f"Warning: failed to write run log to {runs_table}: {exc}")

    if failures:
        raise AssertionError("DQ failures:\n - " + "\n - ".join(failures))

    return {
        "run_id": run_id,
        "row_count": row_count,
        "source_row_count": source_row_count,
        "drift_pct": drift_pct,
        "lag_days": lag_days,
        "sla_status": sla_status,
        "checks_passed": checks_passed,
    }
