# Databricks notebook source
# notebooks/silver/fact_sales_completed.py
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

# --- Config ---
JDBC_URL  = dbutils.secrets.get("kv-revenue", "azsql-jdbc-url")
JDBC_USER = dbutils.secrets.get("kv-revenue", "azsql-user")
JDBC_PWD  = dbutils.secrets.get("kv-revenue", "azsql-password")
JDBC_PROPS = {
    "user": JDBC_USER,
    "password": JDBC_PWD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "encrypt": "true",
    "trustServerCertificate": "false",
}

TARGET = "revenue.silver.fact_sales_completed"

# --- Extract ---
fact         = spark.read.jdbc(JDBC_URL, "dbo.fact_sales",    properties=JDBC_PROPS)
dim_customer = spark.read.jdbc(JDBC_URL, "dbo.dim_customer",  properties=JDBC_PROPS)
dim_product  = spark.read.jdbc(JDBC_URL, "dbo.dim_product",   properties=JDBC_PROPS)
dim_date     = spark.read.jdbc(JDBC_URL, "dbo.dim_date",      properties=JDBC_PROPS)

# --- Transform: governed "Completed only" realized-revenue rule ---
fact_completed = fact.filter(F.col("order_status") == "Completed")

curated = (
    fact_completed.alias("f")
        .join(dim_customer.alias("c"), "customer_key", "inner")
        .join(dim_product.alias("p"),  "product_key",  "inner")
        .join(dim_date.alias("d"),     "date_key",     "inner")
        .select(
            F.col("f.transaction_id"),
            F.col("f.transaction_date"),
            F.col("d.fiscal_year"),
            F.col("d.fiscal_quarter"),
            F.col("d.year").alias("calendar_year"),
            F.col("d.quarter_name"),
            F.col("d.month_name"),
            F.col("c.customer_key"),
            F.col("c.customer_name"),
            F.col("c.segment").alias("customer_segment"),
            F.col("p.product_key"),
            F.col("p.product_name"),
            F.col("p.product_category"),
            F.col("f.region_key"),
            F.col("f.quantity"),
            F.col("f.unit_price"),
            F.col("f.unit_cost"),
            F.col("f.discount_pct"),
            F.col("f.gross_amount"),
            F.col("f.discount_amount"),
            F.col("f.net_revenue"),
            F.col("f.total_cost"),
            F.col("f.gross_margin"),
            F.col("f.gross_margin_pct"),
            F.col("f.order_status"),
            F.current_timestamp().alias("_ingested_at"),
            F.lit("azsql.dbo.fact_sales").alias("_source_system"),
        )
)

# --- Load: full overwrite (v1). Incremental merge is a future iteration. ---
(
    curated.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("fiscal_year", "fiscal_quarter")
        .saveAsTable(TARGET)
)
