# Databricks notebook source
# notebooks/dq/run_checks_task.py
# Nicholas Hidalgo
#
# Final task in the Databricks Workflow. Runs after the silver build.
# Uses the same secret scope as the silver notebook for reconciliation.

from pyspark.sql import SparkSession

from dq.checks import run_checks

spark = SparkSession.builder.getOrCreate()

JDBC_URL = dbutils.secrets.get("kv-revenue", "azsql-jdbc-url")
JDBC_PROPS = {
    "user": dbutils.secrets.get("kv-revenue", "azsql-user"),
    "password": dbutils.secrets.get("kv-revenue", "azsql-password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "encrypt": "true",
    "trustServerCertificate": "false",
}

result = run_checks(
    spark=spark,
    jdbc_url=JDBC_URL,
    jdbc_props=JDBC_PROPS,
)

print(result)
