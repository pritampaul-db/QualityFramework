# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Framework POC - Complete End-to-End Deployment & Execution
# MAGIC
# MAGIC This self-contained notebook runs the entire Metadata-Driven Data Quality Framework.
# MAGIC All code is inlined — no external module imports required.
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Create catalog, schemas, volume
# MAGIC 2. Load mock data into Bronze tables
# MAGIC 3. Load DQ rules into metadata tables
# MAGIC 4. Execute DQ rules (Bronze → Silver) with quarantine
# MAGIC 5. Build Gold layer
# MAGIC 6. Generate DQ reports & dashboards
# MAGIC 7. Feedback loop (mock Collibra writeback)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import json
import uuid
import hashlib
import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_json, struct, monotonically_increasing_id,
    concat_ws, count, length, expr, sum as spark_sum, avg,
    max as spark_max, min as spark_min, round as spark_round, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, IntegerType
)
from delta.tables import DeltaTable

CATALOG = "dq_poc"
PIPELINE_RUN_ID = str(uuid.uuid4())
VOLUME_PATH = "/Volumes/dq_poc/bronze/mock_data"

print(f"{'='*70}")
print(f"DQ FRAMEWORK POC - END-TO-END EXECUTION")
print(f"{'='*70}")
print(f"Run ID:  {PIPELINE_RUN_ID}")
print(f"Catalog: {CATALOG}")
print(f"Started: {datetime.utcnow().isoformat()}")
print(f"{'='*70}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Catalog, Schemas, and Volume

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")

for schema_name in ["bronze", "silver", "gold", "dq_framework"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema_name}")
    print(f"  Created: {CATALOG}.{schema_name}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.mock_data")
print("  Created: Volume dq_poc.bronze.mock_data")
print("\nCatalog and schemas ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Mock Data into Bronze Tables
# MAGIC
# MAGIC CSV files were uploaded to the UC Volume via CLI.

# COMMAND ----------

# Load Bronze tables from Volume
for table_name, file_name in [
    ("bronze.customers", "customers.csv"),
    ("bronze.transactions", "transactions.csv"),
    ("bronze.products", "products.csv"),
]:
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/{file_name}")
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{table_name}")
    print(f"  {CATALOG}.{table_name}: {df.count()} rows")

# Reference table
dim_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{VOLUME_PATH}/dim_country.csv")
)
dim_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.silver.dim_country")
print(f"  {CATALOG}.silver.dim_country: {dim_df.count()} rows")

print("\nBronze data loaded!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preview Bronze Data (with intentional quality issues)

# COMMAND ----------

print("=== Bronze Customers (has nulls, bad emails, age out of range, duplicates) ===")
display(spark.table(f"{CATALOG}.bronze.customers"))

# COMMAND ----------

print("=== Bronze Transactions (has null amounts, negative values, orphan refs) ===")
display(spark.table(f"{CATALOG}.bronze.transactions"))

# COMMAND ----------

print("=== Bronze Products (has null names, negative prices, invalid SKUs) ===")
display(spark.table(f"{CATALOG}.bronze.products"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load DQ Rules into Metadata Tables
# MAGIC
# MAGIC 20 rules across 9 types, with error/warning severity and hard/soft enforcement.

# COMMAND ----------

# ---- DQ Rules (simulating Collibra extraction) ----
MOCK_RULES = [
    {"rule_id": "DQR-000001", "rule_name": "Customer Email Not Null", "rule_type": "not_null", "target_column": "email", "params": "{}", "severity": "error", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a1"},
    {"rule_id": "DQR-000002", "rule_name": "Customer Name Not Null", "rule_type": "not_null", "target_column": "customer_name", "params": "{}", "severity": "error", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a2"},
    {"rule_id": "DQR-000003", "rule_name": "Customer Age Range", "rule_type": "range", "target_column": "age", "params": '{"min": 18, "max": 120}', "severity": "error", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a3"},
    {"rule_id": "DQR-000004", "rule_name": "Valid Email Format", "rule_type": "regex", "target_column": "email", "params": '{"pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$"}', "severity": "error", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a4"},
    {"rule_id": "DQR-000005", "rule_name": "Valid Phone Length", "rule_type": "length", "target_column": "phone", "params": '{"min_length": 10, "max_length": 15}', "severity": "warning", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a5"},
    {"rule_id": "DQR-000006", "rule_name": "Customer Status Allowed Values", "rule_type": "allowed_values", "target_column": "status", "params": '{"values": ["active", "inactive", "suspended", "pending"]}', "severity": "error", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a6"},
    {"rule_id": "DQR-000007", "rule_name": "Unique Customer ID", "rule_type": "uniqueness", "target_column": "customer_id", "params": '{"scope": "table"}', "severity": "error", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a7"},
    {"rule_id": "DQR-000008", "rule_name": "Valid Country Code", "rule_type": "referential_integrity", "target_column": "country_code", "params": '{"ref_table": "dq_poc.silver.dim_country", "ref_column": "country_code"}', "severity": "warning", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a8"},
    {"rule_id": "DQR-000009", "rule_name": "Transaction Amount Not Null", "rule_type": "not_null", "target_column": "amount", "params": "{}", "severity": "error", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a9"},
    {"rule_id": "DQR-000010", "rule_name": "Transaction Amount Range", "rule_type": "range", "target_column": "amount", "params": '{"min": 0.01, "max": 1000000}', "severity": "error", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a10"},
    {"rule_id": "DQR-000011", "rule_name": "Valid Transaction Type", "rule_type": "allowed_values", "target_column": "transaction_type", "params": '{"values": ["purchase", "refund", "transfer", "withdrawal", "deposit"]}', "severity": "error", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a11"},
    {"rule_id": "DQR-000012", "rule_name": "Transaction Date Not Null", "rule_type": "not_null", "target_column": "transaction_date", "params": "{}", "severity": "error", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a12"},
    {"rule_id": "DQR-000013", "rule_name": "Valid Customer Reference", "rule_type": "referential_integrity", "target_column": "customer_id", "params": '{"ref_table": "dq_poc.silver.customers", "ref_column": "customer_id"}', "severity": "error", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a13"},
    {"rule_id": "DQR-000014", "rule_name": "Transaction Freshness", "rule_type": "freshness", "target_column": "transaction_date", "params": '{"max_age_hours": 72}', "severity": "warning", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a14"},
    {"rule_id": "DQR-000015", "rule_name": "Positive Transaction Quantity", "rule_type": "custom_sql", "target_column": "quantity", "params": '{"sql_expression": "quantity > 0 AND quantity <= 10000"}', "severity": "warning", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a15"},
    {"rule_id": "DQR-000016", "rule_name": "Product Name Not Null", "rule_type": "not_null", "target_column": "product_name", "params": "{}", "severity": "error", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a16"},
    {"rule_id": "DQR-000017", "rule_name": "Product Price Range", "rule_type": "range", "target_column": "price", "params": '{"min": 0.01, "max": 99999.99}', "severity": "error", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a17"},
    {"rule_id": "DQR-000018", "rule_name": "Valid Product Category", "rule_type": "allowed_values", "target_column": "category", "params": '{"values": ["electronics", "clothing", "food", "furniture", "sports", "books", "toys"]}', "severity": "warning", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a18"},
    {"rule_id": "DQR-000019", "rule_name": "Product SKU Format", "rule_type": "regex", "target_column": "sku", "params": '{"pattern": "^[A-Z]{3}-[0-9]{4,6}$"}', "severity": "warning", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a19"},
    {"rule_id": "DQR-000020", "rule_name": "Unique Product SKU", "rule_type": "uniqueness", "target_column": "sku", "params": '{"scope": "table"}', "severity": "error", "is_active": True, "version": 1, "effective_from": "2026-01-01", "effective_to": None, "source_system": "collibra", "created_at": "2026-04-01", "updated_at": "2026-04-01", "checksum": "a20"},
]

# ---- Rule-Dataset Mappings ----
MOCK_MAPPINGS = [
    {"mapping_id": "MAP-001", "dataset_name": "bronze.customers", "column_name": "email", "rule_id": "DQR-000001", "enforcement_mode": "hard", "priority": 1, "is_active": True},
    {"mapping_id": "MAP-002", "dataset_name": "bronze.customers", "column_name": "customer_name", "rule_id": "DQR-000002", "enforcement_mode": "hard", "priority": 2, "is_active": True},
    {"mapping_id": "MAP-003", "dataset_name": "bronze.customers", "column_name": "age", "rule_id": "DQR-000003", "enforcement_mode": "soft", "priority": 3, "is_active": True},
    {"mapping_id": "MAP-004", "dataset_name": "bronze.customers", "column_name": "email", "rule_id": "DQR-000004", "enforcement_mode": "soft", "priority": 4, "is_active": True},
    {"mapping_id": "MAP-005", "dataset_name": "bronze.customers", "column_name": "phone", "rule_id": "DQR-000005", "enforcement_mode": "soft", "priority": 5, "is_active": True},
    {"mapping_id": "MAP-006", "dataset_name": "bronze.customers", "column_name": "status", "rule_id": "DQR-000006", "enforcement_mode": "hard", "priority": 6, "is_active": True},
    {"mapping_id": "MAP-007", "dataset_name": "bronze.customers", "column_name": "customer_id", "rule_id": "DQR-000007", "enforcement_mode": "hard", "priority": 7, "is_active": True},
    {"mapping_id": "MAP-008", "dataset_name": "bronze.customers", "column_name": "country_code", "rule_id": "DQR-000008", "enforcement_mode": "soft", "priority": 8, "is_active": True},
    {"mapping_id": "MAP-009", "dataset_name": "bronze.transactions", "column_name": "amount", "rule_id": "DQR-000009", "enforcement_mode": "hard", "priority": 1, "is_active": True},
    {"mapping_id": "MAP-010", "dataset_name": "bronze.transactions", "column_name": "amount", "rule_id": "DQR-000010", "enforcement_mode": "hard", "priority": 2, "is_active": True},
    {"mapping_id": "MAP-011", "dataset_name": "bronze.transactions", "column_name": "transaction_type", "rule_id": "DQR-000011", "enforcement_mode": "hard", "priority": 3, "is_active": True},
    {"mapping_id": "MAP-012", "dataset_name": "bronze.transactions", "column_name": "transaction_date", "rule_id": "DQR-000012", "enforcement_mode": "hard", "priority": 4, "is_active": True},
    {"mapping_id": "MAP-013", "dataset_name": "bronze.transactions", "column_name": "customer_id", "rule_id": "DQR-000013", "enforcement_mode": "soft", "priority": 5, "is_active": True},
    {"mapping_id": "MAP-014", "dataset_name": "bronze.transactions", "column_name": "transaction_date", "rule_id": "DQR-000014", "enforcement_mode": "soft", "priority": 6, "is_active": True},
    {"mapping_id": "MAP-015", "dataset_name": "bronze.transactions", "column_name": "quantity", "rule_id": "DQR-000015", "enforcement_mode": "soft", "priority": 7, "is_active": True},
    {"mapping_id": "MAP-016", "dataset_name": "bronze.products", "column_name": "product_name", "rule_id": "DQR-000016", "enforcement_mode": "hard", "priority": 1, "is_active": True},
    {"mapping_id": "MAP-017", "dataset_name": "bronze.products", "column_name": "price", "rule_id": "DQR-000017", "enforcement_mode": "hard", "priority": 2, "is_active": True},
    {"mapping_id": "MAP-018", "dataset_name": "bronze.products", "column_name": "category", "rule_id": "DQR-000018", "enforcement_mode": "soft", "priority": 3, "is_active": True},
    {"mapping_id": "MAP-019", "dataset_name": "bronze.products", "column_name": "sku", "rule_id": "DQR-000019", "enforcement_mode": "soft", "priority": 4, "is_active": True},
    {"mapping_id": "MAP-020", "dataset_name": "bronze.products", "column_name": "sku", "rule_id": "DQR-000020", "enforcement_mode": "hard", "priority": 5, "is_active": True},
]

print(f"Loaded {len(MOCK_RULES)} DQ rules and {len(MOCK_MAPPINGS)} mappings")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Rules into Delta Metadata Tables

# COMMAND ----------

# Create and populate dq_rules
RULES_SCHEMA = StructType([
    StructField("rule_id", StringType(), False),
    StructField("rule_name", StringType(), True),
    StructField("rule_type", StringType(), False),
    StructField("target_column", StringType(), False),
    StructField("params", StringType(), True),
    StructField("severity", StringType(), False),
    StructField("is_active", BooleanType(), True),
    StructField("version", IntegerType(), True),
    StructField("effective_from", StringType(), True),
    StructField("effective_to", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("checksum", StringType(), True),
])

rules_df = spark.createDataFrame(MOCK_RULES, RULES_SCHEMA)
rules_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.dq_framework.dq_rules")
print(f"dq_rules: {rules_df.count()} rows loaded")

# Create and populate dq_rule_mapping
MAPPINGS_SCHEMA = StructType([
    StructField("mapping_id", StringType(), False),
    StructField("dataset_name", StringType(), False),
    StructField("column_name", StringType(), False),
    StructField("rule_id", StringType(), False),
    StructField("enforcement_mode", StringType(), True),
    StructField("priority", IntegerType(), True),
    StructField("is_active", BooleanType(), True),
])

mappings_df = spark.createDataFrame(MOCK_MAPPINGS, MAPPINGS_SCHEMA)
mappings_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.dq_framework.dq_rule_mapping")
print(f"dq_rule_mapping: {mappings_df.count()} rows loaded")

# COMMAND ----------

print("=== DQ Rules by Type and Severity ===")
display(spark.sql(f"""
    SELECT rule_type, severity, COUNT(*) as count
    FROM {CATALOG}.dq_framework.dq_rules
    GROUP BY rule_type, severity
    ORDER BY rule_type
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: DQ Engine - Execute Rules (Bronze → Silver)
# MAGIC
# MAGIC The engine dynamically loads rules from metadata and applies them using the Strategy Pattern.

# COMMAND ----------

# Create framework tables for logging
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.dq_framework.dq_execution_log (
        execution_id STRING, run_id STRING, rule_id STRING, rule_type STRING,
        dataset_name STRING, target_column STRING, severity STRING,
        enforcement_mode STRING, status STRING, total_records BIGINT,
        passed_count BIGINT, failed_count BIGINT, dq_score DOUBLE,
        error_message STRING, execution_duration_ms BIGINT,
        executed_at TIMESTAMP, executed_by STRING
    ) USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.dq_framework.dq_quarantine (
        quarantine_id STRING, execution_id STRING, run_id STRING,
        rule_id STRING, rule_type STRING, source_table STRING,
        record_payload STRING, quarantine_reason STRING, severity STRING,
        quarantined_at TIMESTAMP, resolved_at TIMESTAMP, resolution_action STRING
    ) USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.dq_framework.dq_scores (
        score_id STRING, run_id STRING, dataset_name STRING,
        total_rules INT, rules_passed INT, rules_failed INT,
        rules_errored INT, overall_dq_score DOUBLE,
        total_records_in BIGINT, total_records_out BIGINT,
        records_quarantined BIGINT, scored_at TIMESTAMP
    ) USING DELTA
""")

# Truncate logs for clean POC run
spark.sql(f"TRUNCATE TABLE {CATALOG}.dq_framework.dq_execution_log")
spark.sql(f"TRUNCATE TABLE {CATALOG}.dq_framework.dq_quarantine")
spark.sql(f"TRUNCATE TABLE {CATALOG}.dq_framework.dq_scores")

print("Framework tables ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ Rule Executor Functions (Strategy Pattern)

# COMMAND ----------

def execute_not_null(df, target_column, params):
    """Check column is not null."""
    total = df.count()
    failed_df = df.filter(col(target_column).isNull())
    failed_count = failed_df.count()
    return total, total - failed_count, failed_count, failed_df

def execute_range(df, target_column, params):
    """Check column value is within [min, max]."""
    min_val, max_val = params["min"], params["max"]
    total = df.count()
    failed_df = df.filter(
        col(target_column).isNull() |
        (col(target_column) < min_val) |
        (col(target_column) > max_val)
    )
    failed_count = failed_df.count()
    return total, total - failed_count, failed_count, failed_df

def execute_regex(df, target_column, params):
    """Check column matches regex pattern."""
    pattern = params["pattern"]
    total = df.count()
    failed_df = df.filter(
        col(target_column).isNull() | ~col(target_column).rlike(pattern)
    )
    failed_count = failed_df.count()
    return total, total - failed_count, failed_count, failed_df

def execute_uniqueness(df, target_column, params):
    """Check column has unique values."""
    total = df.count()
    dupes = df.groupBy(target_column).agg(count("*").alias("_cnt")).filter(col("_cnt") > 1)
    dupe_vals = dupes.select(target_column)
    failed_df = df.join(dupe_vals, on=target_column, how="inner")
    failed_count = failed_df.count()
    return total, total - failed_count, failed_count, failed_df

def execute_allowed_values(df, target_column, params):
    """Check column value is in allowed set."""
    allowed = params["values"]
    total = df.count()
    failed_df = df.filter(col(target_column).isNull() | ~col(target_column).isin(allowed))
    failed_count = failed_df.count()
    return total, total - failed_count, failed_count, failed_df

def execute_length(df, target_column, params):
    """Check string column length is within bounds."""
    min_len, max_len = params["min_length"], params["max_length"]
    total = df.count()
    failed_df = df.filter(
        col(target_column).isNull() |
        (length(col(target_column)) < min_len) |
        (length(col(target_column)) > max_len)
    )
    failed_count = failed_df.count()
    return total, total - failed_count, failed_count, failed_df

def execute_referential_integrity(df, target_column, params):
    """Check column values exist in reference table."""
    ref_table = params["ref_table"]
    ref_column = params["ref_column"]
    try:
        ref_df = spark.table(ref_table)
        total = df.count()
        failed_df = df.join(
            ref_df.select(col(ref_column).alias(target_column)).distinct(),
            on=target_column, how="left_anti"
        )
        null_df = df.filter(col(target_column).isNull())
        failed_df = failed_df.unionByName(null_df)
        failed_count = failed_df.count()
        return total, total - failed_count, failed_count, failed_df
    except Exception as e:
        total = df.count()
        return total, 0, 0, None  # Skip if ref table missing

def execute_freshness(df, target_column, params):
    """Check data is not older than threshold."""
    max_age_hours = params["max_age_hours"]
    total = df.count()
    threshold = f"current_timestamp() - INTERVAL {max_age_hours} HOURS"
    failed_df = df.filter(
        col(target_column).isNull() |
        (col(target_column).cast("timestamp") < expr(threshold))
    )
    failed_count = failed_df.count()
    return total, total - failed_count, failed_count, failed_df

def execute_custom_sql(df, target_column, params):
    """Check using custom SQL expression."""
    sql_expression = params["sql_expression"]
    total = df.count()
    failed_df = df.filter(~expr(sql_expression) | expr(sql_expression).isNull())
    failed_count = failed_df.count()
    return total, total - failed_count, failed_count, failed_df

# Registry
EXECUTORS = {
    "not_null": execute_not_null,
    "range": execute_range,
    "regex": execute_regex,
    "uniqueness": execute_uniqueness,
    "allowed_values": execute_allowed_values,
    "length": execute_length,
    "referential_integrity": execute_referential_integrity,
    "freshness": execute_freshness,
    "custom_sql": execute_custom_sql,
}

print(f"Registered {len(EXECUTORS)} rule executors: {list(EXECUTORS.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ Engine: Process a Dataset

# COMMAND ----------

def run_dq_engine(dataset_name, bronze_table, silver_table, run_id):
    """Execute all DQ rules for a dataset and write Silver output."""

    print(f"\n{'='*60}")
    print(f"PROCESSING: {dataset_name}")
    print(f"{'='*60}")

    # Load rules for this dataset
    rules = spark.sql(f"""
        SELECT r.rule_id, r.rule_name, r.rule_type, r.target_column,
               r.params, r.severity, m.enforcement_mode, m.priority
        FROM {CATALOG}.dq_framework.dq_rules r
        JOIN {CATALOG}.dq_framework.dq_rule_mapping m ON r.rule_id = m.rule_id
        WHERE m.dataset_name = '{dataset_name}' AND r.is_active = true AND m.is_active = true
        ORDER BY m.priority
    """).collect()

    print(f"Loaded {len(rules)} rules")

    # Read Bronze data
    bronze_df = spark.table(f"{CATALOG}.{bronze_table}")
    clean_df = bronze_df
    bronze_count = bronze_df.count()

    rules_passed = 0
    rules_failed = 0
    rules_errored = 0
    all_quarantine_records = []

    for rule_row in rules:
        rule = rule_row.asDict()
        rule_id = rule["rule_id"]
        rule_type = rule["rule_type"]
        target_col = rule["target_column"]
        params = json.loads(rule["params"]) if rule["params"] else {}
        severity = rule["severity"]
        enforcement = rule["enforcement_mode"]
        execution_id = str(uuid.uuid4())

        print(f"\n  [{rule_id}] {rule_type} on '{target_col}' ({severity}/{enforcement})")

        start_time = time.time()
        try:
            executor = EXECUTORS[rule_type]
            total, passed, failed, failed_df = executor(clean_df, target_col, params)
            duration_ms = int((time.time() - start_time) * 1000)
            dq_score = round((passed / total) * 100, 2) if total > 0 else 100.0
            status = "failed" if failed > 0 else "passed"

            icon = "PASS" if status == "passed" else "FAIL"
            print(f"    [{icon}] {passed}/{total} passed (score: {dq_score}%)")

            # Log execution via SQL INSERT (avoids type inference issues)
            executed_at = datetime.utcnow().isoformat()
            spark.sql(f"""
                INSERT INTO {CATALOG}.dq_framework.dq_execution_log VALUES (
                    '{execution_id}', '{run_id}', '{rule_id}', '{rule_type}',
                    '{dataset_name}', '{target_col}', '{severity}', '{enforcement}',
                    '{status}', {total}, {passed}, {failed}, {dq_score},
                    NULL, {duration_ms}, TIMESTAMP '{executed_at}', 'dq_framework_poc'
                )
            """)

            # Handle enforcement
            if status == "failed" and failed_df is not None:
                if enforcement == "hard":
                    clean_df = clean_df.subtract(failed_df)
                    print(f"    [HARD] Removed {failed} records")

                # Quarantine - build using DataFrame transformations (all lit() columns are typed)
                q_df = (
                    failed_df
                    .withColumn("_record_payload", to_json(struct(*failed_df.columns)))
                    .select(
                        concat_ws("-", lit("Q"), monotonically_increasing_id().cast("string")).alias("quarantine_id"),
                        lit(execution_id).cast("string").alias("execution_id"),
                        lit(run_id).cast("string").alias("run_id"),
                        lit(rule_id).cast("string").alias("rule_id"),
                        lit(rule_type).cast("string").alias("rule_type"),
                        lit(dataset_name).cast("string").alias("source_table"),
                        col("_record_payload").alias("record_payload"),
                        lit(f"Failed {rule_type} on '{target_col}'").cast("string").alias("quarantine_reason"),
                        lit(severity).cast("string").alias("severity"),
                        current_timestamp().alias("quarantined_at"),
                        lit(None).cast("timestamp").alias("resolved_at"),
                        lit(None).cast("string").alias("resolution_action"),
                    )
                )
                all_quarantine_records.append(q_df)

            if status == "passed":
                rules_passed += 1
            else:
                rules_failed += 1

        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            print(f"    [ERR] {str(e)[:200]}")
            rules_errored += 1

            # Log error via SQL INSERT
            err_msg = str(e)[:500].replace("'", "''")
            executed_at = datetime.utcnow().isoformat()
            spark.sql(f"""
                INSERT INTO {CATALOG}.dq_framework.dq_execution_log VALUES (
                    '{execution_id}', '{run_id}', '{rule_id}', '{rule_type}',
                    '{dataset_name}', '{target_col}', '{severity}', '{enforcement}',
                    'error', 0, 0, 0, 0.0,
                    '{err_msg}', {duration_ms}, TIMESTAMP '{executed_at}', 'dq_framework_poc'
                )
            """)

    # Write quarantine
    quarantined_count = 0
    if all_quarantine_records:
        combined_q = all_quarantine_records[0]
        for qdf in all_quarantine_records[1:]:
            combined_q = combined_q.unionByName(qdf)
        combined_q.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(f"{CATALOG}.dq_framework.dq_quarantine")
        quarantined_count = combined_q.count()
        print(f"\n  Quarantined: {quarantined_count} records")

    # Write Silver
    silver_df = (
        clean_df
        .withColumn("_dq_run_id", lit(run_id))
        .withColumn("_dq_processed_at", current_timestamp())
    )
    silver_count = silver_df.count()
    silver_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{CATALOG}.{silver_table}")

    # Calculate overall DQ score
    total_rules = rules_passed + rules_failed + rules_errored
    scores = spark.sql(f"""
        SELECT AVG(dq_score) as avg FROM {CATALOG}.dq_framework.dq_execution_log
        WHERE run_id = '{run_id}' AND status IN ('passed', 'failed')
    """).collect()
    overall_score = round(scores[0]["avg"], 2) if scores[0]["avg"] else 100.0

    # Persist score via SQL INSERT
    score_id = str(uuid.uuid4())
    scored_at = datetime.utcnow().isoformat()
    spark.sql(f"""
        INSERT INTO {CATALOG}.dq_framework.dq_scores VALUES (
            '{score_id}', '{run_id}', '{dataset_name}',
            {int(total_rules)}, {int(rules_passed)}, {int(rules_failed)},
            {int(rules_errored)}, {float(overall_score)},
            {int(bronze_count)}, {int(silver_count)}, {int(quarantined_count)},
            TIMESTAMP '{scored_at}'
        )
    """)

    print(f"\n  Summary: {rules_passed} passed, {rules_failed} failed, {rules_errored} errors")
    print(f"  Records: {bronze_count} in → {silver_count} out ({quarantined_count} quarantined)")
    print(f"  DQ Score: {overall_score}%")

    return {
        "dataset": dataset_name, "bronze_count": bronze_count,
        "silver_count": silver_count, "quarantined_count": quarantined_count,
        "dq_score": overall_score, "rules_passed": rules_passed,
        "rules_failed": rules_failed, "run_id": run_id,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute DQ on All Datasets

# COMMAND ----------

# Process Customers
customers_result = run_dq_engine(
    "bronze.customers", "bronze.customers", "silver.customers",
    f"{PIPELINE_RUN_ID}_customers"
)

# COMMAND ----------

# Process Transactions
transactions_result = run_dq_engine(
    "bronze.transactions", "bronze.transactions", "silver.transactions",
    f"{PIPELINE_RUN_ID}_transactions"
)

# COMMAND ----------

# Process Products
products_result = run_dq_engine(
    "bronze.products", "bronze.products", "silver.products",
    f"{PIPELINE_RUN_ID}_products"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze → Silver Summary

# COMMAND ----------

all_results = [customers_result, transactions_result, products_result]

print("=" * 75)
print("BRONZE → SILVER PIPELINE RESULTS")
print("=" * 75)
print(f"{'Dataset':<28} {'Bronze':>8} {'Silver':>8} {'Quarantine':>12} {'DQ Score':>10}")
print("-" * 75)
for r in all_results:
    print(f"{r['dataset']:<28} {r['bronze_count']:>8} {r['silver_count']:>8} "
          f"{r['quarantined_count']:>12} {r['dq_score']:>9.1f}%")
print("-" * 75)
avg_score = sum(r["dq_score"] for r in all_results) / len(all_results)
print(f"{'OVERALL':<28} {'':>8} {'':>8} {'':>12} {avg_score:>9.1f}%")
print("=" * 75)

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Silver Tables

# COMMAND ----------

print("=== Silver Customers ===")
display(spark.table(f"{CATALOG}.silver.customers"))

# COMMAND ----------

print("=== Silver Transactions ===")
display(spark.table(f"{CATALOG}.silver.transactions"))

# COMMAND ----------

print("=== Silver Products ===")
display(spark.table(f"{CATALOG}.silver.products"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Quarantined Records

# COMMAND ----------

print("=== Quarantined Records ===")
display(spark.sql(f"""
    SELECT source_table, rule_id, rule_type, severity, quarantine_reason,
           record_payload, quarantined_at
    FROM {CATALOG}.dq_framework.dq_quarantine ORDER BY source_table, rule_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Build Gold Layer

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

# Customer 360
customers = spark.table(f"{CATALOG}.silver.customers")
transactions = spark.table(f"{CATALOG}.silver.transactions")

txn_metrics = transactions.groupBy("customer_id").agg(
    count("*").alias("total_transactions"),
    spark_sum("amount").alias("total_spend"),
    avg("amount").alias("avg_transaction_amount"),
    spark_max("transaction_date").alias("last_transaction_date"),
)

customer_360 = (
    customers.join(txn_metrics, on="customer_id", how="left")
    .select(
        "customer_id", "customer_name", "email", "age", "status", "country_code",
        "total_transactions",
        spark_round(col("total_spend"), 2).alias("total_spend"),
        spark_round(col("avg_transaction_amount"), 2).alias("avg_transaction_amount"),
        "last_transaction_date",
        when(col("total_transactions") >= 5, "high")
        .when(col("total_transactions") >= 2, "medium")
        .otherwise("low").alias("engagement_tier"),
        current_timestamp().alias("_gold_processed_at"),
    )
)
customer_360.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{CATALOG}.gold.customer_360")
print(f"gold.customer_360: {spark.table(f'{CATALOG}.gold.customer_360').count()} rows")

# DQ Executive Summary
dq_summary = spark.table(f"{CATALOG}.dq_framework.dq_scores").withColumn(
    "_gold_processed_at", current_timestamp()
)
dq_summary.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{CATALOG}.gold.dq_executive_summary")
print(f"gold.dq_executive_summary: {spark.table(f'{CATALOG}.gold.dq_executive_summary').count()} rows")

# COMMAND ----------

print("=== Customer 360 (Gold) ===")
display(spark.table(f"{CATALOG}.gold.customer_360"))

# COMMAND ----------

print("=== DQ Executive Summary (Gold) ===")
display(spark.table(f"{CATALOG}.gold.dq_executive_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: DQ Reports & Dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Executive Scorecard

# COMMAND ----------

display(spark.sql(f"""
    SELECT dataset_name, overall_dq_score, total_rules, rules_passed, rules_failed,
           total_records_in, total_records_out, records_quarantined,
           ROUND((total_records_out / total_records_in) * 100, 1) AS record_pass_pct
    FROM {CATALOG}.dq_framework.dq_scores ORDER BY overall_dq_score
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Rule Execution Detail

# COMMAND ----------

display(spark.sql(f"""
    SELECT dataset_name, rule_id, rule_type, target_column, severity, enforcement_mode,
           status, total_records, passed_count, failed_count, dq_score,
           CASE WHEN dq_score >= 95 THEN 'HEALTHY'
                WHEN dq_score >= 80 THEN 'WARNING' ELSE 'CRITICAL' END AS health
    FROM {CATALOG}.dq_framework.dq_execution_log ORDER BY dq_score
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 DQ Heatmap (Dataset x Rule Type)

# COMMAND ----------

display(spark.sql(f"""
    SELECT dataset_name, rule_type, ROUND(AVG(dq_score), 1) as avg_score,
           CASE WHEN AVG(dq_score) >= 95 THEN 'GREEN'
                WHEN AVG(dq_score) >= 80 THEN 'AMBER' ELSE 'RED' END as health
    FROM {CATALOG}.dq_framework.dq_execution_log
    GROUP BY dataset_name, rule_type ORDER BY dataset_name, rule_type
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Top Failing Rules

# COMMAND ----------

display(spark.sql(f"""
    SELECT rule_id, rule_type, dataset_name, target_column, severity, dq_score,
           failed_count, ROUND((failed_count * 100.0 / total_records), 1) as failure_rate_pct
    FROM {CATALOG}.dq_framework.dq_execution_log
    WHERE status = 'failed' ORDER BY dq_score
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 Overall Framework Health

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'Total Datasets' as metric, CAST(COUNT(DISTINCT dataset_name) AS STRING) as value
    FROM {CATALOG}.dq_framework.dq_execution_log
    UNION ALL SELECT 'Total Rules Executed', CAST(COUNT(*) AS STRING)
    FROM {CATALOG}.dq_framework.dq_execution_log
    UNION ALL SELECT 'Rules Passed', CAST(SUM(CASE WHEN status='passed' THEN 1 ELSE 0 END) AS STRING)
    FROM {CATALOG}.dq_framework.dq_execution_log
    UNION ALL SELECT 'Rules Failed', CAST(SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS STRING)
    FROM {CATALOG}.dq_framework.dq_execution_log
    UNION ALL SELECT 'Records Quarantined', CAST(COUNT(*) AS STRING)
    FROM {CATALOG}.dq_framework.dq_quarantine
    UNION ALL SELECT 'Avg DQ Score', ROUND(AVG(overall_dq_score), 1) || '%'
    FROM {CATALOG}.dq_framework.dq_scores
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Feedback Loop (Mock Collibra Writeback)

# COMMAND ----------

# Generate what would be pushed to Collibra
feedback_data = spark.sql(f"""
    SELECT rule_id, dataset_name, status, dq_score, failed_count, total_records, severity
    FROM {CATALOG}.dq_framework.dq_execution_log
""").collect()

print(f"MOCK COLLIBRA WRITEBACK: {len(feedback_data)} rule status updates")
print(f"\nSample payload (what would be sent to Collibra REST API):")
if feedback_data:
    sample = feedback_data[0].asDict()
    payload = {
        "collibra_asset_id": sample["rule_id"],
        "attributes": {
            "Last Execution Status": sample["status"],
            "Last DQ Score": sample["dq_score"],
            "Last Failed Count": sample["failed_count"],
            "SLA Violation": sample["dq_score"] < 80 and sample["severity"] == "error",
        }
    }
    print(json.dumps(payload, indent=2, default=str))

sla_violations = sum(1 for r in feedback_data if r["dq_score"] < 80 and r["severity"] == "error")
print(f"\nSLA Violations detected: {sla_violations}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Complete!

# COMMAND ----------

print(f"\n{'='*70}")
print(f"DQ FRAMEWORK POC - COMPLETE")
print(f"{'='*70}")
print(f"Run ID:             {PIPELINE_RUN_ID}")
print(f"Completed:          {datetime.utcnow().isoformat()}")
print(f"Datasets:           {len(all_results)}")
print(f"Avg DQ Score:       {avg_score:.1f}%")
print(f"SLA Violations:     {sla_violations}")
print(f"{'='*70}")
print(f"\nTables in Unity Catalog ({CATALOG}):")
print(f"  Bronze: customers, transactions, products")
print(f"  Silver: customers, transactions, products, dim_country")
print(f"  Gold:   customer_360, dq_executive_summary")
print(f"  DQ:     dq_rules, dq_rule_mapping, dq_execution_log, dq_quarantine, dq_scores")
print(f"\nNotebook URL for interactive exploration:")
print(f"  https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485")
