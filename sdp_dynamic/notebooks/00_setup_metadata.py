# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup Dynamic DQ Metadata Tables
# MAGIC
# MAGIC Creates and populates the metadata-driven DQ framework tables:
# MAGIC 1. `dq_framework.dq_datasets` — Registry of all datasets to process
# MAGIC 2. `dq_framework.dq_rules` — Rule definitions (unchanged)
# MAGIC 3. `dq_framework.dq_rule_mapping` — Rule-to-dataset bindings using **dataset_id**
# MAGIC
# MAGIC **Key change from v1:** Mappings reference `dataset_id` (foreign key) instead of `dataset_name` string.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import json
import hashlib
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

CATALOG = "dq_poc"
SCHEMA = "dq_framework"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Schema ready: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create & Populate dq_datasets Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.dq_datasets (
        dataset_id STRING NOT NULL,
        dataset_name STRING NOT NULL,
        source_table STRING NOT NULL,
        primary_key STRING NOT NULL,
        bronze_dlt_table STRING,
        ref_tables STRING,
        is_active BOOLEAN,
        description STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Registry of all datasets processed by the DQ framework'
""")

DATASETS_SCHEMA = StructType([
    StructField("dataset_id", StringType(), False),
    StructField("dataset_name", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("primary_key", StringType(), False),
    StructField("bronze_dlt_table", StringType(), True),
    StructField("ref_tables", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("description", StringType(), True),
])

# ref_tables is JSON: maps "column_ref" -> {"dlt_table": "...", "ref_column": "..."}
datasets = [
    ("DS-001", "customers", "dq_poc.bronze.customers", "customer_id",
     "customers_bronze",
     json.dumps({"country_code_ref": {"dlt_table": "ref_dim_country", "ref_column": "country_code"}}),
     True, "Customer master data"),
    ("DS-002", "transactions", "dq_poc.bronze.transactions", "transaction_id",
     "transactions_bronze",
     json.dumps({"customer_id_ref": {"dlt_table": "customers_bronze", "ref_column": "customer_id"}}),
     True, "Financial transactions"),
    ("DS-003", "products", "dq_poc.bronze.products", "product_id",
     "products_bronze",
     None,
     True, "Product catalog"),
]

from pyspark.sql import functions as F

datasets_df = spark.createDataFrame(datasets, schema=DATASETS_SCHEMA)
datasets_df = (
    datasets_df
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
)
datasets_df.createOrReplaceTempView("__staging_datasets")

spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.dq_datasets AS target
    USING __staging_datasets AS source
    ON target.dataset_id = source.dataset_id
    WHEN MATCHED THEN UPDATE SET
        target.dataset_name = source.dataset_name,
        target.source_table = source.source_table,
        target.primary_key = source.primary_key,
        target.bronze_dlt_table = source.bronze_dlt_table,
        target.ref_tables = source.ref_tables,
        target.is_active = source.is_active,
        target.description = source.description,
        target.updated_at = source.updated_at
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"dq_datasets: {spark.table(f'{CATALOG}.{SCHEMA}.dq_datasets').count()} rows")
display(spark.table(f"{CATALOG}.{SCHEMA}.dq_datasets"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create & Populate dq_rules Table (unchanged schema)

# COMMAND ----------

import os

WS_PATHS = [
    "/Workspace/Users/pritam.paul@databricks.com/Quality_Framework",
    "/Workspace/Repos/pritam.paul@databricks.com/Quality_Framework",
]
BASE_PATH = None
for wp in WS_PATHS:
    if os.path.exists(os.path.join(wp, "mock_collibra", "dq_rules.json")):
        BASE_PATH = wp
        break

with open(os.path.join(BASE_PATH, "mock_collibra", "dq_rules.json")) as f:
    raw_rules = json.load(f)["rules"]

RULES_SCHEMA = StructType([
    StructField("rule_id", StringType(), False),
    StructField("rule_name", StringType(), True),
    StructField("rule_type", StringType(), False),
    StructField("target_column", StringType(), True),
    StructField("params", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("version", IntegerType(), True),
    StructField("effective_from", StringType(), True),
    StructField("effective_to", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("checksum", StringType(), True),
])

now_str = datetime.now().isoformat()
rules_rows = []
for r in raw_rules:
    params_json = json.dumps(r.get("params", {}))
    checksum = hashlib.sha256(json.dumps(r, sort_keys=True).encode()).hexdigest()[:16]
    rules_rows.append((
        r["rule_id"], r["rule_name"], r["rule_type"], r["column"],
        params_json, r["severity"], r.get("is_active", True),
        r.get("version", 1), r.get("effective_from", ""), r.get("effective_to", ""),
        "collibra_mock", now_str, now_str, checksum,
    ))

rules_df = spark.createDataFrame(rules_rows, schema=RULES_SCHEMA)
rules_df.createOrReplaceTempView("__staging_rules")

spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.dq_rules AS target
    USING __staging_rules AS source
    ON target.rule_id = source.rule_id
    WHEN MATCHED THEN UPDATE SET
        target.rule_name = source.rule_name, target.rule_type = source.rule_type,
        target.target_column = source.target_column, target.params = source.params,
        target.severity = source.severity, target.is_active = source.is_active,
        target.version = source.version, target.updated_at = source.updated_at,
        target.checksum = source.checksum
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"dq_rules: {spark.table(f'{CATALOG}.{SCHEMA}.dq_rules').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create & Populate dq_rule_mapping with dataset_id

# COMMAND ----------

# New mapping table uses dataset_id instead of dataset_name
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.dq_dataset_rule_mapping (
        mapping_id STRING NOT NULL,
        dataset_id STRING NOT NULL,
        column_name STRING,
        rule_id STRING NOT NULL,
        enforcement_mode STRING,
        priority INT,
        is_active BOOLEAN
    )
    USING DELTA
    COMMENT 'Maps rules to datasets using dataset_id (FK to dq_datasets)'
""")

# Map old dataset_name -> dataset_id
DS_NAME_TO_ID = {
    "bronze.customers": "DS-001",
    "bronze.transactions": "DS-002",
    "bronze.products": "DS-003",
}

with open(os.path.join(BASE_PATH, "mock_collibra", "dq_rule_mappings.json")) as f:
    raw_mappings = json.load(f)["mappings"]

MAPPING_SCHEMA = StructType([
    StructField("mapping_id", StringType(), False),
    StructField("dataset_id", StringType(), False),
    StructField("column_name", StringType(), True),
    StructField("rule_id", StringType(), False),
    StructField("enforcement_mode", StringType(), True),
    StructField("priority", IntegerType(), True),
    StructField("is_active", BooleanType(), True),
])

mapping_rows = []
for m in raw_mappings:
    ds_id = DS_NAME_TO_ID.get(m["dataset_name"], "UNKNOWN")
    mapping_rows.append((
        m["mapping_id"], ds_id, m.get("column_name", ""),
        m["rule_id"], m["enforcement_mode"],
        m.get("priority", 999), m.get("is_active", True),
    ))

mappings_df = spark.createDataFrame(mapping_rows, schema=MAPPING_SCHEMA)
mappings_df.createOrReplaceTempView("__staging_ds_mappings")

spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.dq_dataset_rule_mapping AS target
    USING __staging_ds_mappings AS source
    ON target.mapping_id = source.mapping_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"dq_dataset_rule_mapping: {spark.table(f'{CATALOG}.{SCHEMA}.dq_dataset_rule_mapping').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verification

# COMMAND ----------

print("=== Datasets ===")
display(spark.sql(f"SELECT dataset_id, dataset_name, source_table, primary_key, is_active FROM {CATALOG}.{SCHEMA}.dq_datasets"))

print("\n=== Rules per Dataset (via dataset_id join) ===")
display(spark.sql(f"""
    SELECT d.dataset_id, d.dataset_name, m.rule_id, r.rule_type, m.enforcement_mode, m.priority
    FROM {CATALOG}.{SCHEMA}.dq_dataset_rule_mapping m
    JOIN {CATALOG}.{SCHEMA}.dq_datasets d ON m.dataset_id = d.dataset_id
    JOIN {CATALOG}.{SCHEMA}.dq_rules r ON m.rule_id = r.rule_id
    WHERE m.is_active AND d.is_active AND r.is_active
    ORDER BY d.dataset_id, m.priority
"""))
