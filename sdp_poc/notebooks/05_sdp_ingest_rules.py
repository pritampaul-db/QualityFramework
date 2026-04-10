# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Ingest DQ Rules from Collibra JSON to Delta Tables
# MAGIC
# MAGIC Reads DQ rules and mappings from `mock_collibra/` JSON files and
# MAGIC upserts them into Delta metadata tables using MERGE.
# MAGIC
# MAGIC **Target tables:**
# MAGIC - `dq_poc.dq_framework.dq_rules` — 20 rule definitions
# MAGIC - `dq_poc.dq_framework.dq_rule_mapping` — 20 dataset-rule associations
# MAGIC
# MAGIC **This notebook is Task 1 in the Lakeflow daily workflow.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import json
import os
from datetime import datetime

CATALOG = "dq_poc"
RULES_SCHEMA = "dq_framework"

# Resolve workspace path to mock_collibra JSON files
WS_PATHS = [
    "/Workspace/Users/pritam.paul@databricks.com/Quality_Framework",
    "/Workspace/Repos/pritam.paul@databricks.com/Quality_Framework",
]
BASE_PATH = None
for wp in WS_PATHS:
    if os.path.exists(os.path.join(wp, "mock_collibra", "dq_rules.json")):
        BASE_PATH = wp
        break

if BASE_PATH is None:
    raise FileNotFoundError("Cannot locate mock_collibra/dq_rules.json in workspace")

RULES_FILE = os.path.join(BASE_PATH, "mock_collibra", "dq_rules.json")
MAPPINGS_FILE = os.path.join(BASE_PATH, "mock_collibra", "dq_rule_mappings.json")

print(f"Rules file:    {RULES_FILE}")
print(f"Mappings file: {MAPPINGS_FILE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ensure Catalog and Schema Exist

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{RULES_SCHEMA}")
print(f"Schema ready: {CATALOG}.{RULES_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Rules from JSON

# COMMAND ----------

with open(RULES_FILE, "r") as f:
    rules_data = json.load(f)
raw_rules = rules_data["rules"]
print(f"Loaded {len(raw_rules)} rules from JSON")

with open(MAPPINGS_FILE, "r") as f:
    mappings_data = json.load(f)
raw_mappings = mappings_data["mappings"]
print(f"Loaded {len(raw_mappings)} mappings from JSON")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare Rules for Delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

# Schema matching the existing dq_poc.dq_framework.dq_rules Delta table
RULES_DF_SCHEMA = StructType([
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

import hashlib

rules_for_delta = []
now_str = datetime.now().isoformat()
for r in raw_rules:
    params_json = json.dumps(r.get("params", {}))
    checksum = hashlib.sha256(json.dumps(r, sort_keys=True).encode()).hexdigest()[:16]
    rules_for_delta.append((
        r["rule_id"],
        r["rule_name"],
        r["rule_type"],
        r["column"],
        params_json,
        r["severity"],
        r.get("is_active", True),
        r.get("version", 1),
        r.get("effective_from", ""),
        r.get("effective_to", ""),
        "collibra_mock",
        now_str,
        now_str,
        checksum,
    ))

rules_df = spark.createDataFrame(rules_for_delta, schema=RULES_DF_SCHEMA)

print(f"Rules DataFrame: {rules_df.count()} rows")
display(rules_df.select("rule_id", "rule_name", "rule_type", "target_column", "severity", "params"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Prepare Mappings for Delta

# COMMAND ----------

# Schema matching the existing dq_poc.dq_framework.dq_rule_mapping Delta table
MAPPINGS_DF_SCHEMA = StructType([
    StructField("mapping_id", StringType(), False),
    StructField("dataset_name", StringType(), False),
    StructField("column_name", StringType(), True),
    StructField("rule_id", StringType(), False),
    StructField("enforcement_mode", StringType(), True),
    StructField("priority", IntegerType(), True),
    StructField("is_active", BooleanType(), True),
])

mappings_for_delta = []
for m in raw_mappings:
    mappings_for_delta.append((
        m["mapping_id"],
        m["dataset_name"],
        m.get("column_name", ""),
        m["rule_id"],
        m["enforcement_mode"],
        m.get("priority", 999),
        m.get("is_active", True),
    ))

mappings_df = spark.createDataFrame(mappings_for_delta, schema=MAPPINGS_DF_SCHEMA)

print(f"Mappings DataFrame: {mappings_df.count()} rows")
display(mappings_df.select("mapping_id", "dataset_name", "rule_id", "enforcement_mode", "priority"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. MERGE Rules into Delta Table

# COMMAND ----------

RULES_TABLE = f"{CATALOG}.{RULES_SCHEMA}.dq_rules"

# Create table if not exists (matches existing schema)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {RULES_TABLE} (
        rule_id STRING,
        rule_name STRING,
        rule_type STRING,
        target_column STRING,
        params STRING,
        severity STRING,
        is_active BOOLEAN,
        version INT,
        effective_from STRING,
        effective_to STRING,
        source_system STRING,
        created_at STRING,
        updated_at STRING,
        checksum STRING
    )
    USING DELTA
""")

# Write to temp view for MERGE
rules_df.createOrReplaceTempView("__staging_rules")

merge_result = spark.sql(f"""
    MERGE INTO {RULES_TABLE} AS target
    USING __staging_rules AS source
    ON target.rule_id = source.rule_id
    WHEN MATCHED THEN UPDATE SET
        target.rule_name = source.rule_name,
        target.rule_type = source.rule_type,
        target.target_column = source.target_column,
        target.params = source.params,
        target.severity = source.severity,
        target.is_active = source.is_active,
        target.version = source.version,
        target.effective_from = source.effective_from,
        target.effective_to = source.effective_to,
        target.source_system = source.source_system,
        target.updated_at = source.updated_at,
        target.checksum = source.checksum
    WHEN NOT MATCHED THEN INSERT *
""")

rules_count = spark.table(RULES_TABLE).count()
print(f"Rules table ({RULES_TABLE}): {rules_count} rows after MERGE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. MERGE Mappings into Delta Table

# COMMAND ----------

MAPPINGS_TABLE = f"{CATALOG}.{RULES_SCHEMA}.dq_rule_mapping"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MAPPINGS_TABLE} (
        mapping_id STRING,
        dataset_name STRING,
        column_name STRING,
        rule_id STRING,
        enforcement_mode STRING,
        priority INT,
        is_active BOOLEAN
    )
    USING DELTA
""")

mappings_df.createOrReplaceTempView("__staging_mappings")

spark.sql(f"""
    MERGE INTO {MAPPINGS_TABLE} AS target
    USING __staging_mappings AS source
    ON target.mapping_id = source.mapping_id
    WHEN MATCHED THEN UPDATE SET
        target.dataset_name = source.dataset_name,
        target.column_name = source.column_name,
        target.rule_id = source.rule_id,
        target.enforcement_mode = source.enforcement_mode,
        target.priority = source.priority,
        target.is_active = source.is_active
    WHEN NOT MATCHED THEN INSERT *
""")

mappings_count = spark.table(MAPPINGS_TABLE).count()
print(f"Mappings table ({MAPPINGS_TABLE}): {mappings_count} rows after MERGE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verification Summary

# COMMAND ----------

print("=" * 60)
print("  DQ Rule Ingestion Complete")
print("=" * 60)
print(f"  Rules:    {rules_count} rows in {RULES_TABLE}")
print(f"  Mappings: {mappings_count} rows in {MAPPINGS_TABLE}")
print(f"  Source:   {RULES_FILE}")
print(f"  Time:     {datetime.now().isoformat()}")
print("=" * 60)

# Summary by type
print("\nRules by type:")
spark.sql(f"""
    SELECT rule_type, severity, COUNT(*) as count
    FROM {RULES_TABLE}
    WHERE is_active = true
    GROUP BY rule_type, severity
    ORDER BY rule_type
""").display()

print("\nRules by dataset:")
spark.sql(f"""
    SELECT m.dataset_name, m.enforcement_mode, COUNT(*) as rule_count
    FROM {MAPPINGS_TABLE} m
    WHERE m.is_active = true
    GROUP BY m.dataset_name, m.enforcement_mode
    ORDER BY m.dataset_name
""").display()
