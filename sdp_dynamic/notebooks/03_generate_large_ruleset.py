# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Generate Large Rule Set for Performance Testing
# MAGIC
# MAGIC Generates a configurable number of DQ rules per dataset to stress-test
# MAGIC the pipeline with a large rule set.
# MAGIC
# MAGIC **Default: 50 rules per dataset = 150 total rules**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "pritam_demo_workspace_catalog"
RULES_PER_DATASET = 50  # Total rules = this × 3 datasets

print(f"Generating {RULES_PER_DATASET} rules per dataset ({RULES_PER_DATASET * 3} total)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Column Metadata per Dataset

# COMMAND ----------

# Columns available in each dataset and their types
DATASET_COLUMNS = {
    "DS-001": {  # customers
        "dataset_name": "customers",
        "string_cols": ["customer_id", "customer_name", "email", "phone", "status", "country_code"],
        "numeric_cols": ["age"],
        "date_cols": ["created_at"],
        "pk": "customer_id",
    },
    "DS-002": {  # transactions
        "dataset_name": "transactions",
        "string_cols": ["transaction_id", "customer_id", "transaction_type", "currency", "merchant", "transaction_date"],
        "numeric_cols": ["amount", "quantity"],
        "date_cols": ["transaction_date"],
        "pk": "transaction_id",
    },
    "DS-003": {  # products
        "dataset_name": "products",
        "string_cols": ["product_id", "product_name", "sku", "category", "supplier"],
        "numeric_cols": ["price", "stock_quantity"],
        "date_cols": ["created_at"],
        "pk": "product_id",
    },
}

# Rule templates: each generates a specific rule_type with params
RULE_TEMPLATES = [
    # not_null on every string column
    {"type": "not_null", "target": "string", "params": {}, "severity": "error", "enforcement": "hard"},
    # not_null on every numeric column
    {"type": "not_null", "target": "numeric", "params": {}, "severity": "error", "enforcement": "hard"},
    # range checks on numeric columns with various thresholds
    {"type": "range", "target": "numeric", "params": {"min": 0, "max": 999999}, "severity": "error", "enforcement": "hard"},
    {"type": "range", "target": "numeric", "params": {"min": -100, "max": 100000}, "severity": "warning", "enforcement": "soft"},
    {"type": "range", "target": "numeric", "params": {"min": 1, "max": 50000}, "severity": "warning", "enforcement": "soft"},
    # length checks on string columns
    {"type": "length", "target": "string", "params": {"min_length": 1, "max_length": 200}, "severity": "warning", "enforcement": "soft"},
    {"type": "length", "target": "string", "params": {"min_length": 2, "max_length": 500}, "severity": "warning", "enforcement": "soft"},
    {"type": "length", "target": "string", "params": {"min_length": 1, "max_length": 100}, "severity": "info", "enforcement": "soft"},
    # regex on string columns
    {"type": "regex", "target": "string", "params": {"pattern": "^.{1,}$"}, "severity": "warning", "enforcement": "soft"},
    {"type": "regex", "target": "string", "params": {"pattern": "^[^\\s].*[^\\s]$"}, "severity": "warning", "enforcement": "soft"},
    # custom_sql on numeric columns
    {"type": "custom_sql", "target": "numeric", "params_template": "{col} >= 0", "severity": "warning", "enforcement": "soft"},
    {"type": "custom_sql", "target": "numeric", "params_template": "{col} IS NOT NULL AND {col} < 10000000", "severity": "warning", "enforcement": "soft"},
    {"type": "custom_sql", "target": "numeric", "params_template": "CAST({col} AS DOUBLE) = {col}", "severity": "info", "enforcement": "soft"},
    # allowed_values on specific columns (will be applied where sensible)
    {"type": "allowed_values", "target": "string", "params": {"values": ["valid", "invalid", "pending", "active", "inactive", "unknown", "new", "old"]}, "severity": "warning", "enforcement": "soft"},
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Rules and Mappings

# COMMAND ----------

import json
import hashlib
from datetime import datetime

all_rules = []
all_mappings = []
rule_counter = 100  # Start from DQR-000100 to avoid conflicts with existing 20 rules
mapping_counter = 100

now_str = datetime.now().isoformat()

for ds_id, ds_info in DATASET_COLUMNS.items():
    ds_name = ds_info["dataset_name"]
    string_cols = ds_info["string_cols"]
    numeric_cols = ds_info["numeric_cols"]
    rules_generated = 0

    # Keep generating rules until we hit the target
    template_idx = 0
    col_idx = 0

    while rules_generated < RULES_PER_DATASET:
        template = RULE_TEMPLATES[template_idx % len(RULE_TEMPLATES)]
        rule_type = template["type"]

        # Pick target column based on template target type
        if template["target"] == "string":
            if not string_cols:
                template_idx += 1
                continue
            col = string_cols[col_idx % len(string_cols)]
        elif template["target"] == "numeric":
            if not numeric_cols:
                template_idx += 1
                continue
            col = numeric_cols[col_idx % len(numeric_cols)]
        elif template["target"] == "date":
            if not ds_info.get("date_cols"):
                template_idx += 1
                continue
            col = ds_info["date_cols"][col_idx % len(ds_info["date_cols"])]
        else:
            col = string_cols[0]

        rule_counter += 1
        mapping_counter += 1
        rule_id = f"DQR-{rule_counter:06d}"
        mapping_id = f"MAP-{mapping_counter:03d}"

        # Build params
        if "params_template" in template:
            params = {"sql_expression": template["params_template"].format(col=col)}
        else:
            params = dict(template.get("params", {}))

        rule_name = f"{ds_name}_{col}_{rule_type}_{rules_generated}"
        checksum = hashlib.sha256(f"{rule_id}{rule_name}".encode()).hexdigest()[:16]

        all_rules.append((
            rule_id, rule_name, rule_type, col,
            json.dumps(params), template["severity"],
            True, 1, "2026-01-01", "",
            "synthetic_generator", now_str, now_str, checksum,
        ))

        all_mappings.append((
            mapping_id, ds_id, col, rule_id,
            template["enforcement"], rules_generated + 1, True,
        ))

        rules_generated += 1
        template_idx += 1
        col_idx += 1

    print(f"  {ds_name}: {rules_generated} rules generated")

print(f"\nTotal: {len(all_rules)} rules, {len(all_mappings)} mappings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Tables

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

# Rules
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

rules_df = spark.createDataFrame(all_rules, schema=RULES_SCHEMA)

# INSERT (append to existing rules, don't overwrite the original 20)
rules_df.createOrReplaceTempView("__staging_new_rules")
spark.sql(f"""
    MERGE INTO {CATALOG}.dq_framework.dq_rules AS target
    USING __staging_new_rules AS source
    ON target.rule_id = source.rule_id
    WHEN MATCHED THEN UPDATE SET
        target.rule_name = source.rule_name, target.rule_type = source.rule_type,
        target.target_column = source.target_column, target.params = source.params,
        target.severity = source.severity, target.is_active = source.is_active,
        target.version = source.version, target.updated_at = source.updated_at,
        target.checksum = source.checksum
    WHEN NOT MATCHED THEN INSERT *
""")

total_rules = spark.table(f"{CATALOG}.dq_framework.dq_rules").count()
print(f"dq_rules: {total_rules} total rows (20 original + {len(all_rules)} generated)")

# Mappings
MAPPING_SCHEMA = StructType([
    StructField("mapping_id", StringType(), False),
    StructField("dataset_id", StringType(), False),
    StructField("column_name", StringType(), True),
    StructField("rule_id", StringType(), False),
    StructField("enforcement_mode", StringType(), True),
    StructField("priority", IntegerType(), True),
    StructField("is_active", BooleanType(), True),
])

mappings_df = spark.createDataFrame(all_mappings, schema=MAPPING_SCHEMA)
mappings_df.createOrReplaceTempView("__staging_new_mappings")
spark.sql(f"""
    MERGE INTO {CATALOG}.dq_framework.dq_dataset_rule_mapping AS target
    USING __staging_new_mappings AS source
    ON target.mapping_id = source.mapping_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

total_mappings = spark.table(f"{CATALOG}.dq_framework.dq_dataset_rule_mapping").count()
print(f"dq_dataset_rule_mapping: {total_mappings} total rows (20 original + {len(all_mappings)} generated)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

print("=== Rules by Dataset ===")
display(spark.sql(f"""
    SELECT d.dataset_name, COUNT(DISTINCT m.rule_id) as total_rules,
           SUM(CASE WHEN r.rule_type = 'not_null' THEN 1 ELSE 0 END) as not_null,
           SUM(CASE WHEN r.rule_type = 'range' THEN 1 ELSE 0 END) as range_rules,
           SUM(CASE WHEN r.rule_type = 'regex' THEN 1 ELSE 0 END) as regex_rules,
           SUM(CASE WHEN r.rule_type = 'length' THEN 1 ELSE 0 END) as length_rules,
           SUM(CASE WHEN r.rule_type = 'custom_sql' THEN 1 ELSE 0 END) as custom_sql,
           SUM(CASE WHEN r.rule_type = 'allowed_values' THEN 1 ELSE 0 END) as allowed_vals,
           SUM(CASE WHEN r.rule_type IN ('uniqueness', 'referential_integrity', 'freshness') THEN 1 ELSE 0 END) as other
    FROM {CATALOG}.dq_framework.dq_dataset_rule_mapping m
    JOIN {CATALOG}.dq_framework.dq_datasets d ON m.dataset_id = d.dataset_id
    JOIN {CATALOG}.dq_framework.dq_rules r ON m.rule_id = r.rule_id
    WHERE m.is_active AND d.is_active AND r.is_active
    GROUP BY d.dataset_name
    ORDER BY d.dataset_name
"""))

print(f"\nTotal active rules: {total_rules}")
print(f"Total active mappings: {total_mappings}")
print(f"Rules per dataset: ~{RULES_PER_DATASET + 7}")
print("\nReady for performance test — run the DQ pipeline!")
