# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Extract Rules from Collibra (Mock)
# MAGIC Extracts DQ rules and mappings from mock Collibra API responses,
# MAGIC validates and transforms them, and loads into Delta metadata tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import sys
import os
import json

# Add project root to path for imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# For Databricks notebooks using Repos:
# sys.path.insert(0, "/Workspace/Repos/<user>/Quality_Framework")

CATALOG = "dq_poc"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Configuration

# COMMAND ----------

import yaml

config_path = os.path.join(project_root, "configs", "pipeline_config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

print("Configuration loaded:")
print(json.dumps(config["collibra"], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract Rules from Collibra (Mock)

# COMMAND ----------

from extract.collibra_api_client import CollibraAPIClient

client = CollibraAPIClient(config, spark=spark)

# Extract rules
raw_rules = client.extract_rules()
print(f"\nExtracted {len(raw_rules)} rules")
print(f"Rule types: {set(r['rule_type'] for r in raw_rules)}")
print(f"Severities: {set(r['severity'] for r in raw_rules)}")

# Extract mappings
raw_mappings = client.extract_rule_mappings()
print(f"\nExtracted {len(raw_mappings)} rule-dataset mappings")
print(f"Datasets: {set(m['dataset_name'] for m in raw_mappings)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transform and Validate Rules

# COMMAND ----------

from extract.rule_transformer import RuleTransformer

# Load JSON schema for validation
schema_path = os.path.join(project_root, "configs", "dq_rules_schema.json")
with open(schema_path, "r") as f:
    rule_schema = json.load(f)

transformer = RuleTransformer(schema=rule_schema)

# Transform rules
valid_rules, invalid_rules = transformer.transform_rules(raw_rules)

if invalid_rules:
    print(f"\nWARNING: {len(invalid_rules)} rules failed validation:")
    for err in transformer.validation_errors:
        print(f"  - {err['rule_id']}: {err['error']}")

# Transform mappings
transformed_mappings = transformer.transform_mappings(raw_mappings)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Preview Transformed Rules

# COMMAND ----------

rules_preview_df = spark.createDataFrame(valid_rules)
print(f"Valid rules to load: {rules_preview_df.count()}")
rules_preview_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load into Delta Tables

# COMMAND ----------

from extract.rule_loader import RuleLoader

loader = RuleLoader(spark, catalog=CATALOG, schema="dq_framework")

# Load rules (MERGE/upsert)
rules_count = loader.load_rules(valid_rules)
print(f"Rules table: {rules_count} total rows")

# Load mappings (MERGE/upsert)
mappings_count = loader.load_mappings(transformed_mappings)
print(f"Mappings table: {mappings_count} total rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Loaded Data

# COMMAND ----------

print("=== DQ Rules ===")
spark.table(f"{CATALOG}.dq_framework.dq_rules").display()

# COMMAND ----------

print("=== DQ Rule Mappings ===")
spark.table(f"{CATALOG}.dq_framework.dq_rule_mapping").display()

# COMMAND ----------

# Summary statistics
print("=== Rules by Type ===")
spark.sql(f"""
    SELECT rule_type, severity, COUNT(*) as count
    FROM {CATALOG}.dq_framework.dq_rules
    WHERE is_active = true
    GROUP BY rule_type, severity
    ORDER BY rule_type
""").display()

# COMMAND ----------

print("=== Rules by Dataset ===")
spark.sql(f"""
    SELECT m.dataset_name, m.enforcement_mode, COUNT(*) as rule_count
    FROM {CATALOG}.dq_framework.dq_rule_mapping m
    WHERE m.is_active = true
    GROUP BY m.dataset_name, m.enforcement_mode
    ORDER BY m.dataset_name
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extraction Metadata

# COMMAND ----------

metadata = client.get_extraction_metadata()
print(json.dumps(metadata, indent=2))
print("\nRule extraction complete! Proceed to notebook 02_execute_dq.py")
