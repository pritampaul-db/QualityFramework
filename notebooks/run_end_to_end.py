# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End DQ Framework Orchestrator
# MAGIC
# MAGIC Runs the complete DQ framework pipeline in sequence:
# MAGIC 1. Setup (catalog, schemas, Bronze data)
# MAGIC 2. Extract rules from Collibra (mock)
# MAGIC 3. Execute DQ rules (Bronze → Silver)
# MAGIC 4. Build Gold layer
# MAGIC 5. Generate reports
# MAGIC 6. Push feedback to Collibra (mock)
# MAGIC
# MAGIC **This is the single notebook to run for the full POC demo.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
import os
import json
import uuid
from datetime import datetime

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

CATALOG = "dq_poc"
PIPELINE_RUN_ID = str(uuid.uuid4())

print(f"DQ Framework POC - End-to-End Run")
print(f"Run ID: {PIPELINE_RUN_ID}")
print(f"Started: {datetime.utcnow().isoformat()}")
print(f"Catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Catalog, Schemas, Load Bronze Data

# COMMAND ----------

# Create catalog and schemas
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
for schema in ["bronze", "silver", "gold", "dq_framework"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# Load mock data paths
# NOTE: Adjust BASE_PATH based on your Databricks environment
# For Repos: f"/Workspace/Repos/<user>/Quality_Framework/mock_data"
# For Volumes: f"/Volumes/{CATALOG}/bronze/mock_data"
BASE_PATH = os.path.join(project_root, "mock_data")

# Load Bronze tables
for table_name, file_name in [
    ("bronze.customers", "customers.csv"),
    ("bronze.transactions", "transactions.csv"),
    ("bronze.products", "products.csv"),
]:
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BASE_PATH}/{file_name}")
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{table_name}")
    print(f"Loaded {CATALOG}.{table_name}: {df.count()} rows")

# Load reference table
dim_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{BASE_PATH}/dim_country.csv")
)
dim_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.silver.dim_country")
print(f"Loaded {CATALOG}.silver.dim_country: {dim_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Extract & Load DQ Rules from Collibra (Mock)

# COMMAND ----------

import yaml

config_path = os.path.join(project_root, "configs", "pipeline_config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

from extract.collibra_api_client import CollibraAPIClient
from extract.rule_transformer import RuleTransformer
from extract.rule_loader import RuleLoader

# Extract
client = CollibraAPIClient(config, spark=spark)
raw_rules = client.extract_rules()
raw_mappings = client.extract_rule_mappings()

# Transform
schema_path = os.path.join(project_root, "configs", "dq_rules_schema.json")
with open(schema_path, "r") as f:
    rule_schema = json.load(f)

transformer = RuleTransformer(schema=rule_schema)
valid_rules, invalid_rules = transformer.transform_rules(raw_rules)
transformed_mappings = transformer.transform_mappings(raw_mappings)

# Load
loader = RuleLoader(spark, catalog=CATALOG, schema="dq_framework")
loader.load_rules(valid_rules)
loader.load_mappings(transformed_mappings)

print(f"\nRules loaded: {len(valid_rules)} valid, {len(invalid_rules)} invalid")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Execute DQ Rules (Bronze → Silver)

# COMMAND ----------

from pipelines.bronze_to_silver import BronzeToSilverPipeline

b2s = BronzeToSilverPipeline(spark, catalog=CATALOG)

datasets = [
    ("bronze.customers", "bronze.customers", "silver.customers"),
    ("bronze.transactions", "bronze.transactions", "silver.transactions"),
    ("bronze.products", "bronze.products", "silver.products"),
]

all_results = []
for ds_name, bronze, silver in datasets:
    result = b2s.process_dataset(
        dataset_name=ds_name,
        bronze_table=bronze,
        silver_table=silver,
        run_id=f"{PIPELINE_RUN_ID}_{ds_name.split('.')[1]}",
    )
    all_results.append(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze → Silver Summary

# COMMAND ----------

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
# MAGIC ## Step 4: Build Gold Layer

# COMMAND ----------

from pipelines.silver_to_gold import SilverToGoldPipeline

gold = SilverToGoldPipeline(spark, catalog=CATALOG)
gold_results = gold.build_all()

print("\nGold tables built:")
for r in gold_results:
    print(f"  {r['table']}: {r['count']} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: DQ Reports & Dashboards

# COMMAND ----------

# Overall Framework Health
print("=== FRAMEWORK HEALTH ===\n")
spark.sql(f"""
    SELECT
        COUNT(DISTINCT dataset_name) as datasets_scanned,
        COUNT(*) as total_rule_executions,
        SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) as rules_passed,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as rules_failed,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as rules_errored,
        ROUND(AVG(dq_score), 1) as avg_dq_score
    FROM {CATALOG}.dq_framework.dq_execution_log
""").display()

# COMMAND ----------

# Rule-level detail
print("=== RULE EXECUTION DETAIL ===\n")
spark.sql(f"""
    SELECT
        dataset_name,
        rule_id,
        rule_type,
        target_column,
        severity,
        enforcement_mode,
        status,
        dq_score,
        failed_count,
        CASE
            WHEN dq_score >= 95 THEN 'HEALTHY'
            WHEN dq_score >= 80 THEN 'WARNING'
            ELSE 'CRITICAL'
        END AS health
    FROM {CATALOG}.dq_framework.dq_execution_log
    ORDER BY dq_score ASC
""").display()

# COMMAND ----------

# Quarantine summary
print("=== QUARANTINE SUMMARY ===\n")
spark.sql(f"""
    SELECT
        source_table,
        rule_type,
        severity,
        COUNT(*) as quarantined_records
    FROM {CATALOG}.dq_framework.dq_quarantine
    GROUP BY source_table, rule_type, severity
    ORDER BY quarantined_records DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Feedback to Collibra (Mock)

# COMMAND ----------

from feedback.collibra_writeback import CollibraWriteback

writeback = CollibraWriteback(spark, catalog=CATALOG, mock_mode=True)
feedback_result = writeback.push_to_collibra()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Complete

# COMMAND ----------

print("\n" + "=" * 75)
print("DQ FRAMEWORK POC - END-TO-END RUN COMPLETE")
print("=" * 75)
print(f"Run ID:           {PIPELINE_RUN_ID}")
print(f"Completed:        {datetime.utcnow().isoformat()}")
print(f"Datasets:         {len(all_results)}")
print(f"Avg DQ Score:     {avg_score:.1f}%")
print(f"Feedback pushed:  {feedback_result['count']} updates (mock)")
print(f"SLA Violations:   {feedback_result.get('sla_violations', 0)}")
print("=" * 75)
print("\nTables created:")
print(f"  Bronze: {CATALOG}.bronze.customers/transactions/products")
print(f"  Silver: {CATALOG}.silver.customers/transactions/products")
print(f"  Gold:   {CATALOG}.gold.customer_360/product_catalog/dq_executive_summary")
print(f"  DQ:     {CATALOG}.dq_framework.dq_rules/dq_rule_mapping/dq_execution_log/dq_quarantine/dq_scores")
