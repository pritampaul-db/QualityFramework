# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — SDP DQ Framework Setup
# MAGIC
# MAGIC Prerequisites before running the DLT pipeline:
# MAGIC 1. Ensure Bronze tables exist (run the original `notebooks/00_setup.py` first)
# MAGIC 2. Create the DQ results schema for SDP output tables
# MAGIC 3. Verify all source data is available
# MAGIC
# MAGIC **Note:** This notebook shares the same Bronze data and Collibra mock files
# MAGIC as the original framework. No data duplication.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

CATALOG = "dq_poc"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Schemas

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")

# Create schemas needed by the SDP pipeline
for schema in ["bronze", "silver", "gold", "dq_sdp_results"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"Schema ready: {CATALOG}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify Bronze Tables Exist

# COMMAND ----------

required_tables = [
    "bronze.customers",
    "bronze.transactions",
    "bronze.products",
    "silver.dim_country",
]

print("Verifying source tables...")
all_ok = True
for table in required_tables:
    fq = f"{CATALOG}.{table}"
    try:
        count = spark.table(fq).count()
        print(f"  {fq}: {count} rows")
    except Exception as e:
        print(f"  {fq}: MISSING — run notebooks/00_setup.py first!")
        all_ok = False

if all_ok:
    print("\nAll source tables verified. Ready to run the DLT pipeline.")
else:
    print("\nSome tables are missing. Run the original setup notebook first:")
    print("  notebooks/00_setup.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Preview Rules That Will Be Applied

# COMMAND ----------

import sys, os
project_root = os.path.dirname(os.path.dirname(os.path.abspath("__file__")))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from sdp_poc.engine.rule_loader import SDPRuleLoader
from sdp_poc.engine.rule_expression_builder import RuleExpressionBuilder

loader = SDPRuleLoader()

for dataset_key in ["bronze.customers", "bronze.transactions", "bronze.products"]:
    rules, mappings = loader.get_rules_for_dataset(dataset_key)
    all_exprs = RuleExpressionBuilder.build_all_expressions(rules, mappings)

    print(f"\n{'='*70}")
    print(f"Dataset: {dataset_key} — {len(all_exprs)} rules")
    print(f"{'='*70}")
    for expr in all_exprs:
        enforcement = expr['enforcement_mode'].upper()
        print(f"  [{enforcement:4s}] {expr['rule_id']} | {expr['rule_type']:25s} | "
              f"{expr['column']:20s} | {expr['sql_expression'][:60]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Next Steps
# MAGIC
# MAGIC 1. **Create a DLT Pipeline** in the Databricks UI:
# MAGIC    - Go to **Workflows → Delta Live Tables → Create Pipeline**
# MAGIC    - Add these notebook paths:
# MAGIC      - `sdp_poc/pipelines/sdp_bronze_sources`
# MAGIC      - `sdp_poc/pipelines/sdp_dq_pipeline`
# MAGIC      - `sdp_poc/pipelines/sdp_gold_pipeline`
# MAGIC    - Set target catalog: `dq_poc`
# MAGIC    - Set target schema: `silver`
# MAGIC    - Click **Start**
# MAGIC
# MAGIC 2. Or use the **deploy notebook**: `sdp_poc/notebooks/01_sdp_deploy_pipeline.py`
