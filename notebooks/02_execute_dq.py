# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Execute Data Quality Rules
# MAGIC Runs the DQ engine against all Bronze datasets, producing Silver output
# MAGIC with quarantine for failed records.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import sys
import os
import uuid

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

CATALOG = "dq_poc"
PIPELINE_RUN_ID = str(uuid.uuid4())
print(f"Pipeline Run ID: {PIPELINE_RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Execute Bronze → Silver with DQ Checks

# COMMAND ----------

from pipelines.bronze_to_silver import BronzeToSilverPipeline

b2s = BronzeToSilverPipeline(spark, catalog=CATALOG)

# Define datasets to process
datasets = [
    {
        "dataset_name": "bronze.customers",
        "bronze_table": "bronze.customers",
        "silver_table": "silver.customers",
    },
    {
        "dataset_name": "bronze.transactions",
        "bronze_table": "bronze.transactions",
        "silver_table": "silver.transactions",
    },
    {
        "dataset_name": "bronze.products",
        "bronze_table": "bronze.products",
        "silver_table": "silver.products",
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Process Customers

# COMMAND ----------

customers_result = b2s.process_dataset(
    dataset_name="bronze.customers",
    bronze_table="bronze.customers",
    silver_table="silver.customers",
    run_id=f"{PIPELINE_RUN_ID}_customers",
)
print(f"\nCustomers Result: {customers_result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Process Transactions

# COMMAND ----------

transactions_result = b2s.process_dataset(
    dataset_name="bronze.transactions",
    bronze_table="bronze.transactions",
    silver_table="silver.transactions",
    run_id=f"{PIPELINE_RUN_ID}_transactions",
)
print(f"\nTransactions Result: {transactions_result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Process Products

# COMMAND ----------

products_result = b2s.process_dataset(
    dataset_name="bronze.products",
    bronze_table="bronze.products",
    silver_table="silver.products",
    run_id=f"{PIPELINE_RUN_ID}_products",
)
print(f"\nProducts Result: {products_result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pipeline Summary

# COMMAND ----------

all_results = [customers_result, transactions_result, products_result]

print("=" * 70)
print("BRONZE → SILVER PIPELINE SUMMARY")
print("=" * 70)
print(f"{'Dataset':<25} {'Bronze':>8} {'Silver':>8} {'Quarantine':>12} {'DQ Score':>10}")
print("-" * 70)
for r in all_results:
    print(f"{r['dataset']:<25} {r['bronze_count']:>8} {r['silver_count']:>8} "
          f"{r['quarantined_count']:>12} {r['dq_score']:>9.1f}%")
print("-" * 70)

avg_score = sum(r["dq_score"] for r in all_results) / len(all_results)
print(f"{'AVERAGE':<25} {'':>8} {'':>8} {'':>12} {avg_score:>9.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. View Silver Tables

# COMMAND ----------

print("=== Silver Customers ===")
spark.table(f"{CATALOG}.silver.customers").display()

# COMMAND ----------

print("=== Silver Transactions ===")
spark.table(f"{CATALOG}.silver.transactions").display()

# COMMAND ----------

print("=== Silver Products ===")
spark.table(f"{CATALOG}.silver.products").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. View Quarantined Records

# COMMAND ----------

print("=== Quarantined Records ===")
quarantine_df = spark.table(f"{CATALOG}.dq_framework.dq_quarantine")
print(f"Total quarantined records: {quarantine_df.count()}")
quarantine_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. View Execution Log

# COMMAND ----------

print("=== Execution Log ===")
spark.sql(f"""
    SELECT
        rule_id,
        rule_type,
        dataset_name,
        target_column,
        severity,
        enforcement_mode,
        status,
        total_records,
        passed_count,
        failed_count,
        dq_score,
        execution_duration_ms
    FROM {CATALOG}.dq_framework.dq_execution_log
    ORDER BY dataset_name, rule_id
""").display()

# COMMAND ----------

print("\nDQ execution complete! Proceed to notebook 03_build_gold.py")
