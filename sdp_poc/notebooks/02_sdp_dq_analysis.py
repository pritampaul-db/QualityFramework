# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — DQ Analysis & Row-Level Inspection
# MAGIC
# MAGIC After the DLT pipeline runs, use this notebook to:
# MAGIC 1. Inspect row-level DQ results (which rules passed/failed per row)
# MAGIC 2. View dataset-level summaries
# MAGIC 3. Drill into quarantined records
# MAGIC 4. Analyze DQ event logs from the DLT expectations system

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

CATALOG = "dq_poc"
SCHEMA = "silver"  # DLT target schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Row-Level DQ Results
# MAGIC
# MAGIC Each row has been annotated with:
# MAGIC - `_dq_DQR_XXXXXX` columns (boolean: did this rule pass?)
# MAGIC - `_dq_passed_rules` / `_dq_failed_rules` (arrays of rule IDs)
# MAGIC - `_dq_row_score` (0-100 per-row DQ score)
# MAGIC - `_dq_all_passed` (boolean: did ALL rules pass?)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Customers — Row-Level Results

# COMMAND ----------

customers_validated = spark.table(f"{CATALOG}.{SCHEMA}.customers_validated")
print(f"Total customer rows: {customers_validated.count()}")

# Show all columns including DQ annotations
display(customers_validated)

# COMMAND ----------

# Show rows that failed at least one rule
failed_customers = customers_validated.filter("_dq_all_passed = false")
print(f"Customers failing at least one rule: {failed_customers.count()}")
display(
    failed_customers.select(
        "customer_id", "customer_name", "email",
        "_dq_failed_rules", "_dq_failed_count", "_dq_row_score"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Transactions — Row-Level Results

# COMMAND ----------

txn_validated = spark.table(f"{CATALOG}.{SCHEMA}.transactions_validated")
print(f"Total transaction rows: {txn_validated.count()}")

failed_txns = txn_validated.filter("_dq_all_passed = false")
print(f"Transactions failing at least one rule: {failed_txns.count()}")
display(
    failed_txns.select(
        "transaction_id", "customer_id", "amount", "transaction_type",
        "_dq_failed_rules", "_dq_failed_count", "_dq_row_score"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Products — Row-Level Results

# COMMAND ----------

prod_validated = spark.table(f"{CATALOG}.{SCHEMA}.products_validated")
print(f"Total product rows: {prod_validated.count()}")

failed_prods = prod_validated.filter("_dq_all_passed = false")
print(f"Products failing at least one rule: {failed_prods.count()}")
display(
    failed_prods.select(
        "product_id", "product_name", "sku", "category",
        "_dq_failed_rules", "_dq_failed_count", "_dq_row_score"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Drill Into Specific Rule Failures

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which rules are failing most across all customers?
# MAGIC SELECT
# MAGIC   rule_id,
# MAGIC   COUNT(*) AS total_records,
# MAGIC   SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed,
# MAGIC   SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) AS failed,
# MAGIC   ROUND(SUM(CASE WHEN passed THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pass_rate
# MAGIC FROM dq_poc.silver.dq_row_level_detail_all
# MAGIC GROUP BY rule_id
# MAGIC ORDER BY pass_rate ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quarantine Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Customer Quarantine

# COMMAND ----------

cust_quarantine = spark.table(f"{CATALOG}.{SCHEMA}.customers_quarantine")
print(f"Quarantined customer rows: {cust_quarantine.count()}")
display(
    cust_quarantine.select(
        "customer_id", "customer_name", "email",
        "_dq_failed_rules", "_quarantine_reason", "_quarantined_at"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Transaction Quarantine

# COMMAND ----------

txn_quarantine = spark.table(f"{CATALOG}.{SCHEMA}.transactions_quarantine")
print(f"Quarantined transaction rows: {txn_quarantine.count()}")
display(
    txn_quarantine.select(
        "transaction_id", "customer_id", "amount",
        "_dq_failed_rules", "_quarantine_reason", "_quarantined_at"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Dataset-Level DQ Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dq_poc.silver.dq_summary_by_dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Per-Rule DQ Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dq_poc.silver.dq_summary_by_rule
# MAGIC ORDER BY pass_rate_pct ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Clean vs Validated Comparison

# COMMAND ----------

from pyspark.sql import functions as F

comparison_data = []
for name in ["customers", "transactions", "products"]:
    validated_count = spark.table(f"{CATALOG}.{SCHEMA}.{name}_validated").count()
    clean_count = spark.table(f"{CATALOG}.{SCHEMA}.{name}_clean").count()
    quarantine_count = spark.table(f"{CATALOG}.{SCHEMA}.{name}_quarantine").count()
    avg_score = (
        spark.table(f"{CATALOG}.{SCHEMA}.{name}_validated")
        .agg(F.round(F.avg("_dq_row_score"), 2))
        .collect()[0][0]
    )
    comparison_data.append({
        "dataset": name,
        "total_rows": validated_count,
        "clean_rows": clean_count,
        "quarantined_rows": quarantine_count,
        "avg_dq_score": avg_score,
        "clean_pct": round(clean_count * 100.0 / validated_count, 2) if validated_count else 0,
    })

comparison_df = spark.createDataFrame(comparison_data)
display(comparison_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. DLT Expectation Event Logs
# MAGIC
# MAGIC DLT automatically captures expectation metrics in the event log.
# MAGIC Query the pipeline's event log for built-in DQ tracking.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Replace <pipeline_id> with your actual DLT pipeline ID
# MAGIC -- SELECT
# MAGIC --   timestamp,
# MAGIC --   details:flow_progress:data_quality:expectations
# MAGIC -- FROM event_log(TABLE(dq_poc.silver.customers_validated))
# MAGIC -- WHERE details:flow_progress:data_quality IS NOT NULL
# MAGIC -- ORDER BY timestamp DESC
# MAGIC -- LIMIT 50
# MAGIC
# MAGIC SELECT 'Uncomment the query above and replace pipeline_id to view DLT event logs' AS note
