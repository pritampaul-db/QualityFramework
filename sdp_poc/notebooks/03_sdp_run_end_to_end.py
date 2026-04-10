# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End SDP DQ Framework Runner
# MAGIC
# MAGIC **Single notebook to demo the complete SDP-based DQ framework.**
# MAGIC
# MAGIC Steps:
# MAGIC 1. Verify Bronze tables exist
# MAGIC 2. Show rules that will be applied (with SQL expressions)
# MAGIC 3. Simulate the DLT pipeline logic (for environments where DLT isn't available)
# MAGIC 4. Demonstrate row-level DQ results
# MAGIC 5. Show quarantine and clean splits
# MAGIC 6. Display DQ summaries
# MAGIC
# MAGIC **For production:** Use the DLT pipeline (`01_sdp_deploy_pipeline`).
# MAGIC This notebook simulates the same logic for demo/POC purposes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
import os
import uuid
from datetime import datetime

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
repo_root = os.path.dirname(project_root)
for p in [project_root, repo_root]:
    if p not in sys.path:
        sys.path.insert(0, p)

from sdp_poc.engine.rule_loader import SDPRuleLoader
from sdp_poc.engine.rule_expression_builder import RuleExpressionBuilder
from sdp_poc.engine.row_level_evaluator import RowLevelEvaluator

from pyspark.sql import functions as F

CATALOG = "dq_poc"
RUN_ID = str(uuid.uuid4())
print(f"Run ID: {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verify Source Tables

# COMMAND ----------

for t in ["bronze.customers", "bronze.transactions", "bronze.products", "silver.dim_country"]:
    print(f"  {CATALOG}.{t}: {spark.table(f'{CATALOG}.{t}').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load & Display Rules

# COMMAND ----------

loader = SDPRuleLoader()

datasets_config = [
    {"name": "customers", "mapping_key": "bronze.customers", "pk": "customer_id"},
    {"name": "transactions", "mapping_key": "bronze.transactions", "pk": "transaction_id"},
    {"name": "products", "mapping_key": "bronze.products", "pk": "product_id"},
]

# Show all rules with their SQL expressions
for ds in datasets_config:
    rules, mappings = loader.get_rules_for_dataset(ds["mapping_key"])
    all_exprs = RuleExpressionBuilder.build_all_expressions(rules, mappings)

    print(f"\n{'='*80}")
    print(f"  {ds['name'].upper()} — {len(all_exprs)} rules")
    print(f"{'='*80}")
    for expr in all_exprs:
        icon = "DROP" if expr["enforcement_mode"] == "hard" else "WARN"
        print(f"  [{icon:4s}] {expr['rule_id']:12s} | {expr['rule_type']:25s} | "
              f"{expr['severity']:8s} | {expr['sql_expression'][:55]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Execute DQ — Row-Level Evaluation
# MAGIC
# MAGIC For each dataset:
# MAGIC - Evaluate every rule per row
# MAGIC - Annotate with `_dq_<rule_id>` boolean columns
# MAGIC - Compute `_dq_row_score`, `_dq_failed_rules`, etc.
# MAGIC - Split into clean and quarantine

# COMMAND ----------

results = {}

for ds in datasets_config:
    dataset_name = ds["name"]
    mapping_key = ds["mapping_key"]

    print(f"\n{'='*80}")
    print(f"  Processing: {dataset_name}")
    print(f"{'='*80}")

    # Load bronze data
    bronze_df = spark.table(f"{CATALOG}.bronze.{dataset_name}")
    print(f"  Bronze rows: {bronze_df.count()}")

    # Get rule expressions
    rules, mappings = loader.get_rules_for_dataset(mapping_key)
    all_exprs = RuleExpressionBuilder.build_all_expressions(rules, mappings)

    # Evaluate all rules per row
    validated_df = RowLevelEvaluator.evaluate(bronze_df, all_exprs)
    validated_count = validated_df.count()
    print(f"  Validated rows (annotated): {validated_count}")

    # Split into clean and quarantine based on hard rules
    clean_df, quarantine_df = RowLevelEvaluator.split_clean_quarantine(
        validated_df, all_exprs
    )
    clean_count = clean_df.count()
    quarantine_count = quarantine_df.count() if quarantine_df else 0

    print(f"  Clean rows: {clean_count}")
    print(f"  Quarantined rows: {quarantine_count}")

    # Compute average DQ score
    avg_score = (
        validated_df.agg(F.round(F.avg("_dq_row_score"), 2))
        .collect()[0][0]
    )
    print(f"  Average row DQ score: {avg_score}%")

    # Persist tables
    validated_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{CATALOG}.silver.{dataset_name}_validated")

    clean_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{CATALOG}.silver.{dataset_name}_clean")

    if quarantine_df and quarantine_count > 0:
        quarantine_with_reason = (
            quarantine_df
            .withColumn("_quarantine_reason",
                         F.concat_ws(", ", F.col("_dq_failed_rules")))
            .withColumn("_quarantined_at", F.current_timestamp())
        )
        quarantine_with_reason.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(f"{CATALOG}.dq_sdp_results.{dataset_name}_quarantine")

    results[dataset_name] = {
        "validated": validated_count,
        "clean": clean_count,
        "quarantine": quarantine_count,
        "avg_score": avg_score,
        "rules_count": len(all_exprs),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Row-Level Results — Deep Dive

# COMMAND ----------

# Show customer rows with their per-rule pass/fail
print("=== Customers: Row-Level DQ Results ===")
cust_val = spark.table(f"{CATALOG}.silver.customers_validated")
display(
    cust_val.select(
        "customer_id", "customer_name", "email", "age", "status",
        "_dq_passed_rules", "_dq_failed_rules",
        "_dq_passed_count", "_dq_failed_count",
        "_dq_row_score", "_dq_all_passed",
    )
)

# COMMAND ----------

# Show individual rule columns for customers
dq_cols = [c for c in cust_val.columns if c.startswith("_dq_DQR_")]
display(
    cust_val.select(
        "customer_id", "customer_name", *dq_cols, "_dq_row_score"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Quarantine Inspection

# COMMAND ----------

for ds in datasets_config:
    name = ds["name"]
    try:
        q_df = spark.table(f"{CATALOG}.dq_sdp_results.{name}_quarantine")
        count = q_df.count()
        if count > 0:
            print(f"\n=== {name.upper()} Quarantine: {count} rows ===")
            display(
                q_df.select(
                    ds["pk"], "_dq_failed_rules", "_quarantine_reason",
                    "_dq_row_score", "_quarantined_at"
                )
            )
    except Exception:
        print(f"  {name}: No quarantine records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Execution Summary

# COMMAND ----------

print(f"\n{'='*80}")
print(f"  SDP DQ FRAMEWORK — EXECUTION SUMMARY")
print(f"  Run ID: {RUN_ID}")
print(f"{'='*80}")

summary_data = []
for name, r in results.items():
    clean_pct = round(r["clean"] / r["validated"] * 100, 1) if r["validated"] else 0
    print(f"\n  {name.upper()}")
    print(f"    Rules evaluated:    {r['rules_count']}")
    print(f"    Total rows:         {r['validated']}")
    print(f"    Clean rows:         {r['clean']} ({clean_pct}%)")
    print(f"    Quarantined rows:   {r['quarantine']}")
    print(f"    Avg row DQ score:   {r['avg_score']}%")
    summary_data.append({
        "dataset": name,
        "rules": r["rules_count"],
        "total": r["validated"],
        "clean": r["clean"],
        "quarantine": r["quarantine"],
        "avg_score": r["avg_score"],
        "clean_pct": clean_pct,
    })

print(f"\n{'='*80}")

summary_df = spark.createDataFrame(summary_data)
display(summary_df)
