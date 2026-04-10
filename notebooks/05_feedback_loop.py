# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Feedback Loop (Collibra Writeback)
# MAGIC Pushes DQ execution results back to Collibra for governance visibility.

# COMMAND ----------

import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

CATALOG = "dq_poc"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate and Push Feedback

# COMMAND ----------

from feedback.collibra_writeback import CollibraWriteback

writeback = CollibraWriteback(spark, catalog=CATALOG, mock_mode=True)
result = writeback.push_to_collibra()

print(f"\nWriteback result: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. View What Would Be Sent

# COMMAND ----------

import json

payloads = writeback.generate_feedback_payload()
print(f"Total payloads to push: {len(payloads)}")
print("\nAll payloads:")
for p in payloads:
    print(json.dumps(p, indent=2, default=str))
    print("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SLA Violation Report

# COMMAND ----------

spark.sql(f"""
    SELECT
        rule_id,
        dataset_name,
        target_column,
        severity,
        dq_score,
        failed_count,
        total_records,
        CASE WHEN dq_score < 80 AND severity = 'error' THEN 'YES' ELSE 'NO' END as sla_violation
    FROM {CATALOG}.dq_framework.dq_execution_log
    WHERE status = 'failed'
    ORDER BY dq_score ASC
""").display()

# COMMAND ----------

print("\nFeedback loop complete!")
print("In production, these results would be pushed to Collibra via REST API,")
print("enabling business users to see actual data health in the governance portal.")
