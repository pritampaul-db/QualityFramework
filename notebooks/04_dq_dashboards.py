# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Data Quality Dashboards & Reporting
# MAGIC SQL queries for DQ observability. These can be used directly in Databricks SQL dashboards.

# COMMAND ----------

CATALOG = "dq_poc"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Executive DQ Scorecard

# COMMAND ----------

spark.sql(f"""
    SELECT
        dataset_name,
        overall_dq_score,
        total_rules,
        rules_passed,
        rules_failed,
        rules_errored,
        total_records_in,
        total_records_out,
        records_quarantined,
        ROUND((total_records_out / total_records_in) * 100, 1) AS record_pass_rate_pct,
        scored_at
    FROM {CATALOG}.dq_framework.dq_scores
    ORDER BY scored_at DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Rule-Level Detail Report

# COMMAND ----------

spark.sql(f"""
    SELECT
        dataset_name,
        rule_id,
        rule_type,
        target_column,
        severity,
        enforcement_mode,
        status,
        total_records,
        passed_count,
        failed_count,
        dq_score,
        execution_duration_ms,
        CASE
            WHEN dq_score >= 95 THEN 'HEALTHY'
            WHEN dq_score >= 80 THEN 'WARNING'
            ELSE 'CRITICAL'
        END AS health_status
    FROM {CATALOG}.dq_framework.dq_execution_log
    ORDER BY dataset_name, dq_score ASC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DQ Score by Rule Type

# COMMAND ----------

spark.sql(f"""
    SELECT
        rule_type,
        COUNT(*) as total_executions,
        SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) as passed,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errored,
        ROUND(AVG(dq_score), 2) as avg_dq_score,
        ROUND(AVG(execution_duration_ms), 0) as avg_duration_ms
    FROM {CATALOG}.dq_framework.dq_execution_log
    GROUP BY rule_type
    ORDER BY avg_dq_score ASC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. DQ Score by Severity

# COMMAND ----------

spark.sql(f"""
    SELECT
        severity,
        COUNT(*) as total_rules,
        SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) as rules_passed,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as rules_failed,
        ROUND(AVG(dq_score), 2) as avg_dq_score,
        SUM(failed_count) as total_failed_records
    FROM {CATALOG}.dq_framework.dq_execution_log
    GROUP BY severity
    ORDER BY severity
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Quarantine Analysis

# COMMAND ----------

spark.sql(f"""
    SELECT
        source_table,
        rule_type,
        severity,
        quarantine_reason,
        COUNT(*) as quarantined_count
    FROM {CATALOG}.dq_framework.dq_quarantine
    GROUP BY source_table, rule_type, severity, quarantine_reason
    ORDER BY quarantined_count DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Enforcement Mode Effectiveness

# COMMAND ----------

spark.sql(f"""
    SELECT
        enforcement_mode,
        dataset_name,
        COUNT(*) as rules_applied,
        SUM(failed_count) as total_failures_caught,
        ROUND(AVG(dq_score), 2) as avg_dq_score
    FROM {CATALOG}.dq_framework.dq_execution_log
    GROUP BY enforcement_mode, dataset_name
    ORDER BY enforcement_mode, dataset_name
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Top Failing Rules (Action Items)

# COMMAND ----------

spark.sql(f"""
    SELECT
        rule_id,
        rule_type,
        dataset_name,
        target_column,
        severity,
        dq_score,
        failed_count,
        total_records,
        ROUND((failed_count / total_records) * 100, 1) as failure_rate_pct
    FROM {CATALOG}.dq_framework.dq_execution_log
    WHERE status = 'failed'
    ORDER BY failure_rate_pct DESC
    LIMIT 10
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Overall Framework Health

# COMMAND ----------

spark.sql(f"""
    SELECT
        'Total Datasets Scanned' as metric,
        CAST(COUNT(DISTINCT dataset_name) AS STRING) as value
    FROM {CATALOG}.dq_framework.dq_execution_log
    UNION ALL
    SELECT
        'Total Rules Executed',
        CAST(COUNT(*) AS STRING)
    FROM {CATALOG}.dq_framework.dq_execution_log
    UNION ALL
    SELECT
        'Rules Passed',
        CAST(SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) AS STRING)
    FROM {CATALOG}.dq_framework.dq_execution_log
    UNION ALL
    SELECT
        'Rules Failed',
        CAST(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS STRING)
    FROM {CATALOG}.dq_framework.dq_execution_log
    UNION ALL
    SELECT
        'Total Records Quarantined',
        CAST(COUNT(*) AS STRING)
    FROM {CATALOG}.dq_framework.dq_quarantine
    UNION ALL
    SELECT
        'Average DQ Score',
        CAST(ROUND(AVG(overall_dq_score), 1) AS STRING) || '%'
    FROM {CATALOG}.dq_framework.dq_scores
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Data Quality Heatmap (Dataset x Rule Type)

# COMMAND ----------

spark.sql(f"""
    SELECT
        dataset_name,
        rule_type,
        ROUND(AVG(dq_score), 1) as avg_score,
        CASE
            WHEN AVG(dq_score) >= 95 THEN 'GREEN'
            WHEN AVG(dq_score) >= 80 THEN 'AMBER'
            ELSE 'RED'
        END as status_color
    FROM {CATALOG}.dq_framework.dq_execution_log
    GROUP BY dataset_name, rule_type
    ORDER BY dataset_name, rule_type
""").display()

# COMMAND ----------

print("Dashboard queries complete!")
print("To create a Databricks SQL Dashboard:")
print("1. Go to SQL > Dashboards > Create Dashboard")
print("2. Copy each query above into a new visualization")
print("3. Use bar charts for scores, tables for details, counters for KPIs")
