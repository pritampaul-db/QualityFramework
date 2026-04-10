# Databricks notebook source
# MAGIC %md
# MAGIC # Dynamic Gold Layer & DQ Results
# MAGIC
# MAGIC Dynamically creates gold tables for each active dataset:
# MAGIC 1. Row-level DQ detail (exploded from _dq.results)
# MAGIC 2. Unified detail across all datasets
# MAGIC 3. Aggregate summaries by dataset and by rule

# COMMAND ----------

import pyspark.pipelines as dlt
from pyspark.sql import functions as F
import json

CATALOG = "dq_poc"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Dataset Registry

# COMMAND ----------

_datasets = (
    spark.table(f"{CATALOG}.dq_framework.dq_datasets")
    .filter("is_active = true")
    .collect()
)
_dataset_configs = [row.asDict() for row in _datasets]
print(f"[Dynamic Gold] Found {len(_dataset_configs)} active datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Row-Level Detail Tables

# COMMAND ----------

def _explode_dq_detail(validated_df, dataset_name, pk_col):
    base = (
        validated_df
        .select(
            F.lit(dataset_name).alias("dataset_name"),
            F.col(pk_col).cast("string").alias("primary_key_value"),
            F.explode("_dq.results").alias("rule"),
            F.col("_dq.row_score").alias("row_score"),
            F.col("_dq.all_passed"),
            F.col("_dq.evaluated_at").alias("evaluated_at"),
            F.col("_lineage.source.file_name").alias("source_file"),
        )
    )
    return base.select(
        "dataset_name", "primary_key_value",
        F.col("rule.rule_id").alias("rule_id"),
        F.col("rule.rule_name").alias("rule_name"),
        F.col("rule.rule_type").alias("rule_type"),
        F.col("rule.column").alias("target_column"),
        F.col("rule.severity").alias("severity"),
        F.col("rule.enforcement").alias("enforcement"),
        F.col("rule.passed").alias("passed"),
        "row_score", "all_passed", "evaluated_at", "source_file",
    )


# Create per-dataset detail tables dynamically
_detail_table_names = []

for _ds in _dataset_configs:
    _ds_name = _ds["dataset_name"]
    _pk = _ds["primary_key"]
    _detail_name = f"dq_row_level_detail_{_ds_name}"
    _validated_name = f"{_ds_name}_validated"
    _detail_table_names.append(_detail_name)

    def _make_detail(
        ds_name=_ds_name, pk=_pk,
        detail_name=_detail_name, validated_name=_validated_name,
    ):
        @dlt.table(
            name=detail_name,
            comment=f"Row-level DQ: one row per ({ds_name}, rule) from _dq.results",
            table_properties={"quality": "gold", "dataset": ds_name},
        )
        def detail():
            return _explode_dq_detail(dlt.read(validated_name), ds_name, pk)

    _make_detail()

print(f"[Dynamic Gold] Registered {len(_detail_table_names)} detail tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unified DQ Detail

# COMMAND ----------

@dlt.table(
    name="dq_row_level_detail_all",
    comment="Unified row-level DQ results across all datasets",
    table_properties={"quality": "gold"},
)
def dq_row_level_detail_all():
    # Union all per-dataset detail tables
    dfs = [dlt.read(name) for name in _detail_table_names]
    result = dfs[0]
    for df in dfs[1:]:
        result = result.unionByName(df)
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQ Aggregate Summaries

# COMMAND ----------

@dlt.table(
    name="dq_summary_by_dataset",
    comment="Aggregate DQ scores per dataset",
    table_properties={"quality": "gold"},
)
def dq_summary_by_dataset():
    detail = dlt.read("dq_row_level_detail_all")
    return (
        detail
        .groupBy("dataset_name")
        .agg(
            F.count("*").alias("total_evaluations"),
            F.sum(F.when(F.col("passed"), 1).otherwise(0)).alias("total_passed"),
            F.sum(F.when(~F.col("passed"), 1).otherwise(0)).alias("total_failed"),
            F.round(
                F.sum(F.when(F.col("passed"), 1).otherwise(0)) * 100.0 / F.count("*"), 2
            ).alias("pass_rate_pct"),
            F.countDistinct("primary_key_value").alias("total_records"),
            F.countDistinct(
                F.when(F.col("all_passed"), F.col("primary_key_value"))
            ).alias("fully_clean_records"),
            F.round(F.avg("row_score"), 2).alias("avg_row_score"),
            F.min("evaluated_at").alias("evaluated_at"),
        )
    )


@dlt.table(
    name="dq_summary_by_rule",
    comment="Aggregate pass/fail per rule across all datasets",
    table_properties={"quality": "gold"},
)
def dq_summary_by_rule():
    detail = dlt.read("dq_row_level_detail_all")
    return (
        detail
        .groupBy("dataset_name", "rule_id", "rule_name", "rule_type", "severity", "enforcement")
        .agg(
            F.count("*").alias("total_records"),
            F.sum(F.when(F.col("passed"), 1).otherwise(0)).alias("passed_count"),
            F.sum(F.when(~F.col("passed"), 1).otherwise(0)).alias("failed_count"),
            F.round(
                F.sum(F.when(F.col("passed"), 1).otherwise(0)) * 100.0 / F.count("*"), 2
            ).alias("pass_rate_pct"),
        )
        .orderBy("dataset_name", "pass_rate_pct")
    )
