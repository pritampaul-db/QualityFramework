# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer & DQ Results — DLT Pipeline
# MAGIC
# MAGIC Reads from `_dq` and `_lineage` struct columns in validated tables.
# MAGIC Builds:
# MAGIC 1. **Row-Level DQ Detail** — Exploded from `_dq.results` array
# MAGIC 2. **DQ Summary by Dataset / Rule** — Aggregated from detail
# MAGIC 3. **Gold Customer 360 / Product Catalog** — Business-ready with full lineage

# COMMAND ----------

import pyspark.pipelines as dlt
from pyspark.sql import functions as F


def _enrich_gold_lineage(df, gold_table_name):
    """Enrich _lineage.gold with gold table name and processing timestamp."""
    if "_lineage" not in df.columns:
        return df
    return df.withColumn("_lineage", F.struct(
        F.col("_lineage.source").alias("source"),
        F.col("_lineage.bronze").alias("bronze"),
        F.col("_lineage.silver").alias("silver"),
        F.struct(
            F.lit(gold_table_name).alias("table"),
            F.current_timestamp().alias("processed_at"),
        ).alias("gold"),
        F.col("_lineage.pipeline").alias("pipeline"),
    ))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row-Level DQ Detail (Explode _dq.results)

# COMMAND ----------

def _explode_dq_detail(validated_df, dataset_name, pk_col):
    """
    Explode _dq.results array into one row per (record, rule) combination.
    Includes lineage source file for traceability.
    """
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
        "dataset_name",
        "primary_key_value",
        F.col("rule.rule_id").alias("rule_id"),
        F.col("rule.rule_name").alias("rule_name"),
        F.col("rule.rule_type").alias("rule_type"),
        F.col("rule.column").alias("target_column"),
        F.col("rule.severity").alias("severity"),
        F.col("rule.enforcement").alias("enforcement"),
        F.col("rule.passed").alias("passed"),
        "row_score",
        "all_passed",
        "evaluated_at",
        "source_file",
    )


@dlt.table(
    name="dq_row_level_detail_customers",
    comment="Row-level DQ: one row per (customer, rule) from _dq.results",
    table_properties={"quality": "gold"},
)
def dq_row_level_detail_customers():
    return _explode_dq_detail(dlt.read("customers_validated"), "customers", "customer_id")


@dlt.table(
    name="dq_row_level_detail_transactions",
    comment="Row-level DQ: one row per (transaction, rule) from _dq.results",
    table_properties={"quality": "gold"},
)
def dq_row_level_detail_transactions():
    return _explode_dq_detail(dlt.read("transactions_validated"), "transactions", "transaction_id")


@dlt.table(
    name="dq_row_level_detail_products",
    comment="Row-level DQ: one row per (product, rule) from _dq.results",
    table_properties={"quality": "gold"},
)
def dq_row_level_detail_products():
    return _explode_dq_detail(dlt.read("products_validated"), "products", "product_id")


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
    return (
        dlt.read("dq_row_level_detail_customers")
        .unionByName(dlt.read("dq_row_level_detail_transactions"))
        .unionByName(dlt.read("dq_row_level_detail_products"))
    )


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


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Business Tables

# COMMAND ----------

@dlt.table(
    name="gold_customer_360",
    comment="Customer 360 — clean customers joined with transaction metrics",
    table_properties={"quality": "gold"},
)
def gold_customer_360():
    customers = dlt.read("customers_clean")
    transactions = dlt.read("transactions_clean")

    txn_metrics = (
        transactions.groupBy("customer_id")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_spend"),
            F.avg("amount").alias("avg_transaction_amount"),
            F.max("transaction_date").alias("last_transaction_date"),
            F.min("transaction_date").alias("first_transaction_date"),
        )
    )

    joined = (
        customers
        .join(txn_metrics, on="customer_id", how="left")
        .select(
            customers["customer_id"],
            customers["customer_name"],
            customers["email"],
            customers["age"],
            customers["status"],
            customers["country_code"],
            F.coalesce(F.col("total_transactions"), F.lit(0)).alias("total_transactions"),
            F.round(F.coalesce(F.col("total_spend"), F.lit(0)), 2).alias("total_spend"),
            F.round(F.coalesce(F.col("avg_transaction_amount"), F.lit(0)), 2).alias("avg_transaction_amount"),
            F.col("last_transaction_date"),
            F.col("first_transaction_date"),
            F.when(F.col("total_transactions") >= 5, "high")
            .when(F.col("total_transactions") >= 2, "medium")
            .otherwise("low").alias("engagement_tier"),
            customers["_dq"],
            customers["_lineage"],
        )
    )
    return _enrich_gold_lineage(joined, "dq_poc.silver.gold_customer_360")


@dlt.table(
    name="gold_product_catalog",
    comment="Product catalog — clean products with DQ score",
    table_properties={"quality": "gold"},
)
def gold_product_catalog():
    products = dlt.read("products_clean")
    selected = (
        products
        .select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("sku"),
            F.col("category"),
            F.round(F.col("price"), 2).alias("unit_price"),
            F.col("stock_quantity"),
            F.col("supplier"),
            F.col("_dq"),
            F.col("_lineage"),
        )
    )
    return _enrich_gold_lineage(selected, "dq_poc.silver.gold_product_catalog")
