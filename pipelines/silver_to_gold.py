# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold Pipeline
# MAGIC Builds business-ready Gold tables from validated Silver data.
# MAGIC Applies aggregations, joins, and business-level transformations.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    current_timestamp, lit, round as spark_round, when
)


class SilverToGoldPipeline:
    """
    Pipeline that builds Gold-layer tables from Silver data.
    Gold tables are business-ready aggregations and joined datasets.
    """

    def __init__(self, spark: SparkSession, catalog: str = "dq_poc"):
        self.spark = spark
        self.catalog = catalog
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

    def build_customer_360(self) -> dict:
        """
        Build a Customer 360 Gold table joining customers with their transactions.
        """
        print("\n[Gold] Building customer_360...")

        customers = self.spark.table(f"{self.catalog}.silver.customers")
        transactions = self.spark.table(f"{self.catalog}.silver.transactions")

        # Aggregate transaction metrics per customer
        txn_metrics = (
            transactions.groupBy("customer_id")
            .agg(
                count("*").alias("total_transactions"),
                spark_sum("amount").alias("total_spend"),
                avg("amount").alias("avg_transaction_amount"),
                spark_max("transaction_date").alias("last_transaction_date"),
                spark_min("transaction_date").alias("first_transaction_date"),
            )
        )

        # Join customers with transaction metrics
        customer_360 = (
            customers
            .join(txn_metrics, on="customer_id", how="left")
            .select(
                customers["customer_id"],
                customers["customer_name"],
                customers["email"],
                customers["age"],
                customers["status"],
                customers["country_code"],
                col("total_transactions"),
                spark_round(col("total_spend"), 2).alias("total_spend"),
                spark_round(col("avg_transaction_amount"), 2).alias("avg_transaction_amount"),
                col("last_transaction_date"),
                col("first_transaction_date"),
                when(col("total_transactions") >= 5, "high")
                .when(col("total_transactions") >= 2, "medium")
                .otherwise("low").alias("engagement_tier"),
                current_timestamp().alias("_gold_processed_at"),
            )
        )

        gold_table = f"{self.catalog}.gold.customer_360"
        customer_360.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(gold_table)

        count_val = self.spark.table(gold_table).count()
        print(f"[Gold] customer_360: {count_val} records written to {gold_table}")
        return {"table": gold_table, "count": count_val}

    def build_product_performance(self) -> dict:
        """
        Build a Product Performance Gold table with sales metrics.
        """
        print("\n[Gold] Building product_performance...")

        products = self.spark.table(f"{self.catalog}.silver.products")
        transactions = self.spark.table(f"{self.catalog}.silver.transactions")

        # Join transactions with products (mock: use transaction_id prefix as product link)
        # For POC, aggregate by merchant as a proxy
        sales_metrics = (
            transactions.groupBy("merchant")
            .agg(
                count("*").alias("total_orders"),
                spark_sum("amount").alias("total_revenue"),
                avg("amount").alias("avg_order_value"),
                spark_sum("quantity").alias("total_units_sold"),
            )
        )

        product_perf = (
            products
            .crossJoin(
                sales_metrics
                .orderBy(col("total_revenue").desc())
                .limit(1)
                .select(
                    col("total_orders").alias("_sample_orders"),
                    col("total_revenue").alias("_sample_revenue"),
                )
            )
            .select(
                products["product_id"],
                products["product_name"],
                products["sku"],
                products["category"],
                spark_round(products["price"], 2).alias("unit_price"),
                products["stock_quantity"],
                current_timestamp().alias("_gold_processed_at"),
            )
        )

        gold_table = f"{self.catalog}.gold.product_catalog"
        product_perf.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(gold_table)

        count_val = self.spark.table(gold_table).count()
        print(f"[Gold] product_catalog: {count_val} records written to {gold_table}")
        return {"table": gold_table, "count": count_val}

    def build_dq_summary(self) -> dict:
        """
        Build a DQ Summary Gold table for executive reporting.
        """
        print("\n[Gold] Building dq_executive_summary...")

        scores = self.spark.table(f"{self.catalog}.dq_framework.dq_scores")
        exec_log = self.spark.table(f"{self.catalog}.dq_framework.dq_execution_log")

        # Dataset-level summary
        dq_summary = (
            scores
            .select(
                col("dataset_name"),
                col("overall_dq_score"),
                col("total_rules"),
                col("rules_passed"),
                col("rules_failed"),
                col("rules_errored"),
                col("total_records_in"),
                col("total_records_out"),
                col("records_quarantined"),
                col("run_id"),
                col("scored_at"),
            )
            .withColumn("_gold_processed_at", current_timestamp())
        )

        gold_table = f"{self.catalog}.gold.dq_executive_summary"
        dq_summary.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(gold_table)

        count_val = self.spark.table(gold_table).count()
        print(f"[Gold] dq_executive_summary: {count_val} records written to {gold_table}")
        return {"table": gold_table, "count": count_val}

    def build_all(self) -> list:
        """Build all Gold tables."""
        results = []
        results.append(self.build_customer_360())
        results.append(self.build_product_performance())
        results.append(self.build_dq_summary())
        return results
