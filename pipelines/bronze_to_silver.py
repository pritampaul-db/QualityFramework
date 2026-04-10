# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver Pipeline
# MAGIC Applies DQ rules during the Bronze-to-Silver transition.
# MAGIC Hard enforcement drops invalid records; soft enforcement flags them.

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit
from engine.dq_engine import DQEngine
from typing import Optional


class BronzeToSilverPipeline:
    """
    Pipeline that reads Bronze data, applies DQ rules, and writes Silver output.
    Failed records are quarantined automatically by the DQ engine.
    """

    def __init__(self, spark: SparkSession, catalog: str = "dq_poc"):
        self.spark = spark
        self.catalog = catalog
        self.engine = DQEngine(spark, catalog)

    def process_dataset(
        self,
        dataset_name: str,
        bronze_table: str,
        silver_table: str,
        run_id: Optional[str] = None,
    ) -> dict:
        """
        Process a single dataset through Bronze → Silver with DQ checks.

        Args:
            dataset_name: Name used for rule mapping lookup (e.g., "bronze.customers")
            bronze_table: Fully qualified Bronze table name
            silver_table: Fully qualified Silver table name
            run_id: Optional run ID for tracking

        Returns:
            Dictionary with processing summary
        """
        print(f"\n{'='*60}")
        print(f"BRONZE → SILVER: {dataset_name}")
        print(f"{'='*60}")

        # 1. Read Bronze data
        bronze_table_fq = f"{self.catalog}.{bronze_table}"
        print(f"[Pipeline] Reading from {bronze_table_fq}")
        bronze_df = self.spark.table(bronze_table_fq)
        bronze_count = bronze_df.count()
        print(f"[Pipeline] Bronze records: {bronze_count}")

        # 2. Execute DQ rules
        clean_df, summary = self.engine.execute(
            df=bronze_df,
            dataset_name=dataset_name,
            run_id=run_id,
        )

        # 3. Add audit columns to Silver data
        silver_df = (
            clean_df
            .withColumn("_dq_run_id", lit(summary.run_id))
            .withColumn("_dq_score", lit(summary.overall_dq_score))
            .withColumn("_dq_processed_at", current_timestamp())
        )

        # 4. Write to Silver table
        silver_table_fq = f"{self.catalog}.{silver_table}"
        print(f"[Pipeline] Writing {silver_df.count()} records to {silver_table_fq}")
        silver_df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(silver_table_fq)

        result = {
            "dataset": dataset_name,
            "bronze_table": bronze_table_fq,
            "silver_table": silver_table_fq,
            "bronze_count": bronze_count,
            "silver_count": silver_df.count(),
            "quarantined_count": summary.records_quarantined,
            "dq_score": summary.overall_dq_score,
            "rules_passed": summary.rules_passed,
            "rules_failed": summary.rules_failed,
            "run_id": summary.run_id,
        }

        print(f"[Pipeline] Complete: {result['silver_count']}/{result['bronze_count']} "
              f"records passed (DQ Score: {result['dq_score']}%)")

        return result
