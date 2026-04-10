# Databricks notebook source
# MAGIC %md
# MAGIC # Rule Loader
# MAGIC Loads transformed rules into Delta tables using MERGE (upsert) semantics.

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, IntegerType, TimestampType
)
from delta.tables import DeltaTable
from typing import List


class RuleLoader:
    """
    Loads validated DQ rules and mappings into Delta Lake tables.
    Uses MERGE for upserts to preserve history and avoid duplicates.
    """

    RULES_SCHEMA = StructType([
        StructField("rule_id", StringType(), False),
        StructField("rule_name", StringType(), True),
        StructField("rule_type", StringType(), False),
        StructField("target_column", StringType(), False),
        StructField("params", StringType(), True),
        StructField("severity", StringType(), False),
        StructField("is_active", BooleanType(), True),
        StructField("version", IntegerType(), True),
        StructField("effective_from", StringType(), True),
        StructField("effective_to", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("checksum", StringType(), True),
    ])

    MAPPINGS_SCHEMA = StructType([
        StructField("mapping_id", StringType(), False),
        StructField("dataset_name", StringType(), False),
        StructField("column_name", StringType(), False),
        StructField("rule_id", StringType(), False),
        StructField("enforcement_mode", StringType(), True),
        StructField("priority", IntegerType(), True),
        StructField("is_active", BooleanType(), True),
    ])

    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.rules_table = f"{catalog}.{schema}.dq_rules"
        self.mappings_table = f"{catalog}.{schema}.dq_rule_mapping"

    def _ensure_table_exists(self, table_name: str, schema: StructType):
        """Create table if it doesn't exist."""
        if not self.spark.catalog.tableExists(table_name):
            empty_df = self.spark.createDataFrame([], schema)
            empty_df.write.format("delta").saveAsTable(table_name)
            print(f"[RuleLoader] Created table: {table_name}")

    def load_rules(self, rules: List[dict]) -> int:
        """
        Load rules into Delta table using MERGE (upsert on rule_id + version).
        Returns count of affected rows.
        """
        if not rules:
            print("[RuleLoader] No rules to load")
            return 0

        self._ensure_table_exists(self.rules_table, self.RULES_SCHEMA)

        source_df = self.spark.createDataFrame(rules, self.RULES_SCHEMA)

        if DeltaTable.isDeltaTable(self.spark, self.rules_table):
            target = DeltaTable.forName(self.spark, self.rules_table)
            merge_result = (
                target.alias("target")
                .merge(
                    source_df.alias("source"),
                    "target.rule_id = source.rule_id AND target.version = source.version"
                )
                .whenMatchedUpdate(
                    condition="target.checksum != source.checksum",
                    set={
                        "rule_name": "source.rule_name",
                        "rule_type": "source.rule_type",
                        "target_column": "source.target_column",
                        "params": "source.params",
                        "severity": "source.severity",
                        "is_active": "source.is_active",
                        "effective_from": "source.effective_from",
                        "effective_to": "source.effective_to",
                        "updated_at": "source.updated_at",
                        "checksum": "source.checksum",
                    }
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            source_df.write.format("delta").saveAsTable(self.rules_table)

        count = self.spark.table(self.rules_table).count()
        print(f"[RuleLoader] Rules table now has {count} rows")
        return count

    def load_mappings(self, mappings: List[dict]) -> int:
        """
        Load mappings into Delta table using MERGE (upsert on mapping_id).
        Returns count of affected rows.
        """
        if not mappings:
            print("[RuleLoader] No mappings to load")
            return 0

        self._ensure_table_exists(self.mappings_table, self.MAPPINGS_SCHEMA)

        source_df = self.spark.createDataFrame(mappings, self.MAPPINGS_SCHEMA)

        if DeltaTable.isDeltaTable(self.spark, self.mappings_table):
            target = DeltaTable.forName(self.spark, self.mappings_table)
            (
                target.alias("target")
                .merge(
                    source_df.alias("source"),
                    "target.mapping_id = source.mapping_id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            source_df.write.format("delta").saveAsTable(self.mappings_table)

        count = self.spark.table(self.mappings_table).count()
        print(f"[RuleLoader] Mappings table now has {count} rows")
        return count
