# Databricks notebook source
# MAGIC %md
# MAGIC # Dynamic Bronze Sources
# MAGIC
# MAGIC Reads the `dq_datasets` registry and dynamically creates Bronze DLT tables
# MAGIC for each active dataset. No hardcoded dataset names.

# COMMAND ----------

import pyspark.pipelines as dlt
from pyspark.sql import functions as F
import json

CATALOG = "dq_poc"
PIPELINE_NAME = "DQ Framework - Dynamic SDP Pipeline"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage Builder

# COMMAND ----------

def _add_bronze_lineage(df, dataset_name, source_table, bronze_dlt_table):
    return df.withColumn("_lineage", F.struct(
        F.struct(
            F.lit(dataset_name).alias("file_name"),
            F.lit(source_table).alias("file_path"),
            F.lit(F.current_date()).alias("file_date"),
            F.lit("delta").alias("format"),
        ).alias("source"),
        F.struct(
            F.lit(f"{CATALOG}.bronze.{dataset_name}").alias("table"),
            F.current_timestamp().alias("ingested_at"),
        ).alias("bronze"),
        F.struct(
            F.lit(None).cast("string").alias("table"),
            F.lit(None).cast("timestamp").alias("validated_at"),
        ).alias("silver"),
        F.struct(
            F.lit(None).cast("string").alias("table"),
            F.lit(None).cast("timestamp").alias("processed_at"),
        ).alias("gold"),
        F.struct(
            F.lit(PIPELINE_NAME).alias("name"),
            F.current_timestamp().alias("updated_at"),
        ).alias("pipeline"),
    ))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Bronze Table Creation

# COMMAND ----------

# Load dataset registry at import time
_datasets = (
    spark.table(f"{CATALOG}.dq_framework.dq_datasets")
    .filter("is_active = true")
    .collect()
)
print(f"[Dynamic Bronze] Found {len(_datasets)} active datasets")

# Dynamically create a Bronze DLT table for each registered dataset
for _ds_row in _datasets:
    _ds = _ds_row.asDict()
    _ds_name = _ds["dataset_name"]
    _bronze_name = _ds["bronze_dlt_table"] or f"{_ds_name}_bronze"
    _source_table = _ds["source_table"]

    # Use a closure to capture the loop variables
    def _make_bronze(ds_name=_ds_name, bronze_name=_bronze_name, source_table=_source_table):

        @dlt.table(
            name=bronze_name,
            comment=f"Bronze {ds_name} with _lineage tracking",
            table_properties={"quality": "bronze", "dataset": ds_name},
        )
        def bronze_table():
            df = spark.table(source_table)
            return _add_bronze_lineage(df, ds_name, source_table, bronze_name)

        return bronze_table

    _make_bronze()

print(f"[Dynamic Bronze] Registered {len(_datasets)} Bronze DLT tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Table (static)

# COMMAND ----------

@dlt.table(
    name="ref_dim_country",
    comment="Reference table — country codes and regions",
    table_properties={"quality": "reference"},
)
def ref_dim_country():
    return spark.table(f"{CATALOG}.silver.dim_country")
