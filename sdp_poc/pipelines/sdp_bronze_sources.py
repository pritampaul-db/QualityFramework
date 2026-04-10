# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Source Tables for DLT
# MAGIC
# MAGIC Reads raw data and stamps each row with `_lineage` struct tracking:
# MAGIC - **source**: landing file name, path, date, format
# MAGIC - **bronze**: table name, ingestion timestamp
# MAGIC
# MAGIC Downstream layers (silver, gold) enrich `_lineage` with their own metadata.

# COMMAND ----------

import pyspark.pipelines as dlt
from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "dq_poc"
VOLUME_PATH = "/Volumes/dq_poc/bronze/mock_data"
PIPELINE_NAME = "DQ Framework - SDP Pipeline"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage Builder

# COMMAND ----------

def _add_bronze_lineage(df, file_name, file_format, bronze_table):
    """
    Stamp every row with _lineage struct containing source + bronze metadata.

    _lineage: STRUCT<
      source:   STRUCT<file_name, file_path, file_date, format>,
      bronze:   STRUCT<table, ingested_at>,
      silver:   STRUCT<table, validated_at>,     -- NULL, filled at silver layer
      gold:     STRUCT<table, processed_at>,     -- NULL, filled at gold layer
      pipeline: STRUCT<name, updated_at>
    >
    """
    file_path = f"{VOLUME_PATH}/{file_name}"

    return df.withColumn("_lineage", F.struct(
        F.struct(
            F.lit(file_name).alias("file_name"),
            F.lit(file_path).alias("file_path"),
            F.lit(F.current_date()).alias("file_date"),
            F.lit(file_format).alias("format"),
        ).alias("source"),
        F.struct(
            F.lit(f"{CATALOG}.bronze.{bronze_table}").alias("table"),
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
# MAGIC ## Bronze Tables

# COMMAND ----------

@dlt.table(
    name="customers_bronze",
    comment="Bronze customers with _lineage tracking source file provenance",
    table_properties={"quality": "bronze"},
)
def customers_bronze():
    df = spark.table(f"{CATALOG}.bronze.customers")
    return _add_bronze_lineage(df, "customers.csv", "csv", "customers")


@dlt.table(
    name="transactions_bronze",
    comment="Bronze transactions with _lineage tracking source file provenance",
    table_properties={"quality": "bronze"},
)
def transactions_bronze():
    df = spark.table(f"{CATALOG}.bronze.transactions")
    return _add_bronze_lineage(df, "transactions.csv", "csv", "transactions")


@dlt.table(
    name="products_bronze",
    comment="Bronze products with _lineage tracking source file provenance",
    table_properties={"quality": "bronze"},
)
def products_bronze():
    df = spark.table(f"{CATALOG}.bronze.products")
    return _add_bronze_lineage(df, "products.csv", "csv", "products")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Tables

# COMMAND ----------

@dlt.table(
    name="ref_dim_country",
    comment="Reference table — country codes and regions",
    table_properties={"quality": "reference"},
)
def ref_dim_country():
    return spark.table(f"{CATALOG}.silver.dim_country")
