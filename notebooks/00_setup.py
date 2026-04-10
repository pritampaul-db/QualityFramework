# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Environment Setup
# MAGIC Creates the catalog, schemas, and loads mock data into Bronze tables.
# MAGIC **Run this notebook first before executing the DQ framework.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Catalog and Schemas

# COMMAND ----------

CATALOG = "dq_poc"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")

for schema in ["bronze", "silver", "gold", "dq_framework"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"Schema created: {CATALOG}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Mock Data into Bronze Tables
# MAGIC
# MAGIC Upload the CSV files from `mock_data/` to a Databricks volume or DBFS,
# MAGIC then adjust the paths below.

# COMMAND ----------

import os

# Adjust these paths based on your environment:
# Option A: If files are uploaded to a Unity Catalog Volume
# BASE_PATH = "/Volumes/dq_poc/bronze/mock_data"
# Option B: If files are uploaded to DBFS
# BASE_PATH = "/dbfs/FileStore/dq_framework/mock_data"
# Option C: If using Repos (files are in the repo)
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/mock_data"

# For Databricks notebooks, you may need to use a Workspace path:
# BASE_PATH = "/Workspace/Repos/<user>/Quality_Framework/mock_data"

print(f"Loading mock data from: {BASE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Load Customers

# COMMAND ----------

customers_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{BASE_PATH}/customers.csv")
)

customers_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.bronze.customers")
print(f"Loaded {spark.table(f'{CATALOG}.bronze.customers').count()} customer records to bronze")
customers_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Load Transactions

# COMMAND ----------

transactions_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{BASE_PATH}/transactions.csv")
)

transactions_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.bronze.transactions")
print(f"Loaded {spark.table(f'{CATALOG}.bronze.transactions').count()} transaction records to bronze")
transactions_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Load Products

# COMMAND ----------

products_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{BASE_PATH}/products.csv")
)

products_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.bronze.products")
print(f"Loaded {spark.table(f'{CATALOG}.bronze.products').count()} product records to bronze")
products_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Load Reference Tables

# COMMAND ----------

dim_country_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{BASE_PATH}/dim_country.csv")
)

dim_country_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.silver.dim_country")
print(f"Loaded {spark.table(f'{CATALOG}.silver.dim_country').count()} country reference records")
dim_country_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify Bronze Tables

# COMMAND ----------

print("=== Bronze Tables ===")
for table in ["bronze.customers", "bronze.transactions", "bronze.products"]:
    count = spark.table(f"{CATALOG}.{table}").count()
    print(f"  {CATALOG}.{table}: {count} rows")

print("\n=== Reference Tables ===")
print(f"  {CATALOG}.silver.dim_country: {spark.table(f'{CATALOG}.silver.dim_country').count()} rows")

print("\nSetup complete! Ready to run the DQ framework.")
