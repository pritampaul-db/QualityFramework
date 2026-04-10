# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Generate Large Mock Dataset for Performance Testing
# MAGIC
# MAGIC Generates realistic synthetic data with intentional DQ issues at configurable scale.
# MAGIC
# MAGIC **Default: 1M customers, 5M transactions, 100K products**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "pritam_demo_workspace_catalog"
NUM_CUSTOMERS = 1_000_000
NUM_TRANSACTIONS = 5_000_000
NUM_PRODUCTS = 100_000

# Percentage of rows with DQ issues (per rule type)
DQ_ERROR_RATE = 0.05  # 5% of rows will have at least one issue

spark.sql(f"USE CATALOG {CATALOG}")
print(f"Generating: {NUM_CUSTOMERS:,} customers, {NUM_TRANSACTIONS:,} transactions, {NUM_PRODUCTS:,} products")
print(f"DQ error rate: {DQ_ERROR_RATE*100}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate Customers

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random

customers_df = (
    spark.range(0, NUM_CUSTOMERS)
    .withColumn("customer_id", F.concat(F.lit("C"), F.lpad(F.col("id").cast("string"), 7, "0")))
    .withColumn("customer_name",
        F.concat(
            F.element_at(F.array(*[F.lit(n) for n in ["Alice","Bob","Carol","Dave","Eve","Frank","Grace","Hank","Iris","Jack","Kate","Leo","Mia","Noah","Olivia","Pete","Quinn","Rose","Sam","Tina","Uma","Vic","Wendy","Xander","Yara","Zoe"]]), (F.abs(F.hash(F.col("id"))) % 26).cast("int") + 1),
            F.lit(" "),
            F.element_at(F.array(*[F.lit(n) for n in ["Smith","Jones","Brown","Wilson","Taylor","Clark","Lewis","Hall","Young","King","Wright","Scott","Green","Baker","Adams","Nelson","Hill","Moore","White","Allen"]]), (F.abs(F.hash(F.col("id") + 1)) % 20).cast("int") + 1)
        )
    )
    .withColumn("email",
        F.concat(
            F.lower(F.concat(
                F.element_at(F.array(*[F.lit(n) for n in ["alice","bob","carol","dave","eve","frank","grace","hank","iris","jack"]]), (F.abs(F.hash(F.col("id"))) % 10).cast("int") + 1),
                F.lit("."),
                F.element_at(F.array(*[F.lit(n) for n in ["smith","jones","brown","wilson","taylor","clark","lewis","hall","young","king"]]), (F.abs(F.hash(F.col("id") + 1)) % 10).cast("int") + 1)
            )),
            F.col("id").cast("string"),
            F.lit("@"),
            F.element_at(F.array(*[F.lit(d) for d in ["gmail.com","yahoo.com","outlook.com","company.com","example.com"]]), (F.abs(F.hash(F.col("id") + 2)) % 5).cast("int") + 1)
        )
    )
    .withColumn("age", (F.abs(F.hash(F.col("id") + 3)) % 62 + 18).cast("int"))
    .withColumn("phone", F.concat(F.lit("555"), F.lpad((F.abs(F.hash(F.col("id") + 4)) % 10000000).cast("string"), 7, "0")).cast("string"))
    .withColumn("status",
        F.element_at(F.array(*[F.lit(s) for s in ["active","active","active","active","inactive","suspended","pending"]]),
                     (F.abs(F.hash(F.col("id") + 5)) % 7).cast("int") + 1)
    )
    .withColumn("country_code",
        F.element_at(F.array(*[F.lit(c) for c in ["US","US","US","UK","CA","DE","FR","JP","CN","AU","IN","BR"]]),
                     (F.abs(F.hash(F.col("id") + 6)) % 12).cast("int") + 1)
    )
    .withColumn("created_at", F.date_sub(F.current_date(), (F.abs(F.hash(F.col("id") + 7)) % 365).cast("int")))
    .drop("id")
)

# Inject DQ issues
error_threshold = int(NUM_CUSTOMERS * DQ_ERROR_RATE)

customers_df = (
    customers_df
    .withColumn("_row_num", F.monotonically_increasing_id())
    # NULL customer_name (not_null violation)
    .withColumn("customer_name", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 1, F.lit(None)).otherwise(F.col("customer_name")))
    # NULL email (not_null violation)
    .withColumn("email", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 2, F.lit(None)).otherwise(F.col("email")))
    # Invalid age (range violation: age < 18 or > 120)
    .withColumn("age", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 3, F.lit(150)).otherwise(F.col("age")))
    # Invalid email format (regex violation)
    .withColumn("email", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 4, F.lit("invalid-email-format")).otherwise(F.col("email")))
    # Short phone (length violation)
    .withColumn("phone", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 5, F.lit("12345")).otherwise(F.col("phone")).cast("string"))
    # Invalid status (allowed_values violation)
    .withColumn("status", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 6, F.lit("deleted")).otherwise(F.col("status")))
    # Invalid country code (referential integrity violation)
    .withColumn("country_code", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 7, F.lit("ZZ")).otherwise(F.col("country_code")))
    .drop("_row_num")
)

# Ensure all string columns are explicitly cast to avoid merge field errors
for col_name in ["customer_id", "customer_name", "email", "phone", "status", "country_code"]:
    customers_df = customers_df.withColumn(col_name, F.col(col_name).cast("string"))

# Write to landing zone as Parquet files (for Auto Loader ingestion)
LANDING_BASE = f"/Volumes/{CATALOG}/bronze/landing_zone"
customers_df.write.format("parquet").mode("overwrite").save(f"{LANDING_BASE}/customers/")
count = spark.read.parquet(f"{LANDING_BASE}/customers/").count()
print(f"Customers: {count:,} rows written to {LANDING_BASE}/customers/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate Transactions

# COMMAND ----------

transactions_df = (
    spark.range(0, NUM_TRANSACTIONS)
    .withColumn("transaction_id", F.concat(F.lit("T"), F.lpad(F.col("id").cast("string"), 8, "0")))
    .withColumn("customer_id",
        F.concat(F.lit("C"), F.lpad((F.abs(F.hash(F.col("id"))) % NUM_CUSTOMERS).cast("string"), 7, "0"))
    )
    .withColumn("amount", F.round((F.abs(F.hash(F.col("id") + 1)) % 5000 + 1).cast("double"), 2))
    .withColumn("transaction_type",
        F.element_at(F.array(*[F.lit(t) for t in ["purchase","purchase","purchase","refund","transfer","withdrawal","deposit"]]),
                     (F.abs(F.hash(F.col("id") + 2)) % 7).cast("int") + 1)
    )
    .withColumn("transaction_date",
        F.date_sub(F.current_date(), (F.abs(F.hash(F.col("id") + 3)) % 20).cast("int")).cast("string")
    )
    .withColumn("quantity", (F.abs(F.hash(F.col("id") + 4)) % 100 + 1).cast("int"))
    .withColumn("currency",
        F.element_at(F.array(*[F.lit(c) for c in ["USD","USD","USD","EUR","GBP","JPY"]]),
                     (F.abs(F.hash(F.col("id") + 5)) % 6).cast("int") + 1)
    )
    .withColumn("merchant",
        F.element_at(F.array(*[F.lit(m) for m in ["Amazon","Walmart","Target","Costco","BestBuy","HomeDepot","Apple","Nike","Starbucks","Uber"]]),
                     (F.abs(F.hash(F.col("id") + 6)) % 10).cast("int") + 1)
    )
    .drop("id")
)

# Inject DQ issues
transactions_df = (
    transactions_df
    .withColumn("_row_num", F.monotonically_increasing_id())
    # NULL amount
    .withColumn("amount", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 1, F.lit(None).cast("double")).otherwise(F.col("amount")))
    # Negative amount (range violation)
    .withColumn("amount", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 2, F.lit(-100.0)).otherwise(F.col("amount")))
    # Amount too large (range violation)
    .withColumn("amount", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 3, F.lit(2000000.0)).otherwise(F.col("amount")))
    # Invalid transaction type
    .withColumn("transaction_type", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 4, F.lit("invalid_type")).otherwise(F.col("transaction_type")))
    # NULL transaction_date
    .withColumn("transaction_date", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 5, F.lit(None).cast("string")).otherwise(F.col("transaction_date")))
    # Invalid customer_id (referential integrity violation)
    .withColumn("customer_id", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 6, F.lit("C9999999")).otherwise(F.col("customer_id")))
    # Zero quantity (custom_sql violation: quantity > 0)
    .withColumn("quantity", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 7, F.lit(0)).otherwise(F.col("quantity")).cast("int"))
    .drop("_row_num")
)

for col_name in ["transaction_id", "customer_id", "transaction_type", "transaction_date", "currency", "merchant"]:
    transactions_df = transactions_df.withColumn(col_name, F.col(col_name).cast("string"))
transactions_df = transactions_df.withColumn("amount", F.col("amount").cast("double"))

transactions_df.write.format("parquet").mode("overwrite").save(f"{LANDING_BASE}/transactions/")
count = spark.read.parquet(f"{LANDING_BASE}/transactions/").count()
print(f"Transactions: {count:,} rows written to {LANDING_BASE}/transactions/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate Products

# COMMAND ----------

products_df = (
    spark.range(0, NUM_PRODUCTS)
    .withColumn("product_id", F.concat(F.lit("P"), F.lpad(F.col("id").cast("string"), 6, "0")))
    .withColumn("product_name",
        F.concat(
            F.element_at(F.array(*[F.lit(a) for a in ["Premium","Ultra","Basic","Pro","Elite","Standard","Deluxe","Classic","Smart","Eco"]]),
                         (F.abs(F.hash(F.col("id"))) % 10).cast("int") + 1),
            F.lit(" "),
            F.element_at(F.array(*[F.lit(n) for n in ["Widget","Gadget","Device","Tool","Sensor","Module","Unit","Pack","Kit","Set"]]),
                         (F.abs(F.hash(F.col("id") + 1)) % 10).cast("int") + 1),
            F.lit(" "),
            F.col("id").cast("string")
        )
    )
    .withColumn("sku",
        F.concat(
            F.element_at(F.array(*[F.lit(c) for c in ["ELC","MEC","SFT","HRD","ACC"]]),
                         (F.abs(F.hash(F.col("id") + 2)) % 5).cast("int") + 1),
            F.lit("-"),
            F.lpad((F.abs(F.hash(F.col("id") + 3)) % 10000).cast("string"), 4, "0")
        )
    )
    .withColumn("price", F.round((F.abs(F.hash(F.col("id") + 4)) % 999 + 1).cast("double"), 2))
    .withColumn("category",
        F.element_at(F.array(*[F.lit(c) for c in ["electronics","clothing","home","sports","toys","books","food","beauty"]]),
                     (F.abs(F.hash(F.col("id") + 5)) % 8).cast("int") + 1)
    )
    .withColumn("stock_quantity", (F.abs(F.hash(F.col("id") + 6)) % 1000).cast("int"))
    .withColumn("supplier",
        F.element_at(F.array(*[F.lit(s) for s in ["SupplierA","SupplierB","SupplierC","SupplierD","SupplierE"]]),
                     (F.abs(F.hash(F.col("id") + 7)) % 5).cast("int") + 1)
    )
    .withColumn("created_at", F.date_sub(F.current_date(), (F.abs(F.hash(F.col("id") + 8)) % 365).cast("int")))
    .drop("id")
)

# Inject DQ issues
products_df = (
    products_df
    .withColumn("_row_num", F.monotonically_increasing_id())
    # NULL product_name
    .withColumn("product_name", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 1, F.lit(None)).otherwise(F.col("product_name")))
    # Negative price (range violation)
    .withColumn("price", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 2, F.lit(-19.99)).otherwise(F.col("price")))
    # Price too high (range violation)
    .withColumn("price", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 3, F.lit(150000.0)).otherwise(F.col("price")))
    # Invalid category
    .withColumn("category", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 4, F.lit("gaming")).otherwise(F.col("category")))
    # Invalid SKU format
    .withColumn("sku", F.when(F.col("_row_num") % F.lit(int(1/DQ_ERROR_RATE)) == 5, F.lit("INVALID_SKU")).otherwise(F.col("sku")))
    .drop("_row_num")
)

for col_name in ["product_id", "product_name", "sku", "category", "supplier"]:
    products_df = products_df.withColumn(col_name, F.col(col_name).cast("string"))
products_df = products_df.withColumn("price", F.col("price").cast("double"))

products_df.write.format("parquet").mode("overwrite").save(f"{LANDING_BASE}/products/")
count = spark.read.parquet(f"{LANDING_BASE}/products/").count()
print(f"Products: {count:,} rows written to {LANDING_BASE}/products/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Summary

# COMMAND ----------

print("=" * 60)
print("  Large Dataset Generation Complete")
print("=" * 60)
for name in ["customers", "transactions", "products"]:
    path = f"{LANDING_BASE}/{name}/"
    count = spark.read.parquet(path).count()
    print(f"  {name:20s} {count:>12,} rows  -> {path}")
print("=" * 60)
print(f"\nDQ error rate: ~{DQ_ERROR_RATE*100}% per rule type")
print(f"Landing zone: {LANDING_BASE}")
print("Ready to run the dynamic DQ pipeline with Auto Loader!")
