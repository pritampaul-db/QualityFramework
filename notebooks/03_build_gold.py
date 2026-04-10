# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Build Gold Layer
# MAGIC Creates business-ready Gold tables from validated Silver data.

# COMMAND ----------

import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

CATALOG = "dq_poc"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Build Gold Tables

# COMMAND ----------

from pipelines.silver_to_gold import SilverToGoldPipeline

gold_pipeline = SilverToGoldPipeline(spark, catalog=CATALOG)
results = gold_pipeline.build_all()

print("\n=== Gold Build Results ===")
for r in results:
    print(f"  {r['table']}: {r['count']} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. View Gold Tables

# COMMAND ----------

print("=== Customer 360 ===")
spark.table(f"{CATALOG}.gold.customer_360").display()

# COMMAND ----------

print("=== Product Catalog ===")
spark.table(f"{CATALOG}.gold.product_catalog").display()

# COMMAND ----------

print("=== DQ Executive Summary ===")
spark.table(f"{CATALOG}.gold.dq_executive_summary").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Business Insights

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer engagement distribution
# MAGIC SELECT
# MAGIC   engagement_tier,
# MAGIC   COUNT(*) as customer_count,
# MAGIC   ROUND(AVG(total_spend), 2) as avg_spend
# MAGIC FROM dq_poc.gold.customer_360
# MAGIC GROUP BY engagement_tier

# COMMAND ----------

print("\nGold layer build complete! Proceed to notebook 04_dq_dashboards.py")
