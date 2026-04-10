# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Deploy AI/BI Lakeview Dashboard
# MAGIC
# MAGIC Creates the **Data Quality Command Center** AI/BI dashboard programmatically
# MAGIC using the Databricks Lakeview REST API.
# MAGIC
# MAGIC ## Dashboard Pages:
# MAGIC 1. **Executive Overview** — KPI counters, pass rates, score distribution, clean vs quarantine
# MAGIC 2. **Rule Analysis** — Per-rule pass rates, health heatmap, failures by rule type & severity
# MAGIC 3. **Row-Level DQ Detail** — Drill into failing rows per dataset with exact failed rules
# MAGIC 4. **Quarantine Zone** — Records dropped by hard-enforcement rules for review
# MAGIC
# MAGIC ## Prerequisites:
# MAGIC - SDP DLT pipeline has run successfully (tables exist in `dq_poc.silver.*`)
# MAGIC - A SQL warehouse is available

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import json
import os
import requests

# Get workspace context
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_URL = f"https://{ctx.browserHostName().get()}"
TOKEN = ctx.apiToken().get()

# SQL Warehouse ID — replace with your warehouse or auto-detect
WAREHOUSE_ID = None  # Will auto-detect if None

print(f"Workspace: {WORKSPACE_URL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Auto-Detect SQL Warehouse (if needed)

# COMMAND ----------

if WAREHOUSE_ID is None:
    print("Auto-detecting SQL warehouse...")
    response = requests.get(
        f"{WORKSPACE_URL}/api/2.0/sql/warehouses",
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    response.raise_for_status()
    warehouses = response.json().get("warehouses", [])

    # Prefer a running warehouse, else any available
    running = [w for w in warehouses if w.get("state") == "RUNNING"]
    candidates = running if running else warehouses

    if candidates:
        WAREHOUSE_ID = candidates[0]["id"]
        print(f"Using warehouse: {candidates[0]['name']} ({WAREHOUSE_ID})")
    else:
        raise RuntimeError("No SQL warehouses found. Create one first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Dashboard Definition

# COMMAND ----------

# Load the Lakeview dashboard JSON
dashboard_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "dashboards", "lakeview_dq_dashboard.json"
)

with open(dashboard_path, "r") as f:
    dashboard_def = json.load(f)

# Replace warehouse placeholder
dashboard_def["warehouse_id"] = WAREHOUSE_ID

print(f"Dashboard: {dashboard_def['displayName']}")
print(f"  Pages: {len(dashboard_def['pages'])}")
print(f"  Datasets: {len(dashboard_def['datasets'])}")
print(f"  Widgets: {len(dashboard_def['widgets'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Dashboard via Lakeview API

# COMMAND ----------

DASHBOARD_NAME = "Data Quality Command Center — SDP Framework"

# Check if dashboard already exists
list_response = requests.get(
    f"{WORKSPACE_URL}/api/2.0/lakeview/dashboards",
    headers={"Authorization": f"Bearer {TOKEN}"},
    params={"page_size": 100},
)
list_response.raise_for_status()

existing_dashboard = None
for d in list_response.json().get("dashboards", []):
    if d.get("display_name") == DASHBOARD_NAME:
        existing_dashboard = d
        break

# Prepare the serialized dashboard payload
serialized = json.dumps(dashboard_def)

if existing_dashboard:
    dashboard_id = existing_dashboard["dashboard_id"]
    print(f"Dashboard already exists ({dashboard_id}). Updating...")
    response = requests.patch(
        f"{WORKSPACE_URL}/api/2.0/lakeview/dashboards/{dashboard_id}",
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
        },
        json={
            "display_name": DASHBOARD_NAME,
            "warehouse_id": WAREHOUSE_ID,
            "serialized_dashboard": serialized,
        },
    )
else:
    print("Creating new dashboard...")
    response = requests.post(
        f"{WORKSPACE_URL}/api/2.0/lakeview/dashboards",
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
        },
        json={
            "display_name": DASHBOARD_NAME,
            "warehouse_id": WAREHOUSE_ID,
            "serialized_dashboard": serialized,
            "parent_path": "/Shared/DQ_Framework",
        },
    )

response.raise_for_status()
result = response.json()
dashboard_id = result.get("dashboard_id", "unknown")

print(f"\nDashboard deployed successfully!")
print(f"  Dashboard ID: {dashboard_id}")
print(f"  URL: {WORKSPACE_URL}/sql/dashboardsv3/{dashboard_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Publish Dashboard (Make Available to Others)

# COMMAND ----------

print("Publishing dashboard...")
publish_response = requests.post(
    f"{WORKSPACE_URL}/api/2.0/lakeview/dashboards/{dashboard_id}/published",
    headers={
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
    },
    json={
        "warehouse_id": WAREHOUSE_ID,
        "embed_credentials": True,
    },
)

if publish_response.status_code == 200:
    print(f"Dashboard published!")
    print(f"  Published URL: {WORKSPACE_URL}/sql/dashboardsv3/{dashboard_id}/published")
else:
    print(f"Publish returned {publish_response.status_code}: {publish_response.text}")
    print("You can publish manually from the dashboard UI.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Dashboard Summary
# MAGIC
# MAGIC ### Pages & Widgets
# MAGIC
# MAGIC | Page | Widgets | Description |
# MAGIC |------|---------|-------------|
# MAGIC | **Executive Overview** | 8 | KPI counters, DQ score by dataset, clean vs quarantine, score distribution |
# MAGIC | **Rule Analysis** | 5 | Per-rule pass rates (bar), rule health matrix, failures by rule type, severity table |
# MAGIC | **Row-Level DQ Detail** | 3 | Failing rows for customers, transactions, products with exact failed rules |
# MAGIC | **Quarantine Zone** | 3 | Quarantined records per dataset with reasons |
# MAGIC
# MAGIC ### Data Sources (19 datasets)
# MAGIC - `dq_poc.silver.dq_summary_by_dataset` — aggregate DQ scores per dataset
# MAGIC - `dq_poc.silver.dq_summary_by_rule` — per-rule pass/fail across datasets
# MAGIC - `dq_poc.silver.dq_row_level_detail_all` — unified row-level detail
# MAGIC - `dq_poc.silver.customers_validated` / `customers_clean` / `customers_quarantine`
# MAGIC - `dq_poc.silver.transactions_validated` / `transactions_clean` / `transactions_quarantine`
# MAGIC - `dq_poc.silver.products_validated` / `products_clean` / `products_quarantine`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Access
# MAGIC
# MAGIC Run this cell to print the dashboard URL:

# COMMAND ----------

print(f"\n{'='*60}")
print(f"  DASHBOARD URL:")
print(f"  {WORKSPACE_URL}/sql/dashboardsv3/{dashboard_id}")
print(f"{'='*60}")
