# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Deploy & Run DLT Pipeline
# MAGIC
# MAGIC Creates and starts a Delta Live Tables pipeline programmatically
# MAGIC using the Databricks SDK.
# MAGIC
# MAGIC **Alternative:** You can also create the pipeline manually in the UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pipeline Configuration

# COMMAND ----------

import json

CATALOG = "dq_poc"
PIPELINE_NAME = "dq_framework_sdp"

# Adjust this to your workspace repo path
# For Databricks Repos:
REPO_BASE = "/Workspace/Repos/<your-user>/Quality_Framework"
# For Databricks Git Folders:
# REPO_BASE = "/Workspace/Users/<your-email>/Quality_Framework"

pipeline_config = {
    "name": PIPELINE_NAME,
    "catalog": CATALOG,
    "target": "silver",
    "development": True,
    "continuous": False,
    "photon": True,
    "channel": "CURRENT",
    "libraries": [
        {"notebook": {"path": f"{REPO_BASE}/sdp_poc/pipelines/sdp_bronze_sources"}},
        {"notebook": {"path": f"{REPO_BASE}/sdp_poc/pipelines/sdp_dq_pipeline"}},
        {"notebook": {"path": f"{REPO_BASE}/sdp_poc/pipelines/sdp_gold_pipeline"}},
    ],
    "clusters": [
        {
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 4,
                "mode": "ENHANCED",
            },
        }
    ],
    "configuration": {
        "pipelines.applyChangesPreviewEnabled": "true",
    },
}

print("Pipeline configuration:")
print(json.dumps(pipeline_config, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create or Update Pipeline

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Check if pipeline already exists
existing = None
for p in w.pipelines.list_pipelines():
    if p.name == PIPELINE_NAME:
        existing = p
        break

if existing:
    print(f"Pipeline '{PIPELINE_NAME}' already exists (ID: {existing.pipeline_id})")
    print("Updating configuration...")
    w.pipelines.update(
        pipeline_id=existing.pipeline_id,
        name=pipeline_config["name"],
        catalog=pipeline_config["catalog"],
        target=pipeline_config["target"],
        development=pipeline_config["development"],
        continuous=pipeline_config["continuous"],
        photon=pipeline_config["photon"],
        channel=pipeline_config["channel"],
        libraries=pipeline_config["libraries"],
        clusters=pipeline_config["clusters"],
        configuration=pipeline_config["configuration"],
    )
    pipeline_id = existing.pipeline_id
    print("Pipeline updated.")
else:
    print(f"Creating new pipeline '{PIPELINE_NAME}'...")
    result = w.pipelines.create(
        name=pipeline_config["name"],
        catalog=pipeline_config["catalog"],
        target=pipeline_config["target"],
        development=pipeline_config["development"],
        continuous=pipeline_config["continuous"],
        photon=pipeline_config["photon"],
        channel=pipeline_config["channel"],
        libraries=pipeline_config["libraries"],
        clusters=pipeline_config["clusters"],
        configuration=pipeline_config["configuration"],
    )
    pipeline_id = result.pipeline_id
    print(f"Pipeline created with ID: {pipeline_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Start Pipeline

# COMMAND ----------

print(f"Starting pipeline {pipeline_id}...")
update = w.pipelines.start_update(pipeline_id=pipeline_id)
print(f"Pipeline update started: {update.update_id}")
print(f"\nMonitor at: #/pipelines/{pipeline_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Wait for Completion (Optional)

# COMMAND ----------

import time

print("Waiting for pipeline to complete...")
while True:
    state = w.pipelines.get(pipeline_id=pipeline_id)
    status = state.latest_updates[0].state if state.latest_updates else "UNKNOWN"
    print(f"  Status: {status}")

    if status in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(15)

if status == "COMPLETED":
    print("\nPipeline completed successfully!")
    print("Output tables available in dq_poc.silver.*")
else:
    print(f"\nPipeline ended with status: {status}")
    print("Check the DLT UI for error details.")
