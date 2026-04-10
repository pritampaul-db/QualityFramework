# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Deploy Dynamic SDP Pipeline
# MAGIC
# MAGIC Creates and starts the DLT pipeline programmatically.

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient

CATALOG = "dq_poc"
PIPELINE_NAME = "dq_framework_sdp_dynamic"
REPO_BASE = "/Workspace/Users/pritam.paul@databricks.com/Quality_Framework"

pipeline_config = {
    "name": PIPELINE_NAME,
    "catalog": CATALOG,
    "target": "silver",
    "development": True,
    "continuous": False,
    "photon": True,
    "channel": "CURRENT",
    "libraries": [
        {"notebook": {"path": f"{REPO_BASE}/sdp_dynamic/pipelines/sdp_bronze_dynamic"}},
        {"notebook": {"path": f"{REPO_BASE}/sdp_dynamic/pipelines/sdp_dq_dynamic"}},
        {"notebook": {"path": f"{REPO_BASE}/sdp_dynamic/pipelines/sdp_gold_dynamic"}},
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
}

print("Pipeline configuration:")
print(json.dumps(pipeline_config, indent=2))

# COMMAND ----------

w = WorkspaceClient()

existing = None
for p in w.pipelines.list_pipelines():
    if p.name == PIPELINE_NAME:
        existing = p
        break

if existing:
    print(f"Pipeline '{PIPELINE_NAME}' exists (ID: {existing.pipeline_id}), updating...")
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
    )
    pipeline_id = existing.pipeline_id
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
    )
    pipeline_id = result.pipeline_id
    print(f"Created with ID: {pipeline_id}")

# COMMAND ----------

import time

print(f"Starting pipeline {pipeline_id}...")
update = w.pipelines.start_update(pipeline_id=pipeline_id)
print(f"Update started: {update.update_id}")

while True:
    state = w.pipelines.get(pipeline_id=pipeline_id)
    status = state.latest_updates[0].state.value if state.latest_updates else "UNKNOWN"
    print(f"  Status: {status}")
    if status in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(15)

if status == "COMPLETED":
    print(f"\nPipeline completed! Tables in {CATALOG}.silver.*")
else:
    print(f"\nPipeline ended: {status}. Check DLT UI for details.")
