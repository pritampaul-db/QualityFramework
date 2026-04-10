# Databricks notebook source
# MAGIC %md
# MAGIC # Collibra Writeback (Feedback Loop)
# MAGIC Pushes DQ execution results back to Collibra for governance visibility.
# MAGIC In POC mode, simulates the API call and logs the payload.

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql import SparkSession
from typing import List, Optional


class CollibraWriteback:
    """
    Pushes DQ execution results back to Collibra.
    Enables the closed-loop governance feedback system.

    In production, this writes to Collibra via REST API:
      PUT /rest/2.0/assets/{assetId}/attributes
    """

    def __init__(self, spark: SparkSession, catalog: str = "dq_poc", mock_mode: bool = True):
        self.spark = spark
        self.catalog = catalog
        self.mock_mode = mock_mode

    def generate_feedback_payload(self, run_id: Optional[str] = None) -> List[dict]:
        """
        Generate feedback payloads from the latest DQ execution results.
        Each payload represents one rule's execution status to push back to Collibra.
        """
        filter_clause = f"AND run_id = '{run_id}'" if run_id else ""

        results = self.spark.sql(f"""
            SELECT
                e.rule_id,
                e.dataset_name,
                e.status,
                e.dq_score,
                e.failed_count,
                e.total_records,
                e.severity,
                e.executed_at,
                s.overall_dq_score as dataset_dq_score
            FROM {self.catalog}.dq_framework.dq_execution_log e
            LEFT JOIN {self.catalog}.dq_framework.dq_scores s
                ON e.run_id = s.run_id
                AND e.dataset_name = s.dataset_name
            WHERE 1=1 {filter_clause}
            ORDER BY e.dataset_name, e.rule_id
        """).collect()

        payloads = []
        for row in results:
            payload = {
                "collibra_asset_id": row["rule_id"],
                "attributes": {
                    "Last Execution Status": row["status"],
                    "Last DQ Score": row["dq_score"],
                    "Last Failed Count": row["failed_count"],
                    "Last Total Records": row["total_records"],
                    "Last Execution Date": str(row["executed_at"]),
                    "Dataset DQ Score": row["dataset_dq_score"],
                    "SLA Violation": row["dq_score"] < 80 if row["severity"] == "error" else False,
                },
                "dataset": row["dataset_name"],
            }
            payloads.append(payload)

        return payloads

    def push_to_collibra(self, run_id: Optional[str] = None) -> dict:
        """
        Push DQ results back to Collibra.

        In mock mode: logs the payload that would be sent.
        In production: makes PUT API calls to update rule asset attributes.
        """
        payloads = self.generate_feedback_payload(run_id)

        if not payloads:
            print("[Writeback] No results to push")
            return {"status": "no_data", "count": 0}

        if self.mock_mode:
            print(f"\n[Writeback] MOCK MODE - Would push {len(payloads)} updates to Collibra")
            print(f"[Writeback] Sample payload:")
            print(json.dumps(payloads[0], indent=2, default=str))

            # Summary by dataset
            datasets = {}
            sla_violations = 0
            for p in payloads:
                ds = p["dataset"]
                if ds not in datasets:
                    datasets[ds] = {"count": 0, "avg_score": 0, "scores": []}
                datasets[ds]["count"] += 1
                datasets[ds]["scores"].append(p["attributes"]["Last DQ Score"])
                if p["attributes"]["SLA Violation"]:
                    sla_violations += 1

            print(f"\n[Writeback] Summary:")
            print(f"  Total updates: {len(payloads)}")
            print(f"  SLA Violations: {sla_violations}")
            for ds, info in datasets.items():
                avg = sum(info["scores"]) / len(info["scores"])
                print(f"  {ds}: {info['count']} rules, avg DQ score: {avg:.1f}%")

            return {
                "status": "mock_success",
                "count": len(payloads),
                "sla_violations": sla_violations,
            }

        # Production implementation:
        # import requests
        # success = 0
        # failures = 0
        # for payload in payloads:
        #     url = f"{base_url}/rest/2.0/assets/{payload['collibra_asset_id']}/attributes"
        #     response = requests.put(url, json=payload["attributes"], headers=headers)
        #     if response.ok:
        #         success += 1
        #     else:
        #         failures += 1
        # return {"status": "completed", "success": success, "failures": failures}
        raise NotImplementedError("Production Collibra writeback not configured")
