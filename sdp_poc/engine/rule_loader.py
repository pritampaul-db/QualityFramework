# Databricks notebook source
# MAGIC %md
# MAGIC # SDP Rule Loader
# MAGIC Loads DQ rules and mappings from Collibra mock JSON files (or Delta tables)
# MAGIC and prepares them for the DLT expectation framework.

# COMMAND ----------

import json
import os
from typing import List, Dict, Tuple


class SDPRuleLoader:
    """
    Loads DQ rules and maps them to datasets.
    Reuses the same mock_collibra JSON files as the original framework.
    Can also load from Delta tables when running in Databricks.
    """

    def __init__(
        self,
        rules_path: str = None,
        mappings_path: str = None,
        spark=None,
        catalog: str = "dq_poc",
        use_delta: bool = False,
    ):
        self.spark = spark
        self.catalog = catalog
        self.use_delta = use_delta
        self._rules = []
        self._mappings = []

        if use_delta and spark:
            self._load_from_delta()
        else:
            # Resolve base path — works in both local and Databricks environments
            base = self._find_repo_root()
            self._rules_path = rules_path or os.path.join(
                base, "mock_collibra", "dq_rules.json"
            )
            self._mappings_path = mappings_path or os.path.join(
                base, "mock_collibra", "dq_rule_mappings.json"
            )
            self._load_from_json()

    @staticmethod
    def _find_repo_root():
        """Find the repo root path, handling both local and Databricks environments."""
        # Try __file__ first (works locally)
        try:
            return os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.abspath(__file__)
            )))
        except NameError:
            pass

        # Databricks Workspace path — try common locations
        workspace_paths = [
            "/Workspace/Users/pritam.paul@databricks.com/Quality_Framework",
            "/Workspace/Repos/pritam.paul@databricks.com/Quality_Framework",
        ]
        for wp in workspace_paths:
            rules_file = os.path.join(wp, "mock_collibra", "dq_rules.json")
            if os.path.exists(rules_file):
                return wp

        # Fallback: try current working directory
        cwd = os.getcwd()
        if os.path.exists(os.path.join(cwd, "mock_collibra", "dq_rules.json")):
            return cwd

        raise FileNotFoundError(
            "Cannot locate mock_collibra/dq_rules.json. "
            "Provide explicit rules_path or use use_delta=True."
        )

    def _load_from_json(self):
        """Load rules and mappings from JSON files."""
        with open(self._rules_path, "r") as f:
            data = json.load(f)
            self._rules = data.get("rules", [])

        with open(self._mappings_path, "r") as f:
            data = json.load(f)
            self._mappings = data.get("mappings", [])

        print(f"[SDPRuleLoader] Loaded {len(self._rules)} rules, "
              f"{len(self._mappings)} mappings from JSON")

    def _load_from_delta(self):
        """Load rules and mappings from Delta tables."""
        rules_df = self.spark.table(
            f"{self.catalog}.dq_framework.dq_rules"
        )
        for row in rules_df.collect():
            r = row.asDict()
            if r.get("params"):
                r["params"] = json.loads(r["params"])
            else:
                r["params"] = {}
            # Normalize column name (Delta uses target_column)
            if "target_column" in r and "column" not in r:
                r["column"] = r["target_column"]
            self._rules.append(r)

        mappings_df = self.spark.table(
            f"{self.catalog}.dq_framework.dq_rule_mapping"
        )
        for row in mappings_df.collect():
            self._mappings.append(row.asDict())

        print(f"[SDPRuleLoader] Loaded {len(self._rules)} rules, "
              f"{len(self._mappings)} mappings from Delta")

    def get_rules_for_dataset(self, dataset_name: str) -> Tuple[List[dict], List[dict]]:
        """
        Get rules and their mappings for a specific dataset.

        Args:
            dataset_name: e.g. "bronze.customers"

        Returns:
            (rules, mappings) - filtered and matched lists
        """
        # Find mapping entries for this dataset
        dataset_mappings = [
            m for m in self._mappings
            if m.get("dataset_name") == dataset_name and m.get("is_active", True)
        ]

        # Get the rule IDs we need
        rule_ids = {m["rule_id"] for m in dataset_mappings}

        # Filter rules
        dataset_rules = [
            r for r in self._rules
            if r.get("rule_id") in rule_ids and r.get("is_active", True)
        ]

        # Sort by priority from mappings
        priority_map = {m["rule_id"]: m.get("priority", 999) for m in dataset_mappings}
        dataset_rules.sort(key=lambda r: priority_map.get(r["rule_id"], 999))

        print(f"[SDPRuleLoader] Dataset '{dataset_name}': "
              f"{len(dataset_rules)} rules, {len(dataset_mappings)} mappings")

        return dataset_rules, dataset_mappings

    def get_all_rules(self) -> Tuple[List[dict], List[dict]]:
        """Return all rules and mappings."""
        return self._rules, self._mappings
