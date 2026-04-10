# Databricks notebook source
# MAGIC %md
# MAGIC # Collibra API Client
# MAGIC Mock client that simulates Collibra REST API extraction.
# MAGIC In production, replace mock methods with actual API calls.

# COMMAND ----------

import json
import os
import hashlib
from datetime import datetime
from typing import Dict, List, Optional


class CollibraAPIClient:
    """
    Client for extracting DQ rules from Collibra.
    In POC mode, reads from local JSON files simulating API responses.
    In production, makes authenticated REST API calls to Collibra.
    """

    def __init__(self, config: dict, spark=None):
        self.config = config
        self.spark = spark
        self.mock_mode = config.get("collibra", {}).get("mock_mode", True)
        self.base_url = config.get("collibra", {}).get("base_url", "")
        self.api_version = config.get("collibra", {}).get("api_version", "2.0")

        if not self.mock_mode:
            # Production: retrieve credentials from Databricks secrets
            # self.api_key = dbutils.secrets.get(scope="dq-framework", key="collibra-api-key")
            pass

    def _load_mock_file(self, file_path: str) -> dict:
        """Load a mock JSON file simulating Collibra API response."""
        # Resolve relative paths from the config directory
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        full_path = os.path.normpath(os.path.join(base_dir, file_path.lstrip("../")))

        # Try the path as-is first, then try relative to base
        for path in [file_path, full_path, os.path.join(base_dir, file_path)]:
            normalized = os.path.normpath(path)
            if os.path.exists(normalized):
                with open(normalized, "r") as f:
                    return json.load(f)

        raise FileNotFoundError(f"Mock file not found: {file_path}")

    def extract_rules(self) -> List[dict]:
        """
        Extract DQ rules from Collibra.

        In mock mode: reads from local JSON file.
        In production: calls GET /rest/2.0/assets?type=Data Quality Rule
        """
        if self.mock_mode:
            mock_path = self.config["collibra"]["mock_rules_path"]
            data = self._load_mock_file(mock_path)
            rules = data.get("rules", [])
            print(f"[CollibraClient] Extracted {len(rules)} rules (mock mode)")
            return rules

        # Production implementation:
        # import requests
        # url = f"{self.base_url}/rest/{self.api_version}/assets"
        # params = {"typeId": DQ_RULE_TYPE_ID, "offset": 0, "limit": 100}
        # headers = {"Authorization": f"Bearer {self.api_key}"}
        # response = requests.get(url, params=params, headers=headers)
        # response.raise_for_status()
        # return self._parse_collibra_response(response.json())
        raise NotImplementedError("Production Collibra API not configured")

    def extract_rule_mappings(self) -> List[dict]:
        """
        Extract dataset-rule mappings from Collibra.

        In mock mode: reads from local JSON file.
        In production: calls GET /rest/2.0/relations
        """
        if self.mock_mode:
            mock_path = self.config["collibra"]["mock_mappings_path"]
            data = self._load_mock_file(mock_path)
            mappings = data.get("mappings", [])
            print(f"[CollibraClient] Extracted {len(mappings)} mappings (mock mode)")
            return mappings

        raise NotImplementedError("Production Collibra API not configured")

    @staticmethod
    def compute_checksum(rule: dict) -> str:
        """Compute SHA-256 checksum of rule payload for change detection."""
        payload = json.dumps(rule, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()

    def get_extraction_metadata(self) -> dict:
        """Return metadata about the current extraction."""
        return {
            "source": "collibra_mock" if self.mock_mode else "collibra",
            "extracted_at": datetime.utcnow().isoformat(),
            "api_version": self.api_version,
            "mock_mode": self.mock_mode,
        }
