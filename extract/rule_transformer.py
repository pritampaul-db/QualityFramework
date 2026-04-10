# Databricks notebook source
# MAGIC %md
# MAGIC # Rule Transformer
# MAGIC Normalizes Collibra-extracted rules into canonical schema for Delta Lake storage.

# COMMAND ----------

import json
import hashlib
from datetime import datetime
from typing import List, Dict, Optional
from jsonschema import validate, ValidationError


class RuleTransformer:
    """
    Transforms raw Collibra rule extracts into the canonical DQ rule schema.
    Validates each rule against the JSON schema before passing through.
    """

    def __init__(self, schema: Optional[dict] = None):
        self.schema = schema
        self.validation_errors = []

    def validate_rule(self, rule: dict) -> bool:
        """Validate a single rule against the JSON schema."""
        if self.schema is None:
            return True
        try:
            validate(instance=rule, schema=self.schema)
            return True
        except ValidationError as e:
            self.validation_errors.append({
                "rule_id": rule.get("rule_id", "UNKNOWN"),
                "error": str(e.message),
                "path": str(e.path),
            })
            return False
        except Exception as e:
            self.validation_errors.append({
                "rule_id": rule.get("rule_id", "UNKNOWN"),
                "error": str(e),
                "path": "",
            })
            return False

    def transform_rule(self, raw_rule: dict) -> dict:
        """Transform a single raw Collibra rule into canonical format."""
        now = datetime.utcnow().isoformat()
        params = raw_rule.get("params", {})

        return {
            "rule_id": raw_rule["rule_id"],
            "rule_name": raw_rule.get("rule_name", ""),
            "rule_type": raw_rule["rule_type"],
            "target_column": raw_rule["column"],
            "params": json.dumps(params),
            "severity": raw_rule["severity"],
            "is_active": raw_rule.get("is_active", True),
            "version": raw_rule.get("version", 1),
            "effective_from": raw_rule.get("effective_from"),
            "effective_to": raw_rule.get("effective_to"),
            "source_system": "collibra",
            "created_at": now,
            "updated_at": now,
            "checksum": hashlib.sha256(
                json.dumps(raw_rule, sort_keys=True).encode()
            ).hexdigest(),
        }

    def transform_mapping(self, raw_mapping: dict) -> dict:
        """Transform a single raw mapping into canonical format."""
        return {
            "mapping_id": raw_mapping["mapping_id"],
            "dataset_name": raw_mapping["dataset_name"],
            "column_name": raw_mapping["column_name"],
            "rule_id": raw_mapping["rule_id"],
            "enforcement_mode": raw_mapping.get("enforcement_mode", "soft"),
            "priority": raw_mapping.get("priority", 99),
            "is_active": raw_mapping.get("is_active", True),
        }

    def transform_rules(self, raw_rules: List[dict]) -> tuple:
        """
        Transform and validate a batch of rules.
        Returns (valid_rules, invalid_rules).
        """
        valid_rules = []
        invalid_rules = []

        for raw_rule in raw_rules:
            if self.validate_rule(raw_rule):
                transformed = self.transform_rule(raw_rule)
                valid_rules.append(transformed)
            else:
                invalid_rules.append(raw_rule)

        print(f"[RuleTransformer] Transformed {len(valid_rules)} valid rules, "
              f"{len(invalid_rules)} invalid rules")

        return valid_rules, invalid_rules

    def transform_mappings(self, raw_mappings: List[dict]) -> List[dict]:
        """Transform a batch of mappings."""
        transformed = [self.transform_mapping(m) for m in raw_mappings]
        print(f"[RuleTransformer] Transformed {len(transformed)} mappings")
        return transformed
