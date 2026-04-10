# Databricks notebook source
# MAGIC %md
# MAGIC # Rule Expression Builder
# MAGIC Converts Collibra DQ rule definitions into SQL expressions that can be used
# MAGIC by DLT expectations and row-level evaluation.
# MAGIC
# MAGIC Each rule type maps to a SQL expression that returns TRUE when the data is valid.

# COMMAND ----------

import json
from typing import List, Dict, Tuple


class RuleExpressionBuilder:
    """
    Converts metadata-driven DQ rules into SQL expressions.

    Each rule becomes a SQL boolean expression:
      - TRUE  = row passes the rule
      - FALSE = row violates the rule

    These expressions are used for:
      1. DLT @dlt.expect / @dlt.expect_all decorators
      2. Row-level boolean annotation columns
      3. Quarantine filtering
    """

    @staticmethod
    def build_expression(rule: dict) -> str:
        """
        Convert a single rule into a SQL expression string.

        Args:
            rule: Dict with rule_type, column, params, etc.

        Returns:
            SQL expression string that evaluates to TRUE when the row is valid.
        """
        rule_type = rule["rule_type"]
        column = rule["column"]
        params = rule.get("params", {})

        builder_map = {
            "not_null": RuleExpressionBuilder._not_null,
            "range": RuleExpressionBuilder._range,
            "regex": RuleExpressionBuilder._regex,
            "uniqueness": RuleExpressionBuilder._uniqueness,
            "allowed_values": RuleExpressionBuilder._allowed_values,
            "length": RuleExpressionBuilder._length,
            "freshness": RuleExpressionBuilder._freshness,
            "referential_integrity": RuleExpressionBuilder._referential_integrity,
            "custom_sql": RuleExpressionBuilder._custom_sql,
        }

        if rule_type not in builder_map:
            raise ValueError(
                f"Unsupported rule type: '{rule_type}'. "
                f"Supported: {list(builder_map.keys())}"
            )

        return builder_map[rule_type](column, params)

    @staticmethod
    def _not_null(column: str, params: dict) -> str:
        return f"`{column}` IS NOT NULL"

    @staticmethod
    def _range(column: str, params: dict) -> str:
        min_val = params.get("min")
        max_val = params.get("max")
        conditions = []
        if min_val is not None:
            conditions.append(f"`{column}` >= {min_val}")
        if max_val is not None:
            conditions.append(f"`{column}` <= {max_val}")
        if not conditions:
            return "TRUE"
        return f"({' AND '.join(conditions)})"

    @staticmethod
    def _regex(column: str, params: dict) -> str:
        pattern = params.get("pattern", ".*")
        # Escape single quotes in the pattern
        pattern_escaped = pattern.replace("'", "\\'")
        return f"`{column}` RLIKE '{pattern_escaped}'"

    @staticmethod
    def _uniqueness(column: str, params: dict) -> str:
        # Uniqueness cannot be fully expressed as a row-level SQL expression.
        # We handle it via a window function expression:
        #   COUNT(*) OVER (PARTITION BY column) = 1
        # This marks duplicate rows as failing.
        return (
            f"(COUNT(1) OVER (PARTITION BY `{column}`)) = 1"
        )

    @staticmethod
    def _allowed_values(column: str, params: dict) -> str:
        values = params.get("values", [])
        if not values:
            return "TRUE"
        quoted = ", ".join(f"'{v}'" for v in values)
        return f"`{column}` IN ({quoted})"

    @staticmethod
    def _length(column: str, params: dict) -> str:
        min_len = params.get("min_length")
        max_len = params.get("max_length")
        conditions = []
        if min_len is not None:
            conditions.append(f"LENGTH(`{column}`) >= {min_len}")
        if max_len is not None:
            conditions.append(f"LENGTH(`{column}`) <= {max_len}")
        if not conditions:
            return "TRUE"
        return f"({' AND '.join(conditions)})"

    @staticmethod
    def _freshness(column: str, params: dict) -> str:
        max_age_hours = params.get("max_age_hours", 24)
        return (
            f"(TIMESTAMPDIFF(HOUR, CAST(`{column}` AS TIMESTAMP), CURRENT_TIMESTAMP()) "
            f"<= {max_age_hours})"
        )

    @staticmethod
    def _referential_integrity(column: str, params: dict) -> str:
        # Referential integrity needs a subquery or join.
        # In DLT, we express it as an EXISTS check against the reference table.
        ref_table = params.get("ref_table", "")
        ref_column = params.get("ref_column", column)
        if not ref_table:
            return "TRUE"
        # Use a live. prefix for DLT references within the pipeline,
        # or fully qualified name for external tables.
        return (
            f"`{column}` IN (SELECT `{ref_column}` FROM live.{ref_table})"
        )

    @staticmethod
    def _custom_sql(column: str, params: dict) -> str:
        expression = params.get("sql_expression", "TRUE")
        return f"({expression})"

    @staticmethod
    def build_expectations_dict(
        rules: List[dict],
        enforcement_filter: str = None,
        mappings: List[dict] = None,
    ) -> Dict[str, str]:
        """
        Build a dict of {rule_name: sql_expression} suitable for
        @dlt.expect_all or @dlt.expect_all_or_drop.

        Args:
            rules: List of rule dicts (from dq_rules.json)
            enforcement_filter: "hard" or "soft" to filter by enforcement mode.
                                If None, includes all rules.
            mappings: Optional list of mapping dicts for enforcement info.

        Returns:
            Dict of {rule_display_name: sql_expression}
        """
        # Build mapping lookup: rule_id -> mapping info
        mapping_lookup = {}
        if mappings:
            for m in mappings:
                mapping_lookup[m["rule_id"]] = m

        expectations = {}
        for rule in rules:
            rule_id = rule["rule_id"]

            # Filter by enforcement if specified
            if enforcement_filter and rule_id in mapping_lookup:
                if mapping_lookup[rule_id].get("enforcement_mode") != enforcement_filter:
                    continue

            # Skip uniqueness for expect_all (needs window function, handled separately)
            if rule["rule_type"] == "uniqueness":
                continue

            # Skip referential_integrity for expect_all (needs subquery)
            if rule["rule_type"] == "referential_integrity":
                continue

            try:
                expr = RuleExpressionBuilder.build_expression(rule)
                # Use rule_id as the key for traceability
                display_name = f"{rule_id}__{rule.get('rule_name', rule['rule_type'])}"
                expectations[display_name] = expr
            except Exception as e:
                print(f"[RuleExpressionBuilder] WARN: Skipping rule {rule_id}: {e}")

        return expectations

    @staticmethod
    def build_all_expressions(
        rules: List[dict],
        mappings: List[dict] = None,
    ) -> List[Dict]:
        """
        Build complete expression list with metadata for all rules.

        Returns:
            List of dicts with: rule_id, rule_name, rule_type, column, severity,
            enforcement_mode, sql_expression, is_row_evaluable
        """
        mapping_lookup = {}
        if mappings:
            for m in mappings:
                mapping_lookup[m["rule_id"]] = m

        result = []
        for rule in rules:
            rule_id = rule["rule_id"]
            mapping = mapping_lookup.get(rule_id, {})

            try:
                expr = RuleExpressionBuilder.build_expression(rule)
                result.append({
                    "rule_id": rule_id,
                    "rule_name": rule.get("rule_name", ""),
                    "rule_type": rule["rule_type"],
                    "column": rule["column"],
                    "severity": rule.get("severity", "warning"),
                    "enforcement_mode": mapping.get("enforcement_mode", "soft"),
                    "sql_expression": expr,
                    "is_row_evaluable": rule["rule_type"] not in (
                        "uniqueness", "referential_integrity"
                    ),
                })
            except Exception as e:
                print(f"[RuleExpressionBuilder] ERROR building {rule_id}: {e}")

        return result
