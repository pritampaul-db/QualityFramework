# Databricks notebook source
# MAGIC %md
# MAGIC # Base Rule Executor
# MAGIC Abstract base class for all rule type executors.

# COMMAND ----------

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RuleExecutionResult:
    """Result of executing a single DQ rule."""
    rule_id: str
    rule_type: str
    target_column: str
    status: str  # passed, failed, error, skipped
    total_records: int = 0
    passed_count: int = 0
    failed_count: int = 0
    error_message: Optional[str] = None
    failed_df: Optional[DataFrame] = None
    execution_duration_ms: int = 0

    @property
    def dq_score(self) -> float:
        if self.total_records == 0:
            return 100.0
        return round((self.passed_count / self.total_records) * 100, 2)


class BaseRuleExecutor(ABC):
    """
    Abstract base class for DQ rule executors.
    Each rule type implements its own executor following the Strategy Pattern.
    """

    def __init__(self, rule: dict):
        self.rule = rule
        self.rule_id = rule["rule_id"]
        self.rule_type = rule["rule_type"]
        self.target_column = rule["target_column"]
        self.params = rule.get("params", {})
        self.severity = rule.get("severity", "warning")

    @abstractmethod
    def execute(self, df: DataFrame) -> RuleExecutionResult:
        """
        Execute the rule against a DataFrame.
        Must return a RuleExecutionResult with pass/fail counts
        and optionally the DataFrame of failed records.
        """
        pass

    def _validate_column_exists(self, df: DataFrame) -> bool:
        """Check if the target column exists in the DataFrame."""
        return self.target_column in df.columns

    def _build_error_result(self, message: str) -> RuleExecutionResult:
        """Build an error result for cases where execution cannot proceed."""
        return RuleExecutionResult(
            rule_id=self.rule_id,
            rule_type=self.rule_type,
            target_column=self.target_column,
            status="error",
            error_message=message,
        )

    def _build_skipped_result(self, reason: str) -> RuleExecutionResult:
        """Build a skipped result."""
        return RuleExecutionResult(
            rule_id=self.rule_id,
            rule_type=self.rule_type,
            target_column=self.target_column,
            status="skipped",
            error_message=reason,
        )
