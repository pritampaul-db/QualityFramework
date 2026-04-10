# Databricks notebook source
# MAGIC %md
# MAGIC # Rule Registry
# MAGIC Maps rule types to their executor classes using the Strategy Pattern.

# COMMAND ----------

from engine.executors.not_null_executor import NotNullExecutor
from engine.executors.range_executor import RangeExecutor
from engine.executors.regex_executor import RegexExecutor
from engine.executors.uniqueness_executor import UniquenessExecutor
from engine.executors.allowed_values_executor import AllowedValuesExecutor
from engine.executors.length_executor import LengthExecutor
from engine.executors.referential_integrity_executor import ReferentialIntegrityExecutor
from engine.executors.freshness_executor import FreshnessExecutor
from engine.executors.custom_sql_executor import CustomSQLExecutor


# Rule type -> Executor class mapping
RULE_EXECUTOR_REGISTRY = {
    "not_null": NotNullExecutor,
    "range": RangeExecutor,
    "regex": RegexExecutor,
    "uniqueness": UniquenessExecutor,
    "allowed_values": AllowedValuesExecutor,
    "length": LengthExecutor,
    "referential_integrity": ReferentialIntegrityExecutor,
    "freshness": FreshnessExecutor,
    "custom_sql": CustomSQLExecutor,
}

# Rule types that need SparkSession injected
SPARK_REQUIRED_TYPES = {"referential_integrity"}


def get_executor(rule: dict, spark=None):
    """
    Get the appropriate executor for a rule type.
    Raises KeyError if the rule type is not registered.
    """
    rule_type = rule["rule_type"]
    if rule_type not in RULE_EXECUTOR_REGISTRY:
        raise KeyError(
            f"Unknown rule type: '{rule_type}'. "
            f"Available types: {list(RULE_EXECUTOR_REGISTRY.keys())}"
        )

    executor_class = RULE_EXECUTOR_REGISTRY[rule_type]

    if rule_type in SPARK_REQUIRED_TYPES:
        return executor_class(rule, spark=spark)

    return executor_class(rule)


def list_supported_types():
    """Return list of all supported rule types."""
    return list(RULE_EXECUTOR_REGISTRY.keys())
