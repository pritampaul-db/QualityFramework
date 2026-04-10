from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from engine.base_rule_executor import BaseRuleExecutor, RuleExecutionResult


class AllowedValuesExecutor(BaseRuleExecutor):
    """Validates that a column value is within a predefined set of allowed values."""

    def execute(self, df: DataFrame) -> RuleExecutionResult:
        if not self._validate_column_exists(df):
            return self._build_error_result(
                f"Column '{self.target_column}' not found in DataFrame"
            )

        allowed = self.params.get("values")
        if not allowed or not isinstance(allowed, list):
            return self._build_error_result(
                "Allowed values rule requires 'values' param as a list"
            )

        total = df.count()
        failed_df = df.filter(
            col(self.target_column).isNull()
            | ~col(self.target_column).isin(allowed)
        )
        failed_count = failed_df.count()

        return RuleExecutionResult(
            rule_id=self.rule_id,
            rule_type=self.rule_type,
            target_column=self.target_column,
            status="failed" if failed_count > 0 else "passed",
            total_records=total,
            passed_count=total - failed_count,
            failed_count=failed_count,
            failed_df=failed_df if failed_count > 0 else None,
        )
