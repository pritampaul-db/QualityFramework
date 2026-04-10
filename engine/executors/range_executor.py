from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from engine.base_rule_executor import BaseRuleExecutor, RuleExecutionResult


class RangeExecutor(BaseRuleExecutor):
    """Validates that a column value falls within [min, max] range."""

    def execute(self, df: DataFrame) -> RuleExecutionResult:
        if not self._validate_column_exists(df):
            return self._build_error_result(
                f"Column '{self.target_column}' not found in DataFrame"
            )

        min_val = self.params.get("min")
        max_val = self.params.get("max")
        if min_val is None or max_val is None:
            return self._build_error_result("Range rule requires 'min' and 'max' params")

        total = df.count()
        # Records that are NULL or outside the range are considered failures
        failed_df = df.filter(
            col(self.target_column).isNull()
            | (col(self.target_column) < min_val)
            | (col(self.target_column) > max_val)
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
