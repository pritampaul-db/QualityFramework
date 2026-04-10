from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from engine.base_rule_executor import BaseRuleExecutor, RuleExecutionResult


class NotNullExecutor(BaseRuleExecutor):
    """Validates that a column does not contain NULL values."""

    def execute(self, df: DataFrame) -> RuleExecutionResult:
        if not self._validate_column_exists(df):
            return self._build_error_result(
                f"Column '{self.target_column}' not found in DataFrame"
            )

        total = df.count()
        failed_df = df.filter(col(self.target_column).isNull())
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
