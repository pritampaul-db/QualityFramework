from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, expr, max as spark_max
from engine.base_rule_executor import BaseRuleExecutor, RuleExecutionResult


class FreshnessExecutor(BaseRuleExecutor):
    """Validates that data is not older than a specified threshold."""

    def execute(self, df: DataFrame) -> RuleExecutionResult:
        if not self._validate_column_exists(df):
            return self._build_error_result(
                f"Column '{self.target_column}' not found in DataFrame"
            )

        max_age_hours = self.params.get("max_age_hours")
        if max_age_hours is None:
            return self._build_error_result(
                "Freshness rule requires 'max_age_hours' param"
            )

        total = df.count()

        # Records where the timestamp column is older than max_age_hours from now
        # or is NULL
        threshold_expr = f"current_timestamp() - INTERVAL {max_age_hours} HOURS"
        failed_df = df.filter(
            col(self.target_column).isNull()
            | (col(self.target_column).cast("timestamp") < expr(threshold_expr))
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
