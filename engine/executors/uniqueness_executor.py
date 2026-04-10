from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count
from engine.base_rule_executor import BaseRuleExecutor, RuleExecutionResult


class UniquenessExecutor(BaseRuleExecutor):
    """Validates that a column contains unique values (no duplicates)."""

    def execute(self, df: DataFrame) -> RuleExecutionResult:
        if not self._validate_column_exists(df):
            return self._build_error_result(
                f"Column '{self.target_column}' not found in DataFrame"
            )

        total = df.count()

        # Find duplicate values
        duplicates = (
            df.groupBy(self.target_column)
            .agg(count("*").alias("_dq_count"))
            .filter(col("_dq_count") > 1)
        )

        # Get all rows that have duplicate values
        duplicate_values = duplicates.select(self.target_column)
        failed_df = df.join(
            duplicate_values,
            on=self.target_column,
            how="inner"
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
