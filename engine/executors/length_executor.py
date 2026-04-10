from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length
from engine.base_rule_executor import BaseRuleExecutor, RuleExecutionResult


class LengthExecutor(BaseRuleExecutor):
    """Validates that a string column's length is within [min_length, max_length]."""

    def execute(self, df: DataFrame) -> RuleExecutionResult:
        if not self._validate_column_exists(df):
            return self._build_error_result(
                f"Column '{self.target_column}' not found in DataFrame"
            )

        min_len = self.params.get("min_length")
        max_len = self.params.get("max_length")
        if min_len is None or max_len is None:
            return self._build_error_result(
                "Length rule requires 'min_length' and 'max_length' params"
            )

        total = df.count()
        failed_df = df.filter(
            col(self.target_column).isNull()
            | (length(col(self.target_column)) < min_len)
            | (length(col(self.target_column)) > max_len)
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
