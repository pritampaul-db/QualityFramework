from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from engine.base_rule_executor import BaseRuleExecutor, RuleExecutionResult


class RegexExecutor(BaseRuleExecutor):
    """Validates that a column value matches a regex pattern."""

    def execute(self, df: DataFrame) -> RuleExecutionResult:
        if not self._validate_column_exists(df):
            return self._build_error_result(
                f"Column '{self.target_column}' not found in DataFrame"
            )

        pattern = self.params.get("pattern")
        if not pattern:
            return self._build_error_result("Regex rule requires 'pattern' param")

        total = df.count()
        # Records where column is NULL or doesn't match the pattern
        failed_df = df.filter(
            col(self.target_column).isNull()
            | ~col(self.target_column).rlike(pattern)
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
