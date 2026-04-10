from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from engine.base_rule_executor import BaseRuleExecutor, RuleExecutionResult


class CustomSQLExecutor(BaseRuleExecutor):
    """Validates records using a custom SQL expression that returns boolean."""

    def execute(self, df: DataFrame) -> RuleExecutionResult:
        sql_expression = self.params.get("sql_expression")
        if not sql_expression:
            return self._build_error_result(
                "Custom SQL rule requires 'sql_expression' param"
            )

        try:
            total = df.count()
            # Records where the SQL expression evaluates to FALSE or NULL are failures
            failed_df = df.filter(~expr(sql_expression) | expr(sql_expression).isNull())
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
        except Exception as e:
            return self._build_error_result(
                f"Custom SQL execution failed: {str(e)}"
            )
