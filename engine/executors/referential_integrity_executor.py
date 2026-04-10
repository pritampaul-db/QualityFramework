from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from engine.base_rule_executor import BaseRuleExecutor, RuleExecutionResult


class ReferentialIntegrityExecutor(BaseRuleExecutor):
    """Validates that column values exist in a reference table."""

    def __init__(self, rule: dict, spark: SparkSession = None):
        super().__init__(rule)
        self.spark = spark

    def execute(self, df: DataFrame) -> RuleExecutionResult:
        if not self._validate_column_exists(df):
            return self._build_error_result(
                f"Column '{self.target_column}' not found in DataFrame"
            )

        ref_table = self.params.get("ref_table")
        ref_column = self.params.get("ref_column")
        if not ref_table or not ref_column:
            return self._build_error_result(
                "Referential integrity rule requires 'ref_table' and 'ref_column' params"
            )

        if self.spark is None:
            return self._build_error_result(
                "SparkSession required for referential integrity checks"
            )

        try:
            ref_df = self.spark.table(ref_table)
        except Exception as e:
            return self._build_error_result(
                f"Reference table '{ref_table}' not found: {str(e)}"
            )

        if ref_column not in ref_df.columns:
            return self._build_error_result(
                f"Column '{ref_column}' not found in reference table '{ref_table}'"
            )

        total = df.count()

        # Left anti join: records in source that DON'T exist in reference
        failed_df = df.join(
            ref_df.select(col(ref_column).alias(self.target_column)).distinct(),
            on=self.target_column,
            how="left_anti"
        )
        # Also include NULLs as failures
        null_df = df.filter(col(self.target_column).isNull())
        failed_df = failed_df.unionByName(null_df)
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
