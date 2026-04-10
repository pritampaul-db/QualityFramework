# Databricks notebook source
# MAGIC %md
# MAGIC # Row-Level DQ Evaluator
# MAGIC Evaluates every DQ rule per row and annotates the DataFrame with:
# MAGIC - A boolean column per rule (`_dq_<rule_id>`)
# MAGIC - Summary columns: passed/failed rule lists, counts, row-level DQ score
# MAGIC
# MAGIC This enables full per-row traceability of which rules passed and which failed.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from typing import List, Dict


class RowLevelEvaluator:
    """
    Annotates each row with per-rule pass/fail results.

    For each rule, adds a column `_dq_<rule_id>` (boolean).
    Then computes summary columns:
      - _dq_passed_rules:  array of rule IDs that passed for this row
      - _dq_failed_rules:  array of rule IDs that failed for this row
      - _dq_passed_count:  number of rules passed
      - _dq_failed_count:  number of rules failed
      - _dq_total_rules:   total rules evaluated
      - _dq_row_score:     per-row DQ score (passed / total * 100)
      - _dq_all_passed:    TRUE if every rule passed
      - _dq_evaluated_at:  evaluation timestamp
    """

    @staticmethod
    def evaluate(
        df: DataFrame,
        rule_expressions: List[Dict],
    ) -> DataFrame:
        """
        Apply all rule expressions to the DataFrame as row-level annotations.

        Args:
            df: Source DataFrame to evaluate.
            rule_expressions: List of dicts from RuleExpressionBuilder.build_all_expressions(),
                each having: rule_id, sql_expression, rule_type, etc.

        Returns:
            DataFrame with original columns + _dq_* annotation columns.
        """
        annotated_df = df
        rule_col_names = []

        for rule_expr in rule_expressions:
            rule_id = rule_expr["rule_id"]
            sql_expr = rule_expr["sql_expression"]
            col_name = f"_dq_{rule_id.replace('-', '_')}"
            rule_col_names.append((col_name, rule_id))

            # Evaluate the SQL expression as a boolean column.
            # CASE handles NULLs: if the expression returns NULL, treat as failed.
            annotated_df = annotated_df.withColumn(
                col_name,
                F.coalesce(F.expr(sql_expr).cast("boolean"), F.lit(False))
            )

        # Build summary columns from the individual rule columns
        annotated_df = RowLevelEvaluator._add_summary_columns(
            annotated_df, rule_col_names
        )

        return annotated_df

    @staticmethod
    def _add_summary_columns(
        df: DataFrame,
        rule_col_names: List[tuple],
    ) -> DataFrame:
        """Add _dq_passed_rules, _dq_failed_rules, counts, score, etc."""

        # _dq_passed_rules: array of rule_ids where the column is True
        passed_array = F.array(*[
            F.when(F.col(col_name), F.lit(rule_id))
            for col_name, rule_id in rule_col_names
        ])
        # Filter nulls from the array (rules that didn't pass)
        df = df.withColumn(
            "_dq_passed_rules",
            F.expr(f"filter(transform(array({','.join(chr(96)+c+chr(96) for c, _ in rule_col_names)}), "
                   f"(x, i) -> CASE WHEN x THEN element_at(array({','.join(repr(rid) for _, rid in rule_col_names)}), i + 1) END), "
                   f"x -> x IS NOT NULL)")
        )

        # _dq_failed_rules: array of rule_ids where the column is False
        df = df.withColumn(
            "_dq_failed_rules",
            F.expr(f"filter(transform(array({','.join(chr(96)+c+chr(96) for c, _ in rule_col_names)}), "
                   f"(x, i) -> CASE WHEN NOT x THEN element_at(array({','.join(repr(rid) for _, rid in rule_col_names)}), i + 1) END), "
                   f"x -> x IS NOT NULL)")
        )

        # _dq_passed_count / _dq_failed_count
        df = df.withColumn("_dq_passed_count", F.size("_dq_passed_rules"))
        df = df.withColumn("_dq_failed_count", F.size("_dq_failed_rules"))

        # _dq_total_rules
        total_rules = len(rule_col_names)
        df = df.withColumn("_dq_total_rules", F.lit(total_rules))

        # _dq_row_score: (passed / total) * 100
        df = df.withColumn(
            "_dq_row_score",
            F.round(
                (F.col("_dq_passed_count") / F.lit(total_rules)) * 100, 2
            )
        )

        # _dq_all_passed: True if no failures
        df = df.withColumn(
            "_dq_all_passed",
            F.col("_dq_failed_count") == 0
        )

        # _dq_evaluated_at
        df = df.withColumn("_dq_evaluated_at", F.current_timestamp())

        return df

    @staticmethod
    def split_clean_quarantine(
        annotated_df: DataFrame,
        rule_expressions: List[Dict],
    ) -> tuple:
        """
        Split the annotated DataFrame into clean and quarantine based on
        hard-enforcement rules only.

        Args:
            annotated_df: DataFrame with _dq_* columns from evaluate().
            rule_expressions: Same list used in evaluate().

        Returns:
            (clean_df, quarantine_df)
        """
        # Get hard enforcement rule IDs
        hard_rule_ids = [
            r["rule_id"] for r in rule_expressions
            if r.get("enforcement_mode") == "hard"
        ]

        if not hard_rule_ids:
            return annotated_df, None

        # Build condition: all hard rules must pass
        hard_col_names = [
            f"_dq_{rid.replace('-', '_')}" for rid in hard_rule_ids
        ]
        all_hard_passed = F.lit(True)
        for col_name in hard_col_names:
            all_hard_passed = all_hard_passed & F.col(col_name)

        clean_df = annotated_df.filter(all_hard_passed)
        quarantine_df = annotated_df.filter(~all_hard_passed)

        return clean_df, quarantine_df

    @staticmethod
    def build_row_dq_detail(
        annotated_df: DataFrame,
        rule_expressions: List[Dict],
        dataset_name: str,
        run_id: str,
    ) -> DataFrame:
        """
        Explode the annotated DataFrame into a long-format row-level DQ detail table.
        One row per (source_row, rule) combination.

        Output columns:
            run_id, dataset_name, primary_key_value, rule_id, rule_name,
            rule_type, column, severity, enforcement_mode, passed, evaluated_at

        Args:
            annotated_df: DataFrame with _dq_* columns.
            rule_expressions: List of rule expression dicts.
            dataset_name: Name of the dataset being evaluated.
            run_id: Pipeline run ID for traceability.

        Returns:
            Long-format DataFrame with one row per rule evaluation.
        """
        # Build an array of structs, one per rule
        rule_structs = []
        for rule_expr in rule_expressions:
            rule_id = rule_expr["rule_id"]
            col_name = f"_dq_{rule_id.replace('-', '_')}"
            rule_structs.append(
                F.struct(
                    F.lit(rule_id).alias("rule_id"),
                    F.lit(rule_expr.get("rule_name", "")).alias("rule_name"),
                    F.lit(rule_expr["rule_type"]).alias("rule_type"),
                    F.lit(rule_expr["column"]).alias("target_column"),
                    F.lit(rule_expr.get("severity", "warning")).alias("severity"),
                    F.lit(rule_expr.get("enforcement_mode", "soft")).alias("enforcement_mode"),
                    F.col(col_name).alias("passed"),
                )
            )

        detail_df = (
            annotated_df
            .withColumn("_dq_rules_array", F.array(*rule_structs))
            .withColumn("_dq_rule_detail", F.explode("_dq_rules_array"))
            .select(
                F.lit(run_id).alias("run_id"),
                F.lit(dataset_name).alias("dataset_name"),
                F.monotonically_increasing_id().alias("row_id"),
                F.col("_dq_rule_detail.rule_id"),
                F.col("_dq_rule_detail.rule_name"),
                F.col("_dq_rule_detail.rule_type"),
                F.col("_dq_rule_detail.target_column"),
                F.col("_dq_rule_detail.severity"),
                F.col("_dq_rule_detail.enforcement_mode"),
                F.col("_dq_rule_detail.passed"),
                F.col("_dq_evaluated_at").alias("evaluated_at"),
            )
        )

        return detail_df
