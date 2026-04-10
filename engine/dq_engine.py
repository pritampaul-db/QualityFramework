# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Engine
# MAGIC Core orchestrator that loads rules, executes them against DataFrames,
# MAGIC persists results, and manages quarantine.

# COMMAND ----------

import json
import time
import uuid
from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass, field

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_json, struct, monotonically_increasing_id,
    concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType
)

from engine.rule_registry import get_executor
from engine.base_rule_executor import RuleExecutionResult


@dataclass
class DQExecutionSummary:
    """Summary of a full DQ execution run against a dataset."""
    run_id: str
    dataset_name: str
    total_rules: int = 0
    rules_passed: int = 0
    rules_failed: int = 0
    rules_errored: int = 0
    rules_skipped: int = 0
    total_records_in: int = 0
    total_records_out: int = 0
    records_quarantined: int = 0
    overall_dq_score: float = 100.0
    results: List[RuleExecutionResult] = field(default_factory=list)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


class DQEngine:
    """
    Metadata-driven Data Quality Engine.

    Loads rules from Delta tables, executes them dynamically against DataFrames,
    persists execution logs, and manages quarantine for failed records.
    """

    EXECUTION_LOG_TABLE = "dq_framework.dq_execution_log"
    QUARANTINE_TABLE = "dq_framework.dq_quarantine"
    SCORES_TABLE = "dq_framework.dq_scores"

    def __init__(self, spark: SparkSession, catalog: str = "dq_poc"):
        self.spark = spark
        self.catalog = catalog
        self._ensure_framework_tables()

    def _ensure_framework_tables(self):
        """Create framework tables if they don't exist."""
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.dq_framework")

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.EXECUTION_LOG_TABLE} (
                execution_id STRING,
                run_id STRING,
                rule_id STRING,
                rule_type STRING,
                dataset_name STRING,
                target_column STRING,
                severity STRING,
                enforcement_mode STRING,
                status STRING,
                total_records BIGINT,
                passed_count BIGINT,
                failed_count BIGINT,
                dq_score DOUBLE,
                error_message STRING,
                execution_duration_ms BIGINT,
                executed_at TIMESTAMP,
                executed_by STRING
            ) USING DELTA
        """)

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.QUARANTINE_TABLE} (
                quarantine_id STRING,
                execution_id STRING,
                run_id STRING,
                rule_id STRING,
                rule_type STRING,
                source_table STRING,
                record_payload STRING,
                quarantine_reason STRING,
                severity STRING,
                quarantined_at TIMESTAMP,
                resolved_at TIMESTAMP,
                resolution_action STRING
            ) USING DELTA
        """)

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.SCORES_TABLE} (
                score_id STRING,
                run_id STRING,
                dataset_name STRING,
                total_rules INT,
                rules_passed INT,
                rules_failed INT,
                rules_errored INT,
                overall_dq_score DOUBLE,
                total_records_in BIGINT,
                total_records_out BIGINT,
                records_quarantined BIGINT,
                scored_at TIMESTAMP
            ) USING DELTA
        """)

    def load_rules_for_dataset(self, dataset_name: str) -> List[dict]:
        """
        Load active rules mapped to a dataset from the metadata tables.
        Returns rules sorted by priority.
        """
        rules_df = self.spark.sql(f"""
            SELECT
                r.rule_id,
                r.rule_name,
                r.rule_type,
                r.target_column,
                r.params,
                r.severity,
                m.enforcement_mode,
                m.priority
            FROM {self.catalog}.dq_framework.dq_rules r
            JOIN {self.catalog}.dq_framework.dq_rule_mapping m
                ON r.rule_id = m.rule_id
            WHERE m.dataset_name = '{dataset_name}'
                AND r.is_active = true
                AND m.is_active = true
                AND (r.effective_to IS NULL OR r.effective_to >= current_date())
            ORDER BY m.priority ASC
        """)

        rules = []
        for row in rules_df.collect():
            rule = row.asDict()
            # Parse JSON params string back to dict
            if rule["params"]:
                rule["params"] = json.loads(rule["params"])
            else:
                rule["params"] = {}
            rules.append(rule)

        print(f"[DQEngine] Loaded {len(rules)} active rules for dataset '{dataset_name}'")
        return rules

    def execute(
        self,
        df: DataFrame,
        dataset_name: str,
        run_id: Optional[str] = None,
    ) -> tuple:
        """
        Execute all mapped DQ rules against a DataFrame.

        Returns:
            (clean_df, summary): Cleaned DataFrame and execution summary
        """
        run_id = run_id or str(uuid.uuid4())
        summary = DQExecutionSummary(
            run_id=run_id,
            dataset_name=dataset_name,
            started_at=datetime.utcnow().isoformat(),
        )

        # Load rules from metadata
        rules = self.load_rules_for_dataset(dataset_name)
        summary.total_rules = len(rules)
        summary.total_records_in = df.count()

        if not rules:
            print(f"[DQEngine] No rules found for '{dataset_name}', passing through")
            summary.total_records_out = summary.total_records_in
            summary.completed_at = datetime.utcnow().isoformat()
            return df, summary

        clean_df = df
        all_quarantine_dfs = []

        for rule in rules:
            print(f"\n[DQEngine] Executing rule {rule['rule_id']} "
                  f"({rule['rule_type']}) on column '{rule['target_column']}'")

            execution_id = str(uuid.uuid4())
            start_time = time.time()

            try:
                executor = get_executor(rule, spark=self.spark)
                result = executor.execute(clean_df)
                result.execution_duration_ms = int((time.time() - start_time) * 1000)
            except Exception as e:
                result = RuleExecutionResult(
                    rule_id=rule["rule_id"],
                    rule_type=rule["rule_type"],
                    target_column=rule["target_column"],
                    status="error",
                    error_message=str(e),
                    execution_duration_ms=int((time.time() - start_time) * 1000),
                )

            summary.results.append(result)

            # Log result
            self._log_execution(
                execution_id=execution_id,
                run_id=run_id,
                rule=rule,
                result=result,
                dataset_name=dataset_name,
            )

            # Print result summary
            status_icon = {
                "passed": "PASS", "failed": "FAIL",
                "error": "ERR", "skipped": "SKIP"
            }.get(result.status, "???")
            print(f"  [{status_icon}] {result.rule_id}: "
                  f"{result.passed_count}/{result.total_records} passed "
                  f"(score: {result.dq_score}%)")

            # Handle enforcement
            enforcement = rule.get("enforcement_mode", "soft")
            if result.status == "failed" and result.failed_df is not None:
                if enforcement == "hard":
                    # Remove failed records from clean_df
                    clean_df = clean_df.subtract(result.failed_df)
                    print(f"  [HARD] Removed {result.failed_count} records")

                # Quarantine failed records (both hard and soft)
                quarantine_df = self._prepare_quarantine(
                    failed_df=result.failed_df,
                    execution_id=execution_id,
                    run_id=run_id,
                    rule=rule,
                    dataset_name=dataset_name,
                )
                all_quarantine_dfs.append(quarantine_df)

            # Update summary counters
            if result.status == "passed":
                summary.rules_passed += 1
            elif result.status == "failed":
                summary.rules_failed += 1
            elif result.status == "error":
                summary.rules_errored += 1
            else:
                summary.rules_skipped += 1

        # Write quarantine records
        if all_quarantine_dfs:
            combined_quarantine = all_quarantine_dfs[0]
            for qdf in all_quarantine_dfs[1:]:
                combined_quarantine = combined_quarantine.unionByName(qdf)
            self._write_quarantine(combined_quarantine)
            summary.records_quarantined = combined_quarantine.count()

        summary.total_records_out = clean_df.count()
        summary.completed_at = datetime.utcnow().isoformat()

        # Calculate overall DQ score
        if summary.total_rules > 0:
            scores = [r.dq_score for r in summary.results if r.status in ("passed", "failed")]
            summary.overall_dq_score = round(sum(scores) / len(scores), 2) if scores else 100.0

        # Persist DQ score
        self._persist_score(summary)

        print(f"\n{'='*60}")
        print(f"[DQEngine] Execution Summary for '{dataset_name}'")
        print(f"  Run ID: {run_id}")
        print(f"  Rules: {summary.rules_passed} passed, {summary.rules_failed} failed, "
              f"{summary.rules_errored} errors, {summary.rules_skipped} skipped")
        print(f"  Records: {summary.total_records_in} in → {summary.total_records_out} out "
              f"({summary.records_quarantined} quarantined)")
        print(f"  Overall DQ Score: {summary.overall_dq_score}%")
        print(f"{'='*60}\n")

        return clean_df, summary

    def _log_execution(
        self,
        execution_id: str,
        run_id: str,
        rule: dict,
        result: RuleExecutionResult,
        dataset_name: str,
    ):
        """Persist execution log entry to Delta table."""
        log_data = [{
            "execution_id": execution_id,
            "run_id": run_id,
            "rule_id": rule["rule_id"],
            "rule_type": rule["rule_type"],
            "dataset_name": dataset_name,
            "target_column": rule["target_column"],
            "severity": rule.get("severity", "warning"),
            "enforcement_mode": rule.get("enforcement_mode", "soft"),
            "status": result.status,
            "total_records": result.total_records,
            "passed_count": result.passed_count,
            "failed_count": result.failed_count,
            "dq_score": result.dq_score,
            "error_message": result.error_message,
            "execution_duration_ms": result.execution_duration_ms,
            "executed_at": datetime.utcnow().isoformat(),
            "executed_by": "dq_framework_poc",
        }]
        log_df = self.spark.createDataFrame(log_data)
        log_df = log_df.withColumn("executed_at", col("executed_at").cast("timestamp"))
        log_df.write.format("delta").mode("append").saveAsTable(
            f"{self.catalog}.{self.EXECUTION_LOG_TABLE}"
        )

    def _prepare_quarantine(
        self,
        failed_df: DataFrame,
        execution_id: str,
        run_id: str,
        rule: dict,
        dataset_name: str,
    ) -> DataFrame:
        """Prepare quarantine records from failed DataFrame."""
        # Serialize each failed record as JSON
        all_columns = failed_df.columns
        quarantine_df = (
            failed_df
            .withColumn("record_payload", to_json(struct(*all_columns)))
            .select(
                lit(None).cast("string").alias("quarantine_id"),
                lit(execution_id).alias("execution_id"),
                lit(run_id).alias("run_id"),
                lit(rule["rule_id"]).alias("rule_id"),
                lit(rule["rule_type"]).alias("rule_type"),
                lit(dataset_name).alias("source_table"),
                col("record_payload"),
                lit(f"Failed {rule['rule_type']} check on column '{rule['target_column']}'")
                    .alias("quarantine_reason"),
                lit(rule.get("severity", "warning")).alias("severity"),
                current_timestamp().alias("quarantined_at"),
                lit(None).cast("timestamp").alias("resolved_at"),
                lit(None).cast("string").alias("resolution_action"),
            )
        )
        # Generate UUIDs for quarantine_id
        quarantine_df = quarantine_df.withColumn(
            "quarantine_id",
            concat_ws("-", lit("Q"), monotonically_increasing_id().cast("string"))
        )
        return quarantine_df

    def _write_quarantine(self, quarantine_df: DataFrame):
        """Write quarantine records to Delta table."""
        quarantine_df.write.format("delta").mode("append").saveAsTable(
            f"{self.catalog}.{self.QUARANTINE_TABLE}"
        )
        print(f"[DQEngine] Wrote {quarantine_df.count()} records to quarantine")

    def _persist_score(self, summary: DQExecutionSummary):
        """Persist dataset-level DQ score."""
        score_data = [{
            "score_id": str(uuid.uuid4()),
            "run_id": summary.run_id,
            "dataset_name": summary.dataset_name,
            "total_rules": summary.total_rules,
            "rules_passed": summary.rules_passed,
            "rules_failed": summary.rules_failed,
            "rules_errored": summary.rules_errored,
            "overall_dq_score": summary.overall_dq_score,
            "total_records_in": summary.total_records_in,
            "total_records_out": summary.total_records_out,
            "records_quarantined": summary.records_quarantined,
            "scored_at": datetime.utcnow().isoformat(),
        }]
        score_df = self.spark.createDataFrame(score_data)
        score_df = score_df.withColumn("scored_at", col("scored_at").cast("timestamp"))
        score_df.write.format("delta").mode("append").saveAsTable(
            f"{self.catalog}.{self.SCORES_TABLE}"
        )
