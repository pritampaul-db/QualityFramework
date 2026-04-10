# Databricks notebook source
# MAGIC %md
# MAGIC # Dynamic DQ Pipeline — Silver Layer
# MAGIC
# MAGIC Reads the `dq_datasets` registry and `dq_dataset_rule_mapping` table,
# MAGIC then dynamically creates validated / clean / quarantine DLT tables
# MAGIC for each active dataset. **No hardcoded dataset names.**
# MAGIC
# MAGIC Adding a new dataset requires only:
# MAGIC 1. Insert a row into `dq_datasets`
# MAGIC 2. Insert rule mappings into `dq_dataset_rule_mapping`
# MAGIC 3. Re-run the pipeline — new tables appear automatically

# COMMAND ----------

import pyspark.pipelines as dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Embedded Engine: Rule Expression Builder

# COMMAND ----------

class RuleExpressionBuilder:
    """Converts DQ rules into SQL expressions. TRUE = row passes."""

    @staticmethod
    def build_expression(rule):
        col = rule["column"]
        params = rule.get("params", {})
        rt = rule["rule_type"]
        builders = {
            "not_null": RuleExpressionBuilder._not_null,
            "range": RuleExpressionBuilder._range,
            "regex": RuleExpressionBuilder._regex,
            "uniqueness": RuleExpressionBuilder._uniqueness,
            "allowed_values": RuleExpressionBuilder._allowed_values,
            "length": RuleExpressionBuilder._length,
            "freshness": RuleExpressionBuilder._freshness,
            "referential_integrity": RuleExpressionBuilder._referential_integrity,
            "custom_sql": RuleExpressionBuilder._custom_sql,
        }
        return builders[rt](col, params)

    @staticmethod
    def _not_null(col, params):
        return f"`{col}` IS NOT NULL"

    @staticmethod
    def _range(col, params):
        parts = []
        if "min" in params: parts.append(f"`{col}` >= {params['min']}")
        if "max" in params: parts.append(f"`{col}` <= {params['max']}")
        return f"({' AND '.join(parts)})" if parts else "TRUE"

    @staticmethod
    def _regex(col, params):
        pattern = params.get("pattern", ".*").replace("'", "\\'")
        return f"`{col}` RLIKE '{pattern}'"

    @staticmethod
    def _uniqueness(col, params):
        return f"(COUNT(1) OVER (PARTITION BY `{col}`)) = 1"

    @staticmethod
    def _allowed_values(col, params):
        vals = params.get("values", [])
        if not vals: return "TRUE"
        quoted = ", ".join(f"'{v}'" for v in vals)
        return f"`{col}` IN ({quoted})"

    @staticmethod
    def _length(col, params):
        parts = []
        if "min_length" in params: parts.append(f"LENGTH(`{col}`) >= {params['min_length']}")
        if "max_length" in params: parts.append(f"LENGTH(`{col}`) <= {params['max_length']}")
        return f"({' AND '.join(parts)})" if parts else "TRUE"

    @staticmethod
    def _freshness(col, params):
        h = params.get("max_age_hours", 24)
        return f"(TIMESTAMPDIFF(HOUR, CAST(`{col}` AS TIMESTAMP), CURRENT_TIMESTAMP()) <= {h})"

    @staticmethod
    def _referential_integrity(col, params):
        return "TRUE"  # Handled via broadcast join, not SQL expression

    @staticmethod
    def _custom_sql(col, params):
        return f"({params.get('sql_expression', 'TRUE')})"

    @staticmethod
    def build_expectations_dict(rules, mappings=None):
        """Build {name: sql} dict for @dlt.expect_all — all rules for observability.
        Skips uniqueness (window func) and referential_integrity (join-based)."""
        mapping_lookup = {m["rule_id"]: m for m in (mappings or [])}
        expectations = {}
        for rule in rules:
            if rule["rule_type"] in ("uniqueness", "referential_integrity"):
                continue
            try:
                expr = RuleExpressionBuilder.build_expression(rule)
                expectations[f"{rule['rule_id']}__{rule.get('rule_name', '')}"] = expr
            except Exception:
                pass
        return expectations

    @staticmethod
    def build_all_expressions(rules, mappings=None):
        """Build full metadata list for row-level evaluation."""
        mapping_lookup = {m["rule_id"]: m for m in (mappings or [])}
        result = []
        for rule in rules:
            rid = rule["rule_id"]
            m = mapping_lookup.get(rid, {})
            try:
                expr = RuleExpressionBuilder.build_expression(rule)
                entry = {
                    "rule_id": rid,
                    "rule_name": rule.get("rule_name", ""),
                    "rule_type": rule["rule_type"],
                    "column": rule["column"],
                    "severity": rule.get("severity", "warning"),
                    "enforcement_mode": m.get("enforcement_mode", "soft"),
                    "sql_expression": expr,
                }
                if rule["rule_type"] == "referential_integrity":
                    entry["params"] = rule.get("params", {})
                result.append(entry)
            except Exception:
                pass
        return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Embedded Engine: Row-Level Evaluator

# COMMAND ----------

class RowLevelEvaluator:
    """Annotates each row with _dq struct and enriches _lineage.silver."""

    @staticmethod
    def evaluate(df, rule_expressions, silver_table_name="", ref_tables=None):
        regular_exprs = []
        window_exprs = []
        ref_integrity_exprs = []
        for r in rule_expressions:
            if r["rule_type"] == "uniqueness":
                window_exprs.append(r)
            elif r["rule_type"] == "referential_integrity":
                ref_integrity_exprs.append(r)
            else:
                regular_exprs.append(r)

        evaluated = df
        temp_cols = []

        # Regular rules
        for r in regular_exprs:
            tmp = f"__tmp_{r['rule_id'].replace('-', '_')}"
            temp_cols.append((tmp, r))
            evaluated = evaluated.withColumn(
                tmp, F.coalesce(F.expr(r["sql_expression"]).cast("boolean"), F.lit(False))
            )

        # Window rules (uniqueness)
        for r in window_exprs:
            tmp = f"__tmp_{r['rule_id'].replace('-', '_')}"
            temp_cols.append((tmp, r))
            existing = [f"`{c}`" for c in evaluated.columns]
            evaluated = evaluated.selectExpr(
                *existing,
                f"COALESCE(CAST(({r['sql_expression']}) AS BOOLEAN), FALSE) AS `{tmp}`"
            )

        # Referential integrity via broadcast join
        ref_tables = ref_tables or {}
        for r in ref_integrity_exprs:
            tmp = f"__tmp_{r['rule_id'].replace('-', '_')}"
            col_name = r["column"]
            params = r.get("params", {})
            ref_key = f"{col_name}_ref"
            if ref_key in ref_tables:
                ref_df = ref_tables[ref_key]
                ref_col = params.get("ref_column", col_name)
                ref_alias = f"__ref_{r['rule_id'].replace('-', '_')}"
                ref_values = ref_df.select(F.col(ref_col).alias(ref_alias)).distinct()
                evaluated = (
                    evaluated
                    .join(F.broadcast(ref_values), evaluated[col_name] == ref_values[ref_alias], "left")
                    .withColumn(tmp, F.col(ref_alias).isNotNull())
                    .drop(ref_alias)
                )
                temp_cols.append((tmp, r))

        # Build _dq struct
        result_structs = []
        for tmp, r in temp_cols:
            result_structs.append(F.struct(
                F.lit(r["rule_id"]).alias("rule_id"),
                F.lit(r["rule_name"]).alias("rule_name"),
                F.lit(r["rule_type"]).alias("rule_type"),
                F.lit(r["column"]).alias("column"),
                F.lit(r["severity"]).alias("severity"),
                F.lit(r["enforcement_mode"]).alias("enforcement"),
                F.col(tmp).alias("passed"),
            ))

        evaluated = evaluated.withColumn("__dq_results", F.array(*result_structs))

        passed_count_expr = " + ".join(f"CAST(`{t}` AS INT)" for t, _ in temp_cols)
        total = len(temp_cols)
        passed_parts = [f"CASE WHEN `{t}` THEN '{r['rule_id']}' END" for t, r in temp_cols]
        failed_parts = [f"CASE WHEN NOT `{t}` THEN '{r['rule_id']}' END" for t, r in temp_cols]

        evaluated = (
            evaluated
            .withColumn("__passed_count", F.expr(passed_count_expr) if passed_count_expr else F.lit(0))
            .withColumn("__failed_count", F.lit(total) - F.col("__passed_count"))
        )

        evaluated = evaluated.withColumn("_dq", F.struct(
            F.col("__dq_results").alias("results"),
            F.expr(f"ARRAY_COMPACT(ARRAY({', '.join(passed_parts)}))").alias("passed_rules") if passed_parts else F.array().cast("array<string>").alias("passed_rules"),
            F.expr(f"ARRAY_COMPACT(ARRAY({', '.join(failed_parts)}))").alias("failed_rules") if failed_parts else F.array().cast("array<string>").alias("failed_rules"),
            F.col("__passed_count").cast("int").alias("passed_count"),
            F.col("__failed_count").cast("int").alias("failed_count"),
            F.lit(total).cast("int").alias("total_rules"),
            F.round(F.col("__passed_count") / F.lit(max(total, 1)) * 100, 2).alias("row_score"),
            (F.col("__failed_count") == 0).alias("all_passed"),
            F.current_timestamp().alias("evaluated_at"),
        ))

        cols_to_drop = [t for t, _ in temp_cols] + ["__dq_results", "__passed_count", "__failed_count"]
        evaluated = evaluated.drop(*cols_to_drop)

        if "_lineage" in evaluated.columns and silver_table_name:
            evaluated = evaluated.withColumn("_lineage", F.struct(
                F.col("_lineage.source").alias("source"),
                F.col("_lineage.bronze").alias("bronze"),
                F.struct(
                    F.lit(silver_table_name).alias("table"),
                    F.current_timestamp().alias("validated_at"),
                ).alias("silver"),
                F.col("_lineage.gold").alias("gold"),
                F.col("_lineage.pipeline").alias("pipeline"),
            ))

        return evaluated

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean / Quarantine Filters

# COMMAND ----------

def _filter_clean(df):
    return df.filter(
        ~F.exists(F.col("_dq.results"), lambda r: (r["enforcement"] == "hard") & (~r["passed"]))
    )

def _filter_quarantine(df):
    return (
        df.filter(
            F.exists(F.col("_dq.results"), lambda r: (r["enforcement"] == "hard") & (~r["passed"]))
        )
        .withColumn("_quarantine_reason", F.concat_ws(", ", F.col("_dq.failed_rules")))
        .withColumn("_quarantined_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Metadata from Delta Tables

# COMMAND ----------

CATALOG = "dq_poc"

# Load all active datasets
_all_datasets = spark.table(f"{CATALOG}.dq_framework.dq_datasets").filter("is_active = true").collect()

# Load all active rules
_all_rules_raw = spark.table(f"{CATALOG}.dq_framework.dq_rules").filter("is_active = true").collect()
_all_rules = {}
for row in _all_rules_raw:
    r = row.asDict()
    if r.get("params") and isinstance(r["params"], str):
        r["params"] = json.loads(r["params"])
    else:
        r["params"] = {}
    if "target_column" in r and "column" not in r:
        r["column"] = r["target_column"]
    _all_rules[r["rule_id"]] = r

# Load all active mappings (new table with dataset_id)
_all_mappings_raw = spark.table(f"{CATALOG}.dq_framework.dq_dataset_rule_mapping").filter("is_active = true").collect()
_all_mappings = [row.asDict() for row in _all_mappings_raw]

print(f"[Dynamic DQ] Loaded {len(_all_datasets)} datasets, {len(_all_rules)} rules, {len(_all_mappings)} mappings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Per-Dataset Rule Configuration

# COMMAND ----------

def _get_dataset_config(dataset_id):
    """Get rules, expressions, expectations, and ref_info for a dataset."""
    # Get mappings for this dataset
    ds_mappings = sorted(
        [m for m in _all_mappings if m["dataset_id"] == dataset_id],
        key=lambda m: m.get("priority", 999)
    )
    # Get matching rules
    ds_rules = [_all_rules[m["rule_id"]] for m in ds_mappings if m["rule_id"] in _all_rules]
    # Convert mappings to old format for compatibility (enforcement_mode keyed by rule_id)
    mappings_compat = [{"rule_id": m["rule_id"], "enforcement_mode": m["enforcement_mode"]} for m in ds_mappings]

    all_exprs = RuleExpressionBuilder.build_all_expressions(ds_rules, mappings_compat)
    expectations = RuleExpressionBuilder.build_expectations_dict(ds_rules, mappings_compat)

    return ds_rules, mappings_compat, all_exprs, expectations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic DLT Table Generation
# MAGIC
# MAGIC For each active dataset, creates:
# MAGIC - `{name}_validated` — materialized table with _dq struct (source of truth)
# MAGIC - `{name}_clean` — view filtering to rows passing all hard rules
# MAGIC - `{name}_quarantine` — view filtering to rows failing any hard rule

# COMMAND ----------

for _ds_row in _all_datasets:
    _ds = _ds_row.asDict()
    _dataset_id = _ds["dataset_id"]
    _ds_name = _ds["dataset_name"]
    _bronze_dlt = _ds["bronze_dlt_table"] or f"{_ds_name}_bronze"
    _pk = _ds["primary_key"]
    _ref_tables_json = _ds.get("ref_tables")

    # Parse ref_tables config
    _ref_config = json.loads(_ref_tables_json) if _ref_tables_json else {}

    # Get rule configuration for this dataset
    _rules, _mappings, _exprs, _expectations = _get_dataset_config(_dataset_id)

    print(f"[Dynamic DQ] Registering {_ds_name}: {len(_exprs)} rules, bronze={_bronze_dlt}")

    # Use closures to capture loop variables
    def _make_pipeline(
        ds_name=_ds_name,
        bronze_dlt=_bronze_dlt,
        exprs=_exprs,
        expectations=_expectations,
        ref_config=_ref_config,
    ):
        validated_name = f"{ds_name}_validated"
        clean_name = f"{ds_name}_clean"
        quarantine_name = f"{ds_name}_quarantine"

        @dlt.table(
            name=validated_name,
            comment=f"Source of truth — all {ds_name} with _dq + _lineage",
            table_properties={"quality": "silver", "dataset": ds_name},
        )
        @dlt.expect_all(expectations)
        def validated():
            # Build ref_tables from DLT reads based on dataset config
            ref_tables = {}
            for ref_key, ref_info in ref_config.items():
                ref_tables[ref_key] = dlt.read(ref_info["dlt_table"])
            return RowLevelEvaluator.evaluate(
                dlt.read(bronze_dlt), exprs,
                f"{CATALOG}.silver.{validated_name}", ref_tables
            )

        @dlt.view(
            name=clean_name,
            comment=f"{ds_name} passing all hard rules — view over validated",
        )
        def clean():
            return _filter_clean(dlt.read(validated_name))

        @dlt.view(
            name=quarantine_name,
            comment=f"{ds_name} failing any hard rule — view over validated",
        )
        def quarantine():
            return _filter_quarantine(dlt.read(validated_name))

    _make_pipeline()

print(f"[Dynamic DQ] Registered {len(_all_datasets)} dataset pipelines")
