# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Declarative Pipeline — DQ Framework
# MAGIC
# MAGIC Single DLT pipeline notebook that:
# MAGIC 1. Reads Bronze tables (customers, transactions, products)
# MAGIC 2. Applies DQ expectations (hard → drop, soft → warn)
# MAGIC 3. Annotates every row with a **single `_dq` struct column** (schema-stable)
# MAGIC 4. Produces Silver validated (table), clean & quarantine (views)
# MAGIC
# MAGIC **Two metadata struct columns:**
# MAGIC - `_dq` — schema-stable DQ results (adding rules = no schema change)
# MAGIC - `_lineage` — end-to-end row provenance (source file → bronze → silver → gold)

# COMMAND ----------

import pyspark.pipelines as dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
import os
import uuid
from datetime import datetime
from typing import List, Dict, Tuple

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
        ref = params.get("ref_table", "")
        ref_col = params.get("ref_column", col)
        if not ref: return "TRUE"
        dlt_ref_map = {"dim_country": "ref_dim_country", "customers": "customers_bronze"}
        live_name = dlt_ref_map.get(ref, ref)
        return f"`{col}` IN (SELECT `{ref_col}` FROM live.{live_name})"

    @staticmethod
    def _custom_sql(col, params):
        return f"({params.get('sql_expression', 'TRUE')})"

    @staticmethod
    def build_expectations_dict(rules, enforcement_filter=None, mappings=None):
        mapping_lookup = {m["rule_id"]: m for m in (mappings or [])}
        expectations = {}
        for rule in rules:
            rid = rule["rule_id"]
            if enforcement_filter and rid in mapping_lookup:
                if mapping_lookup[rid].get("enforcement_mode") != enforcement_filter:
                    continue
            if rule["rule_type"] in ("uniqueness", "referential_integrity"):
                continue
            try:
                expr = RuleExpressionBuilder.build_expression(rule)
                expectations[f"{rid}__{rule.get('rule_name', '')}"] = expr
            except Exception:
                pass
        return expectations

    @staticmethod
    def build_all_expressions(rules, mappings=None):
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
                # Carry params through for ref_integrity (needed for broadcast join)
                if rule["rule_type"] == "referential_integrity":
                    entry["params"] = rule.get("params", {})
                result.append(entry)
            except Exception:
                pass
        return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Embedded Engine: Row-Level Evaluator (Struct-Based)

# COMMAND ----------

class RowLevelEvaluator:
    """
    Annotates each row with:
    - `_dq` struct: schema-stable DQ results
    - Enriches `_lineage.silver`: adds silver table name + validation timestamp
    """

    @staticmethod
    def evaluate(df, rule_expressions, silver_table_name="", ref_tables=None):
        # Step 1: Evaluate each rule as a temporary boolean column
        # Separate rules by evaluation strategy
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
        temp_cols = []  # (temp_col_name, rule_expr_dict)

        # Regular rules: simple withColumn using F.expr
        for r in regular_exprs:
            tmp = f"__tmp_{r['rule_id'].replace('-', '_')}"
            temp_cols.append((tmp, r))
            evaluated = evaluated.withColumn(
                tmp, F.coalesce(F.expr(r["sql_expression"]).cast("boolean"), F.lit(False))
            )

        # Window rules (uniqueness): must use selectExpr for window functions
        for r in window_exprs:
            tmp = f"__tmp_{r['rule_id'].replace('-', '_')}"
            temp_cols.append((tmp, r))
            existing = [f"`{c}`" for c in evaluated.columns]
            evaluated = evaluated.selectExpr(
                *existing,
                f"COALESCE(CAST(({r['sql_expression']}) AS BOOLEAN), FALSE) AS `{tmp}`"
            )

        # Referential integrity rules: use broadcast join instead of subquery
        ref_tables = ref_tables or {}
        for r in ref_integrity_exprs:
            tmp = f"__tmp_{r['rule_id'].replace('-', '_')}"
            col_name = r["column"]
            params = r.get("params", {})
            ref_key = f"{col_name}_ref"
            if ref_key in ref_tables:
                ref_df = ref_tables[ref_key]
                ref_col = params.get("ref_column", col_name)
                # Broadcast join: match source column against reference values
                ref_alias = f"__ref_{r['rule_id'].replace('-', '_')}"
                ref_values = ref_df.select(F.col(ref_col).alias(ref_alias)).distinct()
                evaluated = (
                    evaluated
                    .join(
                        F.broadcast(ref_values),
                        evaluated[col_name] == ref_values[ref_alias],
                        "left"
                    )
                    .withColumn(tmp, F.col(ref_alias).isNotNull())
                    .drop(ref_alias)
                )
                temp_cols.append((tmp, r))

        # Step 2: Build the _dq.results array of structs
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

        # Step 3: Derive summary fields from the results array
        passed_count_expr = " + ".join(f"CAST(`{t}` AS INT)" for t, _ in temp_cols)
        total = len(temp_cols)

        # Build passed/failed rule ID arrays
        passed_parts = [f"CASE WHEN `{t}` THEN '{r['rule_id']}' END" for t, r in temp_cols]
        failed_parts = [f"CASE WHEN NOT `{t}` THEN '{r['rule_id']}' END" for t, r in temp_cols]

        evaluated = (
            evaluated
            .withColumn("__passed_count", F.expr(passed_count_expr))
            .withColumn("__failed_count", F.lit(total) - F.col("__passed_count"))
        )

        # Step 4: Assemble the single _dq struct
        evaluated = evaluated.withColumn("_dq", F.struct(
            F.col("__dq_results").alias("results"),
            F.expr(f"ARRAY_COMPACT(ARRAY({', '.join(passed_parts)}))").alias("passed_rules"),
            F.expr(f"ARRAY_COMPACT(ARRAY({', '.join(failed_parts)}))").alias("failed_rules"),
            F.col("__passed_count").cast("int").alias("passed_count"),
            F.col("__failed_count").cast("int").alias("failed_count"),
            F.lit(total).cast("int").alias("total_rules"),
            F.round(F.col("__passed_count") / F.lit(total) * 100, 2).alias("row_score"),
            (F.col("__failed_count") == 0).alias("all_passed"),
            F.current_timestamp().alias("evaluated_at"),
        ))

        # Step 5: Drop all temporary columns — only _dq remains
        cols_to_drop = [t for t, _ in temp_cols] + ["__dq_results", "__passed_count", "__failed_count"]
        evaluated = evaluated.drop(*cols_to_drop)

        # Step 6: Enrich _lineage.silver (if _lineage column exists from bronze)
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
# MAGIC ## Load Rules from JSON

# COMMAND ----------

CATALOG = "dq_poc"
RULES_TABLE = f"{CATALOG}.dq_framework.dq_rules"
MAPPINGS_TABLE = f"{CATALOG}.dq_framework.dq_rule_mapping"


def _load_rules_from_delta():
    """Load rules and mappings from Delta tables (production path)."""
    rules = []
    for row in spark.table(RULES_TABLE).collect():
        r = row.asDict()
        # params stored as JSON string in Delta — parse it
        if r.get("params") and isinstance(r["params"], str):
            r["params"] = json.loads(r["params"])
        elif not r.get("params"):
            r["params"] = {}
        # Normalize column name (Delta may use target_column)
        if "target_column" in r and "column" not in r:
            r["column"] = r["target_column"]
        rules.append(r)

    mappings = []
    for row in spark.table(MAPPINGS_TABLE).collect():
        mappings.append(row.asDict())

    print(f"[RuleLoader] Loaded {len(rules)} rules, {len(mappings)} mappings from Delta")
    return rules, mappings


def _load_rules_from_json():
    """Load rules and mappings from JSON files (fallback for POC/dev)."""
    ws_paths = [
        "/Workspace/Users/pritam.paul@databricks.com/Quality_Framework",
        "/Workspace/Repos/pritam.paul@databricks.com/Quality_Framework",
    ]
    base = None
    for wp in ws_paths:
        if os.path.exists(os.path.join(wp, "mock_collibra", "dq_rules.json")):
            base = wp
            break
    if base is None:
        raise FileNotFoundError("Cannot find mock_collibra/dq_rules.json in workspace")
    with open(os.path.join(base, "mock_collibra", "dq_rules.json")) as f:
        rules = json.load(f)["rules"]
    with open(os.path.join(base, "mock_collibra", "dq_rule_mappings.json")) as f:
        mappings = json.load(f)["mappings"]
    print(f"[RuleLoader] Loaded {len(rules)} rules, {len(mappings)} mappings from JSON (fallback)")
    return rules, mappings


def _load_rules():
    """Load rules from Delta tables first; fall back to JSON if tables don't exist."""
    try:
        spark.table(RULES_TABLE).limit(1).collect()
        return _load_rules_from_delta()
    except Exception:
        print(f"[RuleLoader] Delta tables not found ({RULES_TABLE}), falling back to JSON")
        return _load_rules_from_json()


ALL_RULES, ALL_MAPPINGS = _load_rules()

def get_rules_for_dataset(dataset_key):
    ds_maps = [m for m in ALL_MAPPINGS if m["dataset_name"] == dataset_key and m.get("is_active", True)]
    rule_ids = {m["rule_id"] for m in ds_maps}
    ds_rules = [r for r in ALL_RULES if r["rule_id"] in rule_ids and r.get("is_active", True)]
    priority = {m["rule_id"]: m.get("priority", 999) for m in ds_maps}
    ds_rules.sort(key=lambda r: priority.get(r["rule_id"], 999))
    return ds_rules, ds_maps

def get_dataset_expectations(dataset_key):
    rules, mappings = get_rules_for_dataset(dataset_key)
    all_exprs = RuleExpressionBuilder.build_all_expressions(rules, mappings)
    # Pass all rules (hard + soft) to expect_all for observability in DLT UI.
    # No enforcement_filter → includes both hard and soft rules.
    # Still skips uniqueness + referential_integrity (window funcs / subqueries).
    all_expectations = RuleExpressionBuilder.build_expectations_dict(rules, None, mappings)

    # Build ref_tables mapping for referential integrity rules
    ref_info = {}
    for r in rules:
        if r["rule_type"] == "referential_integrity":
            params = r.get("params", {})
            ref_info[f"{r['column']}_ref"] = {
                "ref_table": params.get("ref_table", ""),
                "ref_column": params.get("ref_column", r["column"]),
            }

    return rules, mappings, all_exprs, all_expectations, ref_info

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean / Quarantine Filters (using _dq struct)

# COMMAND ----------

def _filter_clean(df):
    """Rows where ALL hard-enforcement rules passed. Uses _dq.results array."""
    return df.filter(
        ~F.exists(
            F.col("_dq.results"),
            lambda r: (r["enforcement"] == "hard") & (~r["passed"])
        )
    )

def _filter_quarantine(df):
    """Rows where ANY hard-enforcement rule failed."""
    return (
        df.filter(
            F.exists(
                F.col("_dq.results"),
                lambda r: (r["enforcement"] == "hard") & (~r["passed"])
            )
        )
        .withColumn("_quarantine_reason", F.concat_ws(", ", F.col("_dq.failed_rules")))
        .withColumn("_quarantined_at", F.current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers Pipeline

# COMMAND ----------

_, _, _cust_exprs, _cust_expect, _cust_ref_info = get_dataset_expectations("bronze.customers")

@dlt.table(name="customers_validated", comment="Source of truth — all customers with _dq + _lineage", table_properties={"quality": "silver"})
@dlt.expect_all(_cust_expect)
def customers_validated():
    # Build ref_tables from DLT reads for referential integrity checks
    ref_tables = {}
    if "country_code_ref" in _cust_ref_info:
        ref_tables["country_code_ref"] = dlt.read("ref_dim_country")
    return RowLevelEvaluator.evaluate(dlt.read("customers_bronze"), _cust_exprs, "dq_poc.silver.customers_validated", ref_tables)

@dlt.view(name="customers_clean", comment="Customers passing all hard rules — view over validated (no data duplication)")
def customers_clean():
    return _filter_clean(dlt.read("customers_validated"))

@dlt.view(name="customers_quarantine", comment="Customers failing any hard rule — view over validated (no data duplication)")
def customers_quarantine():
    return _filter_quarantine(dlt.read("customers_validated"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Transactions Pipeline

# COMMAND ----------

_, _, _txn_exprs, _txn_expect, _txn_ref_info = get_dataset_expectations("bronze.transactions")

@dlt.table(name="transactions_validated", comment="Source of truth — all transactions with _dq + _lineage", table_properties={"quality": "silver"})
@dlt.expect_all(_txn_expect)
def transactions_validated():
    ref_tables = {}
    if "customer_id_ref" in _txn_ref_info:
        ref_tables["customer_id_ref"] = dlt.read("customers_bronze")
    return RowLevelEvaluator.evaluate(dlt.read("transactions_bronze"), _txn_exprs, "dq_poc.silver.transactions_validated", ref_tables)

@dlt.view(name="transactions_clean", comment="Transactions passing all hard rules — view over validated")
def transactions_clean():
    return _filter_clean(dlt.read("transactions_validated"))

@dlt.view(name="transactions_quarantine", comment="Transactions failing any hard rule — view over validated")
def transactions_quarantine():
    return _filter_quarantine(dlt.read("transactions_validated"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Products Pipeline

# COMMAND ----------

_, _, _prod_exprs, _prod_expect, _prod_ref_info = get_dataset_expectations("bronze.products")

@dlt.table(name="products_validated", comment="Source of truth — all products with _dq + _lineage", table_properties={"quality": "silver"})
@dlt.expect_all(_prod_expect)
def products_validated():
    # Products have no ref integrity rules, but pass empty dict for consistency
    return RowLevelEvaluator.evaluate(dlt.read("products_bronze"), _prod_exprs, "dq_poc.silver.products_validated", {})

@dlt.view(name="products_clean", comment="Products passing all hard rules — view over validated")
def products_clean():
    return _filter_clean(dlt.read("products_validated"))

@dlt.view(name="products_quarantine", comment="Products failing any hard rule — view over validated")
def products_quarantine():
    return _filter_quarantine(dlt.read("products_validated"))
