# Metadata-Driven Data Quality Framework — SDP Implementation

## Design Document v2.0

**Author:** Pritam Paul
**Date:** April 7, 2026
**Status:** Deployed & Running
**Engine:** Spark Declarative Pipelines (Delta Live Tables)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Problem Statement](#2-problem-statement)
3. [Architecture Overview](#3-architecture-overview)
4. [System Components](#4-system-components)
5. [Data Model](#5-data-model)
6. [Rule Engine Design](#6-rule-engine-design)
7. [Pipeline Design — DLT Implementation](#7-pipeline-design--dlt-implementation)
8. [Row-Level DQ Tracking](#8-row-level-dq-tracking)
9. [Lineage Tracking](#9-lineage-tracking)
10. [Observability & Dashboards](#10-observability--dashboards)
11. [Workflow Orchestration](#11-workflow-orchestration)
12. [Deployment Model](#12-deployment-model)
13. [Execution Sequence](#13-execution-sequence)
14. [Configuration Reference](#14-configuration-reference)
15. [Design Decisions & Trade-offs](#15-design-decisions--trade-offs)
16. [Future Enhancements](#16-future-enhancements)

---

## 1. Executive Summary

This document describes the **SDP (Spark Declarative Pipeline)** implementation of the Metadata-Driven Data Quality Framework. The framework integrates **Collibra** (governance) with **Databricks DLT** (execution) to deliver centrally-defined, automatically-executed data quality checks with full row-level traceability.

**Key capabilities:**
- 20 DQ rules defined externally in Collibra (mocked as JSON for POC)
- 9 rule types: not_null, range, regex, uniqueness, allowed_values, length, freshness, referential_integrity, custom_sql
- Per-row DQ scoring via a schema-stable `_dq` struct column
- End-to-end lineage via a `_lineage` struct column (source file → bronze → silver → gold)
- Hard/soft enforcement with automatic clean/quarantine split
- Gold-layer aggregation tables for observability
- 4-page Lakeview AI/BI dashboard (19 datasets, 20 widgets)
- Daily orchestrated workflow

---

## 2. Problem Statement

Data quality rules are typically:
- Hardcoded in pipeline logic, making them brittle and opaque
- Disconnected from governance tools, creating a gap between policy and execution
- Evaluated at the dataset level, with no visibility into which specific rows failed which rules

**This framework solves these problems by:**

| Problem | Solution |
|---|---|
| Rules hardcoded in pipelines | Rules defined externally in Collibra, loaded dynamically at runtime |
| No governance integration | Closed-loop: Collibra → Databricks → (future) Collibra feedback |
| Dataset-level pass/fail only | Row-level evaluation: every row carries its own DQ scorecard |
| No traceability | `_dq` struct tracks per-rule pass/fail; `_lineage` tracks data provenance |
| Manual quality reporting | Automated Gold aggregation tables + Lakeview dashboard |

---

## 3. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                        GOVERNANCE LAYER                              │
│                                                                      │
│   mock_collibra/dq_rules.json          20 rules, 9 types            │
│   mock_collibra/dq_rule_mappings.json  20 mappings, hard/soft        │
└──────────────────────┬───────────────────────────────────────────────┘
                       │ loaded at DLT import time
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     RULE ENGINE (embedded in DLT)                     │
│                                                                      │
│   RuleExpressionBuilder          Rule dict → SQL boolean expression  │
│   RowLevelEvaluator              SQL expressions → _dq struct        │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     DLT PIPELINE (3 notebooks)                       │
│                                                                      │
│   ┌─────────────┐   ┌──────────────────┐   ┌──────────────────┐     │
│   │   BRONZE     │──▶│     SILVER        │──▶│      GOLD        │     │
│   │             │   │                  │   │                  │     │
│   │ + _lineage  │   │ + _dq struct     │   │ Exploded detail  │     │
│   │             │   │ + _lineage       │   │ Aggregated KPIs  │     │
│   │ 3 tables    │   │ + clean/quarant. │   │ Business tables  │     │
│   │ 1 ref table │   │ 9 tables/views   │   │ 7 tables         │     │
│   └─────────────┘   └──────────────────┘   └──────────────────┘     │
└──────────────────────────────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     OBSERVABILITY LAYER                               │
│                                                                      │
│   Lakeview AI/BI Dashboard — 4 pages, 19 datasets, 20 widgets       │
│   DLT Event Log — native expectation metrics                         │
│   Workflow — daily scheduled orchestration                            │
└──────────────────────────────────────────────────────────────────────┘
```

**Catalog:** `dq_poc`
**Schemas:** `bronze`, `silver`, `gold`, `dq_sdp_results`

---

## 4. System Components

### 4.1 Directory Structure

```
sdp_poc/
├── configs/
│   └── sdp_pipeline_config.yaml       # Framework configuration
├── dashboards/
│   ├── 01_sdp_dq_dashboard.sql        # SQL reference queries
│   └── lakeview_dq_dashboard.json     # Lakeview API dashboard definition
├── engine/
│   ├── __init__.py
│   ├── rule_loader.py                 # SDPRuleLoader — loads rules from JSON/Delta
│   ├── rule_expression_builder.py     # RuleExpressionBuilder — rule → SQL
│   └── row_level_evaluator.py         # RowLevelEvaluator — SQL → _dq columns
├── notebooks/
│   ├── 00_sdp_setup.py                # Schema creation + rule preview
│   ├── 01_sdp_deploy_pipeline.py      # Programmatic DLT deployment
│   ├── 02_sdp_dq_analysis.py          # Post-run analysis queries
│   ├── 03_sdp_run_end_to_end.py       # Standalone demo (non-DLT)
│   └── 04_sdp_deploy_dashboard.py     # Dashboard deployment via API
├── pipelines/
│   ├── __init__.py
│   ├── sdp_bronze_sources.py          # DLT Bronze layer
│   ├── sdp_dq_pipeline.py            # DLT Silver layer (core DQ)
│   └── sdp_gold_pipeline.py          # DLT Gold layer (aggregation)
└── workflows/
    └── sdp_dq_framework_daily.json    # Daily orchestration definition
```

### 4.2 Shared Resources (from parent project)

```
Quality_Framework/
├── mock_collibra/
│   ├── dq_rules.json                  # 20 rule definitions
│   └── dq_rule_mappings.json          # 20 dataset-rule associations
└── mock_data/
    ├── customers.csv                  # 17 rows (intentional quality issues)
    ├── transactions.csv               # 16 rows (intentional quality issues)
    ├── products.csv                   # 15 rows (intentional quality issues)
    └── dim_country.csv                # 12 rows (reference table)
```

### 4.3 Component Responsibilities

| Component | Role | Used By |
|---|---|---|
| `engine/rule_loader.py` | Load rules from JSON or Delta, filter by dataset | Setup notebook, standalone runner |
| `engine/rule_expression_builder.py` | Convert rule metadata → SQL expressions | Setup notebook, standalone runner |
| `engine/row_level_evaluator.py` | Evaluate SQL per row, produce _dq columns | Standalone runner |
| `pipelines/sdp_dq_pipeline.py` | DLT Silver layer with **embedded** engine copies | DLT runtime |
| `pipelines/sdp_bronze_sources.py` | DLT Bronze layer with lineage stamping | DLT runtime |
| `pipelines/sdp_gold_pipeline.py` | DLT Gold layer — explode, aggregate, business tables | DLT runtime |

> **Why embedded copies in DLT?** DLT notebooks run in an isolated execution context where `__file__` is unavailable, `%run` is unsupported, and `sys.path` imports from local modules don't work. The engine classes are therefore inlined in `sdp_dq_pipeline.py`. The `engine/` folder remains the canonical reference implementation, used by non-DLT notebooks.

---

## 5. Data Model

### 5.1 Rule Definition Schema

Rules are defined in `mock_collibra/dq_rules.json`:

```json
{
  "rule_id": "DQR-000003",
  "rule_name": "customer_age_valid_range",
  "rule_type": "range",
  "column": "age",
  "params": { "min": 18, "max": 120 },
  "severity": "error",
  "is_active": true,
  "version": 1,
  "effective_from": "2025-01-01",
  "owner": "data_quality_team"
}
```

**Rule ID pattern:** `DQR-XXXXXX` (6-digit zero-padded)

### 5.2 Rule Mapping Schema

Mappings in `mock_collibra/dq_rule_mappings.json` bind rules to datasets:

```json
{
  "mapping_id": "MAP-000003",
  "dataset_name": "bronze.customers",
  "column_name": "age",
  "rule_id": "DQR-000003",
  "enforcement_mode": "hard",
  "priority": 3,
  "is_active": true
}
```

**Enforcement modes:**
- **hard** — rows failing this rule are moved to quarantine (excluded from clean output)
- **soft** — rows are flagged but retained in clean output; DLT tracks metrics via `@dlt.expect_all()`

### 5.3 Rule Inventory

#### Customers (8 rules)

| Rule ID | Type | Column | Enforcement | Severity |
|---|---|---|---|---|
| DQR-000001 | not_null | email | hard | error |
| DQR-000002 | not_null | customer_name | hard | error |
| DQR-000003 | range | age | hard | error |
| DQR-000004 | regex | email | hard | error |
| DQR-000005 | length | phone | soft | warning |
| DQR-000006 | allowed_values | status | hard | error |
| DQR-000007 | uniqueness | customer_id | hard | error |
| DQR-000008 | referential_integrity | country_code | soft | warning |

#### Transactions (7 rules)

| Rule ID | Type | Column | Enforcement | Severity |
|---|---|---|---|---|
| DQR-000009 | not_null | amount | hard | error |
| DQR-000010 | range | amount | hard | error |
| DQR-000011 | allowed_values | transaction_type | hard | error |
| DQR-000012 | not_null | transaction_date | hard | error |
| DQR-000013 | referential_integrity | customer_id | hard | error |
| DQR-000014 | freshness | transaction_date | soft | warning |
| DQR-000015 | custom_sql | quantity | soft | warning |

#### Products (5 rules)

| Rule ID | Type | Column | Enforcement | Severity |
|---|---|---|---|---|
| DQR-000016 | not_null | product_name | hard | error |
| DQR-000017 | range | price | hard | error |
| DQR-000018 | allowed_values | category | soft | warning |
| DQR-000019 | regex | sku | soft | warning |
| DQR-000020 | uniqueness | sku | hard | error |

### 5.4 Table Inventory

#### Bronze Layer (4 DLT tables)

| Table | Source | Columns Added |
|---|---|---|
| `customers_bronze` | `dq_poc.bronze.customers` | `_lineage` struct |
| `transactions_bronze` | `dq_poc.bronze.transactions` | `_lineage` struct |
| `products_bronze` | `dq_poc.bronze.products` | `_lineage` struct |
| `ref_dim_country` | `dq_poc.silver.dim_country` | `_lineage` struct |

#### Silver Layer (9 DLT tables/views)

| Table | Type | Description |
|---|---|---|
| `customers_validated` | table | All rows + `_dq` struct + `_lineage` (source of truth) |
| `customers_clean` | view | Rows passing all hard-enforcement rules |
| `customers_quarantine` | view | Rows failing any hard-enforcement rule |
| `transactions_validated` | table | All rows + `_dq` + `_lineage` |
| `transactions_clean` | view | Clean transactions |
| `transactions_quarantine` | view | Quarantined transactions |
| `products_validated` | table | All rows + `_dq` + `_lineage` |
| `products_clean` | view | Clean products |
| `products_quarantine` | view | Quarantined products |

#### Gold Layer (7 DLT tables)

| Table | Description |
|---|---|
| `dq_row_level_detail_customers` | Exploded: one row per (customer_row × rule) |
| `dq_row_level_detail_transactions` | Exploded: one row per (transaction_row × rule) |
| `dq_row_level_detail_products` | Exploded: one row per (product_row × rule) |
| `dq_row_level_detail_all` | Union of all three detail tables |
| `dq_summary_by_dataset` | Aggregate pass rates per dataset |
| `dq_summary_by_rule` | Aggregate pass rates per rule |
| `gold_customer_360` | Clean customers + transaction metrics + engagement tier |
| `gold_product_catalog` | Clean products + DQ score |

---

## 6. Rule Engine Design

The rule engine converts Collibra rule metadata into executable SQL. It has three components that form a pipeline: **Load → Build → Evaluate**.

### 6.1 SDPRuleLoader

**File:** `engine/rule_loader.py`
**Class:** `SDPRuleLoader`

Loads rules and mappings from either JSON files (POC) or Delta tables (production).

```
SDPRuleLoader(use_delta=False)
  │
  ├─ _find_repo_root()          # Resolves path across local / Workspace / Repos
  ├─ _load_from_json()          # Reads mock_collibra/*.json
  │   └─ Returns: _rules (list[dict]), _mappings (list[dict])
  │
  ├─ get_rules_for_dataset("bronze.customers")
  │   ├─ Filter mappings by dataset_name + is_active
  │   ├─ Match rule_ids to rules
  │   ├─ Sort by mapping priority
  │   └─ Returns: (rules, mappings) tuple
  │
  └─ get_all_rules()            # Returns all rules + mappings
```

**Path resolution order:**
1. `__file__` (works locally and in standard notebooks)
2. Hardcoded workspace paths (`/Workspace/Users/...`, `/Workspace/Repos/...`)
3. Current working directory
4. Raises `FileNotFoundError` with guidance

### 6.2 RuleExpressionBuilder

**File:** `engine/rule_expression_builder.py`
**Class:** `RuleExpressionBuilder`

Converts each rule definition into a SQL boolean expression where **TRUE = row passes**.

#### Rule Type → SQL Mapping

| Rule Type | Generated SQL Expression |
|---|---|
| `not_null` | `` `column` IS NOT NULL `` |
| `range` | `` (`column` >= {min} AND `column` <= {max}) `` |
| `regex` | `` `column` RLIKE '{pattern}' `` |
| `uniqueness` | `` (COUNT(1) OVER (PARTITION BY `column`)) = 1 `` |
| `allowed_values` | `` `column` IN ('val1', 'val2', ...) `` |
| `length` | `` (LENGTH(`column`) >= {min} AND LENGTH(`column`) <= {max}) `` |
| `freshness` | `` (TIMESTAMPDIFF(HOUR, CAST(`column` AS TIMESTAMP), CURRENT_TIMESTAMP()) <= {hours}) `` |
| `referential_integrity` | `` `column` IN (SELECT `ref_col` FROM live.{ref_table}) `` |
| `custom_sql` | `` ({sql_expression}) `` |

#### Key Methods

**`build_expression(rule)`** — Single rule → SQL string.

**`build_expectations_dict(rules, enforcement_filter, mappings)`** — Returns `{display_name: sql}` dict for `@dlt.expect_all()`. Skips `uniqueness` and `referential_integrity` (these require window functions or subqueries that DLT expectations don't support).

**`build_all_expressions(rules, mappings)`** — Returns full metadata list for row-level evaluation:
```python
[{
    "rule_id": "DQR-000003",
    "rule_name": "customer_age_valid_range",
    "rule_type": "range",
    "column": "age",
    "severity": "error",
    "enforcement_mode": "hard",
    "sql_expression": "(`age` >= 18 AND `age` <= 120)"
}, ...]
```

### 6.3 RowLevelEvaluator

**File:** `engine/row_level_evaluator.py`
**Class:** `RowLevelEvaluator`

Runs each SQL expression against every row using `F.expr()` and produces annotation columns.

#### Evaluation Flow

```
Input DataFrame (N columns)
        │
        ▼
evaluate(df, rule_expressions)
        │
        ├── For each rule:
        │     withColumn("_dq_DQR_000001", F.coalesce(F.expr(sql).cast("boolean"), lit(False)))
        │
        ├── _add_summary_columns()
        │     ├── _dq_passed_rules:  array of passing rule IDs
        │     ├── _dq_failed_rules:  array of failing rule IDs
        │     ├── _dq_passed_count:  size(passed_rules)
        │     ├── _dq_failed_count:  size(failed_rules)
        │     ├── _dq_total_rules:   literal count
        │     ├── _dq_row_score:     (passed / total) * 100
        │     ├── _dq_all_passed:    failed_count == 0
        │     └── _dq_evaluated_at:  current_timestamp()
        │
        ▼
Output DataFrame (N + R + 8 columns, where R = number of rules)
```

#### Split Logic

**`split_clean_quarantine(annotated_df, rule_expressions)`**:
- Identifies rules with `enforcement_mode == "hard"`
- Builds a compound boolean: `_dq_DQR_000001 AND _dq_DQR_000002 AND ...`
- `clean_df` = rows where ALL hard rules pass
- `quarantine_df` = rows where ANY hard rule fails

#### Detail Exploder

**`build_row_dq_detail(annotated_df, rule_expressions, dataset_name, run_id)`**:
- Creates an array of structs (one per rule) with `{rule_id, rule_name, rule_type, column, severity, enforcement, passed}`
- Explodes into long-format: one row per (source_row × rule) combination
- Used by Gold layer for granular analytics

---

## 7. Pipeline Design — DLT Implementation

The DLT pipeline consists of three notebooks registered as libraries in a single pipeline. DLT resolves the dependency graph automatically.

### 7.1 Bronze Layer — `sdp_bronze_sources.py`

**Purpose:** Read raw data from pre-existing Bronze Delta tables and stamp with lineage metadata.

```
dq_poc.bronze.customers       ──▶  customers_bronze      (DLT table)
dq_poc.bronze.transactions    ──▶  transactions_bronze    (DLT table)
dq_poc.bronze.products        ──▶  products_bronze        (DLT table)
dq_poc.silver.dim_country     ──▶  ref_dim_country        (DLT table)
```

Each row receives a `_lineage` struct (see Section 9).

### 7.2 Silver Layer — `sdp_dq_pipeline.py`

**Purpose:** Core DQ evaluation. This is the most critical notebook.

#### Architecture per Dataset

For each dataset (customers, transactions, products), the pipeline performs:

```
        ┌─────────────────────┐
        │  Load rules from    │
        │  mock_collibra JSON │
        └─────────┬───────────┘
                  │
                  ▼
        ┌─────────────────────┐
        │ build_all_expressions│   Rule metadata → SQL expressions
        │ build_expectations   │   Soft rules → @dlt.expect_all dict
        └─────────┬───────────┘
                  │
                  ▼
   ┌──────────────────────────────┐
   │     @dlt.table                │
   │     @dlt.expect_all(soft)     │   DLT tracks soft rule metrics natively
   │                              │
   │  1. Read from Bronze DLT     │
   │  2. Evaluate ALL rules       │   F.expr(sql) → boolean per rule
   │  3. Build _dq struct         │   Schema-stable struct column
   │  4. Enrich _lineage.silver   │   Add silver table name + timestamp
   │                              │
   │  → xxx_validated             │   SOURCE OF TRUTH (all rows)
   └──────────────┬───────────────┘
                  │
         ┌────────┴────────┐
         ▼                 ▼
   ┌───────────┐    ┌──────────────┐
   │ xxx_clean │    │xxx_quarantine│
   │  (view)   │    │   (view)     │
   │           │    │              │
   │ all hard  │    │ any hard     │
   │ rules pass│    │ rule fails   │
   └───────────┘    └──────────────┘
```

#### DLT Expectations Integration

Soft-enforcement rules are registered with DLT's native expectation framework:

```python
soft_expectations = RuleExpressionBuilder.build_expectations_dict(
    rules, enforcement_filter="soft", mappings=mappings
)

@dlt.table(name="customers_validated")
@dlt.expect_all(soft_expectations)
def customers_validated():
    ...
```

This gives DLT's built-in tracking (event log metrics, UI badges) for soft rules, while hard rules are handled by the row-level evaluator + clean/quarantine split.

**Skipped from `@dlt.expect_all`:** `uniqueness` (requires window function) and `referential_integrity` (requires subquery). Both are still evaluated at the row level via the evaluator.

#### The `_dq` Struct Column

Instead of adding N boolean columns (one per rule), the DLT pipeline packs everything into a single schema-stable struct:

```
_dq: STRUCT<
  results:       ARRAY<STRUCT<rule_id, rule_name, rule_type, column, severity, enforcement, passed>>
  passed_rules:  ARRAY<STRING>
  failed_rules:  ARRAY<STRING>
  passed_count:  INT
  failed_count:  INT
  total_rules:   INT
  row_score:     DOUBLE       -- 0 to 100
  all_passed:    BOOLEAN
  evaluated_at:  TIMESTAMP
>
```

**Why a struct?** Adding a new rule does NOT change the table schema — the `results` array simply gains an additional element. This avoids schema evolution issues in Delta and downstream consumers.

#### DLT Reference Table Resolution

Referential integrity rules need to query other DLT tables. The embedded `RuleExpressionBuilder` maps logical names to DLT `live.` references:

```python
dlt_ref_map = {
    "dim_country": "ref_dim_country",
    "customers": "customers_bronze"
}
# Generates: `country_code` IN (SELECT `country_code` FROM live.ref_dim_country)
```

### 7.3 Gold Layer — `sdp_gold_pipeline.py`

**Purpose:** Explode row-level results for analytics, compute aggregates, build business tables.

#### Row-Level Detail (Exploded)

```
customers_validated._dq.results  ──explode──▶  dq_row_level_detail_customers
transactions_validated._dq.results           ──▶  dq_row_level_detail_transactions
products_validated._dq.results               ──▶  dq_row_level_detail_products
                                                         │
                                                         ▼
                                              dq_row_level_detail_all (UNION)
```

Each row in the detail table represents one rule evaluation on one source row:

| Column | Description |
|---|---|
| `dataset_name` | e.g., "customers" |
| `rule_id` | e.g., "DQR-000003" |
| `rule_name` | e.g., "customer_age_valid_range" |
| `rule_type` | e.g., "range" |
| `target_column` | e.g., "age" |
| `severity` | "error" or "warning" |
| `enforcement_mode` | "hard" or "soft" |
| `passed` | true / false |
| `evaluated_at` | timestamp |

#### Aggregate Summaries

**`dq_summary_by_dataset`:**

| Column | Description |
|---|---|
| `dataset_name` | Dataset identifier |
| `total_evaluations` | Total rule × row evaluations |
| `passed_count` | Evaluations that passed |
| `failed_count` | Evaluations that failed |
| `pass_rate_pct` | (passed / total) × 100 |
| `fully_clean_records` | Rows where all rules passed |
| `avg_row_score` | Average _dq.row_score |

**`dq_summary_by_rule`:**

| Column | Description |
|---|---|
| `rule_id` | Rule identifier |
| `rule_name` | Human-readable name |
| `rule_type` | Rule type |
| `severity` | error / warning |
| `enforcement_mode` | hard / soft |
| `total_evaluated` | Total rows this rule was applied to |
| `passed_count` / `failed_count` | Per-rule counts |
| `pass_rate_pct` | Per-rule pass rate |

#### Business Tables

**`gold_customer_360`:**
- Source: `customers_clean` LEFT JOIN `transactions_clean`
- Adds: `total_transactions`, `total_spend`, `avg_transaction_amount`
- Adds: `engagement_tier` (Platinum / Gold / Silver / Bronze based on spend)
- Enriches `_lineage.gold` with table name and processing timestamp

**`gold_product_catalog`:**
- Source: `products_clean`
- Adds: `dq_quality_score` from `_dq.row_score`
- Enriches `_lineage.gold`

---

## 8. Row-Level DQ Tracking

This is the core differentiator of the SDP implementation. Every row in `*_validated` tables carries a complete DQ scorecard.

### 8.1 Example: A Failing Customer Row

Given a customer with `age = 150`:

```json
{
  "customer_id": "C006",
  "customer_name": "Frank Wilson",
  "age": 150,
  "_dq": {
    "results": [
      {"rule_id": "DQR-000001", "rule_type": "not_null",  "column": "email",  "passed": true},
      {"rule_id": "DQR-000002", "rule_type": "not_null",  "column": "customer_name", "passed": true},
      {"rule_id": "DQR-000003", "rule_type": "range",     "column": "age",    "passed": false},
      {"rule_id": "DQR-000004", "rule_type": "regex",     "column": "email",  "passed": true},
      {"rule_id": "DQR-000005", "rule_type": "length",    "column": "phone",  "passed": true},
      {"rule_id": "DQR-000006", "rule_type": "allowed_values", "column": "status", "passed": true},
      {"rule_id": "DQR-000007", "rule_type": "uniqueness","column": "customer_id", "passed": true},
      {"rule_id": "DQR-000008", "rule_type": "referential_integrity", "column": "country_code", "passed": true}
    ],
    "passed_rules": ["DQR-000001","DQR-000002","DQR-000004","DQR-000005","DQR-000006","DQR-000007","DQR-000008"],
    "failed_rules": ["DQR-000003"],
    "passed_count": 7,
    "failed_count": 1,
    "total_rules": 8,
    "row_score": 87.5,
    "all_passed": false,
    "evaluated_at": "2026-04-07T06:00:00Z"
  }
}
```

This row would appear in `customers_validated` and `customers_quarantine` (because DQR-000003 is hard-enforcement), but NOT in `customers_clean`.

### 8.2 Schema Stability

The `_dq` struct uses an array of results rather than individual columns. This means:
- Adding a new rule → array gains one element → **no schema change**
- Removing a rule → array loses one element → **no schema change**
- Delta table schema remains stable regardless of rule count changes
- Downstream consumers (dashboards, notebooks) don't break when rules change

---

## 9. Lineage Tracking

Every row carries a `_lineage` struct that tracks its journey through the medallion architecture.

### 9.1 Lineage Struct Schema

```
_lineage: STRUCT<
  source: STRUCT<
    file_name:    STRING    -- e.g., "customers.csv"
    file_path:    STRING    -- e.g., "/Volumes/dq_poc/bronze/mock_data/customers.csv"
    file_date:    STRING    -- e.g., "2026-04-07"
    format:       STRING    -- e.g., "csv"
  >
  bronze: STRUCT<
    table:        STRING    -- e.g., "customers_bronze"
    ingested_at:  TIMESTAMP
  >
  silver: STRUCT<
    table:        STRING    -- e.g., "customers_validated"  (NULL until silver)
    validated_at: TIMESTAMP                                 (NULL until silver)
  >
  gold: STRUCT<
    table:        STRING    -- e.g., "gold_customer_360"    (NULL until gold)
    processed_at: TIMESTAMP                                 (NULL until gold)
  >
  pipeline: STRUCT<
    name:         STRING    -- e.g., "DQ Framework - SDP Pipeline"
    updated_at:   TIMESTAMP
  >
>
```

### 9.2 Progressive Enrichment

| Layer | Fields Set | Fields NULL |
|---|---|---|
| Bronze | `source.*`, `bronze.*`, `pipeline.*` | `silver.*`, `gold.*` |
| Silver | `source.*`, `bronze.*`, `silver.*`, `pipeline.*` | `gold.*` |
| Gold | All fields populated | — |

This enables full provenance queries: "Where did this gold record come from? Which file? When was it ingested? When validated? When did it reach gold?"

---

## 10. Observability & Dashboards

### 10.1 Lakeview AI/BI Dashboard

**Name:** "Data Quality Command Center — SDP Framework"
**Definition:** `dashboards/lakeview_dq_dashboard.json`

#### Page 1 — Executive Overview (8 widgets)

| Widget | Type | Source |
|---|---|---|
| Overall DQ Score | Counter | `dq_summary_by_dataset` (weighted avg) |
| Total Records Evaluated | Counter | `dq_row_level_detail_all` (distinct rows) |
| Active Rules | Counter | `dq_summary_by_rule` (count) |
| Total Failures | Counter | `dq_row_level_detail_all` (where passed = false) |
| DQ Pass Rate by Dataset | Bar | `dq_summary_by_dataset` |
| Clean vs Quarantine | Stacked Bar | `*_clean` / `*_quarantine` counts |
| Score Distribution | Grouped Bar | 5 buckets: Perfect (100), Good (80-99), Fair (60-79), Poor (40-59), Critical (<40) |
| Dataset Summary Table | Table | `dq_summary_by_dataset` |

#### Page 2 — Rule Analysis (5 widgets)

| Widget | Type | Source |
|---|---|---|
| Pass Rate by Rule | Horizontal Bar | `dq_summary_by_rule` (ordered worst-first) |
| Rule Health Matrix | Table | Color-coded: GREEN ≥95%, AMBER 80-95%, RED <80% |
| Failures by Rule Type | Bar | Grouped by rule_type |
| Rules by Severity | Table | error vs warning breakdown |
| Full Rule Detail | Table | `dq_summary_by_rule` (all columns) |

#### Page 3 — Row-Level Detail (3 widgets)

| Widget | Type | Source |
|---|---|---|
| Failing Customers | Table | `customers_validated` WHERE `_dq.all_passed = false` |
| Failing Transactions | Table | `transactions_validated` WHERE `_dq.all_passed = false` |
| Failing Products | Table | `products_validated` WHERE `_dq.all_passed = false` |

#### Page 4 — Quarantine Zone (3 widgets)

| Widget | Type | Source |
|---|---|---|
| Quarantined Customers | Table | `customers_quarantine` |
| Quarantined Transactions | Table | `transactions_quarantine` |
| Quarantined Products | Table | `products_quarantine` |

### 10.2 DLT Event Log

DLT natively tracks expectation metrics in its event log:
- Number of records that passed/failed each `@dlt.expect_all` rule
- Available via `event_log` table for the pipeline

### 10.3 SQL Reference Queries

`dashboards/01_sdp_dq_dashboard.sql` contains 20 standalone SQL queries that can be used to manually create dashboards or run ad-hoc analysis outside the Lakeview dashboard.

---

## 11. Workflow Orchestration

**Definition:** `workflows/sdp_dq_framework_daily.json`

### 11.1 Task DAG

```
00_verify_setup
      │
      ▼
01_run_dlt_pipeline    (DLT pipeline: full_refresh=false)
      │
      ▼
02_dq_analysis         (post-run reporting)
```

### 11.2 Schedule

| Setting | Value |
|---|---|
| Cron | `0 0 6 * * ?` (6:00 AM UTC daily) |
| Status | PAUSED (enable manually after deployment) |
| Max concurrent runs | 1 |
| Notifications | Email on failure |

### 11.3 Task Details

| Task | Type | Depends On | Description |
|---|---|---|---|
| `00_verify_setup` | Notebook | — | Verify schemas and Bronze tables exist |
| `01_run_dlt_pipeline` | DLT Pipeline | 00 | Execute the 3-notebook DLT pipeline |
| `02_dq_analysis` | Notebook | 01 | Run analysis queries, generate reports |

---

## 12. Deployment Model

### 12.1 Prerequisites

1. Databricks workspace with Unity Catalog enabled
2. Catalog `dq_poc` with CREATE permissions
3. A SQL warehouse (for dashboard deployment)
4. Workspace path for the repository (Git folder or Repos)

### 12.2 Deployment Steps

| Step | Notebook | What It Does |
|---|---|---|
| 1 | `notebooks/00_setup.py` (parent) | Creates catalog, schemas, loads CSVs → Bronze Delta tables |
| 2 | `sdp_poc/notebooks/00_sdp_setup.py` | Creates `dq_sdp_results` schema, verifies Bronze tables, previews rules |
| 3 | `sdp_poc/notebooks/01_sdp_deploy_pipeline.py` | Creates DLT pipeline via SDK, starts it, waits for completion |
| 4 | `sdp_poc/notebooks/04_sdp_deploy_dashboard.py` | Deploys Lakeview dashboard via REST API |

### 12.3 DLT Pipeline Configuration

```json
{
  "name": "dq_framework_sdp",
  "catalog": "dq_poc",
  "target": "silver",
  "development": true,
  "continuous": false,
  "photon": true,
  "channel": "CURRENT",
  "libraries": [
    "sdp_poc/pipelines/sdp_bronze_sources",
    "sdp_poc/pipelines/sdp_dq_pipeline",
    "sdp_poc/pipelines/sdp_gold_pipeline"
  ],
  "clusters": [{
    "label": "default",
    "autoscale": { "min_workers": 1, "max_workers": 4, "mode": "ENHANCED" }
  }]
}
```

---

## 13. Execution Sequence

Complete data flow from a cold start:

```
PHASE 1 — FOUNDATION (one-time, notebook: 00_setup.py)
│
├─ CREATE CATALOG dq_poc
├─ CREATE SCHEMA bronze, silver, gold, dq_framework
├─ LOAD mock_data/customers.csv      → dq_poc.bronze.customers     (17 rows)
├─ LOAD mock_data/transactions.csv   → dq_poc.bronze.transactions  (16 rows)
├─ LOAD mock_data/products.csv       → dq_poc.bronze.products      (15 rows)
└─ LOAD mock_data/dim_country.csv    → dq_poc.silver.dim_country   (12 rows)

PHASE 2 — SDP SETUP (one-time, notebook: 00_sdp_setup.py)
│
├─ CREATE SCHEMA dq_sdp_results
├─ Verify all Bronze tables exist
├─ SDPRuleLoader() → reads mock_collibra/*.json → 20 rules, 20 mappings
├─ RuleExpressionBuilder.build_all_expressions() → preview SQL per dataset
└─ Output: human-readable rule preview (read-only, no data changes)

PHASE 3 — DLT PIPELINE (repeatable, notebook: 01_sdp_deploy_pipeline.py)
│
├─ Create/update DLT pipeline "dq_framework_sdp" via Databricks SDK
├─ Start pipeline update
│
│   DLT INTERNAL EXECUTION ORDER:
│   │
│   ├─ sdp_bronze_sources.py
│   │   ├─ Read dq_poc.bronze.customers     → customers_bronze      + _lineage
│   │   ├─ Read dq_poc.bronze.transactions  → transactions_bronze   + _lineage
│   │   ├─ Read dq_poc.bronze.products      → products_bronze       + _lineage
│   │   └─ Read dq_poc.silver.dim_country   → ref_dim_country       + _lineage
│   │
│   ├─ sdp_dq_pipeline.py
│   │   ├─ [IMPORT TIME] Load 20 rules + 20 mappings from JSON
│   │   ├─ [IMPORT TIME] Build SQL expressions + expectation dicts per dataset
│   │   │
│   │   ├─ customers_validated:
│   │   │   ├─ Read customers_bronze
│   │   │   ├─ @dlt.expect_all(soft_rules)        ← DLT native tracking
│   │   │   ├─ Evaluate ALL 8 rules per row       ← F.expr() → boolean
│   │   │   ├─ Pack into _dq struct               ← schema-stable
│   │   │   └─ Enrich _lineage.silver
│   │   ├─ customers_clean:       WHERE all hard rules pass
│   │   ├─ customers_quarantine:  WHERE any hard rule fails
│   │   │
│   │   ├─ transactions_validated → transactions_clean / transactions_quarantine
│   │   └─ products_validated     → products_clean / products_quarantine
│   │
│   └─ sdp_gold_pipeline.py
│       ├─ Explode _dq.results → dq_row_level_detail_customers/transactions/products
│       ├─ UNION ALL → dq_row_level_detail_all
│       ├─ Aggregate → dq_summary_by_dataset
│       ├─ Aggregate → dq_summary_by_rule
│       ├─ customers_clean + transactions_clean → gold_customer_360
│       └─ products_clean → gold_product_catalog
│
└─ Pipeline COMPLETED

PHASE 4 — DASHBOARD (one-time, notebook: 04_sdp_deploy_dashboard.py)
│
├─ Auto-detect SQL warehouse
├─ Load lakeview_dq_dashboard.json
├─ POST /api/2.0/lakeview/dashboards → create dashboard
└─ POST /api/2.0/lakeview/dashboards/{id}/published → publish
```

---

## 14. Configuration Reference

**File:** `configs/sdp_pipeline_config.yaml`

```yaml
framework:
  name: "dq_quality_framework_sdp"
  version: "2.0.0"
  engine: "spark_declarative_pipeline"

catalog:
  name: "dq_poc"
  schemas:
    bronze: "bronze"
    silver: "silver"
    gold: "gold"
    dq_results: "dq_sdp_results"

collibra:
  mock_mode: true
  mock_rules_path: "mock_collibra/dq_rules.json"
  mock_mappings_path: "mock_collibra/dq_rule_mappings.json"

datasets:
  customers:
    source_table: "bronze.customers"
    primary_key: "customer_id"
    validated_table: "customers_validated"
    clean_table: "customers_clean"
    quarantine_table: "customers_quarantine"
  transactions:
    source_table: "bronze.transactions"
    primary_key: "transaction_id"
    # ... same pattern
  products:
    source_table: "bronze.products"
    primary_key: "product_id"
    # ... same pattern

dlt:
  pipeline_name: "dq_framework_sdp"
  target_schema: "silver"
  continuous: false
  development: true
  photon: true

row_level_tracking:
  enabled: true
  # Annotation columns per rule + summary columns
```

---

## 15. Design Decisions & Trade-offs

### 15.1 Embedded Engine in DLT vs. Imports

| Approach | Pros | Cons |
|---|---|---|
| **Chosen: Embed engine classes in DLT notebook** | Works reliably in DLT's isolated context | Code duplication between `engine/` and `sdp_dq_pipeline.py` |
| Alternative: `%run` imports | Single source of truth | `%run` not supported in DLT |
| Alternative: Install as wheel | Clean separation | Overhead for POC; wheel must be rebuilt on every change |

**Mitigation:** The `engine/` folder is the canonical reference. Changes should be made there first, then synced to the DLT notebook.

### 15.2 `_dq` Struct vs. Individual Columns

| Approach | Pros | Cons |
|---|---|---|
| **Chosen: Single `_dq` struct** | Schema-stable, clean namespace, easy to explode | Slightly more complex SQL to query nested fields |
| Alternative: `_dq_DQR_000001` columns | Simpler SQL | Schema changes with every rule add/remove; column bloat |

### 15.3 Clean/Quarantine as Views vs. Tables

| Approach | Pros | Cons |
|---|---|---|
| **Chosen: DLT views** | No data duplication; `_validated` is source of truth | Views recompute on query (negligible for small data) |
| Alternative: Materialized tables | Faster reads | 3x storage; data consistency risk |

### 15.4 Rule Loading at Import Time vs. Runtime

| Approach | Pros | Cons |
|---|---|---|
| **Chosen: Load at import time** | Rules are fixed for the pipeline run; deterministic | Rules can't change mid-run (acceptable for batch) |
| Alternative: Load per table function | Could pick up rule changes mid-pipeline | Non-deterministic; different tables could evaluate different rule versions |

### 15.5 JSON File Rules vs. Delta Table Rules

| Approach | Pros | Cons |
|---|---|---|
| **Chosen: JSON files (POC)** | Simple, version-controlled, no dependencies | Not suitable for production rule management |
| Production path: Delta tables | Auditable, queryable, supports concurrent updates | Requires extraction pipeline from Collibra |

The `SDPRuleLoader` supports both modes via `use_delta=True`.

---

## 16. Future Enhancements

| Enhancement | Description | Complexity |
|---|---|---|
| **Collibra feedback loop** | Push DQ scores and violations back to Collibra via REST API | Medium |
| **Delta table rule source** | Replace JSON with `dq_framework.dq_rules` Delta table as the live rule source | Low |
| **Streaming mode** | Set `continuous: true` in DLT for near-real-time DQ evaluation | Low |
| **Rule versioning** | Track rule version in `_dq.results` to know which rule version was applied | Low |
| **Alerting** | Trigger Slack/email alerts when dataset DQ score drops below threshold | Medium |
| **Trend analysis** | Gold table tracking DQ scores over time (daily snapshots) | Medium |
| **Wheel packaging** | Package engine as a Python wheel to eliminate DLT code duplication | Medium |
| **Dynamic dataset onboarding** | Config-driven: add a new dataset by adding entries to rules + mappings only | High |
| **Cross-dataset rules** | Rules that span multiple tables (e.g., transaction amounts match invoice totals) | High |
| **DQ-gated promotion** | Block Gold table refresh if Silver DQ score is below threshold | Medium |

---

## Appendix A: Test Data Quality Issues

The mock data contains intentional quality issues for testing all 9 rule types:

### Customers (17 rows, 10 quality issues)

| Row | Issue | Rule Triggered |
|---|---|---|
| C004 | NULL customer_name | DQR-000002 (not_null) |
| C005 | Email: "invalid-email-format" | DQR-000004 (regex) |
| C006 | Age: 150 (> 120) | DQR-000003 (range) |
| C007 | Country: "XX" (not in dim_country) | DQR-000008 (referential_integrity) |
| C008 | NULL email | DQR-000001 (not_null) |
| C009 | Age: 17 (< 18) | DQR-000003 (range) |
| C010 | Phone: "12345" (< 10 chars) | DQR-000005 (length) |
| C011 | Status: "deleted" (not in allowed) | DQR-000006 (allowed_values) |
| C013 | Age: -5 | DQR-000003 (range) |
| C014 | Email: "karen@@double.com" | DQR-000004 (regex) |
| C015 | Phone: 16 digits (> 15 chars) | DQR-000005 (length) |
| C017 | Duplicate customer_id: C001 | DQR-000007 (uniqueness) |

### Transactions (16 rows, 9 quality issues)

| Row | Issue | Rule Triggered |
|---|---|---|
| T003 | NULL amount | DQR-000009 (not_null) |
| T005 | customer_id: C999 (not in customers) | DQR-000013 (referential_integrity) |
| T006 | transaction_type: "invalid_type" | DQR-000011 (allowed_values) |
| T007 | Amount: -100.00 | DQR-000010 (range) |
| T008 | Amount: 2,000,000 (> 1M) | DQR-000010 (range) |
| T010 | NULL transaction_date | DQR-000012 (not_null) |
| T011 | Quantity: 0 (fails > 0) | DQR-000015 (custom_sql) |
| T012 | Quantity: -2 | DQR-000015 (custom_sql) |
| T013 | Old date (> 72 hours) | DQR-000014 (freshness) |
| T014 | Quantity: 15,000 (> 10,000) | DQR-000015 (custom_sql) |

### Products (15 rows, 6 quality issues)

| Row | Issue | Rule Triggered |
|---|---|---|
| P003 | NULL product_name | DQR-000016 (not_null) |
| P007 | Price: -19.99 | DQR-000017 (range) |
| P010 | SKU: "INVALID_SKU" (fails regex) | DQR-000019 (regex) |
| P011 | Price: 150,000 (> 99,999.99) | DQR-000017 (range) |
| P012 | Category: "gaming" (not in allowed) | DQR-000018 (allowed_values) |
| P014 | Duplicate SKU: "ELC-1234" | DQR-000020 (uniqueness) |

---

## Appendix B: SQL Expression Examples

Generated by `RuleExpressionBuilder` for the customers dataset:

```sql
-- DQR-000001: not_null on email
`email` IS NOT NULL

-- DQR-000003: range on age
(`age` >= 18 AND `age` <= 120)

-- DQR-000004: regex on email
`email` RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

-- DQR-000005: length on phone
(LENGTH(`phone`) >= 10 AND LENGTH(`phone`) <= 15)

-- DQR-000006: allowed_values on status
`status` IN ('active', 'inactive', 'suspended', 'pending')

-- DQR-000007: uniqueness on customer_id
(COUNT(1) OVER (PARTITION BY `customer_id`)) = 1

-- DQR-000008: referential_integrity on country_code
`country_code` IN (SELECT `country_code` FROM live.ref_dim_country)
```
