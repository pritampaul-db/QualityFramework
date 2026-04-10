# High-Level Architecture Document: Metadata-Driven Data Quality Framework

## Collibra - Databricks Integration

**Version:** 1.0  
**Date:** 2026-04-01  
**Status:** Draft

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Principles](#2-architecture-principles)
3. [System Context](#3-system-context)
4. [Logical Architecture](#4-logical-architecture)
5. [Component Architecture](#5-component-architecture)
6. [Data Architecture](#6-data-architecture)
7. [Integration Architecture](#7-integration-architecture)
8. [Execution Architecture](#8-execution-architecture)
9. [Observability & Monitoring](#9-observability--monitoring)
10. [Security Architecture](#10-security-architecture)
11. [Deployment Architecture](#11-deployment-architecture)
12. [Best Practices](#12-best-practices)
13. [Non-Functional Requirements](#13-non-functional-requirements)
14. [Risk & Mitigation](#14-risk--mitigation)
15. [Appendix](#15-appendix)

---

## 1. Executive Summary

This document defines the high-level architecture for a **Metadata-Driven Data Quality (DQ) Framework** that bridges Collibra Data Intelligence Cloud (governance) with Databricks (execution). The framework enables organizations to:

- Define and manage data quality rules centrally in Collibra
- Automatically extract, version, and execute those rules at scale in Databricks
- Establish a closed-loop feedback system between governance and engineering
- Provide full observability, auditability, and lineage across the DQ lifecycle

The architecture follows a **metadata-driven** approach where rules are never hardcoded. Instead, they are dynamically resolved from Delta Lake tables at runtime, enabling business users to modify quality rules without code changes.

---

## 2. Architecture Principles

| Principle | Description |
|-----------|-------------|
| **Metadata-Driven** | All rule definitions, mappings, and configurations are stored as metadata in Delta tables. No hardcoded logic in pipelines. |
| **Separation of Concerns** | Governance (rule definition) is decoupled from execution (rule processing). Each layer has a single responsibility. |
| **Idempotent Execution** | Rule execution is idempotent — re-running the same rules on the same data produces the same results. |
| **Schema-First Design** | All rule types follow a strict JSON schema contract. Free-text rules are not permitted. |
| **Fail-Safe Defaults** | Invalid or unparseable rules default to soft enforcement (flag, don't drop) to prevent silent data loss. |
| **Observability by Design** | Every rule execution produces audit records. No rule runs without a corresponding log entry. |
| **Open/Closed for Rule Types** | The engine is open for extension (new rule types) but closed for modification (existing rule logic doesn't change). |
| **Governance Feedback Loop** | Execution results flow back to Collibra so business stakeholders have visibility into actual data health. |

---

## 3. System Context

```
+-------------------------+          +----------------------------+
|   Business Users /      |          |    Data Engineering        |
|   Data Stewards         |          |    Teams                   |
+------------+------------+          +-------------+--------------+
             |                                     |
             | Define Rules                        | Build Pipelines
             v                                     v
+-------------------------+          +----------------------------+
|                         |  REST    |                            |
|   Collibra DI Cloud     +--------->+   Databricks Workspace     |
|   (Governance Hub)      |  API     |   (Execution Platform)     |
|                         +<---------+                            |
+-------------------------+  Results +----------------------------+
                                               |
                                               v
                                     +----------------------------+
                                     |   Consumers                |
                                     |   - Dashboards (SQL/BI)    |
                                     |   - Alerting Systems       |
                                     |   - Downstream Pipelines   |
                                     +----------------------------+
```

### External System Boundaries

| System | Role | Interface |
|--------|------|-----------|
| Collibra DI Cloud | Source of truth for DQ rules, glossary, ownership | REST API v2.0 |
| Databricks Workspace | Rule storage, execution, orchestration, reporting | Notebooks, Workflows, SQL Warehouses |
| Unity Catalog | Governance layer for Databricks assets (future) | Native integration |
| Power BI / Databricks SQL | Visualization and reporting | JDBC/ODBC, Dashboards |
| Alerting (Slack/Email) | Real-time notifications on DQ failures | Webhooks / SMTP (future) |

---

## 4. Logical Architecture

The framework consists of **eight logical layers**, each with a distinct responsibility:

```
+================================================================+
|                    GOVERNANCE LAYER (Collibra)                  |
|  Rule Definition | Business Glossary | Ownership & Stewardship |
+================================================================+
                              |
                        REST API v2.0
                              |
+================================================================+
|                   INTEGRATION LAYER                             |
|  API Client | Rule Extraction | Schedule & Orchestration       |
+================================================================+
                              |
+================================================================+
|                   METADATA STORAGE LAYER (Delta Lake)           |
|  dq_rules | dq_rule_mapping | dq_execution_log | dq_quarantine|
+================================================================+
                              |
+================================================================+
|                   RULE ENGINE LAYER                             |
|  Rule Parser | Rule Validator | Dynamic Rule Executor          |
+================================================================+
                              |
+================================================================+
|                   EXECUTION LAYER (Pipelines)                   |
|  Bronze -> Silver | Hard/Soft Enforcement | Quarantine          |
+================================================================+
                              |
+================================================================+
|                   DATA STORAGE LAYER (Medallion)                |
|  Bronze (Raw) | Silver (Validated) | Gold (Business-Ready)     |
+================================================================+
                              |
+================================================================+
|                   OBSERVABILITY LAYER                           |
|  Execution Logs | DQ Scores | Trend Analytics | Alerts         |
+================================================================+
                              |
+================================================================+
|                   FEEDBACK LAYER (Collibra Writeback)           |
|  Rule Status | DQ Scores | SLA Violations                     |
+================================================================+
```

---

## 5. Component Architecture

### 5.1 Governance Layer (Collibra)

**Responsibility:** Single source of truth for all data quality rule definitions.

**Components:**
- **DQ Rule Assets** — Structured rule definitions with type, target column, parameters, and severity
- **Business Glossary** — Maps business terms to technical column names
- **Data Ownership Registry** — Defines stewards and owners per dataset/domain

**Rule Schema Contract:**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["rule_id", "rule_type", "column", "severity"],
  "properties": {
    "rule_id": { "type": "string", "pattern": "^DQR-[0-9]{6}$" },
    "rule_type": {
      "type": "string",
      "enum": ["not_null", "range", "regex", "uniqueness", "referential_integrity",
               "allowed_values", "length", "freshness", "custom_sql"]
    },
    "column": { "type": "string" },
    "params": { "type": "object" },
    "severity": { "type": "string", "enum": ["error", "warning", "info"] },
    "is_active": { "type": "boolean", "default": true },
    "version": { "type": "integer", "minimum": 1 },
    "effective_from": { "type": "string", "format": "date" },
    "effective_to": { "type": ["string", "null"], "format": "date" }
  }
}
```

**Best Practices:**
- Enforce structured JSON definitions — reject free-text rule descriptions
- Version every rule change; never overwrite in place
- Assign each rule a globally unique ID with a standard prefix (e.g., `DQR-000123`)
- Tag rules with domain, dataset, and priority for filtering during execution

---

### 5.2 Integration Layer

**Responsibility:** Extract rules from Collibra, transform them into the canonical schema, and land them in Delta Lake.

**Components:**

| Component | Description |
|-----------|-------------|
| `collibra_api_client.py` | Authenticated REST client for Collibra v2.0 API |
| `rule_extractor.py` | Fetches DQ rule assets, dataset-rule mappings, and glossary |
| `rule_transformer.py` | Normalizes Collibra output into the canonical rule schema |
| `rule_loader.py` | Writes validated rules into Delta tables with merge (upsert) semantics |

**API Endpoints Used:**

```
GET /rest/2.0/assets?typeId={DQ_RULE_TYPE_ID}&offset=0&limit=100
GET /rest/2.0/assets/{assetId}/attributes
GET /rest/2.0/relations?sourceId={datasetAssetId}&relationType={hasDQRule}
```

**Extraction Pattern:**

```
[Collibra API] --> JSON Response --> Schema Validation --> Delta Merge (UPSERT)
```

**Best Practices:**
- Use **incremental extraction** — track last sync timestamp and only pull changed rules
- Validate every extracted rule against the JSON schema before writing to Delta
- Log extraction metadata: source timestamp, record count, schema version
- Implement retry logic with exponential backoff for API calls
- Store raw API responses in a landing zone (Bronze) before transformation
- Use Databricks Workflows for scheduling (cron-based, e.g., every 6 hours)

---

### 5.3 Metadata Storage Layer (Delta Lake)

**Responsibility:** Persist all rule definitions, mappings, and execution history as governed Delta tables.

**Table Designs:**

#### `catalog.dq_framework.dq_rules`

| Column | Type | Description |
|--------|------|-------------|
| rule_id | STRING | Unique rule identifier (PK) |
| rule_type | STRING | Type enum (not_null, range, regex, etc.) |
| target_column | STRING | Column the rule applies to |
| params | STRING (JSON) | Rule-specific parameters |
| severity | STRING | error / warning / info |
| is_active | BOOLEAN | Whether rule is currently enforced |
| version | INT | Rule version number |
| effective_from | DATE | Start of validity window |
| effective_to | DATE | End of validity window (NULL = current) |
| source_system | STRING | Always "collibra" for this integration |
| created_at | TIMESTAMP | Record creation time |
| updated_at | TIMESTAMP | Last modification time |
| checksum | STRING | SHA-256 of rule payload for change detection |

**Partitioning:** By `rule_type`  
**Z-Ordering:** By `rule_id`, `is_active`

#### `catalog.dq_framework.dq_rule_mapping`

| Column | Type | Description |
|--------|------|-------------|
| mapping_id | STRING | Unique mapping identifier (PK) |
| dataset_name | STRING | Fully qualified table name (catalog.schema.table) |
| column_name | STRING | Target column |
| rule_id | STRING | FK to dq_rules |
| enforcement_mode | STRING | hard / soft |
| priority | INT | Execution priority (lower = first) |
| is_active | BOOLEAN | Mapping active flag |

**Partitioning:** By `dataset_name`

#### `catalog.dq_framework.dq_execution_log`

| Column | Type | Description |
|--------|------|-------------|
| execution_id | STRING | Unique execution ID (PK) |
| run_id | STRING | Pipeline run identifier |
| rule_id | STRING | FK to dq_rules |
| dataset_name | STRING | Table that was validated |
| status | STRING | passed / failed / error / skipped |
| total_records | BIGINT | Records evaluated |
| passed_count | BIGINT | Records passing the rule |
| failed_count | BIGINT | Records failing the rule |
| error_message | STRING | Error details if status = error |
| dq_score | DOUBLE | (passed / total) * 100 |
| execution_duration_ms | BIGINT | Rule execution time in milliseconds |
| executed_at | TIMESTAMP | Execution timestamp |
| executed_by | STRING | Service principal or user |

**Partitioning:** By `executed_at` (date)  
**Z-Ordering:** By `dataset_name`, `rule_id`  
**Retention:** 90-day OPTIMIZE, 365-day VACUUM

#### `catalog.dq_framework.dq_quarantine`

| Column | Type | Description |
|--------|------|-------------|
| quarantine_id | STRING | Unique quarantine record ID |
| execution_id | STRING | FK to dq_execution_log |
| rule_id | STRING | FK to dq_rules |
| source_table | STRING | Original table |
| record_key | STRING | Business key of quarantined record |
| record_payload | STRING (JSON) | Serialized failed record |
| quarantine_reason | STRING | Human-readable failure reason |
| quarantined_at | TIMESTAMP | When record was quarantined |
| resolved_at | TIMESTAMP | When/if record was resolved |
| resolution_action | STRING | reprocessed / deleted / manually_fixed |

**Best Practices:**
- Use **Delta MERGE (upsert)** for rule ingestion — never truncate-and-reload
- Enable **Delta Time Travel** for rule versioning and audit (retain 30+ days)
- Apply table constraints (NOT NULL on PKs, CHECK on enums)
- Use Unity Catalog for access control and lineage tracking
- Partition large tables by date; Z-Order by high-cardinality filter columns
- Schedule regular OPTIMIZE and VACUUM operations

---

### 5.4 Rule Engine Layer

**Responsibility:** Dynamically parse, validate, and execute DQ rules against DataFrames at runtime.

**Design Pattern:** Strategy Pattern — each rule type has a dedicated executor class, selected at runtime based on `rule_type`.

**Component Breakdown:**

```
rule_engine/
  |-- rule_registry.py        # Maps rule_type -> executor class
  |-- rule_validator.py        # Validates rule params before execution
  |-- base_rule_executor.py    # Abstract base class
  |-- executors/
  |     |-- not_null_executor.py
  |     |-- range_executor.py
  |     |-- regex_executor.py
  |     |-- uniqueness_executor.py
  |     |-- referential_integrity_executor.py
  |     |-- allowed_values_executor.py
  |     |-- length_executor.py
  |     |-- freshness_executor.py
  |     |-- custom_sql_executor.py
  |-- dq_engine.py             # Orchestrates rule loading, validation, execution
```

**Supported Rule Types:**

| Rule Type | Description | Parameters |
|-----------|-------------|------------|
| `not_null` | Column must not contain NULL values | — |
| `range` | Column value must fall within [min, max] | `min`, `max` |
| `regex` | Column value must match a regex pattern | `pattern` |
| `uniqueness` | Column must contain unique values | `scope` (table/partition) |
| `referential_integrity` | Value must exist in a reference table | `ref_table`, `ref_column` |
| `allowed_values` | Value must be in a predefined set | `values` (array) |
| `length` | String length must be within [min_length, max_length] | `min_length`, `max_length` |
| `freshness` | Data must not be older than threshold | `max_age_hours` |
| `custom_sql` | Custom SQL expression returning boolean | `sql_expression` |

**Execution Flow:**

```
1. Load active rules for target dataset from dq_rule_mapping + dq_rules
2. Sort rules by priority
3. Validate each rule's params against its type schema
4. For each valid rule:
   a. Resolve the executor via rule_registry
   b. Execute against the DataFrame
   c. Capture pass/fail counts and flagged records
   d. Write execution log entry
   e. If enforcement_mode == 'hard': filter out failed records
   f. If enforcement_mode == 'soft': flag failed records + write to quarantine
5. Return the cleaned DataFrame and execution summary
```

**Best Practices:**
- Use the **Strategy Pattern** for rule executors — easy to add new types without modifying the engine
- Validate rule params before execution — fail fast with clear error messages
- Never modify the source DataFrame — always return a new DataFrame
- Use `DataFrame.cache()` judiciously when a DataFrame is evaluated by multiple rules
- Execute rules in priority order to short-circuit when hard enforcement drops records
- Wrap each rule execution in try/except — a single failing rule should not crash the pipeline
- Log execution duration per rule for performance monitoring

---

### 5.5 Execution Layer (Pipeline Integration)

**Responsibility:** Integrate the DQ engine into data pipelines at the appropriate stage.

**Recommended Execution Points:**

```
Bronze (Raw) ----[DQ: Schema & Completeness]----> Silver (Validated)
                                                        |
                                               [DQ: Business Rules]
                                                        |
                                                        v
                                                  Gold (Curated)
```

| Stage | Rule Types Applied | Enforcement |
|-------|-------------------|-------------|
| Bronze -> Silver | not_null, range, regex, allowed_values, length | Hard (drop) or Soft (quarantine) |
| Silver -> Gold | uniqueness, referential_integrity, freshness, custom_sql | Soft (flag + alert) |

**Enforcement Modes:**

```
Hard Enforcement:
  - Failed records are REMOVED from the output DataFrame
  - Failed records are written to dq_quarantine
  - Pipeline continues with clean data only

Soft Enforcement:
  - Failed records are FLAGGED with a __dq_flag column
  - Failed records are written to dq_quarantine
  - All records pass through to the next stage
  - Alerts are triggered if failure rate exceeds threshold
```

**Pipeline Integration Pattern (Pseudo-code):**

```python
# In bronze_to_silver.py
from engine.dq_engine import DQEngine

# 1. Read bronze data
bronze_df = spark.read.table("catalog.bronze.customers")

# 2. Initialize DQ engine
engine = DQEngine(spark, dataset="catalog.bronze.customers")

# 3. Execute all mapped rules
result = engine.execute(bronze_df)

# 4. Write validated data to Silver
result.clean_df.write.mode("overwrite").saveAsTable("catalog.silver.customers")

# 5. Execution log is automatically persisted by the engine
# 6. Quarantined records are automatically persisted by the engine
```

**Best Practices:**
- Apply DQ checks **after** schema evolution handling but **before** business transformations
- Use hard enforcement sparingly — prefer soft enforcement with alerting for most rules
- Set configurable failure thresholds (e.g., if >5% records fail, halt the pipeline)
- Write quarantined records with enough context to reprocess them later
- Make pipeline stages idempotent — re-running should not duplicate DQ log entries

---

### 5.6 Data Storage Layer (Medallion Architecture)

```
+============+       +==============+       +============+
|   BRONZE   | ----> |    SILVER    | ----> |    GOLD    |
|   (Raw)    |       |  (Validated) |       | (Curated)  |
+============+       +==============+       +============+
| - Raw      |       | - Schema     |       | - Aggregated |
|   ingested |       |   enforced   |       | - Joined     |
|   data     |       | - DQ rules   |       | - Business   |
| - Append   |       |   applied    |       |   metrics    |
|   only     |       | - Quarantine |       | - SLA-bound  |
|            |       |   separated  |       |              |
+============+       +==============+       +============+
```

**Best Practices:**
- Bronze is **append-only** and immutable — never modify raw data
- Silver contains the DQ-validated view — this is where most rules execute
- Gold tables should reference Silver (not Bronze) to guarantee data quality
- Use Unity Catalog to organize: `catalog.{layer}.{domain}_{entity}`
- Enable Change Data Feed (CDF) on Silver tables for downstream consumers
- Apply table ACLs: Bronze (engineering only), Silver (analysts), Gold (business users)

---

## 6. Data Architecture

### Data Flow Diagram

```
[Source Systems]
       |
       v
+------+-------+
|    BRONZE     | <-- Raw ingestion (append-only)
+------+-------+
       |
       | DQ Engine: Schema + Basic Rules
       |
+------+-------+
|    SILVER     | <-- Validated + cleaned
+------+-------+  \
       |            \--> [dq_quarantine] (failed records)
       |
       | DQ Engine: Business Rules
       |
+------+-------+
|     GOLD      | <-- Business-ready, aggregated
+--------------+
       |
       v
[Consumers: Dashboards, ML Models, APIs]
```

### Metadata Flow Diagram

```
[Collibra] --REST API--> [Integration Layer] --Delta MERGE--> [dq_rules]
                                                               [dq_rule_mapping]

[DQ Engine] --reads--> [dq_rules + dq_rule_mapping]
            --writes-> [dq_execution_log]
            --writes-> [dq_quarantine]

[Feedback Layer] --reads--> [dq_execution_log]
                 --REST API--> [Collibra] (writeback)
```

---

## 7. Integration Architecture

### 7.1 Collibra -> Databricks (Inbound)

| Aspect | Detail |
|--------|--------|
| Protocol | HTTPS (REST API v2.0) |
| Authentication | OAuth 2.0 client credentials / API key |
| Format | JSON |
| Frequency | Scheduled (every 6 hours default, configurable) |
| Pattern | Incremental extraction with last-modified filter |
| Error Handling | Retry with exponential backoff (3 attempts, 1s/2s/4s) |

### 7.2 Databricks -> Collibra (Outbound / Feedback)

| Aspect | Detail |
|--------|--------|
| Protocol | HTTPS (REST API v2.0) |
| Authentication | OAuth 2.0 client credentials |
| Format | JSON |
| Frequency | Post-execution (event-driven) or batch (daily rollup) |
| Payload | Rule status, DQ score, failed count, SLA violations |
| Error Handling | Write to dead-letter queue on failure; retry on next cycle |

### 7.3 Integration Error Handling

```
API Call Failed?
  |
  +-- Retryable (429, 500, 503)?
  |     +-- Yes -> Exponential backoff (max 3 retries)
  |     +-- Still failing -> Log error, write to dead-letter table, alert
  |
  +-- Non-retryable (400, 401, 403)?
        +-- Log error with full request/response
        +-- Alert operations team
        +-- Skip this rule/batch, continue processing
```

---

## 8. Execution Architecture

### 8.1 Orchestration (Databricks Workflows)

**Workflow: `dq_framework_daily`**

```
Task 1: extract_rules
  |   - Runs collibra_api_client.py
  |   - Extracts new/changed rules
  |   - Merges into dq_rules and dq_rule_mapping
  |
Task 2: execute_dq_bronze_to_silver  (depends on Task 1)
  |   - For each configured dataset:
  |     - Loads Bronze data
  |     - Runs DQ engine
  |     - Writes Silver output + quarantine
  |
Task 3: execute_dq_silver_to_gold  (depends on Task 2)
  |   - For each configured dataset:
  |     - Loads Silver data
  |     - Runs business-level DQ rules
  |     - Writes Gold output
  |
Task 4: generate_dq_report  (depends on Tasks 2 & 3)
  |   - Computes DQ scores per dataset
  |   - Updates dashboards
  |   - Triggers alerts if thresholds breached
  |
Task 5: feedback_to_collibra  (depends on Task 4)
      - Pushes execution results back to Collibra
```

### 8.2 Compute Configuration

| Task | Cluster Type | Recommendation |
|------|-------------|----------------|
| Rule Extraction | Single-node | Small (4 cores, 16 GB) — API-bound, not compute-bound |
| DQ Execution (small datasets <1TB) | Standard cluster | Medium (8-16 workers, autoscaling) |
| DQ Execution (large datasets >1TB) | Photon-enabled cluster | Large (16-32 workers, Photon for SQL-heavy rules) |
| Reporting | SQL Warehouse | Serverless SQL Warehouse (auto-scales to demand) |

**Best Practices:**
- Use **job clusters** (not all-purpose) for scheduled DQ runs — lower cost
- Enable **autoscaling** to handle variable dataset sizes
- Use **Photon** for SQL-heavy rules (custom_sql, referential_integrity)
- Set cluster tags for cost attribution: `team=data-quality`, `env=production`

---

## 9. Observability & Monitoring

### 9.1 DQ Metrics

| Metric | Formula | Granularity |
|--------|---------|-------------|
| Rule Pass Rate | (passed_count / total_records) * 100 | Per rule, per execution |
| Dataset DQ Score | AVG(rule_pass_rate) across all rules for a dataset | Per dataset, per execution |
| Domain DQ Score | AVG(dataset_dq_score) across all datasets in a domain | Per domain, daily |
| Trend (7-day) | Moving average of dataset DQ score | Per dataset, rolling |
| Quarantine Rate | quarantined_records / total_records | Per dataset, per execution |

### 9.2 Alerting Thresholds

| Condition | Severity | Action |
|-----------|----------|--------|
| DQ Score < 95% | Warning | Log + Dashboard highlight |
| DQ Score < 80% | Error | Alert (Slack/Email) + Pipeline pause |
| Rule execution error | Error | Alert + Auto-retry once |
| Extraction failure | Error | Alert + Fallback to cached rules |
| Quarantine rate > 10% | Warning | Alert data steward |

### 9.3 Dashboard Components

**Executive Dashboard (Gold-level):**
- Overall DQ health score across all domains
- Trend chart (30-day rolling DQ score)
- Top 10 failing rules
- SLA compliance status

**Operational Dashboard (Silver-level):**
- Per-dataset DQ scores
- Rule execution history
- Quarantine volume and resolution rate
- Execution duration trends (performance monitoring)

**Best Practices:**
- Use Databricks SQL dashboards for real-time DQ monitoring
- Set up scheduled alerts via Databricks SQL Alerts
- Track execution duration trends to catch performance regression early
- Create separate dashboards for executives (high-level) vs. data engineers (detail)

---

## 10. Security Architecture

### 10.1 Authentication & Authorization

| Component | Auth Method | Details |
|-----------|-------------|---------|
| Collibra API access | OAuth 2.0 / API Key | Stored in Databricks Secrets (scope: `dq-framework`) |
| Databricks Workspace | Service Principal | Dedicated SP for DQ framework with least-privilege |
| Delta Tables | Unity Catalog ACLs | Role-based: `dq_admin`, `dq_engineer`, `dq_viewer` |
| DQ Dashboards | Workspace permissions | Share with data steward groups |

### 10.2 Data Protection

| Concern | Mitigation |
|---------|------------|
| Secrets in code | Use Databricks Secret Scopes — never hardcode API keys or tokens |
| PII in quarantine | Apply column-level masking on quarantine tables for sensitive fields |
| Audit trail | dq_execution_log is append-only; retention policy = 365 days minimum |
| Network | Private Link between Databricks and Collibra (if both in cloud) |

### 10.3 Access Control Matrix

| Role | dq_rules | dq_rule_mapping | dq_execution_log | dq_quarantine | Bronze | Silver | Gold |
|------|----------|-----------------|-------------------|---------------|--------|--------|------|
| DQ Admin | Read/Write | Read/Write | Read | Read | Read | Read | Read |
| DQ Engineer | Read | Read/Write | Read/Write | Read/Write | Read/Write | Read/Write | Read |
| Data Analyst | Read | Read | Read | — | — | Read | Read |
| Business User | — | — | Read (summary) | — | — | — | Read |

---

## 11. Deployment Architecture

### 11.1 Environment Strategy

| Environment | Purpose | Data | Rule Source |
|-------------|---------|------|-------------|
| Dev | Development and unit testing | Sample/synthetic data | Local rule configs (JSON files) |
| Staging | Integration testing and validation | Production-like (anonymized) | Collibra Staging instance |
| Production | Live execution | Production data | Collibra Production instance |

### 11.2 Repository Structure

```
dq-framework/
|
|-- extract/                        # Integration Layer
|   |-- collibra_api_client.py      # REST client with auth + retry
|   |-- rule_extractor.py           # Fetch rules from Collibra
|   |-- rule_transformer.py         # Normalize to canonical schema
|   |-- rule_loader.py              # Delta MERGE writer
|
|-- engine/                         # Rule Engine Layer
|   |-- dq_engine.py                # Main orchestrator
|   |-- rule_registry.py            # Rule type -> executor mapping
|   |-- rule_validator.py           # Schema validation for rules
|   |-- base_rule_executor.py       # Abstract base class
|   |-- executors/                  # One executor per rule type
|       |-- not_null_executor.py
|       |-- range_executor.py
|       |-- regex_executor.py
|       |-- uniqueness_executor.py
|       |-- referential_integrity_executor.py
|       |-- allowed_values_executor.py
|       |-- length_executor.py
|       |-- freshness_executor.py
|       |-- custom_sql_executor.py
|
|-- pipelines/                      # Execution Layer
|   |-- bronze_to_silver.py         # DQ-integrated pipeline stage
|   |-- silver_to_gold.py           # Business rule validation stage
|
|-- feedback/                       # Feedback Layer
|   |-- collibra_writeback.py       # Push results to Collibra
|
|-- configs/                        # Configuration
|   |-- dq_rules_schema.json        # JSON schema for rule validation
|   |-- pipeline_config.yaml        # Dataset configs, thresholds, schedules
|   |-- env/
|       |-- dev.yaml
|       |-- staging.yaml
|       |-- prod.yaml
|
|-- tests/                          # Testing
|   |-- unit/
|   |   |-- test_rule_executors.py
|   |   |-- test_rule_validator.py
|   |   |-- test_dq_engine.py
|   |-- integration/
|       |-- test_collibra_extraction.py
|       |-- test_pipeline_e2e.py
|
|-- dashboards/                     # SQL definitions for DQ dashboards
|   |-- executive_dq_overview.sql
|   |-- operational_dq_detail.sql
|
|-- workflows/                      # Databricks Workflow definitions
|   |-- dq_framework_daily.json
|
|-- docs/                           # Documentation
|   |-- architecture.md             # This document
|   |-- runbook.md                  # Operational runbook
|   |-- adding_new_rule_types.md    # Extension guide
```

### 11.3 CI/CD Pipeline

```
[Git Push] -> [Unit Tests] -> [Integration Tests (Staging)] -> [Deploy to Production]
                                     |
                                     +-- Validate rule schema contracts
                                     +-- Run DQ engine against test datasets
                                     +-- Verify Collibra API connectivity
```

**Best Practices:**
- Use Databricks Asset Bundles (DABs) for deploying workflows and cluster configs
- Version all rule schemas — breaking changes require migration scripts
- Run integration tests against staging Collibra + Databricks before promoting to prod
- Use branch-based deployments: `main` -> prod, `develop` -> staging

---

## 12. Best Practices

### Rule Management
1. **Structured over free-text** — Every rule must conform to the JSON schema. Reject unstructured definitions.
2. **Atomic rules** — One rule tests one condition on one column. Compose complex checks from multiple atomic rules.
3. **Version everything** — Use Delta Time Travel + explicit version columns. Never overwrite rule history.
4. **Effective dating** — Rules have `effective_from` and `effective_to` dates. This supports scheduled rule rollouts and sunsets.
5. **Ownership** — Every rule must have an assigned steward in Collibra who is accountable for its correctness.

### Engine Design
6. **No hardcoding** — Rules are resolved from metadata at runtime. Adding a new rule means adding a row, not changing code.
7. **Open for extension** — Adding a new rule type means adding one executor class and registering it. No existing code changes.
8. **Fail gracefully** — A single rule failure should not crash the pipeline. Log the error, skip the rule, continue.
9. **Idempotent execution** — Use `execution_id` + `rule_id` as a natural key. Re-running writes the same log entries.
10. **Performance-aware** — Cache DataFrames evaluated by multiple rules. Use broadcast joins for small reference tables.

### Pipeline Integration
11. **DQ at the boundary** — Apply DQ checks at layer transitions (Bronze->Silver, Silver->Gold), not within a single layer.
12. **Prefer soft enforcement** — Flag and quarantine by default. Reserve hard enforcement for critical data integrity rules.
13. **Circuit breakers** — If failure rate exceeds a threshold (configurable), halt the pipeline and alert. Don't push bad data downstream.
14. **Quarantine with context** — Store enough information with quarantined records to reprocess them without going back to source.

### Operations
15. **Audit everything** — Every rule execution produces a log entry. No exceptions.
16. **Monitor DQ scores as SLIs** — Treat data quality scores like service-level indicators. Set SLOs and alert on breaches.
17. **Trend over point-in-time** — A single DQ score is less useful than a trend. Dashboard should always show historical context.
18. **Close the loop** — Push results back to Collibra. Governance without feedback is governance theater.
19. **Regular hygiene** — Deactivate unused rules. Review quarantine tables weekly. VACUUM Delta tables on schedule.

### Security
20. **Least privilege** — The DQ service principal should only access the tables it needs. No workspace-admin privileges.
21. **Secrets management** — All API keys, tokens, and credentials live in Databricks Secret Scopes. Zero secrets in code or configs.
22. **Column-level security** — Apply masking on PII columns in quarantine tables.

---

## 13. Non-Functional Requirements

| Requirement | Target | Measurement |
|-------------|--------|-------------|
| Rule execution latency | < 5 min for datasets up to 1 TB | `execution_duration_ms` in dq_execution_log |
| Rule extraction latency | < 2 min for full sync of 10K rules | Workflow task duration |
| Availability | 99.5% (scheduled execution must succeed) | Failed run ratio over 30 days |
| Scalability | Support 10K+ rules across 500+ datasets | Periodic load testing |
| Auditability | 100% of executions logged | COUNT(*) in execution_log vs. expected |
| Data freshness | DQ results available within 1 hour of pipeline completion | Timestamp delta |
| Recovery | Automated retry + manual rerun capability | Idempotent execution design |

---

## 14. Risk & Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Collibra API downtime | Medium | High | Cache last-known-good rules in Delta. Execute from cache if API is unreachable. Alert ops team. |
| Complex rules (cross-table joins) | High | Medium | Support `referential_integrity` and `custom_sql` rule types. Document performance implications. Set timeout per rule. |
| Performance degradation on large datasets | Medium | High | Use Photon-enabled clusters. Partition data. Limit rule scope with column pruning. Monitor execution duration trends. |
| Rule conflicts (contradictory rules on same column) | Medium | Medium | Validate rule compatibility during extraction. Priority ordering resolves conflicts deterministically. |
| Schema evolution (source columns renamed/dropped) | Medium | High | Validate column existence before rule execution. Auto-deactivate rules targeting missing columns. Alert steward. |
| Silent data loss from overzealous hard enforcement | Low | Critical | Default to soft enforcement. Require explicit approval for hard enforcement rules. Monitor quarantine volume. |
| Stale rules after Collibra changes | Low | Medium | Incremental sync every 6 hours. Change detection via checksums. Force-sync option available on-demand. |

---

## 15. Appendix

### A. Glossary

| Term | Definition |
|------|------------|
| DQ | Data Quality |
| Medallion Architecture | Bronze/Silver/Gold layered data architecture |
| Hard Enforcement | Invalid records are dropped from the pipeline |
| Soft Enforcement | Invalid records are flagged but not removed |
| Quarantine | Isolated storage for records that fail DQ checks |
| DQ Score | Percentage of records passing all applicable rules |
| Feedback Loop | Process of pushing execution results back to the governance tool |
| Effective Dating | Associating validity windows with rules for time-aware execution |

### B. Rule Type Reference

| Rule Type | Parameters | Example |
|-----------|-----------|---------|
| not_null | — | `{"rule_type": "not_null", "column": "email"}` |
| range | min, max | `{"rule_type": "range", "column": "age", "params": {"min": 0, "max": 120}}` |
| regex | pattern | `{"rule_type": "regex", "column": "email", "params": {"pattern": "^[^@]+@[^@]+$"}}` |
| uniqueness | scope | `{"rule_type": "uniqueness", "column": "customer_id", "params": {"scope": "table"}}` |
| referential_integrity | ref_table, ref_column | `{"rule_type": "referential_integrity", "column": "country_code", "params": {"ref_table": "dim_country", "ref_column": "code"}}` |
| allowed_values | values | `{"rule_type": "allowed_values", "column": "status", "params": {"values": ["active", "inactive"]}}` |
| length | min_length, max_length | `{"rule_type": "length", "column": "phone", "params": {"min_length": 10, "max_length": 15}}` |
| freshness | max_age_hours | `{"rule_type": "freshness", "column": "updated_at", "params": {"max_age_hours": 24}}` |
| custom_sql | sql_expression | `{"rule_type": "custom_sql", "column": "*", "params": {"sql_expression": "revenue >= 0 AND revenue < 1000000"}}` |

### C. Decision Log

| Decision | Rationale | Date |
|----------|-----------|------|
| Delta Lake for rule storage | Time Travel for versioning, MERGE for upserts, native Databricks integration | 2026-04-01 |
| Strategy Pattern for rule engine | Extensible without modifying core engine. Each rule type is isolated and testable. | 2026-04-01 |
| Soft enforcement as default | Prevents silent data loss. Hard enforcement requires explicit opt-in per rule. | 2026-04-01 |
| 6-hour sync cadence | Balances rule freshness with API load. On-demand sync available for urgent changes. | 2026-04-01 |
| Quarantine table over dead-letter topic | Simpler to query, reprocess, and audit within Databricks. Kafka/EventHub can be added later if streaming is needed. | 2026-04-01 |

---

*This document should be reviewed and updated as the framework evolves. All architectural decisions should be recorded in the Decision Log (Appendix C).*
