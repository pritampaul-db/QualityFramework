# 📘 Metadata-Driven Data Quality Framework

### Collibra → Databricks Integration

---

## 🧭 Overview

This repository/document describes an **end-to-end, metadata-driven Data Quality (DQ) framework** integrating:

* Collibra Data Intelligence Cloud for **governance and rule definition**
* Databricks for **data processing and rule execution**

The architecture enables centralized rule management, scalable execution, and a closed-loop feedback system for enterprise data quality.

---

## 🎯 Objectives

* Centralize DQ rule definition in Collibra
* Enable automated extraction and execution of rules in Databricks
* Implement a **metadata-driven rule engine** (no hardcoding)
* Provide observability, auditability, and reporting
* Establish a **governance feedback loop**

---

## 🏗️ Architecture Summary

```
Collibra (DQ Rules)
        ↓
Extraction Layer (API आधारित ingestion)
        ↓
Landing Zone (JSON नियम)
        ↓
Delta Tables (Rules Repository)
        ↓
DQ Engine (PySpark / Deequ / GE)
        ↓
Data Pipelines (Bronze → Silver → Gold)
        ↓
DQ Results + Metrics
        ↓
(Optional) Push Back to Collibra
```

---

## 🧩 Architecture Components

### 1. Governance Layer (Collibra)

* Defines and manages:

  * Data Quality Rules
  * Business glossary mappings
  * Data ownership & stewardship

#### Rule Structure (Recommended)

```json
{
  "rule_type": "range",
  "column": "age",
  "min": 0,
  "max": 120,
  "severity": "error"
}
```

---

### 2. Integration Layer (Rule Extraction)

* Uses Collibra REST APIs to extract:

  * DQ rules
  * Dataset-rule mappings
* Scheduled via:

  * Databricks Workflows
  * Airflow

#### Example API

```
GET /rest/2.0/assets?type=Data Quality Rule
```

---

### 3. Metadata Storage Layer (Delta Lake)

All rules are stored as Delta tables:

#### `dq_rules`

| Column    | Description       |
| --------- | ----------------- |
| rule_id   | Unique identifier |
| rule_type | Type of rule      |
| column    | Target column     |
| params    | JSON parameters   |
| severity  | Error / Warning   |

#### `dq_rule_mapping`

| dataset | column | rule_id |

#### `dq_execution_log`

| run_id | rule_id | status | failed_count | timestamp |

---

### 4. Rule Engine (Databricks)

A generic PySpark-based engine dynamically executes rules.

#### Example Implementation

```python
def apply_rule(df, rule):
    if rule['rule_type'] == 'not_null':
        return df.filter(col(rule['column']).isNull())

    elif rule['rule_type'] == 'range':
        return df.filter(
            (col(rule['column']) < rule['min']) |
            (col(rule['column']) > rule['max'])
        )
```

---

### 5. Execution Layer

Rules are applied during pipeline processing:

#### Recommended Stage:

* **Bronze → Silver transition**

#### Execution Patterns:

* **Hard Enforcement**

  * Drop invalid records
* **Soft Enforcement**

  * Flag invalid records
  * Store in quarantine tables

---

### 6. Data Storage (Medallion Architecture)

| Layer  | Description              |
| ------ | ------------------------ |
| Bronze | Raw ingested data        |
| Silver | Cleaned + validated data |
| Gold   | Business-ready data      |

---

### 7. Observability & Reporting

* Store execution results in `dq_execution_log`
* Track:

  * Rule pass/fail counts
  * Data quality scores
  * Historical trends

#### Visualization Tools:

* Databricks SQL
* Power BI

---

### 8. Feedback Loop (Optional but Recommended)

Push execution results back to Collibra:

* Rule status (pass/fail)
* Data quality score
* SLA violations

**Benefit:** Business users gain visibility into actual data health.

---

## 🔄 End-to-End Workflow

1. Define rules in Collibra
2. Extract rules via API
3. Store rules in Delta tables
4. Execute rules in Databricks pipelines
5. Persist results and metrics
6. (Optional) Push results back to Collibra

---

## ⚙️ Technology Stack

| Layer         | Tools                          |
| ------------- | ------------------------------ |
| Governance    | Collibra                       |
| Orchestration | Databricks Workflows           |
| Processing    | PySpark                        |
| DQ Frameworks | Custom Python     |
| Storage       | Delta Lake                     |
| Visualization | Databricks SQL                 |

---

## 🚀 Advanced Features

* Rule versioning using Delta Time Travel
* Parameterized rules (threshold-based validation)
* Dataset-level scoring:

  ```
  DQ Score = (Passed Rules / Total Rules) * 100
  ```
* Auto rule suggestion via profiling/AI

---

## ⚠️ Challenges & Considerations

* Translating business rules into executable logic
* Managing complex rules (joins, cross-table checks)
* Performance impact on large datasets
* Rule prioritization and conflict resolution

---

## ✅ Best Practices

* Use structured rule definitions (avoid free text)
* Keep rules atomic and reusable
* Build a metadata-driven engine (no hardcoding)
* Implement audit logging and lineage tracking
* Define severity levels (error vs warning)
* Enable feedback loop to governance

---

## 📂 Suggested Repository Structure

```
/dq-framework
  /extract
    collibra_api_client.py
  /transform
    rule_parser.py
  /engine
    dq_engine.py
  /configs
    dq_rules_schema.json
  /pipelines
    bronze_to_silver.py
  /logs
    dq_execution_log
```

---

## 🧪 Future Enhancements

* Streaming DQ validation
* Real-time alerting (Slack / Email)
* Integration with Unity Catalog for governance alignment
* AI-based anomaly detection

---

## 🤝 Contributing

* Follow modular design principles
* Add new rule types via rule engine extensions
* Maintain backward compatibility for rule schemas

---

## 📌 Summary

This framework bridges **data governance and data engineering**, enabling:

✔ Centralized rule management
✔ Scalable execution on big data
✔ End-to-end data quality visibility
✔ Strong governance feedback loop

---

For enhancements or production implementation support, extend this framework with organization-specific requirements.
