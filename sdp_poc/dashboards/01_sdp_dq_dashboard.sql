-- =============================================================================
-- Data Quality Command Center — SQL Queries for AI/BI Dashboard
-- =============================================================================
-- These queries match the Lakeview dashboard definition (lakeview_dq_dashboard.json).
-- Use these to manually create widgets in Databricks SQL or AI/BI Dashboard.
-- =============================================================================

-- =====================================================================
-- PAGE 1: EXECUTIVE OVERVIEW
-- =====================================================================

-- KPI Counter 1: Overall DQ Score
SELECT
    ROUND(AVG(pass_rate_pct), 1) AS overall_dq_score
FROM dq_poc.silver.dq_summary_by_dataset;

-- KPI Counter 2: Total Records Evaluated
SELECT
    SUM(total_records) AS total_records
FROM dq_poc.silver.dq_summary_by_dataset;

-- KPI Counter 3: Total Active Rules
SELECT
    COUNT(DISTINCT rule_id) AS total_rules
FROM dq_poc.silver.dq_summary_by_rule;

-- KPI Counter 4: Total Rule Failures
SELECT
    SUM(failed_count) AS total_failures
FROM dq_poc.silver.dq_summary_by_rule;

-- Bar Chart: DQ Pass Rate by Dataset
SELECT
    dataset_name,
    total_records,
    fully_clean_records,
    total_records - fully_clean_records AS impacted_records,
    pass_rate_pct,
    avg_row_score,
    total_evaluations,
    ROUND(fully_clean_records * 100.0 / total_records, 1) AS clean_pct
FROM dq_poc.silver.dq_summary_by_dataset
ORDER BY pass_rate_pct ASC;

-- Stacked Bar: Clean vs Quarantined Rows
SELECT 'customers' AS dataset,
    (SELECT COUNT(*) FROM dq_poc.silver.customers_clean) AS clean_rows,
    (SELECT COUNT(*) FROM dq_poc.silver.customers_quarantine) AS quarantine_rows,
    (SELECT COUNT(*) FROM dq_poc.silver.customers_validated) AS total_rows
UNION ALL
SELECT 'transactions',
    (SELECT COUNT(*) FROM dq_poc.silver.transactions_clean),
    (SELECT COUNT(*) FROM dq_poc.silver.transactions_quarantine),
    (SELECT COUNT(*) FROM dq_poc.silver.transactions_validated)
UNION ALL
SELECT 'products',
    (SELECT COUNT(*) FROM dq_poc.silver.products_clean),
    (SELECT COUNT(*) FROM dq_poc.silver.products_quarantine),
    (SELECT COUNT(*) FROM dq_poc.silver.products_validated);

-- Grouped Bar: Row-Level DQ Score Distribution
SELECT score_bucket, dataset, SUM(row_count) AS row_count FROM (
  SELECT
    CASE
      WHEN _dq_row_score = 100 THEN '1 - Perfect (100%)'
      WHEN _dq_row_score >= 80 THEN '2 - Good (80-99%)'
      WHEN _dq_row_score >= 60 THEN '3 - Fair (60-79%)'
      WHEN _dq_row_score >= 40 THEN '4 - Poor (40-59%)'
      ELSE '5 - Critical (< 40%)'
    END AS score_bucket,
    'customers' AS dataset, 1 AS row_count
  FROM dq_poc.silver.customers_validated
  UNION ALL
  SELECT
    CASE
      WHEN _dq_row_score = 100 THEN '1 - Perfect (100%)'
      WHEN _dq_row_score >= 80 THEN '2 - Good (80-99%)'
      WHEN _dq_row_score >= 60 THEN '3 - Fair (60-79%)'
      WHEN _dq_row_score >= 40 THEN '4 - Poor (40-59%)'
      ELSE '5 - Critical (< 40%)'
    END AS score_bucket,
    'transactions' AS dataset, 1 AS row_count
  FROM dq_poc.silver.transactions_validated
  UNION ALL
  SELECT
    CASE
      WHEN _dq_row_score = 100 THEN '1 - Perfect (100%)'
      WHEN _dq_row_score >= 80 THEN '2 - Good (80-99%)'
      WHEN _dq_row_score >= 60 THEN '3 - Fair (60-79%)'
      WHEN _dq_row_score >= 40 THEN '4 - Poor (40-59%)'
      ELSE '5 - Critical (< 40%)'
    END AS score_bucket,
    'products' AS dataset, 1 AS row_count
  FROM dq_poc.silver.products_validated
) GROUP BY score_bucket, dataset
ORDER BY score_bucket;

-- Table: Dataset DQ Score Summary
SELECT 'customers' AS dataset,
    ROUND(AVG(_dq_row_score), 2) AS avg_score,
    MIN(_dq_row_score) AS min_score,
    MAX(_dq_row_score) AS max_score,
    COUNT(*) AS total_rows
FROM dq_poc.silver.customers_validated
UNION ALL
SELECT 'transactions',
    ROUND(AVG(_dq_row_score), 2), MIN(_dq_row_score),
    MAX(_dq_row_score), COUNT(*)
FROM dq_poc.silver.transactions_validated
UNION ALL
SELECT 'products',
    ROUND(AVG(_dq_row_score), 2), MIN(_dq_row_score),
    MAX(_dq_row_score), COUNT(*)
FROM dq_poc.silver.products_validated;


-- =====================================================================
-- PAGE 2: RULE ANALYSIS
-- =====================================================================

-- Horizontal Bar: Pass Rate by Rule (sorted worst-first)
SELECT
    r.rule_id,
    r.dataset_name,
    r.total_records,
    r.passed_count,
    r.failed_count,
    r.pass_rate_pct,
    CASE
        WHEN r.pass_rate_pct >= 95 THEN 'Healthy'
        WHEN r.pass_rate_pct >= 80 THEN 'Warning'
        ELSE 'Critical'
    END AS health_status
FROM dq_poc.silver.dq_summary_by_rule r
ORDER BY r.pass_rate_pct ASC;

-- Table: Rule Health Matrix (GREEN / AMBER / RED)
SELECT
    dataset_name,
    rule_id,
    pass_rate_pct,
    failed_count,
    CASE
        WHEN pass_rate_pct >= 95 THEN 'GREEN'
        WHEN pass_rate_pct >= 80 THEN 'AMBER'
        ELSE 'RED'
    END AS health_status
FROM dq_poc.silver.dq_summary_by_rule
ORDER BY dataset_name, pass_rate_pct ASC;

-- Bar: Failures by Rule Type
SELECT
    CASE
        WHEN rule_id IN ('DQR-000001','DQR-000002','DQR-000009','DQR-000012','DQR-000016')
            THEN 'Not Null'
        WHEN rule_id IN ('DQR-000003','DQR-000010','DQR-000017') THEN 'Range'
        WHEN rule_id IN ('DQR-000004','DQR-000019') THEN 'Regex/Format'
        WHEN rule_id IN ('DQR-000005') THEN 'Length'
        WHEN rule_id IN ('DQR-000006','DQR-000011','DQR-000018') THEN 'Allowed Values'
        WHEN rule_id IN ('DQR-000007','DQR-000020') THEN 'Uniqueness'
        WHEN rule_id IN ('DQR-000008','DQR-000013') THEN 'Referential Integrity'
        WHEN rule_id IN ('DQR-000014') THEN 'Freshness'
        WHEN rule_id IN ('DQR-000015') THEN 'Custom SQL'
        ELSE 'Other'
    END AS rule_type,
    COUNT(*) AS total_evaluations,
    SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed_count,
    SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) AS failed_count,
    ROUND(SUM(CASE WHEN passed THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        AS pass_rate_pct
FROM dq_poc.silver.dq_row_level_detail_all
GROUP BY 1
ORDER BY pass_rate_pct ASC;

-- Table: Rules by Severity (error vs warning)
SELECT
    dataset_name,
    rule_id,
    pass_rate_pct,
    failed_count,
    CASE
        WHEN rule_id IN (
            'DQR-000001','DQR-000002','DQR-000003','DQR-000004',
            'DQR-000006','DQR-000007','DQR-000009','DQR-000010',
            'DQR-000011','DQR-000012','DQR-000013','DQR-000016',
            'DQR-000017','DQR-000020'
        ) THEN 'error'
        ELSE 'warning'
    END AS severity
FROM dq_poc.silver.dq_summary_by_rule
ORDER BY severity, pass_rate_pct ASC;


-- =====================================================================
-- PAGE 3: ROW-LEVEL DQ DETAIL
-- =====================================================================

-- Table: Customers — Failing Rows (worst DQ score first)
SELECT
    customer_id,
    customer_name,
    email,
    age,
    status,
    _dq_failed_count,
    _dq_passed_count,
    _dq_total_rules,
    _dq_row_score,
    CONCAT_WS(', ', _dq_failed_rules) AS failed_rules,
    _dq_evaluated_at
FROM dq_poc.silver.customers_validated
WHERE _dq_all_passed = false
ORDER BY _dq_row_score ASC
LIMIT 50;

-- Table: Transactions — Failing Rows
SELECT
    transaction_id,
    customer_id,
    amount,
    transaction_type,
    transaction_date,
    _dq_failed_count,
    _dq_passed_count,
    _dq_total_rules,
    _dq_row_score,
    CONCAT_WS(', ', _dq_failed_rules) AS failed_rules,
    _dq_evaluated_at
FROM dq_poc.silver.transactions_validated
WHERE _dq_all_passed = false
ORDER BY _dq_row_score ASC
LIMIT 50;

-- Table: Products — Failing Rows
SELECT
    product_id,
    product_name,
    sku,
    category,
    price,
    _dq_failed_count,
    _dq_passed_count,
    _dq_total_rules,
    _dq_row_score,
    CONCAT_WS(', ', _dq_failed_rules) AS failed_rules,
    _dq_evaluated_at
FROM dq_poc.silver.products_validated
WHERE _dq_all_passed = false
ORDER BY _dq_row_score ASC
LIMIT 50;


-- =====================================================================
-- PAGE 4: QUARANTINE ZONE
-- =====================================================================

-- Table: Quarantined Customers
SELECT
    customer_id,
    customer_name,
    email,
    _dq_failed_count,
    _dq_row_score,
    CONCAT_WS(', ', _dq_failed_rules) AS failed_rules,
    _quarantine_reason,
    _quarantined_at
FROM dq_poc.silver.customers_quarantine
ORDER BY _dq_row_score ASC;

-- Table: Quarantined Transactions
SELECT
    transaction_id,
    customer_id,
    amount,
    transaction_type,
    _dq_failed_count,
    _dq_row_score,
    CONCAT_WS(', ', _dq_failed_rules) AS failed_rules,
    _quarantine_reason,
    _quarantined_at
FROM dq_poc.silver.transactions_quarantine
ORDER BY _dq_row_score ASC;

-- Table: Quarantined Products
SELECT
    product_id,
    product_name,
    sku,
    _dq_failed_count,
    _dq_row_score,
    CONCAT_WS(', ', _dq_failed_rules) AS failed_rules,
    _quarantine_reason,
    _quarantined_at
FROM dq_poc.silver.products_quarantine
ORDER BY _dq_row_score ASC;
