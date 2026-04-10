-- =============================================================================
-- Executive DQ Overview Dashboard
-- Use these queries in Databricks SQL Dashboard
-- =============================================================================

-- Widget 1: Overall DQ Score (Counter)
SELECT
    ROUND(AVG(overall_dq_score), 1) AS overall_dq_score
FROM dq_poc.dq_framework.dq_scores;

-- Widget 2: DQ Score by Dataset (Bar Chart)
SELECT
    dataset_name,
    overall_dq_score,
    total_rules,
    rules_passed,
    rules_failed,
    total_records_in,
    total_records_out,
    records_quarantined
FROM dq_poc.dq_framework.dq_scores
ORDER BY overall_dq_score ASC;

-- Widget 3: Rule Pass Rate by Type (Horizontal Bar)
SELECT
    rule_type,
    COUNT(*) AS total_executions,
    SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
    ROUND(
        SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        1
    ) AS pass_rate_pct
FROM dq_poc.dq_framework.dq_execution_log
GROUP BY rule_type
ORDER BY pass_rate_pct ASC;

-- Widget 4: Top Failing Rules (Table)
SELECT
    rule_id,
    rule_type,
    dataset_name,
    target_column,
    severity,
    dq_score,
    failed_count,
    total_records,
    ROUND((failed_count * 100.0 / total_records), 1) AS failure_rate_pct
FROM dq_poc.dq_framework.dq_execution_log
WHERE status = 'failed'
ORDER BY failure_rate_pct DESC
LIMIT 10;

-- Widget 5: Quarantine Volume by Dataset (Pie Chart)
SELECT
    source_table,
    COUNT(*) AS quarantined_records
FROM dq_poc.dq_framework.dq_quarantine
GROUP BY source_table;

-- Widget 6: DQ Heatmap - Dataset x Rule Type (Pivot Table)
SELECT
    dataset_name,
    rule_type,
    ROUND(AVG(dq_score), 1) AS avg_score,
    CASE
        WHEN AVG(dq_score) >= 95 THEN 'GREEN'
        WHEN AVG(dq_score) >= 80 THEN 'AMBER'
        ELSE 'RED'
    END AS health_status
FROM dq_poc.dq_framework.dq_execution_log
GROUP BY dataset_name, rule_type
ORDER BY dataset_name, rule_type;

-- Widget 7: Severity Distribution (Donut Chart)
SELECT
    severity,
    COUNT(*) AS rule_count,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
    ROUND(AVG(dq_score), 1) AS avg_score
FROM dq_poc.dq_framework.dq_execution_log
GROUP BY severity;
