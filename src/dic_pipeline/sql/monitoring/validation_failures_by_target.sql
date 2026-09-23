-- Which dataset fails validation most frequently?
-- Only stages that validate rows; a run with validation switched off is not evidence of clean data.
SELECT
    stage,
    target,
    COUNT(*) AS executions,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_executions,
    SUM(CASE WHEN rejected_count > 0 THEN 1 ELSE 0 END) AS executions_with_rejects,
    SUM(processed_count) AS processed_records,
    SUM(rejected_count) AS rejected_records,
    TRY_DIVIDE(SUM(rejected_count), SUM(processed_count)) AS rejected_share
FROM pipeline_runs
WHERE validation_enabled
GROUP BY stage, target
ORDER BY rejected_share DESC NULLS LAST, rejected_records DESC, stage, target
