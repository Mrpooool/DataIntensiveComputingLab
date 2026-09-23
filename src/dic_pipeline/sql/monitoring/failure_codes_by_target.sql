-- Which validation rules reject the records? One row per stage, target and error code.
SELECT
    stage,
    target,
    code,
    COUNT(*) AS executions_with_code,
    SUM(code_count) AS records
FROM pipeline_runs
LATERAL VIEW EXPLODE(FROM_JSON(validation_failure_counts_json, 'MAP<STRING, BIGINT>')) AS code, code_count
WHERE validation_enabled AND status = 'success'
GROUP BY stage, target, code
ORDER BY records DESC, stage, target, code
