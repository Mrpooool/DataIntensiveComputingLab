-- Which dataset requires the longest processing time? Successful runs only.
SELECT
    stage,
    target,
    COUNT(*) AS executions,
    AVG(execution_seconds) AS avg_seconds,
    MIN(execution_seconds) AS min_seconds,
    MAX(execution_seconds) AS max_seconds,
    MAX_BY(execution_seconds, started_at) AS latest_seconds,
    TRY_DIVIDE(SUM(processed_count), SUM(execution_seconds)) AS records_per_second
FROM pipeline_runs
WHERE status = 'success'
GROUP BY stage, target
ORDER BY avg_seconds DESC, stage, target
