-- How has processing time changed over multiple executions of the same stage and target?
SELECT
    stage,
    target,
    ROW_NUMBER() OVER w AS execution_number,
    run_id,
    DATE_FORMAT(started_at, 'yyyy-MM-dd HH:mm:ss') AS started_at_utc,
    mode,
    processed_count,
    execution_seconds,
    execution_seconds - LAG(execution_seconds) OVER w AS change_seconds,
    AVG(execution_seconds) OVER (
        PARTITION BY stage, target ORDER BY started_at ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3_seconds,
    TRY_DIVIDE(processed_count, execution_seconds) AS records_per_second
FROM pipeline_runs
WHERE status = 'success'
WINDOW w AS (PARTITION BY stage, target ORDER BY started_at)
ORDER BY stage, target, started_at
