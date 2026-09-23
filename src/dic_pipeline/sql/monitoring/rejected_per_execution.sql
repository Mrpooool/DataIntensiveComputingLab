-- How many records were rejected during each execution? One row per run and target.
SELECT
    run_id,
    DATE_FORMAT(started_at, 'yyyy-MM-dd HH:mm:ss') AS started_at_utc,
    stage,
    target,
    mode,
    status,
    processed_count,
    rejected_count,
    duplicate_count,
    TRY_DIVIDE(rejected_count, processed_count) AS rejected_share
FROM pipeline_runs
WHERE stage IN ('ingestion', 'incremental_update')
ORDER BY started_at, run_id, target
