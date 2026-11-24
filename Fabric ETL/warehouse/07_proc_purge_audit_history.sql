-- 07_proc_purge_audit_history v1.0: installs dbo.usp_purge_audit_history (schema v8.18)
-- Deletes old rows from the load logs so they do not grow forever. Run it by hand or as a quarterly
-- pipeline activity; it is not part of the weekly load.
--
--     EXEC dbo.usp_purge_audit_history;                                   -- dry run: counts only
--     EXEC dbo.usp_purge_audit_history @keep_months = 36, @dry_run = 0;   -- deletes
--
-- What it removes, older than @keep_months:
--   stage_load_log and stage_load_reject rows, except every row of each source's latest successful run
--   (the data-entry correction list reads those); routine run-log rows of the loaders and person-link
--   procedures (script names starting 03 or 06).
-- What it never removes: warehouse_schema_version, migration, DDL, seed, security and admin-clear runs in
--   warehouse_run_log, and anything newer than @keep_months. @keep_months below 12 is refused.
-- The 36-month default is a placeholder: set it from the data retention section of the privacy policy.
-- Admin only: no role is granted EXECUTE.

DROP PROCEDURE IF EXISTS dbo.usp_purge_audit_history;
GO

CREATE PROCEDURE dbo.usp_purge_audit_history
    @keep_months INT = 36,
    @dry_run BIT = 1,
    @pipeline_run_id VARCHAR(100) = NULL
AS
BEGIN

IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.18')
    THROW 50011, 'The warehouse schema is older than v8.18. Run migrations/0005_keys_and_cleanup.sql first.', 1;
IF @keep_months IS NULL OR @keep_months < 12
    THROW 50060, 'Keep at least 12 months of load history (@keep_months >= 12).', 1;

DECLARE @log_start DATETIME2(6) = SYSDATETIME();
DECLARE @cutoff DATETIME2(6) = DATEADD(month, -@keep_months, @log_start);

DROP TABLE IF EXISTS #keep_runs;
SELECT DISTINCT l.run_id
INTO #keep_runs
FROM dbo.stage_load_log l
JOIN (SELECT source_system, MAX(warehouse_load_datetime) AS last_time
      FROM dbo.stage_load_log WHERE load_status = 'SUCCESS'
      GROUP BY source_system) m
  ON m.source_system = l.source_system AND m.last_time = l.warehouse_load_datetime;

DECLARE @n_log INT = (SELECT COUNT(*) FROM dbo.stage_load_log l
                      WHERE l.warehouse_load_datetime < @cutoff
                        AND NOT EXISTS (SELECT 1 FROM #keep_runs k WHERE k.run_id = l.run_id));
DECLARE @n_reject INT = (SELECT COUNT(*) FROM dbo.stage_load_reject r
                         WHERE r.detected_datetime < @cutoff
                           AND NOT EXISTS (SELECT 1 FROM #keep_runs k WHERE k.run_id = r.run_id));
DECLARE @n_run INT = (SELECT COUNT(*) FROM dbo.warehouse_run_log w
                      WHERE w.run_start < @cutoff AND (w.script_name LIKE '03%' OR w.script_name LIKE '06%'));

IF @dry_run = 0
BEGIN
    INSERT INTO dbo.warehouse_run_log (script_name, script_version, run_start, run_status, run_by, change_summary, pipeline_run_id)
    VALUES ('07_purge_audit_history', 'v1.0', @log_start, 'RUNNING', USER_NAME(),
            CONCAT('Purge load history older than ', @keep_months, ' months'), @pipeline_run_id);

    BEGIN TRY
        BEGIN TRANSACTION;

        DELETE FROM dbo.stage_load_reject
        WHERE detected_datetime < @cutoff
          AND NOT EXISTS (SELECT 1 FROM #keep_runs k WHERE k.run_id = dbo.stage_load_reject.run_id);

        DELETE FROM dbo.stage_load_log
        WHERE warehouse_load_datetime < @cutoff
          AND NOT EXISTS (SELECT 1 FROM #keep_runs k WHERE k.run_id = dbo.stage_load_log.run_id);

        DELETE FROM dbo.warehouse_run_log
        WHERE run_start < @cutoff AND (script_name LIKE '03%' OR script_name LIKE '06%');

        UPDATE dbo.warehouse_run_log
        SET run_end = SYSDATETIME(), run_status = 'SUCCESS',
            notes = CONCAT('Deleted ', @n_log, ' load-log, ', @n_reject, ' reject and ', @n_run, ' run-log rows')
        WHERE script_name = '07_purge_audit_history' AND run_start = @log_start;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        UPDATE dbo.warehouse_run_log
        SET run_end = SYSDATETIME(), run_status = 'FAILED', error_message = ERROR_MESSAGE()
        WHERE script_name = '07_purge_audit_history' AND run_start = @log_start;
        THROW;
    END CATCH;
END;

SELECT t.table_name, t.rows_older_than_cutoff,
       CASE WHEN @dry_run = 1 THEN 0 ELSE t.rows_older_than_cutoff END AS rows_deleted,
       @cutoff AS cutoff
FROM (VALUES ('stage_load_log', @n_log), ('stage_load_reject', @n_reject), ('warehouse_run_log', @n_run))
     t (table_name, rows_older_than_cutoff);

END;
GO
