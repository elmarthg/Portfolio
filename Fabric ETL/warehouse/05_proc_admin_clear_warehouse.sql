-- 05_proc_admin_clear_warehouse v2.3: installs dbo.usp_admin_clear_warehouse_data
-- Replaces 05_proc_reset_warehouse (usp_reset_warehouse_data), which this script removes.
--
-- The warehouse is persistent now, so nothing clears it on a schedule. This procedure is the
-- manual, last-resort way to empty the loaded tables, for example to rebuild a test warehouse.
-- It is NOT part of the weekly pipeline.
--
-- What you lose: every client version history, every deleted flag and the date it was detected,
-- and every key Apricot or a report may have stored (client_key, enrollment_key and so on are
-- reissued on the next load). Silver only holds the latest export, so this history cannot be
-- rebuilt from it.
-- Before using it on a warehouse that matters, prefer a restore in place
-- (Warehouse > Settings > Restore points), which keeps everything as of that point.
--
-- It refuses to run unless you type the confirmation phrase exactly:
--     EXEC dbo.usp_admin_clear_warehouse_data @confirm = 'DELETE ALL LOADED DATA';
-- Reference and control tables are never touched: dim_date, dim_source, etl_source_config,
-- kpi_target, dim_hud_code, security_user_program_access and the run logs.
-- v2.1: also clears dbo.person_client_link, rpt.person and rpt.person_client (schema v8.15). Person keys
-- restart, so any person_key Apricot stored is no longer valid. dbo.match_decision is KEPT: reviewer
-- decisions are keyed on source IDs, not warehouse keys, and apply again after the reload.
-- v2.2: also empties dbo.match_review_queue (it holds client keys; the next usp_apply_match_suggestions
-- rebuilds it). dbo.apricot_profile_map is KEPT: it is keyed on source IDs and records what Apricot holds.
-- v2.3: also empties dbo.dim_program_history (schema v8.18); logs its own version correctly.

DROP PROCEDURE IF EXISTS dbo.usp_reset_warehouse_data;
GO

DROP PROCEDURE IF EXISTS dbo.usp_admin_clear_warehouse_data;
GO

CREATE PROCEDURE dbo.usp_admin_clear_warehouse_data
    @confirm VARCHAR(100) = NULL
AS
BEGIN
    IF @confirm IS NULL OR @confirm <> 'DELETE ALL LOADED DATA'
    BEGIN
        DECLARE @rows_msg NVARCHAR(2048) = CONCAT(
            'Nothing was deleted. This empties every loaded table and erases client history (',
            (SELECT COUNT(*) FROM dim_client), ' client versions, ',
            (SELECT COUNT(*) FROM fact_enrollment), ' enrollments). ',
            'If you really mean it, run: EXEC dbo.usp_admin_clear_warehouse_data @confirm = ''DELETE ALL LOADED DATA'';');
        THROW 50013, @rows_msg, 1;
    END;

    DECLARE @log_start DATETIME2(6) = SYSDATETIME();

    UPDATE dbo.warehouse_run_log
    SET run_end = SYSDATETIME(), run_status = 'ABANDONED'
    WHERE script_name = '05_admin_clear_warehouse' AND run_status = 'RUNNING';

    INSERT INTO dbo.warehouse_run_log (script_name, script_version, run_start, run_status, run_by, change_summary)
    VALUES ('05_admin_clear_warehouse', 'v2.3', @log_start, 'RUNNING', USER_NAME(),
            'Manual clear of all loaded tables (history erased)');

    BEGIN TRY
        BEGIN TRANSACTION;

        IF OBJECT_ID('rpt.person', 'U') IS NOT NULL
            DELETE FROM rpt.person;
        IF OBJECT_ID('rpt.person_client', 'U') IS NOT NULL
            DELETE FROM rpt.person_client;
        IF OBJECT_ID('dbo.person_client_link', 'U') IS NOT NULL
            DELETE FROM dbo.person_client_link;
        IF OBJECT_ID('dbo.match_review_queue', 'U') IS NOT NULL
            DELETE FROM dbo.match_review_queue;

        DELETE FROM fact_current_living_situation;
        DELETE FROM fact_enrollment_stage;
        DELETE FROM fact_consent;
        DELETE FROM fact_ce_event;
        DELETE FROM fact_financial_assistance;
        DELETE FROM fact_assessment_response;
        DELETE FROM fact_assessment;
        DELETE FROM fact_document;
        DELETE FROM fact_bed_availability;
        DELETE FROM fact_bed_night;
        DELETE FROM fact_case_note;
        DELETE FROM fact_service;
        DELETE FROM bridge_household_client;
        DELETE FROM bridge_client_source;
        DELETE FROM bridge_enrollment_grant;
        DELETE FROM bridge_program_site;
        DELETE FROM fact_enrollment;

        DELETE FROM dim_bed;
        DELETE FROM dim_staff;
        DELETE FROM dim_client_identifier;
        DELETE FROM dim_client;
        DELETE FROM dim_household;
        DELETE FROM dim_service_item;
        DELETE FROM dim_funding_source;
        DELETE FROM dim_grant;
        IF OBJECT_ID('dbo.dim_program_history', 'U') IS NOT NULL
            DELETE FROM dbo.dim_program_history;
        DELETE FROM dim_program;
        DELETE FROM dim_organization;

        UPDATE dbo.warehouse_run_log
        SET run_end = SYSDATETIME(), run_status = 'SUCCESS'
        WHERE script_name = '05_admin_clear_warehouse' AND run_start = @log_start;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        UPDATE dbo.warehouse_run_log
        SET run_end = SYSDATETIME(), run_status = 'FAILED', error_message = ERROR_MESSAGE()
        WHERE script_name = '05_admin_clear_warehouse' AND run_start = @log_start;
        THROW;
    END CATCH;
END;
GO

-- EXEC dbo.usp_admin_clear_warehouse_data;                                     -- refuses and shows what it would erase
-- EXEC dbo.usp_admin_clear_warehouse_data @confirm = 'DELETE ALL LOADED DATA'; -- actually clears
