-- migrations/0001_persistent_loads.sql  (warehouse schema v8.13 -> v8.14)
--
-- Makes the warehouse safe to keep from run to run instead of wiping it every week.
-- Run once, after 01_ddl_v8.13 has built the baseline, and before installing 03a v2.0 and 03b v3.0.
-- Safe to rerun: every change checks first, so a second run changes nothing.
-- Never drops a table or a column. Create a user-defined restore point first
-- (Warehouse > Settings > Restore points > Add) so you can undo it.
--
-- What it adds:
--   1. Soft-delete columns where they were missing, so records that disappear from
--      a source are flagged instead of silently lingering:
--        is_deleted + deleted_detected_datetime on dim_program, dim_service_item, dim_bed,
--        fact_consent, fact_bed_availability
--        deleted_detected_datetime on dim_organization, dim_grant, dim_funding_source,
--        dim_household (they already had is_deleted)
--   2. Run tracking: pipeline_run_id on warehouse_run_log, stage_load_log and
--      stage_load_reject; run_id, rows_changed and rows_deleted on stage_load_log.
--   3. Reporting views that leave out deleted rows. v_fact_enrollment_all / _primary gain
--      length_of_stay_to_date (open enrollments are counted to today, Pacific time), because the
--      stored length_of_stay_days is now filled only once an enrollment has exited.
--      v_data_quality_unresolved stops counting rows flagged deleted. They list their columns on
--      purpose: warehouse_reader is denied the SSN hash and other sensitive dim_client columns, and
--      Fabric checks those denies even through a view (it has no ownership chaining), so a view
--      that selected * from dim_client would fail for report users. Listing columns keeps it usable.

-- 0. LOG RUN START
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'ABANDONED'
WHERE script_name = 'migration_0001' AND run_status = 'RUNNING';

INSERT INTO dbo.warehouse_run_log (script_name, script_version, run_start, run_status, run_by, change_summary)
VALUES ('migration_0001', 'v8.14', SYSDATETIME(), 'RUNNING', USER_NAME(),
        'Persistent loads: soft-delete columns, run tracking columns, active-row views');
GO

-- 1. SOFT-DELETE COLUMNS
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_program') AND name = 'is_deleted')
    ALTER TABLE dbo.dim_program ADD is_deleted BIT;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_program') AND name = 'deleted_detected_datetime')
    ALTER TABLE dbo.dim_program ADD deleted_detected_datetime DATETIME2(6);

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_service_item') AND name = 'is_deleted')
    ALTER TABLE dbo.dim_service_item ADD is_deleted BIT;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_service_item') AND name = 'deleted_detected_datetime')
    ALTER TABLE dbo.dim_service_item ADD deleted_detected_datetime DATETIME2(6);

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_bed') AND name = 'is_deleted')
    ALTER TABLE dbo.dim_bed ADD is_deleted BIT;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_bed') AND name = 'deleted_detected_datetime')
    ALTER TABLE dbo.dim_bed ADD deleted_detected_datetime DATETIME2(6);

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_consent') AND name = 'is_deleted')
    ALTER TABLE dbo.fact_consent ADD is_deleted BIT;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_consent') AND name = 'deleted_detected_datetime')
    ALTER TABLE dbo.fact_consent ADD deleted_detected_datetime DATETIME2(6);

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_bed_availability') AND name = 'is_deleted')
    ALTER TABLE dbo.fact_bed_availability ADD is_deleted BIT;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_bed_availability') AND name = 'deleted_detected_datetime')
    ALTER TABLE dbo.fact_bed_availability ADD deleted_detected_datetime DATETIME2(6);

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_organization') AND name = 'deleted_detected_datetime')
    ALTER TABLE dbo.dim_organization ADD deleted_detected_datetime DATETIME2(6);
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_grant') AND name = 'deleted_detected_datetime')
    ALTER TABLE dbo.dim_grant ADD deleted_detected_datetime DATETIME2(6);
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_funding_source') AND name = 'deleted_detected_datetime')
    ALTER TABLE dbo.dim_funding_source ADD deleted_detected_datetime DATETIME2(6);
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_household') AND name = 'deleted_detected_datetime')
    ALTER TABLE dbo.dim_household ADD deleted_detected_datetime DATETIME2(6);
GO

-- 2. RUN TRACKING COLUMNS
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.warehouse_run_log') AND name = 'pipeline_run_id')
    ALTER TABLE dbo.warehouse_run_log ADD pipeline_run_id VARCHAR(100);

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.stage_load_log') AND name = 'run_id')
    ALTER TABLE dbo.stage_load_log ADD run_id VARCHAR(50);
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.stage_load_log') AND name = 'pipeline_run_id')
    ALTER TABLE dbo.stage_load_log ADD pipeline_run_id VARCHAR(100);
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.stage_load_log') AND name = 'rows_changed')
    ALTER TABLE dbo.stage_load_log ADD rows_changed INT;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.stage_load_log') AND name = 'rows_deleted')
    ALTER TABLE dbo.stage_load_log ADD rows_deleted INT;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.stage_load_reject') AND name = 'pipeline_run_id')
    ALTER TABLE dbo.stage_load_reject ADD pipeline_run_id VARCHAR(100);
GO

-- 3. ACTIVE-ROW VIEWS (explicit columns; denied columns are left out so report users can read them)
DROP VIEW IF EXISTS dbo.v_fact_service_active;
DROP VIEW IF EXISTS dbo.v_fact_case_note_active;
DROP VIEW IF EXISTS dbo.v_fact_consent_active;
DROP VIEW IF EXISTS dbo.v_fact_enrollment_primary;
DROP VIEW IF EXISTS dbo.v_fact_enrollment_all;
GO

CREATE VIEW dbo.v_fact_enrollment_all AS
SELECT f.*,
       DATEDIFF(day, f.enrollment_start_date,
                COALESCE(f.enrollment_exit_date, CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'Pacific Standard Time' AS DATE)))
           AS length_of_stay_to_date
FROM dbo.fact_enrollment f
WHERE ISNULL(f.is_deleted, 0) = 0;
GO

CREATE VIEW dbo.v_fact_enrollment_primary AS
SELECT * FROM dbo.v_fact_enrollment_all WHERE is_co_enrollment = 0;
GO

-- Migration 0004 (v8.17) redefines this view; a rerun of 0001 after it must not put the old one back.
IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.17')
BEGIN
    DROP VIEW IF EXISTS dbo.v_data_quality_unresolved;
    EXEC('CREATE VIEW dbo.v_data_quality_unresolved AS
SELECT ''fact_enrollment'' AS fact_table, ''program_id'' AS column_name,
       COUNT(*) AS unresolved_rows FROM dbo.fact_enrollment WHERE program_id IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT ''fact_enrollment'', ''client_version_key'',
       COUNT(*) FROM dbo.fact_enrollment WHERE client_version_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT ''fact_enrollment'', ''household_key'',
       COUNT(*) FROM dbo.fact_enrollment WHERE household_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT ''fact_enrollment'', ''grant_key'',
       COUNT(*) FROM dbo.fact_enrollment WHERE grant_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT ''fact_service'', ''enrollment_key'',
       COUNT(*) FROM dbo.fact_service WHERE enrollment_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT ''fact_service'', ''program_id'',
       COUNT(*) FROM dbo.fact_service WHERE program_id IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT ''fact_service'', ''service_item_key'',
       COUNT(*) FROM dbo.fact_service WHERE service_item_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT ''fact_case_note'', ''enrollment_key'',
       COUNT(*) FROM dbo.fact_case_note WHERE enrollment_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT ''fact_case_note'', ''program_id'',
       COUNT(*) FROM dbo.fact_case_note WHERE program_id IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT ''fact_assessment'', ''enrollment_key'',
       COUNT(*) FROM dbo.fact_assessment WHERE enrollment_key IS NULL
UNION ALL SELECT ''fact_assessment'', ''program_id'',
       COUNT(*) FROM dbo.fact_assessment WHERE program_id IS NULL
UNION ALL SELECT ''fact_document'', ''enrollment_key'',
       COUNT(*) FROM dbo.fact_document WHERE enrollment_key IS NULL
UNION ALL SELECT ''fact_document'', ''program_id'',
       COUNT(*) FROM dbo.fact_document WHERE program_id IS NULL
UNION ALL SELECT ''fact_bed_night'', ''bed_key'',
       COUNT(*) FROM dbo.fact_bed_night WHERE bed_key IS NULL
UNION ALL SELECT ''fact_bed_night'', ''enrollment_key'',
       COUNT(*) FROM dbo.fact_bed_night WHERE enrollment_key IS NULL
UNION ALL SELECT ''fact_bed_night'', ''program_id'',
       COUNT(*) FROM dbo.fact_bed_night WHERE program_id IS NULL
UNION ALL SELECT ''fact_bed_availability'', ''bed_key'',
       COUNT(*) FROM dbo.fact_bed_availability WHERE bed_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT ''fact_bed_availability'', ''program_id'',
       COUNT(*) FROM dbo.fact_bed_availability WHERE program_id IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT ''fact_financial_assistance'', ''enrollment_key'',
       COUNT(*) FROM dbo.fact_financial_assistance WHERE enrollment_key IS NULL
UNION ALL SELECT ''fact_financial_assistance'', ''program_id'',
       COUNT(*) FROM dbo.fact_financial_assistance WHERE program_id IS NULL
UNION ALL SELECT ''fact_financial_assistance'', ''grant_key'',
       COUNT(*) FROM dbo.fact_financial_assistance WHERE grant_key IS NULL
UNION ALL SELECT ''fact_financial_assistance'', ''service_key'',
       COUNT(*) FROM dbo.fact_financial_assistance WHERE service_key IS NULL
UNION ALL SELECT ''fact_ce_event'', ''enrollment_key'',
       COUNT(*) FROM dbo.fact_ce_event WHERE enrollment_key IS NULL
UNION ALL SELECT ''fact_ce_event'', ''program_id'',
       COUNT(*) FROM dbo.fact_ce_event WHERE program_id IS NULL
UNION ALL SELECT ''fact_consent'', ''enrollment_key'',
       COUNT(*) FROM dbo.fact_consent WHERE enrollment_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT ''fact_consent'', ''program_id'',
       COUNT(*) FROM dbo.fact_consent WHERE program_id IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT ''fact_enrollment_stage'', ''enrollment_key'',
       COUNT(*) FROM dbo.fact_enrollment_stage WHERE enrollment_key IS NULL
UNION ALL SELECT ''fact_current_living_situation'', ''enrollment_key'',
       COUNT(*) FROM dbo.fact_current_living_situation WHERE enrollment_key IS NULL
UNION ALL SELECT ''bridge_enrollment_grant'', ''grant_key'',
       COUNT(*) FROM dbo.bridge_enrollment_grant WHERE grant_key IS NULL
UNION ALL SELECT ''kpi_target'', ''grant not found'',
       COUNT(*) FROM dbo.v_kpi_target WHERE source_grant_id IS NOT NULL AND grant_key IS NULL');
END;
GO

-- Migration 0005 (v8.18) renames household_key and redefines this view; a rerun of 0001 after it leaves it alone.
IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.18')
BEGIN
    DROP VIEW IF EXISTS dbo.v_dim_client_current;
    EXEC('CREATE VIEW dbo.v_dim_client_current AS
SELECT client_version_key, client_key, valid_from, household_key,
       salesforce_id, salesforce_account_id, external_id, apricot_record_id, wellsky_client_uid,
       hmis_client_id, hmis_personal_id, champ_client_id,
       first_name, middle_name, last_name, date_of_birth, sex, gender, race, race_detail, ethnicity,
       marital_status, number_of_children, education_level, primary_language,
       name_data_quality, ssn_data_quality, dob_data_quality,
       email, home_phone, mobile_phone, address_line1, city, state, postal_code, county,
       military_branch, military_service_component, discharge_status, military_entry_date, military_discharge_date,
       is_post_9_11, service_connected, period_of_service, veteran_status,
       homeless_status, housing_status, length_of_homelessness,
       has_conviction, conviction_type, conviction_date, probation_parole_status, parole_end_date,
       last_release_date, experience_during_incarceration, service_type, lead_source,
       source_system, source_agency, source_added_date, source_last_updated,
       created_date, last_modified_date, load_datetime
FROM dbo.dim_client
WHERE is_current = 1 AND ISNULL(is_deleted, 0) = 0');
END;
GO

CREATE VIEW dbo.v_fact_service_active AS
SELECT * FROM dbo.fact_service WHERE ISNULL(is_deleted, 0) = 0;
GO

CREATE VIEW dbo.v_fact_case_note_active AS
SELECT * FROM dbo.fact_case_note WHERE ISNULL(is_deleted, 0) = 0;
GO

CREATE VIEW dbo.v_fact_consent_active AS
SELECT consent_key, client_key, client_version_key, enrollment_key, program_id, staff_key, document_key,
       consent_type, consent_scope, permission_granted, consent_status, documentation_type,
       signed_date, signed_date_key, effective_start_date, effective_end_date, revoked_date,
       client_signature_captured, staff_signature_captured, is_current,
       source_system, source_record_id, source_consent_id, source_enrollment_id, source_client_id, source_personal_id,
       source_file_name, source_file_date, source_added_date, source_last_updated,
       source_created_by, source_updated_by, load_datetime
FROM dbo.fact_consent
WHERE ISNULL(is_deleted, 0) = 0;
GO

-- 4. SCHEMA VERSION + LOG RUN END
IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.14')
    INSERT INTO dbo.warehouse_schema_version (version, applied_date, applied_by, summary, details)
    VALUES ('v8.14', SYSDATETIME(), USER_NAME(),
            'Persistent loads: soft-delete columns on 9 tables, pipeline_run_id and change counts in the logs, active-row views',
            'migrations/0001_persistent_loads.sql');

UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'SUCCESS'
WHERE script_name = 'migration_0001' AND run_status = 'RUNNING';
GO

-- 5. CHECK: every row should say 1
-- Each check is worked out on its own: Fabric cannot mix catalog views or metadata functions with a
-- warehouse table in one query (Msg 15816), so the result is built from variables only.
DECLARE @ok1 INT = CASE WHEN EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.14') THEN 1 ELSE 0 END;
DECLARE @ok2 INT = CASE WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_consent') AND name = 'is_deleted') THEN 1 ELSE 0 END;
DECLARE @ok3 INT = CASE WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.stage_load_log') AND name = 'rows_changed') THEN 1 ELSE 0 END;
DECLARE @ok4 INT = CASE WHEN OBJECT_ID('dbo.v_dim_client_current') IS NOT NULL THEN 1 ELSE 0 END;
SELECT check_name, ok FROM (VALUES
    ('v8.14 recorded', @ok1),
    ('fact_consent.is_deleted', @ok2),
    ('stage_load_log.rows_changed', @ok3),
    ('v_dim_client_current', @ok4)
) v (check_name, ok);
GO
