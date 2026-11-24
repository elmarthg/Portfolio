-- migrations/0004_assessments.sql  (warehouse schema v8.16 -> v8.17)
--
-- Salesforce assessments. CIS assesses people first and enrolls them only once they qualify, so an
-- assessed-only contact is a client with an assessment, not an enrollment with a missing date
-- (docs/design_decisions.md, decision 13). nb_silver_sf_cis_conform v1.4 writes lh_silver.dbo.conf_sf_cis_assessment and
-- 03a v2.1 loads it into dbo.fact_assessment, which the baseline DDL already has.
-- Run once, after 0003 and before installing 03a v2.1. Safe to rerun. Never drops a table or a column.
--
-- What it adds:
--   1. fact_assessment: is_deleted and deleted_detected_datetime (persistent loads, as on every other
--      fact) and assessment_source (Salesforce "Pre-Assessment Source").
--   2. dbo.v_fact_assessment_active: assessments not flagged deleted.
--   3. dbo.v_data_quality_unresolved: leaves out deleted assessments, and no longer counts an assessment
--      of someone who was not enrolled as a missing enrollment.
--   4. dbo.v_dq_corrections: the data-entry correction list for site and QA staff (docs/design_decisions.md, decision 14).
--      04_security v1.10 adds the dq_corrector role that can read it.

-- 0. LOG RUN START
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'ABANDONED'
WHERE script_name = 'migration_0004' AND run_status = 'RUNNING';

INSERT INTO dbo.warehouse_run_log (script_name, script_version, run_start, run_status, run_by, change_summary)
VALUES ('migration_0004', 'v8.17', SYSDATETIME(), 'RUNNING', USER_NAME(),
        'Salesforce assessments; data-entry correction list');
GO

IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.16')
    THROW 50011, 'The warehouse schema is older than v8.16. Run migrations/0003_matching.sql first.', 1;
GO

-- 1. COLUMNS
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_assessment') AND name = 'is_deleted')
    ALTER TABLE dbo.fact_assessment ADD is_deleted BIT;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_assessment') AND name = 'deleted_detected_datetime')
    ALTER TABLE dbo.fact_assessment ADD deleted_detected_datetime DATETIME2(6);
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_assessment') AND name = 'assessment_source')
    ALTER TABLE dbo.fact_assessment ADD assessment_source VARCHAR(255);
GO

-- 2. ACTIVE VIEW
DROP VIEW IF EXISTS dbo.v_fact_assessment_active;
GO

CREATE VIEW dbo.v_fact_assessment_active AS
SELECT * FROM dbo.fact_assessment WHERE ISNULL(is_deleted, 0) = 0;
GO

-- 3. DATA QUALITY VIEW (same as migration 0001 except the two fact_assessment rows)
DROP VIEW IF EXISTS dbo.v_data_quality_unresolved;
GO

CREATE VIEW dbo.v_data_quality_unresolved AS
SELECT 'fact_enrollment' AS fact_table, 'program_id' AS column_name,
       COUNT(*) AS unresolved_rows FROM dbo.fact_enrollment WHERE program_id IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT 'fact_enrollment', 'client_version_key',
       COUNT(*) FROM dbo.fact_enrollment WHERE client_version_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT 'fact_enrollment', 'household_key',
       COUNT(*) FROM dbo.fact_enrollment WHERE household_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT 'fact_enrollment', 'grant_key',
       COUNT(*) FROM dbo.fact_enrollment WHERE grant_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT 'fact_service', 'enrollment_key',
       COUNT(*) FROM dbo.fact_service WHERE enrollment_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT 'fact_service', 'program_id',
       COUNT(*) FROM dbo.fact_service WHERE program_id IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT 'fact_service', 'service_item_key',
       COUNT(*) FROM dbo.fact_service WHERE service_item_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT 'fact_case_note', 'enrollment_key',
       COUNT(*) FROM dbo.fact_case_note WHERE enrollment_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT 'fact_case_note', 'program_id',
       COUNT(*) FROM dbo.fact_case_note WHERE program_id IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT 'fact_assessment', 'enrollment_key',
       COUNT(*) FROM dbo.fact_assessment WHERE enrollment_key IS NULL AND ISNULL(is_deleted, 0) = 0
       -- a Salesforce assessment of someone who was not enrolled has no enrollment, by design
       AND NOT (source_system = 'sf_cis' AND assessment_status = 'Not enrolled')
UNION ALL SELECT 'fact_assessment', 'program_id',
       COUNT(*) FROM dbo.fact_assessment WHERE program_id IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT 'fact_document', 'enrollment_key',
       COUNT(*) FROM dbo.fact_document WHERE enrollment_key IS NULL
UNION ALL SELECT 'fact_document', 'program_id',
       COUNT(*) FROM dbo.fact_document WHERE program_id IS NULL
UNION ALL SELECT 'fact_bed_night', 'bed_key',
       COUNT(*) FROM dbo.fact_bed_night WHERE bed_key IS NULL
UNION ALL SELECT 'fact_bed_night', 'enrollment_key',
       COUNT(*) FROM dbo.fact_bed_night WHERE enrollment_key IS NULL
UNION ALL SELECT 'fact_bed_night', 'program_id',
       COUNT(*) FROM dbo.fact_bed_night WHERE program_id IS NULL
UNION ALL SELECT 'fact_bed_availability', 'bed_key',
       COUNT(*) FROM dbo.fact_bed_availability WHERE bed_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT 'fact_bed_availability', 'program_id',
       COUNT(*) FROM dbo.fact_bed_availability WHERE program_id IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT 'fact_financial_assistance', 'enrollment_key',
       COUNT(*) FROM dbo.fact_financial_assistance WHERE enrollment_key IS NULL
UNION ALL SELECT 'fact_financial_assistance', 'program_id',
       COUNT(*) FROM dbo.fact_financial_assistance WHERE program_id IS NULL
UNION ALL SELECT 'fact_financial_assistance', 'grant_key',
       COUNT(*) FROM dbo.fact_financial_assistance WHERE grant_key IS NULL
UNION ALL SELECT 'fact_financial_assistance', 'service_key',
       COUNT(*) FROM dbo.fact_financial_assistance WHERE service_key IS NULL
UNION ALL SELECT 'fact_ce_event', 'enrollment_key',
       COUNT(*) FROM dbo.fact_ce_event WHERE enrollment_key IS NULL
UNION ALL SELECT 'fact_ce_event', 'program_id',
       COUNT(*) FROM dbo.fact_ce_event WHERE program_id IS NULL
UNION ALL SELECT 'fact_consent', 'enrollment_key',
       COUNT(*) FROM dbo.fact_consent WHERE enrollment_key IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT 'fact_consent', 'program_id',
       COUNT(*) FROM dbo.fact_consent WHERE program_id IS NULL AND ISNULL(is_deleted, 0) = 0
UNION ALL SELECT 'fact_enrollment_stage', 'enrollment_key',
       COUNT(*) FROM dbo.fact_enrollment_stage WHERE enrollment_key IS NULL
UNION ALL SELECT 'fact_current_living_situation', 'enrollment_key',
       COUNT(*) FROM dbo.fact_current_living_situation WHERE enrollment_key IS NULL
UNION ALL SELECT 'bridge_enrollment_grant', 'grant_key',
       COUNT(*) FROM dbo.bridge_enrollment_grant WHERE grant_key IS NULL
UNION ALL SELECT 'kpi_target', 'grant not found',
       COUNT(*) FROM dbo.v_kpi_target WHERE source_grant_id IS NOT NULL AND grant_key IS NULL;;
GO

-- 4. DATA-ENTRY CORRECTION LIST (docs/design_decisions.md, decision 14)
--    One row per source record the latest load had to hold back, in plain language, for the staff who
--    can fix it in the source system. It lists only what the most recent successful load of each source
--    rejected, so an item disappears by itself once it is fixed and the next weekly run loads it.
--    The client profile itself is always loaded; only the incomplete record (an enrollment with no
--    start date, a service whose client is missing) is held back. IDs only: no names or dates of birth.
GO

-- Migration 0006 (v8.19) redefines this view; a rerun of 0004 after it leaves it alone.
IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.19')
BEGIN
    DROP VIEW IF EXISTS dbo.v_dq_corrections;
    EXEC('CREATE VIEW dbo.v_dq_corrections AS
WITH runs AS (
    SELECT source_system, run_id, MAX(warehouse_load_datetime) AS loaded
    FROM dbo.stage_load_log
    WHERE load_status = ''SUCCESS'' AND entity_name = ''dim_client''
    GROUP BY source_system, run_id
),
last_run AS (
    SELECT source_system, run_id, loaded
    FROM (SELECT r.*, ROW_NUMBER() OVER (PARTITION BY source_system ORDER BY loaded DESC, run_id DESC) AS rn FROM runs r) x
    WHERE rn = 1
),
open_items AS (
    SELECT r.source_system, r.entity_name, r.source_record_id, r.reject_reason,
           r.referenced_client_id, r.referenced_program, l.loaded AS last_seen
    FROM dbo.stage_load_reject r
    JOIN last_run l ON l.source_system = r.source_system AND l.run_id = r.run_id
    WHERE r.entity_name IN (''fact_enrollment'', ''fact_assessment'', ''fact_service'', ''fact_case_note'', ''fact_consent'')
),
first_seen AS (
    SELECT source_system, entity_name, source_record_id, reject_reason, MIN(detected_datetime) AS first_detected
    FROM dbo.stage_load_reject
    GROUP BY source_system, entity_name, source_record_id, reject_reason
)
SELECT o.source_system,
       CASE WHEN o.source_system = ''sf_cis'' THEN ''Salesforce CIS''
            ELSE CONCAT(''Clarity '', ISNULL(s.county, o.source_system)) END AS system_name,
       CASE o.entity_name WHEN ''fact_enrollment'' THEN ''Enrollment'' WHEN ''fact_service'' THEN ''Service''
            WHEN ''fact_case_note'' THEN ''Case note'' WHEN ''fact_consent'' THEN ''Release of information''
            WHEN ''fact_assessment'' THEN ''Assessment'' END AS record_type,
       o.source_record_id AS record_id,
       o.referenced_client_id AS client_id,
       o.referenced_program AS program_ref,
       p.program_name,
       CASE WHEN o.reject_reason = ''missing_enrollment_start_date'' AND o.source_system = ''sf_cis''
                THEN ''Contact Status says the client was enrolled, but Date Enrolled is blank''
            WHEN o.reject_reason = ''missing_enrollment_start_date''
                THEN ''The enrollment has no Project Start Date''
            WHEN o.reject_reason = ''orphan_client'' AND o.source_system = ''sf_cis''
                THEN ''The contact for this record is not in the Salesforce export''
            WHEN o.reject_reason = ''orphan_client''
                THEN ''This record belongs to a client who is not in the Clarity client export''
            ELSE o.reject_reason END AS problem,
       CASE WHEN o.reject_reason = ''missing_enrollment_start_date'' AND o.source_system = ''sf_cis''
                THEN ''Enter Date Enrolled on the contact, or change Contact Status if they were never enrolled''
            WHEN o.reject_reason = ''missing_enrollment_start_date''
                THEN ''Enter the Project Start Date on the enrollment in Clarity''
            WHEN o.reject_reason = ''orphan_client'' AND o.source_system = ''sf_cis''
                THEN ''Check that the contact exists and is not deleted in Salesforce''
            WHEN o.reject_reason = ''orphan_client''
                THEN ''Check the client in Clarity: the profile may have been merged or deleted. Ask the HMIS admin if unsure''
            ELSE ''Send this item to the data team'' END AS how_to_fix,
       o.reject_reason,
       f.first_detected,
       o.last_seen,
       DATEDIFF(day, f.first_detected, o.last_seen) AS days_open
FROM open_items o
LEFT JOIN first_seen f
       ON f.source_system = o.source_system AND f.entity_name = o.entity_name
      AND f.source_record_id = o.source_record_id AND f.reject_reason = o.reject_reason
LEFT JOIN dbo.dim_source s ON s.source_system = o.source_system
LEFT JOIN (SELECT source_system, COALESCE(hmis_project_id, program_code) AS program_ref, MAX(program_name) AS program_name
           FROM dbo.dim_program GROUP BY source_system, COALESCE(hmis_project_id, program_code)) p
       ON p.source_system = o.source_system AND p.program_ref = o.referenced_program');
    -- dropping the view dropped its grants: give back what 04_security_v1.10 grants on it
    IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'dq_corrector' AND type = 'R')
        GRANT SELECT ON dbo.v_dq_corrections TO dq_corrector;
    IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'pipeline_monitor' AND type = 'R')
        GRANT SELECT ON dbo.v_dq_corrections TO pipeline_monitor;
END;
GO

-- 5. SCHEMA VERSION + LOG RUN END
IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.17')
    INSERT INTO dbo.warehouse_schema_version (version, applied_date, applied_by, summary, details)
    VALUES ('v8.17', SYSDATETIME(), USER_NAME(),
            'Salesforce assessments: fact_assessment is_deleted, deleted_detected_datetime, assessment_source; v_fact_assessment_active; v_dq_corrections',
            'migrations/0004_assessments.sql');

UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'SUCCESS'
WHERE script_name = 'migration_0004' AND run_status = 'RUNNING';
GO

-- 6. CHECK: every row should say 1. Then install 03a_proc_load_sf_cis.sql v2.1 and rerun 04_security.
-- Each check is worked out on its own: Fabric cannot mix catalog views or metadata functions with a
-- warehouse table in one query (Msg 15816), so the result is built from variables only.
DECLARE @ok1 INT = CASE WHEN EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.17') THEN 1 ELSE 0 END;
DECLARE @ok2 INT = CASE WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_assessment') AND name = 'is_deleted') THEN 1 ELSE 0 END;
DECLARE @ok3 INT = CASE WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_assessment') AND name = 'assessment_source') THEN 1 ELSE 0 END;
DECLARE @ok4 INT = CASE WHEN OBJECT_ID('dbo.v_fact_assessment_active') IS NOT NULL THEN 1 ELSE 0 END;
DECLARE @ok5 INT = CASE WHEN OBJECT_ID('dbo.v_data_quality_unresolved') IS NOT NULL THEN 1 ELSE 0 END;
DECLARE @ok6 INT = CASE WHEN OBJECT_ID('dbo.v_dq_corrections') IS NOT NULL THEN 1 ELSE 0 END;
SELECT check_name, ok FROM (VALUES
    ('v8.17 recorded', @ok1),
    ('fact_assessment.is_deleted', @ok2),
    ('fact_assessment.assessment_source', @ok3),
    ('v_fact_assessment_active', @ok4),
    ('v_data_quality_unresolved', @ok5),
    ('v_dq_corrections', @ok6)
) v (check_name, ok);
GO
