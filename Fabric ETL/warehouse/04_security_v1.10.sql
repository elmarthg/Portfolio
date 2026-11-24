-- 04_security v1.10: row-level security policy (created OFF), reader, Apricot integration, match reviewer,
-- pipeline monitor and data-entry corrector roles, column and table denies.
-- v1.10 (schema v8.17): dq_corrector reads the data-entry correction list (dbo.v_dq_corrections) and the
-- tables behind it, for QA and site staff who fix records in Salesforce or Clarity. It holds record IDs and
-- program names only. pipeline_monitor reads it too. Skipped quietly if migration 0004 has not run yet.
-- v1.9 (schema v8.16): match_reviewer reads the review queue and profiles (same column denies as report users)
-- and records decisions; pipeline_monitor reads the health views and the log tables behind them;
-- apricot_integration records which Apricot profile each source profile went to (dbo.apricot_profile_map). Fabric runs a
-- procedure with the caller's permissions (no ownership chaining, no EXECUTE AS), so match_reviewer also needs
-- INSERT and UPDATE on dbo.match_decision. Keep that role to the few trained reviewers; every decision row
-- records who made it and nothing is ever deleted. Skipped quietly if migration 0003 has not run yet.
-- v1.8 (schema v8.15): both roles can read the rpt schema (rpt.person, rpt.person_client); report users are
-- denied dbo.match_decision (reviewer names and notes). Skipped quietly if migration 0002 has not run yet.
-- Fabric has no ownership chaining: a view never lets a user read columns or tables they are denied, so
-- reporting tables in rpt are stored copies, not views.
-- Run after 01 and 02. Rerun after every DDL rebuild. Add a program_id = -1 admin grant before turning the policy ON.
-- Workspace Admin, Member and Contributor roles bypass everything here; report users must be Viewers or item-shared.

UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'ABANDONED'
WHERE script_name = '04_security' AND run_status = 'RUNNING';

INSERT INTO dbo.warehouse_run_log (
    script_name, script_version, run_start, run_status, run_by, change_summary
)
VALUES (
    '04_security', 'v1.10', SYSDATETIME(), 'RUNNING', USER_NAME(),
    'Roles warehouse_reader and apricot_integration; full SSN table readable only by the Apricot integration'
);
GO

-- 1. ACCESS CONTROL TABLE
IF OBJECT_ID('dbo.security_user_program_access', 'U') IS NULL
CREATE TABLE security_user_program_access (
    access_key              BIGINT IDENTITY NOT NULL,
    user_principal_name     VARCHAR(255) NOT NULL,
    program_id              INT NOT NULL,
    access_level            VARCHAR(20),
    granted_by              VARCHAR(255),
    granted_date            DATETIME2(6),
    expiry_date             DATETIME2(6),
    is_active               BIT,
    notes                   VARCHAR(500),
    load_datetime           DATETIME2(6)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_security_user_program_access')
ALTER TABLE security_user_program_access
    ADD CONSTRAINT PK_security_user_program_access
    PRIMARY KEY NONCLUSTERED (access_key) NOT ENFORCED;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_security_user_program_access_program')
ALTER TABLE security_user_program_access
    ADD CONSTRAINT FK_security_user_program_access_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
GO

-- 2. RLS PREDICATE FUNCTION
IF EXISTS (SELECT 1 FROM sys.security_policies WHERE name = 'ClientProgramAccessPolicy')
    DROP SECURITY POLICY dbo.ClientProgramAccessPolicy;
GO

IF OBJECT_ID('dbo.fn_rls_client_program_access', 'IF') IS NOT NULL
    DROP FUNCTION dbo.fn_rls_client_program_access;
GO

CREATE FUNCTION dbo.fn_rls_client_program_access(@program_id INT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS fn_access_result
    WHERE
        EXISTS (
            SELECT 1 FROM dbo.security_user_program_access a
            WHERE a.program_id = -1
              AND a.user_principal_name = USER_NAME()
              AND ISNULL(a.is_active, 0) = 1
              AND (a.expiry_date IS NULL OR a.expiry_date > SYSDATETIME())
        )
        OR @program_id IS NULL
        OR @program_id = -1
        OR EXISTS (
            SELECT 1 FROM dbo.security_user_program_access a
            WHERE a.program_id = @program_id
              AND a.user_principal_name = USER_NAME()
              AND ISNULL(a.is_active, 0) = 1
              AND (a.expiry_date IS NULL OR a.expiry_date > SYSDATETIME())
        );
GO

-- 3. SEED ADMIN GRANT FOR CURRENT USER
IF NOT EXISTS (
    SELECT 1 FROM security_user_program_access
    WHERE program_id = -1 AND user_principal_name = USER_NAME()
)
INSERT INTO security_user_program_access (
    user_principal_name, program_id, access_level, granted_by,
    granted_date, is_active, notes, load_datetime
)
VALUES (
    USER_NAME(), -1, 'admin', USER_NAME(),
    SYSDATETIME(), 1, 'DBA blanket bypass; program_id = -1 means all programs', SYSDATETIME()
);
GO

-- 4. SECURITY POLICY (created but DISABLED by default)
CREATE SECURITY POLICY dbo.ClientProgramAccessPolicy
    ADD FILTER PREDICATE dbo.fn_rls_client_program_access(program_id) ON dbo.fact_enrollment,
    ADD FILTER PREDICATE dbo.fn_rls_client_program_access(program_id) ON dbo.fact_service,
    ADD FILTER PREDICATE dbo.fn_rls_client_program_access(program_id) ON dbo.fact_assessment,
    ADD FILTER PREDICATE dbo.fn_rls_client_program_access(program_id) ON dbo.fact_document,
    ADD FILTER PREDICATE dbo.fn_rls_client_program_access(program_id) ON dbo.fact_bed_night,
    ADD FILTER PREDICATE dbo.fn_rls_client_program_access(program_id) ON dbo.fact_bed_availability,
    ADD FILTER PREDICATE dbo.fn_rls_client_program_access(program_id) ON dbo.fact_financial_assistance,
    ADD FILTER PREDICATE dbo.fn_rls_client_program_access(program_id) ON dbo.fact_case_note,
    ADD FILTER PREDICATE dbo.fn_rls_client_program_access(program_id) ON dbo.fact_ce_event,
    ADD FILTER PREDICATE dbo.fn_rls_client_program_access(program_id) ON dbo.fact_consent,
    ADD FILTER PREDICATE dbo.fn_rls_client_program_access(program_id) ON dbo.fact_enrollment_stage,
    ADD FILTER PREDICATE dbo.fn_rls_client_program_access(program_id) ON dbo.fact_current_living_situation,
    ADD FILTER PREDICATE dbo.fn_rls_client_program_access(program_id) ON dbo.dim_bed
    WITH (STATE = OFF);
GO

-- 5. ROLES AND DENIES
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'warehouse_reader' AND type = 'R')
    CREATE ROLE warehouse_reader;
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'apricot_integration' AND type = 'R')
    CREATE ROLE apricot_integration;
GO

GRANT SELECT ON SCHEMA::dbo TO warehouse_reader;
DENY SELECT ON dbo.dim_client_identifier          TO warehouse_reader;
DENY SELECT ON dbo.dim_client(ssn_hash)           TO warehouse_reader;
DENY SELECT ON dbo.dim_client(ssn_last4)          TO warehouse_reader;
DENY SELECT ON dbo.dim_client(gender_identity)    TO warehouse_reader;
DENY SELECT ON dbo.dim_client(sexual_orientation) TO warehouse_reader;
DENY SELECT ON dbo.stage_load_reject              TO warehouse_reader;
DENY SELECT ON dbo.security_user_program_access   TO warehouse_reader;
DENY SELECT ON dbo.fact_assessment_response       TO warehouse_reader;
DENY SELECT ON dbo.fact_consent(notes)            TO warehouse_reader;
DENY SELECT ON dbo.fact_enrollment_stage(dv_survivor, dv_when_occurred, dv_currently_fleeing, hiv_aids) TO warehouse_reader;
DENY SELECT ON dbo.fact_current_living_situation(location_details) TO warehouse_reader;
GO

GRANT SELECT ON SCHEMA::dbo TO apricot_integration;
GRANT UNMASK ON dbo.dim_client_identifier(ssn_raw) TO apricot_integration;
DENY SELECT ON dbo.security_user_program_access TO apricot_integration;
GO

-- 5b. PERSON LAYER (schema v8.15)
IF SCHEMA_ID('rpt') IS NOT NULL
BEGIN
    GRANT SELECT ON SCHEMA::rpt TO warehouse_reader;
    GRANT SELECT ON SCHEMA::rpt TO apricot_integration;
END;

IF OBJECT_ID('dbo.match_decision', 'U') IS NOT NULL
    DENY SELECT ON dbo.match_decision TO warehouse_reader;
GO

-- 5c. MATCHING AND HEALTH (schema v8.16)
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'match_reviewer' AND type = 'R')
    CREATE ROLE match_reviewer;
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'pipeline_monitor' AND type = 'R')
    CREATE ROLE pipeline_monitor;
GO

IF OBJECT_ID('dbo.match_review_queue', 'U') IS NOT NULL
BEGIN
    GRANT SELECT ON SCHEMA::dbo TO match_reviewer;
    GRANT SELECT ON SCHEMA::rpt TO match_reviewer;
    DENY SELECT ON dbo.dim_client_identifier          TO match_reviewer;
    DENY SELECT ON dbo.dim_client(ssn_hash)           TO match_reviewer;
    DENY SELECT ON dbo.dim_client(ssn_last4)          TO match_reviewer;
    DENY SELECT ON dbo.dim_client(gender_identity)    TO match_reviewer;
    DENY SELECT ON dbo.dim_client(sexual_orientation) TO match_reviewer;
    DENY SELECT ON dbo.security_user_program_access   TO match_reviewer;
    DENY SELECT ON dbo.fact_assessment_response       TO match_reviewer;
    DENY SELECT ON dbo.fact_consent(notes)            TO match_reviewer;
    DENY SELECT ON dbo.fact_enrollment_stage(dv_survivor, dv_when_occurred, dv_currently_fleeing, hiv_aids) TO match_reviewer;
    DENY SELECT ON dbo.fact_current_living_situation(location_details) TO match_reviewer;
    GRANT INSERT, UPDATE ON dbo.match_decision TO match_reviewer;
    GRANT EXECUTE ON dbo.usp_record_match_decision TO match_reviewer;
    GRANT INSERT, UPDATE ON dbo.apricot_profile_map TO apricot_integration;

    GRANT SELECT ON dbo.warehouse_run_log     TO pipeline_monitor;
    GRANT SELECT ON dbo.stage_load_log        TO pipeline_monitor;
    GRANT SELECT ON dbo.stage_load_reject     TO pipeline_monitor;
    GRANT SELECT ON dbo.match_review_queue    TO pipeline_monitor;
    GRANT SELECT ON dbo.match_decision        TO pipeline_monitor;
    GRANT SELECT ON dbo.person_client_link    TO pipeline_monitor;
    GRANT SELECT ON dbo.apricot_profile_map   TO pipeline_monitor;
    GRANT SELECT ON dbo.bridge_client_source  TO pipeline_monitor;
    GRANT SELECT ON dbo.fact_enrollment       TO pipeline_monitor;
    GRANT SELECT ON dbo.fact_service          TO pipeline_monitor;
    GRANT SELECT ON dbo.fact_case_note        TO pipeline_monitor;
    GRANT SELECT ON rpt.person                TO pipeline_monitor;
    GRANT SELECT ON dbo.v_health_run_steps    TO pipeline_monitor;
    GRANT SELECT ON dbo.v_health_runs         TO pipeline_monitor;
    GRANT SELECT ON dbo.v_health_table_changes TO pipeline_monitor;
    GRANT SELECT ON dbo.v_health_issues       TO pipeline_monitor;
    GRANT SELECT ON dbo.v_health_people       TO pipeline_monitor;
    GRANT SELECT ON dbo.v_apricot_worklist    TO pipeline_monitor;
END;
GO

-- 5d. DATA-ENTRY CORRECTION LIST (schema v8.17)
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'dq_corrector' AND type = 'R')
    CREATE ROLE dq_corrector;
GO

-- Fabric checks the tables behind a view, so the role needs each one (all IDs and names of programs only).
IF OBJECT_ID('dbo.v_dq_corrections', 'V') IS NOT NULL
BEGIN
    GRANT SELECT ON dbo.v_dq_corrections  TO dq_corrector;
    GRANT SELECT ON dbo.stage_load_reject TO dq_corrector;
    GRANT SELECT ON dbo.stage_load_log    TO dq_corrector;
    GRANT SELECT ON dbo.dim_source        TO dq_corrector;
    GRANT SELECT ON dbo.dim_program       TO dq_corrector;
    GRANT SELECT ON dbo.v_dq_corrections  TO pipeline_monitor;
    GRANT SELECT ON dbo.dim_source        TO pipeline_monitor;
    GRANT SELECT ON dbo.dim_program       TO pipeline_monitor;
END;
GO

-- Members (report users get warehouse_reader; the account or service principal that runs the Apricot import gets apricot_integration):
-- ALTER ROLE warehouse_reader ADD MEMBER [someone@example.org];
-- ALTER ROLE apricot_integration ADD MEMBER [apricot-import-service-principal];
-- ALTER ROLE match_reviewer ADD MEMBER [reviewer@example.org];
-- ALTER ROLE pipeline_monitor ADD MEMBER [data-team@example.org];
-- ALTER ROLE dq_corrector ADD MEMBER [qa-staff@example.org];
-- Reviewers and monitors must not be workspace Admin, Member or Contributor on the warehouse's workspace
-- (those roles bypass every grant and deny here). Give them the warehouse through item sharing, and run the
-- review notebook from a separate workspace where they are Contributors. When the RLS policy is turned ON,
-- reviewers also need the all-programs grant (program_id = -1), since matching crosses programs.

-- 6. ONBOARDING A USER — pattern

-- INSERT INTO security_user_program_access (
--     user_principal_name, program_id, access_level,
--     granted_by, granted_date, is_active, notes, load_datetime
-- )
-- SELECT
--     'someone@example.org', p.program_id, 'read',
--     USER_NAME(), SYSDATETIME(), 1, 'Program staff, read-only', SYSDATETIME()
-- FROM dim_program p
-- WHERE p.program_code IN ('REPLACE_ME');

-- Then enable the policy:
-- ALTER SECURITY POLICY dbo.ClientProgramAccessPolicy WITH (STATE = ON);

-- 7. VERIFICATION
SELECT name, is_enabled FROM sys.security_policies WHERE name = 'ClientProgramAccessPolicy';
SELECT OBJECT_NAME(object_id) AS table_name, predicate_definition FROM sys.security_predicates;
SELECT COUNT(*) AS admin_grants FROM security_user_program_access WHERE program_id = -1;
SELECT name, type_desc FROM sys.database_principals WHERE name IN ('warehouse_reader', 'apricot_integration');
GO

DECLARE @my_admin_grant INT;

SELECT @my_admin_grant = COUNT(*)
FROM security_user_program_access
WHERE program_id = -1
  AND user_principal_name = USER_NAME()
  AND ISNULL(is_active, 0) = 1
  AND (expiry_date IS NULL OR expiry_date > SYSDATETIME());

IF @my_admin_grant = 1
BEGIN
    PRINT 'OK: admin grant present for ' + USER_NAME() + '.';
    PRINT 'The policy can be enabled with: ALTER SECURITY POLICY dbo.ClientProgramAccessPolicy WITH (STATE = ON);';
END
ELSE
BEGIN
    RAISERROR('WARNING: no active admin grant for %s. Do NOT enable the policy — you would see no rows.', 16, 1, USER_NAME());
END
GO

SELECT COUNT(*) AS my_active_admin_grant
FROM security_user_program_access
WHERE program_id = -1
  AND user_principal_name = USER_NAME()
  AND ISNULL(is_active, 0) = 1
  AND (expiry_date IS NULL OR expiry_date > SYSDATETIME());
GO

-- 8. LOG RUN END
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'SUCCESS'
WHERE script_name = '04_security' AND run_status = 'RUNNING';
GO
