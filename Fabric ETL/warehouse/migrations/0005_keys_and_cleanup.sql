-- migrations/0005_keys_and_cleanup.sql  (warehouse schema v8.17 -> v8.18)
--
-- The fixes from the 2026-11-23 warehouse audit. Run once, after 0004, then install 03a v2.2, 03b v3.2 and
-- 07_proc_purge_audit_history BEFORE the next load: 03a v2.1 and 03b v3.1 stop with an error on v8.18
-- (they still write dim_client.household_key and dim_bed.is_current). Safe to rerun.
--
-- Nothing is changed until every check in section 1 passes. If one fails, the message lists what to fix.
--
-- What it does:
--   1. Checks that every key and link about to be declared already holds in the data.
--   2. fact_bed_night: is_deleted and deleted_detected_datetime, so nights that leave the source are flagged
--      like every other fact instead of deleted (03b v3.2). dbo.v_fact_bed_night_active.
--   3. dim_bed: drops valid_from, valid_to and is_current. The loader never versioned beds (it updates them in
--      place), so those columns were always empty or always 1 and suggested history that does not exist.
--   4. dim_client.household_key is renamed current_household_key: it is the household of the client's latest
--      enrollment, overwritten in place, not part of the client's version history. bridge_household_client
--      keeps the real history. v_dim_client_current is recreated with the new name.
--   5. Widths: stage_load_log.run_id and stage_load_reject.run_id go from 50 to 60 characters, matching the
--      person-link and matching procedures; kpi_target.program_code goes from 100 to 50, matching dim_program.
--      Fabric cannot change a column's type in place (ALTER COLUMN is preview only), so each column is copied
--      to a new one, the old one dropped and the new one renamed, inside one transaction.
--   6. dim_program_history: one row per version of each program's tracked attributes (type 2 history beside
--      the type 1 dim_program), so a report can show a program as it was on any date. Seeded from dim_program;
--      03a v2.2 and 03b v3.2 keep it up to date.
--   7. dbo.v_dim_enrollment: enrollment as a dimension for Power BI, so facts relate to a dimension, never
--      fact to fact.
--   8. Keys. Fabric checks none of these (NOT ENFORCED); the loaders' integrity checks do. They document the
--      model in the warehouse itself, help the query optimizer and let tools read the relationships.
--        7 primary keys on the tables that had none,
--        18 unique keys: each fact's (source_system, source_record_id), program_code, client and link versions,
--        39 foreign keys: every source_system to dim_source, the person and matching links, program history.
--      Links on the durable client_key (it repeats across a client's versions) cannot be foreign keys; facts
--      already have FK_..._client_version on client_version_key.
--   Not changed: notes columns stay VARCHAR(MAX) where they hold source text (consent, document, run log) and
--   VARCHAR(500) where we write them (config, decisions); NOT NULL cannot be added to existing Fabric columns,
--   so 03a v2.2 and 03b v3.2 check the required columns before every commit instead.

-- 0. LOG RUN START
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'ABANDONED'
WHERE script_name = 'migration_0005' AND run_status = 'RUNNING';

INSERT INTO dbo.warehouse_run_log (script_name, script_version, run_start, run_status, run_by, change_summary)
VALUES ('migration_0005', 'v8.18', SYSDATETIME(), 'RUNNING', USER_NAME(),
        'Declared keys, bed-night soft delete, program history, enrollment dimension, column clean-up');
GO

IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.17')
    THROW 50011, 'The warehouse schema is older than v8.17. Run migrations/0004_assessments.sql first.', 1;
GO

-- 1. CHECKS BEFORE ANY CHANGE
-- 1a. Every key about to be declared must already hold: no duplicates, no empty primary key columns.
DECLARE @problems VARCHAR(8000);
SELECT @problems = STRING_AGG(CAST(CONCAT(check_name, ' (', problem_rows, ')') AS VARCHAR(MAX)), '; ')
FROM (
    SELECT 'PK_rpt_person: duplicate person_key' AS check_name, (SELECT COUNT(*) FROM (SELECT person_key FROM rpt.person WHERE person_key IS NOT NULL GROUP BY person_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_rpt_person: empty person_key' AS check_name, (SELECT COUNT(*) FROM rpt.person WHERE person_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'PK_rpt_person_client: duplicate client_key' AS check_name, (SELECT COUNT(*) FROM (SELECT client_key FROM rpt.person_client WHERE client_key IS NOT NULL GROUP BY client_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_rpt_person_client: empty client_key' AS check_name, (SELECT COUNT(*) FROM rpt.person_client WHERE client_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'PK_match_review_queue: duplicate source_system_a, source_id_a, source_system_b, source_id_b' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system_a, source_id_a, source_system_b, source_id_b FROM dbo.match_review_queue WHERE source_system_a IS NOT NULL AND source_id_a IS NOT NULL AND source_system_b IS NOT NULL AND source_id_b IS NOT NULL GROUP BY source_system_a, source_id_a, source_system_b, source_id_b HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_match_review_queue: empty source_system_a, source_id_a, source_system_b, source_id_b' AS check_name, (SELECT COUNT(*) FROM dbo.match_review_queue WHERE source_system_a IS NULL OR source_id_a IS NULL OR source_system_b IS NULL OR source_id_b IS NULL) AS problem_rows
    UNION ALL
    SELECT 'PK_apricot_profile_map: duplicate source_system, source_id, exported_datetime' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_id, exported_datetime FROM dbo.apricot_profile_map WHERE source_system IS NOT NULL AND source_id IS NOT NULL AND exported_datetime IS NOT NULL GROUP BY source_system, source_id, exported_datetime HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_apricot_profile_map: empty source_system, source_id, exported_datetime' AS check_name, (SELECT COUNT(*) FROM dbo.apricot_profile_map WHERE source_system IS NULL OR source_id IS NULL OR exported_datetime IS NULL) AS problem_rows
    UNION ALL
    SELECT 'PK_warehouse_run_log: duplicate run_key' AS check_name, (SELECT COUNT(*) FROM (SELECT run_key FROM dbo.warehouse_run_log WHERE run_key IS NOT NULL GROUP BY run_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_warehouse_run_log: empty run_key' AS check_name, (SELECT COUNT(*) FROM dbo.warehouse_run_log WHERE run_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'PK_warehouse_schema_version: duplicate version_key' AS check_name, (SELECT COUNT(*) FROM (SELECT version_key FROM dbo.warehouse_schema_version WHERE version_key IS NOT NULL GROUP BY version_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_warehouse_schema_version: empty version_key' AS check_name, (SELECT COUNT(*) FROM dbo.warehouse_schema_version WHERE version_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_enrollment_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_enrollment WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_service_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_service WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_case_note_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_case_note WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_assessment_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_assessment WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_document_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_document WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_bed_night_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_bed_night WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_bed_availability_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_bed_availability WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_financial_assistance_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_financial_assistance WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_ce_event_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_ce_event WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_consent_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_consent WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_enrollment_stage_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_enrollment_stage WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_current_living_situation_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_current_living_situation WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_warehouse_schema_version_version: duplicate version' AS check_name, (SELECT COUNT(*) FROM (SELECT version FROM dbo.warehouse_schema_version WHERE version IS NOT NULL GROUP BY version HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_dim_program_code: duplicate program_code' AS check_name, (SELECT COUNT(*) FROM (SELECT program_code FROM dbo.dim_program WHERE program_code IS NOT NULL GROUP BY program_code HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_dim_client_version_start: duplicate client_key, valid_from' AS check_name, (SELECT COUNT(*) FROM (SELECT client_key, valid_from FROM dbo.dim_client WHERE client_key IS NOT NULL AND valid_from IS NOT NULL GROUP BY client_key, valid_from HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_person_client_link_start: duplicate client_key, valid_from' AS check_name, (SELECT COUNT(*) FROM (SELECT client_key, valid_from FROM dbo.person_client_link WHERE client_key IS NOT NULL AND valid_from IS NOT NULL GROUP BY client_key, valid_from HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_dim_date_full_date: duplicate full_date' AS check_name, (SELECT COUNT(*) FROM (SELECT full_date FROM dbo.dim_date WHERE full_date IS NOT NULL GROUP BY full_date HAVING COUNT(*) > 1) d) AS problem_rows
) x
WHERE problem_rows > 0;
IF @problems IS NOT NULL
    THROW 50051, @problems, 1;
GO

-- 1b. Every link about to be declared must already hold: no value without a matching row.
DECLARE @problems VARCHAR(8000);
SELECT @problems = STRING_AGG(CAST(CONCAT(check_name, ' (', problem_rows, ')') AS VARCHAR(MAX)), '; ')
FROM (
    SELECT 'FK_dim_site_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_site c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_grant_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_grant c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_funding_source_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_funding_source c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_household_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_household c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_organization_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_organization c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_client_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_client c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_client_identifier_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_client_identifier c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_program_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_program c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_service_item_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_service_item c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_bed_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_bed c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_staff_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_staff c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_bridge_household_client_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_household_client c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_bridge_client_source_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_client_source c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_service_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_case_note_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_case_note c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_assessment_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_assessment_response_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_assessment_response c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_document_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_document c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_bed_night_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_night c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_bed_availability_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_availability c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_financial_assistance_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_financial_assistance c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_ce_event_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_ce_event c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_consent_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_consent c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_stage_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment_stage c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_current_living_situation_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_current_living_situation c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_bridge_enrollment_grant_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_enrollment_grant c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_bridge_program_site_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_program_site c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_rpt_person_client_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM rpt.person_client c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_apricot_profile_map_source: source_system not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.apricot_profile_map c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_match_decision_source_a: source_system_a not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.match_decision c WHERE c.source_system_a IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system_a)) AS problem_rows
    UNION ALL
    SELECT 'FK_match_decision_source_b: source_system_b not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.match_decision c WHERE c.source_system_b IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system_b)) AS problem_rows
    UNION ALL
    SELECT 'FK_match_review_queue_source_a: source_system_a not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.match_review_queue c WHERE c.source_system_a IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system_a)) AS problem_rows
    UNION ALL
    SELECT 'FK_match_review_queue_source_b: source_system_b not in dbo.dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.match_review_queue c WHERE c.source_system_b IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system_b)) AS problem_rows
    UNION ALL
    SELECT 'FK_person_client_link_decision: decision_key not in dbo.match_decision' AS check_name, (SELECT COUNT(*) FROM dbo.person_client_link c WHERE c.decision_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.match_decision p WHERE p.decision_key = c.decision_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_rpt_person_client_person: person_key not in rpt.person' AS check_name, (SELECT COUNT(*) FROM rpt.person_client c WHERE c.person_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM rpt.person p WHERE p.person_key = c.person_key)) AS problem_rows
    UNION ALL
    SELECT 'kpi_target.program_code longer than 50 characters' AS check_name, (SELECT COUNT(*) FROM dbo.kpi_target WHERE LEN(program_code) > 50) AS problem_rows
) x
WHERE problem_rows > 0;
IF @problems IS NOT NULL
    THROW 50052, @problems, 1;
GO

-- 1c. dim_client's household link. The column is renamed by section 4, so the check names whichever exists
--     (dynamic SQL: a batch that names a missing column does not compile).
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_client') AND name = 'household_key')
    EXEC('IF EXISTS (SELECT 1 FROM dbo.dim_client c WHERE c.household_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_household p WHERE p.household_key = c.household_key)) THROW 50052, ''FK_dim_client_current_household: dim_client.household_key not in dim_household'', 1;');
ELSE
    EXEC('IF EXISTS (SELECT 1 FROM dbo.dim_client c WHERE c.current_household_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_household p WHERE p.household_key = c.current_household_key)) THROW 50052, ''FK_dim_client_current_household: dim_client.current_household_key not in dim_household'', 1;');
GO

-- 2. BED NIGHTS: SOFT DELETE
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_bed_night') AND name = 'is_deleted')
    ALTER TABLE dbo.fact_bed_night ADD is_deleted BIT;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_bed_night') AND name = 'deleted_detected_datetime')
    ALTER TABLE dbo.fact_bed_night ADD deleted_detected_datetime DATETIME2(6);
GO

UPDATE dbo.fact_bed_night SET is_deleted = 0 WHERE is_deleted IS NULL;
GO

DROP VIEW IF EXISTS dbo.v_fact_bed_night_active;
GO

CREATE VIEW dbo.v_fact_bed_night_active AS
SELECT * FROM dbo.fact_bed_night WHERE ISNULL(is_deleted, 0) = 0;
GO

-- 3. DIM_BED: DROP THE UNUSED HISTORY COLUMNS
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_bed') AND name = 'valid_from')
    ALTER TABLE dbo.dim_bed DROP COLUMN valid_from;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_bed') AND name = 'valid_to')
    ALTER TABLE dbo.dim_bed DROP COLUMN valid_to;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_bed') AND name = 'is_current')
    ALTER TABLE dbo.dim_bed DROP COLUMN is_current;
GO

-- 4. DIM_CLIENT.HOUSEHOLD_KEY -> CURRENT_HOUSEHOLD_KEY
--    The foreign key is dropped first and declared again on the new name in section 8.
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_client') AND name = 'household_key')
BEGIN
    BEGIN TRANSACTION;
    IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_client_household')
        ALTER TABLE dbo.dim_client DROP CONSTRAINT FK_dim_client_household;
    EXEC sp_rename 'dbo.dim_client.household_key', 'current_household_key', 'COLUMN';
    COMMIT;
END;
GO

DROP VIEW IF EXISTS dbo.v_dim_client_current;
GO

CREATE VIEW dbo.v_dim_client_current AS
SELECT client_version_key, client_key, valid_from, current_household_key,
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
WHERE is_current = 1 AND ISNULL(is_deleted, 0) = 0;
GO

-- 5. COLUMN WIDTHS (copy to a new column, drop the old one, rename; one transaction per column)
--    Runs only until v8.18 is recorded, so a rerun never repeats it. The copy runs through EXEC because Fabric
--    compiles the whole batch first and would not find the new column (Msg 207).
DECLARE @done INT = CASE WHEN EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.18') THEN 1 ELSE 0 END;
IF @done = 0 AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.stage_load_log') AND name = 'run_id_v818')
BEGIN
    BEGIN TRANSACTION;
    ALTER TABLE dbo.stage_load_log ADD run_id_v818 VARCHAR(60);
    EXEC('UPDATE dbo.stage_load_log SET run_id_v818 = run_id');   -- dynamic: the batch compiles before run_id_v818 exists
    ALTER TABLE dbo.stage_load_log DROP COLUMN run_id;
    EXEC sp_rename 'dbo.stage_load_log.run_id_v818', 'run_id', 'COLUMN';
    COMMIT;
END;
IF @done = 0 AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.stage_load_reject') AND name = 'run_id_v818')
BEGIN
    BEGIN TRANSACTION;
    ALTER TABLE dbo.stage_load_reject ADD run_id_v818 VARCHAR(60);
    EXEC('UPDATE dbo.stage_load_reject SET run_id_v818 = run_id');   -- dynamic: the batch compiles before run_id_v818 exists
    ALTER TABLE dbo.stage_load_reject DROP COLUMN run_id;
    EXEC sp_rename 'dbo.stage_load_reject.run_id_v818', 'run_id', 'COLUMN';
    COMMIT;
END;
IF @done = 0 AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.kpi_target') AND name = 'program_code_v818')
BEGIN
    BEGIN TRANSACTION;
    ALTER TABLE dbo.kpi_target ADD program_code_v818 VARCHAR(50);
    EXEC('UPDATE dbo.kpi_target SET program_code_v818 = program_code');   -- dynamic: the batch compiles before program_code_v818 exists
    ALTER TABLE dbo.kpi_target DROP COLUMN program_code;
    EXEC sp_rename 'dbo.kpi_target.program_code_v818', 'program_code', 'COLUMN';
    COMMIT;
END;
GO

-- 5b. v_kpi_target selects t.* from kpi_target. A star view keeps the column list it was created with, so after
--     program_code was replaced above it is recreated to pick up the new column.
DROP VIEW IF EXISTS dbo.v_kpi_target;
GO

CREATE VIEW dbo.v_kpi_target AS
SELECT t.*, g.grant_key, g.grant_name, p.program_id, p.program_name
FROM dbo.kpi_target t
LEFT JOIN dbo.dim_grant g
  ON g.source_system = t.grant_source_system
 AND t.source_grant_id IN (g.salesforce_id, g.apricot_grant_id, g.hmis_grant_id, g.wellsky_grant_id)
LEFT JOIN dbo.dim_program p
  ON p.program_code = t.program_code;
GO

-- 6. PROGRAM HISTORY
--    One row per version of a program's tracked attributes. The first version of every program starts on
--    1900-01-01, so facts older than the warehouse still find the program as first recorded; later versions
--    start when the loader saw the change (half-open: valid_to = the next version's valid_from).
IF OBJECT_ID('dbo.dim_program_history', 'U') IS NULL
CREATE TABLE dbo.dim_program_history (
    program_history_key       BIGINT IDENTITY NOT NULL,
    program_id                INT          NOT NULL,
    program_code              VARCHAR(50),
    program_name              VARCHAR(255),
    organization_key          BIGINT,
    project_type              INT,
    housing_type              VARCHAR(100),
    target_population         VARCHAR(100),
    coc_code                  VARCHAR(20),
    city                      VARCHAR(100),
    state                     VARCHAR(10),
    county                    VARCHAR(100),
    program_status            VARCHAR(50),
    operating_start_date      DATE,
    operating_end_date        DATE,
    is_deleted                BIT,
    source_system             VARCHAR(50),
    valid_from                DATETIME2(6) NOT NULL,
    valid_to                  DATETIME2(6),
    is_current                BIT          NOT NULL,
    row_hash                  VARCHAR(64)  NOT NULL,
    run_id                    VARCHAR(60),
    load_datetime             DATETIME2(6) NOT NULL
);
GO

INSERT INTO dbo.dim_program_history (
    program_id, program_code, program_name, organization_key, project_type, housing_type, target_population,
    coc_code, city, state, county, program_status, operating_start_date, operating_end_date, is_deleted,
    source_system, valid_from, valid_to, is_current, row_hash, run_id, load_datetime
)
SELECT
    x.program_id, x.program_code, x.program_name, x.organization_key, x.project_type, x.housing_type, x.target_population,
    x.coc_code, x.city, x.state, x.county, x.program_status, x.operating_start_date, x.operating_end_date, x.is_deleted,
    x.source_system, CAST('1900-01-01' AS DATETIME2(6)), NULL, 1, x.row_hash, 'migration_0005', SYSDATETIME()
FROM (
    SELECT p.program_id, p.program_code, p.program_name, p.organization_key, p.project_type, p.housing_type,
           p.target_population, p.coc_code, p.city, p.state, p.county, p.program_status,
           p.operating_start_date, p.operating_end_date, ISNULL(p.is_deleted, 0) AS is_deleted, p.source_system,
           CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONCAT(
               ISNULL(p.program_code, ''), '|', ISNULL(p.program_name, ''), '|',
               ISNULL(CAST(p.organization_key AS VARCHAR(20)), ''), '|', ISNULL(CAST(p.project_type AS VARCHAR(20)), ''), '|',
               ISNULL(p.housing_type, ''), '|', ISNULL(p.target_population, ''), '|', ISNULL(p.coc_code, ''), '|',
               ISNULL(p.city, ''), '|', ISNULL(p.state, ''), '|', ISNULL(p.county, ''), '|', ISNULL(p.program_status, ''), '|',
               ISNULL(CONVERT(VARCHAR(10), p.operating_start_date, 23), ''), '|',
               ISNULL(CONVERT(VARCHAR(10), p.operating_end_date, 23), ''), '|',
               CAST(ISNULL(p.is_deleted, 0) AS VARCHAR(1))
           )), 2) AS row_hash
    FROM dbo.dim_program p
) x
WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_program_history h WHERE h.program_id = x.program_id);
GO

-- 7. ENROLLMENT AS A DIMENSION
DROP VIEW IF EXISTS dbo.v_dim_enrollment;
GO

CREATE VIEW dbo.v_dim_enrollment AS
SELECT e.enrollment_key, e.source_system, e.source_record_id,
       e.program_id, p.program_code, p.program_name, e.project_type_code,
       e.household_key, e.is_co_enrollment, e.is_head_of_household,
       e.enrollment_start_date, e.enrollment_start_date_key,
       e.enrollment_exit_date, e.enrollment_exit_date_key,
       e.housing_move_in_date, e.enrollment_status,
       e.destination_at_exit, e.destination_category, e.exit_reason,
       CAST(ISNULL(e.is_deleted, 0) AS BIT) AS is_deleted
FROM dbo.fact_enrollment e
LEFT JOIN dbo.dim_program p ON p.program_id = e.program_id;
GO

-- 8. KEYS (NOT ENFORCED)
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_rpt_person')
    ALTER TABLE rpt.person ADD CONSTRAINT PK_rpt_person
        PRIMARY KEY NONCLUSTERED (person_key) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_rpt_person_client')
    ALTER TABLE rpt.person_client ADD CONSTRAINT PK_rpt_person_client
        PRIMARY KEY NONCLUSTERED (client_key) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_match_review_queue')
    ALTER TABLE dbo.match_review_queue ADD CONSTRAINT PK_match_review_queue
        PRIMARY KEY NONCLUSTERED (source_system_a, source_id_a, source_system_b, source_id_b) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_apricot_profile_map')
    ALTER TABLE dbo.apricot_profile_map ADD CONSTRAINT PK_apricot_profile_map
        PRIMARY KEY NONCLUSTERED (source_system, source_id, exported_datetime) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_warehouse_run_log')
    ALTER TABLE dbo.warehouse_run_log ADD CONSTRAINT PK_warehouse_run_log
        PRIMARY KEY NONCLUSTERED (run_key) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_warehouse_schema_version')
    ALTER TABLE dbo.warehouse_schema_version ADD CONSTRAINT PK_warehouse_schema_version
        PRIMARY KEY NONCLUSTERED (version_key) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_dim_program_history')
    ALTER TABLE dbo.dim_program_history ADD CONSTRAINT PK_dim_program_history
        PRIMARY KEY NONCLUSTERED (program_history_key) NOT ENFORCED;
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_fact_enrollment_source_record')
    ALTER TABLE dbo.fact_enrollment ADD CONSTRAINT UQ_fact_enrollment_source_record
        UNIQUE NONCLUSTERED (source_system, source_record_id) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_fact_service_source_record')
    ALTER TABLE dbo.fact_service ADD CONSTRAINT UQ_fact_service_source_record
        UNIQUE NONCLUSTERED (source_system, source_record_id) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_fact_case_note_source_record')
    ALTER TABLE dbo.fact_case_note ADD CONSTRAINT UQ_fact_case_note_source_record
        UNIQUE NONCLUSTERED (source_system, source_record_id) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_fact_assessment_source_record')
    ALTER TABLE dbo.fact_assessment ADD CONSTRAINT UQ_fact_assessment_source_record
        UNIQUE NONCLUSTERED (source_system, source_record_id) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_fact_document_source_record')
    ALTER TABLE dbo.fact_document ADD CONSTRAINT UQ_fact_document_source_record
        UNIQUE NONCLUSTERED (source_system, source_record_id) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_fact_bed_night_source_record')
    ALTER TABLE dbo.fact_bed_night ADD CONSTRAINT UQ_fact_bed_night_source_record
        UNIQUE NONCLUSTERED (source_system, source_record_id) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_fact_bed_availability_source_record')
    ALTER TABLE dbo.fact_bed_availability ADD CONSTRAINT UQ_fact_bed_availability_source_record
        UNIQUE NONCLUSTERED (source_system, source_record_id) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_fact_financial_assistance_source_record')
    ALTER TABLE dbo.fact_financial_assistance ADD CONSTRAINT UQ_fact_financial_assistance_source_record
        UNIQUE NONCLUSTERED (source_system, source_record_id) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_fact_ce_event_source_record')
    ALTER TABLE dbo.fact_ce_event ADD CONSTRAINT UQ_fact_ce_event_source_record
        UNIQUE NONCLUSTERED (source_system, source_record_id) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_fact_consent_source_record')
    ALTER TABLE dbo.fact_consent ADD CONSTRAINT UQ_fact_consent_source_record
        UNIQUE NONCLUSTERED (source_system, source_record_id) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_fact_enrollment_stage_source_record')
    ALTER TABLE dbo.fact_enrollment_stage ADD CONSTRAINT UQ_fact_enrollment_stage_source_record
        UNIQUE NONCLUSTERED (source_system, source_record_id) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_fact_current_living_situation_source_record')
    ALTER TABLE dbo.fact_current_living_situation ADD CONSTRAINT UQ_fact_current_living_situation_source_record
        UNIQUE NONCLUSTERED (source_system, source_record_id) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_warehouse_schema_version_version')
    ALTER TABLE dbo.warehouse_schema_version ADD CONSTRAINT UQ_warehouse_schema_version_version
        UNIQUE NONCLUSTERED (version) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_dim_program_code')
    ALTER TABLE dbo.dim_program ADD CONSTRAINT UQ_dim_program_code
        UNIQUE NONCLUSTERED (program_code) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_dim_client_version_start')
    ALTER TABLE dbo.dim_client ADD CONSTRAINT UQ_dim_client_version_start
        UNIQUE NONCLUSTERED (client_key, valid_from) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_person_client_link_start')
    ALTER TABLE dbo.person_client_link ADD CONSTRAINT UQ_person_client_link_start
        UNIQUE NONCLUSTERED (client_key, valid_from) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_dim_date_full_date')
    ALTER TABLE dbo.dim_date ADD CONSTRAINT UQ_dim_date_full_date
        UNIQUE NONCLUSTERED (full_date) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_dim_program_history_start')
    ALTER TABLE dbo.dim_program_history ADD CONSTRAINT UQ_dim_program_history_start
        UNIQUE NONCLUSTERED (program_id, valid_from) NOT ENFORCED;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_site_source')
    ALTER TABLE dbo.dim_site ADD CONSTRAINT FK_dim_site_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_grant_source')
    ALTER TABLE dbo.dim_grant ADD CONSTRAINT FK_dim_grant_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_funding_source_source')
    ALTER TABLE dbo.dim_funding_source ADD CONSTRAINT FK_dim_funding_source_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_household_source')
    ALTER TABLE dbo.dim_household ADD CONSTRAINT FK_dim_household_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_organization_source')
    ALTER TABLE dbo.dim_organization ADD CONSTRAINT FK_dim_organization_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_client_source')
    ALTER TABLE dbo.dim_client ADD CONSTRAINT FK_dim_client_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_client_identifier_source')
    ALTER TABLE dbo.dim_client_identifier ADD CONSTRAINT FK_dim_client_identifier_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_program_source')
    ALTER TABLE dbo.dim_program ADD CONSTRAINT FK_dim_program_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_service_item_source')
    ALTER TABLE dbo.dim_service_item ADD CONSTRAINT FK_dim_service_item_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_bed_source')
    ALTER TABLE dbo.dim_bed ADD CONSTRAINT FK_dim_bed_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_staff_source')
    ALTER TABLE dbo.dim_staff ADD CONSTRAINT FK_dim_staff_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_household_client_source')
    ALTER TABLE dbo.bridge_household_client ADD CONSTRAINT FK_bridge_household_client_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_client_source_source')
    ALTER TABLE dbo.bridge_client_source ADD CONSTRAINT FK_bridge_client_source_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_enrollment_source')
    ALTER TABLE dbo.fact_enrollment ADD CONSTRAINT FK_fact_enrollment_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_service_source')
    ALTER TABLE dbo.fact_service ADD CONSTRAINT FK_fact_service_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_case_note_source')
    ALTER TABLE dbo.fact_case_note ADD CONSTRAINT FK_fact_case_note_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_assessment_source')
    ALTER TABLE dbo.fact_assessment ADD CONSTRAINT FK_fact_assessment_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_assessment_response_source')
    ALTER TABLE dbo.fact_assessment_response ADD CONSTRAINT FK_fact_assessment_response_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_document_source')
    ALTER TABLE dbo.fact_document ADD CONSTRAINT FK_fact_document_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_bed_night_source')
    ALTER TABLE dbo.fact_bed_night ADD CONSTRAINT FK_fact_bed_night_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_bed_availability_source')
    ALTER TABLE dbo.fact_bed_availability ADD CONSTRAINT FK_fact_bed_availability_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_financial_assistance_source')
    ALTER TABLE dbo.fact_financial_assistance ADD CONSTRAINT FK_fact_financial_assistance_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_ce_event_source')
    ALTER TABLE dbo.fact_ce_event ADD CONSTRAINT FK_fact_ce_event_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_consent_source')
    ALTER TABLE dbo.fact_consent ADD CONSTRAINT FK_fact_consent_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_enrollment_stage_source')
    ALTER TABLE dbo.fact_enrollment_stage ADD CONSTRAINT FK_fact_enrollment_stage_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_current_living_situation_source')
    ALTER TABLE dbo.fact_current_living_situation ADD CONSTRAINT FK_fact_current_living_situation_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_enrollment_grant_source')
    ALTER TABLE dbo.bridge_enrollment_grant ADD CONSTRAINT FK_bridge_enrollment_grant_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_program_site_source')
    ALTER TABLE dbo.bridge_program_site ADD CONSTRAINT FK_bridge_program_site_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_rpt_person_client_source')
    ALTER TABLE rpt.person_client ADD CONSTRAINT FK_rpt_person_client_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_apricot_profile_map_source')
    ALTER TABLE dbo.apricot_profile_map ADD CONSTRAINT FK_apricot_profile_map_source
        FOREIGN KEY (source_system) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_match_decision_source_a')
    ALTER TABLE dbo.match_decision ADD CONSTRAINT FK_match_decision_source_a
        FOREIGN KEY (source_system_a) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_match_decision_source_b')
    ALTER TABLE dbo.match_decision ADD CONSTRAINT FK_match_decision_source_b
        FOREIGN KEY (source_system_b) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_match_review_queue_source_a')
    ALTER TABLE dbo.match_review_queue ADD CONSTRAINT FK_match_review_queue_source_a
        FOREIGN KEY (source_system_a) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_match_review_queue_source_b')
    ALTER TABLE dbo.match_review_queue ADD CONSTRAINT FK_match_review_queue_source_b
        FOREIGN KEY (source_system_b) REFERENCES dbo.dim_source (source_system) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_person_client_link_decision')
    ALTER TABLE dbo.person_client_link ADD CONSTRAINT FK_person_client_link_decision
        FOREIGN KEY (decision_key) REFERENCES dbo.match_decision (decision_key) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_rpt_person_client_person')
    ALTER TABLE rpt.person_client ADD CONSTRAINT FK_rpt_person_client_person
        FOREIGN KEY (person_key) REFERENCES rpt.person (person_key) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_client_current_household')
    ALTER TABLE dbo.dim_client ADD CONSTRAINT FK_dim_client_current_household
        FOREIGN KEY (current_household_key) REFERENCES dbo.dim_household (household_key) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_program_history_program')
    ALTER TABLE dbo.dim_program_history ADD CONSTRAINT FK_dim_program_history_program
        FOREIGN KEY (program_id) REFERENCES dbo.dim_program (program_id) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_program_history_organization')
    ALTER TABLE dbo.dim_program_history ADD CONSTRAINT FK_dim_program_history_organization
        FOREIGN KEY (organization_key) REFERENCES dbo.dim_organization (organization_key) NOT ENFORCED;
GO

-- 9. RECORD VERSION AND LOG RUN END
IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.18')
    INSERT INTO dbo.warehouse_schema_version (version, applied_date, applied_by, summary)
    VALUES ('v8.18', SYSDATETIME(), USER_NAME(),
            'Declared keys, bed-night soft delete, dim_bed and household_key clean-up, column widths, program history, v_dim_enrollment');
GO

UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'SUCCESS'
WHERE script_name = 'migration_0005' AND run_status = 'RUNNING';
GO

-- 10. CHECK: every row should show ok = 1
DECLARE @ok1 INT = CASE WHEN EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.18') THEN 1 ELSE 0 END;
DECLARE @ok2 INT = CASE WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_bed_night') AND name = 'is_deleted')
                         AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_bed') AND name IN ('valid_from', 'valid_to', 'is_current'))
                        THEN 1 ELSE 0 END;
DECLARE @ok3 INT = CASE WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_client') AND name = 'current_household_key')
                         AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_client') AND name = 'household_key')
                        THEN 1 ELSE 0 END;
DECLARE @ok4 INT = CASE WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.stage_load_log') AND name = 'run_id')
                         AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE name IN ('run_id_v818', 'program_code_v818'))
                        THEN 1 ELSE 0 END;
DECLARE @ok5 INT = CASE WHEN OBJECT_ID('dbo.dim_program_history') IS NOT NULL AND OBJECT_ID('dbo.v_dim_enrollment') IS NOT NULL
                         AND OBJECT_ID('dbo.v_fact_bed_night_active') IS NOT NULL THEN 1 ELSE 0 END;
DECLARE @pk INT = (SELECT COUNT(*) FROM sys.key_constraints WHERE name IN ('PK_rpt_person', 'PK_rpt_person_client', 'PK_match_review_queue', 'PK_apricot_profile_map', 'PK_warehouse_run_log', 'PK_warehouse_schema_version', 'PK_dim_program_history'));
DECLARE @uq INT = (SELECT COUNT(*) FROM sys.key_constraints WHERE name IN ('UQ_fact_enrollment_source_record', 'UQ_fact_service_source_record', 'UQ_fact_case_note_source_record', 'UQ_fact_assessment_source_record', 'UQ_fact_document_source_record', 'UQ_fact_bed_night_source_record', 'UQ_fact_bed_availability_source_record', 'UQ_fact_financial_assistance_source_record', 'UQ_fact_ce_event_source_record', 'UQ_fact_consent_source_record', 'UQ_fact_enrollment_stage_source_record', 'UQ_fact_current_living_situation_source_record', 'UQ_warehouse_schema_version_version', 'UQ_dim_program_code', 'UQ_dim_client_version_start', 'UQ_person_client_link_start', 'UQ_dim_date_full_date', 'UQ_dim_program_history_start'));
DECLARE @fk INT = (SELECT COUNT(*) FROM sys.foreign_keys WHERE name IN ('FK_dim_site_source', 'FK_dim_grant_source', 'FK_dim_funding_source_source', 'FK_dim_household_source', 'FK_dim_organization_source', 'FK_dim_client_source', 'FK_dim_client_identifier_source', 'FK_dim_program_source', 'FK_dim_service_item_source', 'FK_dim_bed_source', 'FK_dim_staff_source', 'FK_bridge_household_client_source', 'FK_bridge_client_source_source', 'FK_fact_enrollment_source', 'FK_fact_service_source', 'FK_fact_case_note_source', 'FK_fact_assessment_source', 'FK_fact_assessment_response_source', 'FK_fact_document_source', 'FK_fact_bed_night_source', 'FK_fact_bed_availability_source', 'FK_fact_financial_assistance_source', 'FK_fact_ce_event_source', 'FK_fact_consent_source', 'FK_fact_enrollment_stage_source', 'FK_fact_current_living_situation_source', 'FK_bridge_enrollment_grant_source', 'FK_bridge_program_site_source', 'FK_rpt_person_client_source', 'FK_apricot_profile_map_source', 'FK_match_decision_source_a', 'FK_match_decision_source_b', 'FK_match_review_queue_source_a', 'FK_match_review_queue_source_b', 'FK_person_client_link_decision', 'FK_rpt_person_client_person', 'FK_dim_client_current_household', 'FK_dim_program_history_program', 'FK_dim_program_history_organization'));
DECLARE @hist_missing INT = (SELECT COUNT(*) FROM dbo.dim_program p
                             WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_program_history h WHERE h.program_id = p.program_id AND h.is_current = 1));
SELECT check_name, ok FROM (VALUES
    ('v8.18 recorded', @ok1),
    ('bed nights flaggable; dim_bed history columns gone', @ok2),
    ('dim_client.current_household_key', @ok3),
    ('run_id and program_code widths swapped', @ok4),
    ('program history, enrollment dimension, active bed-night view', @ok5),
    ('7 primary keys', CASE WHEN @pk = 7 THEN 1 ELSE 0 END),
    ('18 unique keys', CASE WHEN @uq = 18 THEN 1 ELSE 0 END),
    ('39 foreign keys', CASE WHEN @fk = 39 THEN 1 ELSE 0 END),
    ('every program has a current history row', CASE WHEN @hist_missing = 0 THEN 1 ELSE 0 END)
) v (check_name, ok);
