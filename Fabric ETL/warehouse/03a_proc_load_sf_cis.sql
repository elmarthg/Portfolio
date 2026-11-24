-- 03a_proc_load_sf_cis v2.3: installs dbo.usp_load_sf_cis
-- Loads Salesforce from lh_silver.dbo.conf_sf_cis_* into a PERSISTENT warehouse.
-- Pipeline: EXEC dbo.usp_load_sf_cis @min_silver_load_datetime = '<TriggerTime>', @pipeline_run_id = '<pipeline().RunId>';
--
-- v2.3 (schema v8.19, needs migrations/0006): an enrollment or assessment with a date outside dim_date (2000 to 2040)
--   is held back and listed in v_dq_corrections (reason 'date_outside_calendar:<column>'), and every declared key
--   on the tables this procedure writes is checked before COMMIT (generated list). is_placed is BIT.
--
-- v2.2 (schema v8.18, needs migrations/0005): dim_client.current_household_key (renamed from household_key);
--   program history in dim_program_history; required columns (source_system, source_record_id, client keys,
--   load_datetime) checked before commit, since Fabric cannot add NOT NULL to existing columns.
--
-- v2.1 (schema v8.17, needs migrations/0004 and nb_silver_sf_cis_conform v1.4):
--   Assessments. lh_silver.dbo.conf_sf_cis_assessment is loaded into fact_assessment with the same rules
--   as every other table (insert new, update changed, flag gone, restore if back). An assessment of
--   someone who went on to enroll points at their primary Salesforce enrollment. Assessed-only contacts
--   are clients with an assessment and no enrollment, so they are no longer rejected as enrollments with
--   a missing start date (docs/design_decisions.md, decision 13).
--
-- v2.0 (schema v8.14, needs migrations/0001). Same rules as 03b v3.0:
--   1. Persistent: inserts new rows, updates only changed rows, flags rows gone from Salesforce
--      (is_deleted = 1 + deleted_detected_datetime) and restores rows that come back.
--      Running twice on the same Silver changes nothing.
--   2. One transaction: everything commits together or nothing does.
--   3. One timestamp per run (@etl_time); client versions are half-open (valid_to = next valid_from).
--   4. A fact keeps the client version it was loaded with unless it moves to another client_key.
--   5. client_key is found from any earlier version, so it is never reissued.
--   6. Household links: exits now update end_date and is_current (v1.x left them at the first load),
--      and links of deleted enrollments are removed.
--   7. Integrity checks before COMMIT; any problem rolls the run back.
--   8. stage_load_log gets one row per table with rows changed and rows flagged deleted.
-- Salesforce enrollment IDs are '<Contact Id>|<start date>[|<co-enrolled program>]'. When staff change
-- Date_Enrolled__c the old ID disappears, so that enrollment is flagged deleted and the new one is
-- inserted. See docs/design_decisions.md (open question A) on re-enrollments.

DROP PROCEDURE IF EXISTS dbo.usp_load_sf_cis;
GO

CREATE PROCEDURE dbo.usp_load_sf_cis
    @min_silver_load_datetime DATETIME2(6) = NULL,
    @allow_mass_delete BIT = 0,
    @pipeline_run_id VARCHAR(100) = NULL
AS
BEGIN

DECLARE @etl_time DATETIME2(6) = SYSDATETIME();
DECLARE @run_id VARCHAR(60) = CONCAT('sf_cis_', CONVERT(VARCHAR(30), SYSDATETIME(), 126));
DECLARE @cal_min DATE = (SELECT MIN(full_date) FROM dim_date);
DECLARE @cal_max DATE = (SELECT MAX(full_date) FROM dim_date);

IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.19')
    THROW 50011, 'The warehouse schema is older than v8.19. Run migrations/0006_key_checks_and_dates.sql first.', 1;

BEGIN TRY

-- 0. LOG RUN START (outside the transaction)
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'ABANDONED'
WHERE script_name IN ('03a_load_sf_cis', '03_load_all') AND run_status = 'RUNNING';

INSERT INTO dbo.warehouse_run_log (
    script_name, script_version, run_start, run_status, run_by, change_summary, pipeline_run_id
)
VALUES (
    '03a_load_sf_cis', 'v2.3', @etl_time, 'RUNNING', USER_NAME(),
    'Persistent Salesforce load from lh_silver conf_sf_cis_*', @pipeline_run_id
);

-- 0b. Preflight (reads only)
IF NOT EXISTS (SELECT 1 FROM etl_source_config WHERE source_system = 'sf_cis')
    THROW 50005, 'sf_cis has no row in etl_source_config. Run 02b_seed_etl_source_config.', 1;

IF EXISTS (SELECT 1 FROM etl_source_config WHERE source_system = 'sf_cis' AND block_reason IS NOT NULL)
BEGIN
    DECLARE @block_msg NVARCHAR(2048) = CONCAT('sf_cis is blocked in etl_source_config: ',
        (SELECT block_reason FROM etl_source_config WHERE source_system = 'sf_cis'));
    THROW 50003, @block_msg, 1;
END;

IF NOT EXISTS (SELECT 1 FROM lh_silver.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'conf_sf_cis_assessment')
    THROW 50001, 'lh_silver.dbo.conf_sf_cis_assessment was not found. Run nb_silver_sf_cis_conform v1.4 and let the SQL endpoint sync.', 1;

DROP TABLE IF EXISTS #sf_program;
DROP TABLE IF EXISTS #sf_client;
DROP TABLE IF EXISTS #sf_enrollment;
DROP TABLE IF EXISTS #sf_assessment;
SELECT * INTO #sf_program    FROM lh_silver.dbo.conf_sf_cis_program    WHERE source_system = 'sf_cis';
SELECT * INTO #sf_client     FROM lh_silver.dbo.conf_sf_cis_client     WHERE source_system = 'sf_cis';
SELECT * INTO #sf_enrollment FROM lh_silver.dbo.conf_sf_cis_enrollment WHERE source_system = 'sf_cis';
SELECT * INTO #sf_assessment FROM lh_silver.dbo.conf_sf_cis_assessment WHERE source_system = 'sf_cis';

IF NOT EXISTS (SELECT 1 FROM #sf_program) OR NOT EXISTS (SELECT 1 FROM #sf_client)
   OR NOT EXISTS (SELECT 1 FROM #sf_enrollment)
    THROW 50001, 'No Silver rows for sf_cis. Run nb_silver_sf_cis_conform and let the SQL endpoint sync.', 1;

IF @min_silver_load_datetime IS NOT NULL
BEGIN
    DECLARE @stale VARCHAR(200) = NULL;
    SELECT @stale = STRING_AGG(t, ', ') FROM (
        SELECT 'program' AS t WHERE (SELECT MIN(load_datetime) FROM #sf_program) < @min_silver_load_datetime
        UNION ALL SELECT 'client'     WHERE (SELECT MIN(load_datetime) FROM #sf_client) < @min_silver_load_datetime
        UNION ALL SELECT 'enrollment' WHERE (SELECT MIN(load_datetime) FROM #sf_enrollment) < @min_silver_load_datetime
        UNION ALL SELECT 'assessment' WHERE (SELECT MIN(load_datetime) FROM #sf_assessment) < @min_silver_load_datetime
    ) x;
    IF @stale IS NOT NULL
    BEGIN
        DECLARE @stale_msg NVARCHAR(2048) = CONCAT('Silver for sf_cis is older than this run (', @stale,
            '). The notebook did not finish or the SQL endpoint has not synced yet. Wait and rerun.');
        THROW 50004, @stale_msg, 1;
    END;
END;

-- 0c. Records gone from Salesforce, counted before anything changes
DECLARE @max_delete_pct DECIMAL(5,2) = 5.0;
DECLARE @live_c INT = (SELECT COUNT(*) FROM dim_client WHERE source_system = 'sf_cis' AND is_current = 1 AND ISNULL(is_deleted, 0) = 0);
DECLARE @gone_c INT = (SELECT COUNT(*) FROM dim_client d WHERE d.source_system = 'sf_cis' AND d.is_current = 1 AND ISNULL(d.is_deleted, 0) = 0
    AND NOT EXISTS (SELECT 1 FROM #sf_client c WHERE dbo.fn_normalize_key(c.salesforce_id) = dbo.fn_normalize_key(d.salesforce_id)));
DECLARE @live_e INT = (SELECT COUNT(*) FROM fact_enrollment WHERE source_system = 'sf_cis' AND ISNULL(is_deleted, 0) = 0);
DECLARE @gone_e INT = (SELECT COUNT(*) FROM fact_enrollment f WHERE f.source_system = 'sf_cis' AND ISNULL(f.is_deleted, 0) = 0
    AND NOT EXISTS (SELECT 1 FROM #sf_enrollment e WHERE dbo.fn_normalize_key(e.source_record_id) = dbo.fn_normalize_key(f.source_record_id)));

DECLARE @live_a INT = (SELECT COUNT(*) FROM fact_assessment WHERE source_system = 'sf_cis' AND ISNULL(is_deleted, 0) = 0);
DECLARE @gone_a INT = (SELECT COUNT(*) FROM fact_assessment f WHERE f.source_system = 'sf_cis' AND ISNULL(f.is_deleted, 0) = 0
    AND NOT EXISTS (SELECT 1 FROM #sf_assessment a WHERE dbo.fn_normalize_key(a.source_record_id) = dbo.fn_normalize_key(f.source_record_id)));

IF @allow_mass_delete = 0 AND (
       (@live_c > 0 AND @gone_c * 100.0 / @live_c > @max_delete_pct)
    OR (@live_e > 0 AND @gone_e * 100.0 / @live_e > @max_delete_pct)
    OR (@live_a > 0 AND @gone_a * 100.0 / @live_a > @max_delete_pct))
BEGIN
    DECLARE @del_msg NVARCHAR(2048) = CONCAT('More than ', @max_delete_pct, '% of sf_cis records would be marked deleted (contacts ',
        @gone_c, '/', @live_c, ', enrollments ', @gone_e, '/', @live_e, ', assessments ', @gone_a, '/', @live_a,
        '). Check the Salesforce export. If it is real, rerun with @allow_mass_delete = 1.');
    THROW 50006, @del_msg, 1;
END;

DECLARE @del_hh_links INT = 0;

-- =====================================================================
BEGIN TRANSACTION;
-- =====================================================================

-- 1. dim_program: Salesforce block 10001-19999, set by the PROGRAMS list in the Silver notebook
DROP TABLE IF EXISTS #prog;
SELECT program_id,
       LTRIM(RTRIM(program_code))      AS program_code,
       LTRIM(RTRIM(sf_picklist_value)) AS sf_picklist_value,
       LTRIM(RTRIM(program_name))      AS program_name
INTO #prog
FROM #sf_program
WHERE program_id IS NOT NULL
  AND program_code IS NOT NULL AND LTRIM(RTRIM(program_code)) <> ''
  AND sf_picklist_value IS NOT NULL AND LTRIM(RTRIM(sf_picklist_value)) <> '';

INSERT INTO dim_program (program_id, program_code, sf_picklist_value, program_name, source_system, is_deleted, load_datetime)
SELECT s.program_id, s.program_code, s.sf_picklist_value, s.program_name, 'sf_cis', 0, @etl_time
FROM #prog s
WHERE NOT EXISTS (SELECT 1 FROM dim_program p WHERE p.program_id = s.program_id);

UPDATE tgt
SET tgt.program_name      = s.program_name,
    tgt.program_code      = s.program_code,
    tgt.sf_picklist_value = s.sf_picklist_value,
    tgt.is_deleted        = 0,
    tgt.deleted_detected_datetime = NULL,
    tgt.load_datetime     = @etl_time
FROM dim_program tgt
JOIN #prog s ON s.program_id = tgt.program_id
WHERE NOT EXISTS (
    SELECT tgt.program_name, tgt.program_code, tgt.sf_picklist_value, ISNULL(tgt.is_deleted, 0)
    INTERSECT
    SELECT s.program_name, s.program_code, s.sf_picklist_value, 0);

UPDATE dim_program
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = 'sf_cis' AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #prog s WHERE s.program_id = dim_program.program_id);

-- 1b. dim_program_history: a new version whenever a tracked attribute changes (schema v8.18).
--     The first version of a program starts on 1900-01-01; a later one starts at this run.
DROP TABLE IF EXISTS #ph;
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
INTO #ph
FROM dim_program p
WHERE p.source_system = 'sf_cis';

UPDATE h
SET h.valid_to = @etl_time, h.is_current = 0
FROM dim_program_history h
JOIN #ph n ON n.program_id = h.program_id
WHERE h.is_current = 1 AND h.row_hash <> n.row_hash;

INSERT INTO dim_program_history (
    program_id, program_code, program_name, organization_key, project_type, housing_type, target_population,
    coc_code, city, state, county, program_status, operating_start_date, operating_end_date, is_deleted,
    source_system, valid_from, valid_to, is_current, row_hash, run_id, load_datetime
)
SELECT n.program_id, n.program_code, n.program_name, n.organization_key, n.project_type, n.housing_type, n.target_population,
       n.coc_code, n.city, n.state, n.county, n.program_status, n.operating_start_date, n.operating_end_date, n.is_deleted,
       n.source_system,
       CASE WHEN EXISTS (SELECT 1 FROM dim_program_history h WHERE h.program_id = n.program_id)
            THEN @etl_time ELSE CAST('1900-01-01' AS DATETIME2(6)) END,
       NULL, 1, n.row_hash, @run_id, @etl_time
FROM #ph n
WHERE NOT EXISTS (SELECT 1 FROM dim_program_history h WHERE h.program_id = n.program_id AND h.is_current = 1);

-- 2. dim_client: SCD type 2 on salesforce_id
DROP TABLE IF EXISTS #client_staged;

SELECT
    dbo.fn_normalize_key(salesforce_id) AS salesforce_key,
    CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONCAT(
        ISNULL(LTRIM(RTRIM(first_name)),''),           '|',
        ISNULL(LTRIM(RTRIM(middle_name)),''),          '|',
        ISNULL(LTRIM(RTRIM(last_name)),''),            '|',
        ISNULL(CONVERT(VARCHAR(20), date_of_birth, 120),''), '|',
        ISNULL(LTRIM(RTRIM(sex)),''),                  '|',
        ISNULL(LTRIM(RTRIM(gender)),''),               '|',
        ISNULL(LTRIM(RTRIM(race)),''),                 '|',
        ISNULL(LTRIM(RTRIM(race_detail)),''),          '|',
        ISNULL(LTRIM(RTRIM(ethnicity)),''),            '|',
        ISNULL(LTRIM(RTRIM(marital_status)),''),       '|',
        ISNULL(LTRIM(RTRIM(number_of_children)),''),   '|',
        ISNULL(LTRIM(RTRIM(education_level)),''),      '|',
        ISNULL(LTRIM(RTRIM(email)),''),                '|',
        ISNULL(LTRIM(RTRIM(home_phone)),''),           '|',
        ISNULL(LTRIM(RTRIM(mobile_phone)),''),         '|',
        ISNULL(LTRIM(RTRIM(address_line1)),''),        '|',
        ISNULL(LTRIM(RTRIM(city)),''),                 '|',
        ISNULL(LTRIM(RTRIM(state)),''),                '|',
        ISNULL(LTRIM(RTRIM(postal_code)),''),          '|',
        ISNULL(LTRIM(RTRIM(county_name)),''),          '|',
        ISNULL(LTRIM(RTRIM(veteran_status)),''),       '|',
        ISNULL(LTRIM(RTRIM(military_branch)),''),      '|',
        ISNULL(LTRIM(RTRIM(military_service_component)),''), '|',
        ISNULL(LTRIM(RTRIM(discharge_status)),''),     '|',
        ISNULL(CONVERT(VARCHAR(20), military_entry_date, 120),''), '|',
        ISNULL(CONVERT(VARCHAR(20), military_discharge_date, 120),''), '|',
        ISNULL(LTRIM(RTRIM(is_post_9_11)),''),         '|',
        ISNULL(LTRIM(RTRIM(service_connected)),''),    '|',
        ISNULL(LTRIM(RTRIM(period_of_service)),''),    '|',
        ISNULL(LTRIM(RTRIM(homeless_status)),''),      '|',
        ISNULL(LTRIM(RTRIM(housing_status)),''),       '|',
        ISNULL(LTRIM(RTRIM(length_of_homelessness)),''), '|',
        ISNULL(LTRIM(RTRIM(has_conviction)),''),       '|',
        ISNULL(LTRIM(RTRIM(conviction_type)),''),      '|',
        ISNULL(CONVERT(VARCHAR(20), conviction_date, 120),''), '|',
        ISNULL(LTRIM(RTRIM(probation_parole_status)),''), '|',
        ISNULL(CONVERT(VARCHAR(20), parole_end_date, 120),''), '|',
        ISNULL(CONVERT(VARCHAR(20), last_release_date, 120),''), '|',
        ISNULL(LTRIM(RTRIM(experience_during_incarceration)),''), '|',
        ISNULL(LTRIM(RTRIM(service_type)),''),         '|',
        ISNULL(LTRIM(RTRIM(lead_source)),''),          '|',
        CASE WHEN ISNULL(is_deleted,0) = 1 THEN '1' ELSE '0' END
    )), 2) AS row_hash,
    salesforce_id, salesforce_account_id, external_id,
    first_name, middle_name, last_name,
    date_of_birth, sex, gender, race, race_detail, ethnicity,
    marital_status, number_of_children, education_level,
    email, home_phone, mobile_phone,
    address_line1, city, state, postal_code, county_name,
    veteran_status,
    disability_status, mental_health_condition, substance_abuse, times_homeless_3yr,
    military_branch, military_service_component, discharge_status,
    military_entry_date, military_discharge_date, is_post_9_11,
    service_connected, period_of_service,
    homeless_status, housing_status, length_of_homelessness,
    has_conviction, conviction_type, conviction_date,
    probation_parole_status, parole_end_date, last_release_date,
    experience_during_incarceration,
    service_type, lead_source,
    owner_id, created_by_id, created_date,
    last_modified_date, is_deleted
INTO #client_staged
FROM (
    SELECT *, ROW_NUMBER() OVER (
            PARTITION BY dbo.fn_normalize_key(salesforce_id)
            ORDER BY ISNULL(last_modified_date,'1900-01-01') DESC
        ) AS rn
    FROM #sf_client
    WHERE salesforce_id IS NOT NULL AND LTRIM(RTRIM(salesforce_id)) <> ''
) x
WHERE x.rn = 1;

DROP TABLE IF EXISTS #client_keyed;

SELECT
    s.*,
    cur.client_key        AS current_client_key,
    cur.row_hash          AS existing_row_hash,
    cur.current_household_key AS existing_household_key,
    anyv.client_key       AS any_client_key,
    COALESCE(
        cur.client_key,
        anyv.client_key,
        ISNULL((SELECT MAX(client_key) FROM dim_client), 0)
            + SUM(CASE WHEN cur.client_key IS NULL AND anyv.client_key IS NULL THEN 1 ELSE 0 END)
                  OVER (ORDER BY s.salesforce_key ROWS UNBOUNDED PRECEDING)
    ) AS resolved_client_key
INTO #client_keyed
FROM #client_staged s
LEFT JOIN dim_client cur
  ON cur.source_system = 'sf_cis'
 AND cur.is_current = 1
 AND dbo.fn_normalize_key(cur.salesforce_id) = s.salesforce_key
LEFT JOIN (
    SELECT dbo.fn_normalize_key(salesforce_id) AS salesforce_key, MAX(client_key) AS client_key
    FROM dim_client
    WHERE source_system = 'sf_cis'
    GROUP BY dbo.fn_normalize_key(salesforce_id)
) anyv ON anyv.salesforce_key = s.salesforce_key;

UPDATE dc
SET dc.is_current    = 0,
    dc.valid_to      = @etl_time,
    dc.load_datetime = @etl_time
FROM dim_client dc
JOIN #client_keyed k
  ON dc.client_key = k.current_client_key
 AND dc.is_current = 1
WHERE k.existing_row_hash <> k.row_hash;

INSERT INTO dim_client (
    client_key, is_current, valid_from, valid_to, row_hash, current_household_key,
    salesforce_id, salesforce_account_id, external_id,
    first_name, middle_name, last_name,
    date_of_birth, sex, gender, race, race_detail, ethnicity,
    marital_status, number_of_children, education_level,
    email, home_phone, mobile_phone,
    address_line1, city, state, postal_code, county,
    veteran_status,
    military_branch, military_service_component, discharge_status,
    military_entry_date, military_discharge_date, is_post_9_11,
    service_connected, period_of_service,
    homeless_status, housing_status, length_of_homelessness,
    has_conviction, conviction_type, conviction_date,
    probation_parole_status, parole_end_date, last_release_date,
    experience_during_incarceration,
    service_type, lead_source,
    source_system, owner_id, created_by_id, created_date,
    last_modified_date, is_deleted, deleted_detected_datetime, load_datetime
)
SELECT
    k.resolved_client_key, 1,
    CASE WHEN k.current_client_key IS NULL AND k.any_client_key IS NULL
         THEN CAST('1900-01-01' AS DATETIME2(6))
         ELSE @etl_time END,
    NULL, k.row_hash, k.existing_household_key,
    LTRIM(RTRIM(k.salesforce_id)), k.salesforce_account_id, k.external_id,
    k.first_name, k.middle_name, k.last_name,
    k.date_of_birth, k.sex, k.gender, k.race, k.race_detail, k.ethnicity,
    k.marital_status, k.number_of_children, k.education_level,
    k.email, k.home_phone, k.mobile_phone,
    k.address_line1, k.city, k.state, k.postal_code, k.county_name,
    k.veteran_status,
    k.military_branch, k.military_service_component, k.discharge_status,
    k.military_entry_date, k.military_discharge_date, k.is_post_9_11,
    k.service_connected, k.period_of_service,
    k.homeless_status, k.housing_status, k.length_of_homelessness,
    k.has_conviction, k.conviction_type, k.conviction_date,
    k.probation_parole_status, k.parole_end_date, k.last_release_date,
    k.experience_during_incarceration,
    k.service_type, k.lead_source,
    'sf_cis', k.owner_id, k.created_by_id, k.created_date,
    k.last_modified_date,
    CASE WHEN ISNULL(k.is_deleted, 0) = 1 THEN 1 ELSE 0 END,
    CASE WHEN ISNULL(k.is_deleted, 0) = 1 THEN @etl_time END,
    @etl_time
FROM #client_keyed k
WHERE k.current_client_key IS NULL
   OR k.existing_row_hash <> k.row_hash;

-- Back in Salesforce but still flagged deleted (unchanged values): restore
UPDATE dim_client
SET is_deleted = 0, deleted_detected_datetime = NULL, load_datetime = @etl_time
WHERE source_system = 'sf_cis' AND is_current = 1 AND is_deleted = 1
  AND EXISTS (SELECT 1 FROM #client_staged c WHERE c.salesforce_key = dbo.fn_normalize_key(dim_client.salesforce_id)
              AND ISNULL(c.is_deleted, 0) = 0);

-- Gone from Salesforce: flag the current version
UPDATE dim_client
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = 'sf_cis' AND is_current = 1 AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #client_staged c WHERE c.salesforce_key = dbo.fn_normalize_key(dim_client.salesforce_id));

-- 3. bridge_client_source
INSERT INTO bridge_client_source (
    client_key, source_system, source_client_id, source_personal_id,
    match_method, match_confidence, matched_by, matched_date,
    is_active, load_datetime
)
SELECT DISTINCT
    dc.client_key, 'sf_cis', LTRIM(RTRIM(dc.salesforce_id)), NULL,
    'single_source', 1.0000, 'system', @etl_time, 1, @etl_time
FROM dim_client dc
WHERE dc.is_current = 1
  AND dc.source_system = 'sf_cis'
  AND dc.salesforce_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM bridge_client_source b
    WHERE b.source_system = 'sf_cis'
      AND dbo.fn_normalize_key(b.source_client_id) = dbo.fn_normalize_key(dc.salesforce_id)
      AND b.is_active = 1
  );

-- 4. fact_enrollment
DROP TABLE IF EXISTS #enr;
SELECT
    x.*,
    LTRIM(RTRIM(x.salesforce_id))          AS salesforce_id_clean,
    LTRIM(RTRIM(x.source_record_id))       AS source_record_id_clean,
    LTRIM(RTRIM(x.hvrp_participant_type))  AS hvrp_participant_type_clean,
    LTRIM(RTRIM(x.program_code))           AS program_code_clean,
    dbo.fn_normalize_key(x.salesforce_id)  AS salesforce_key
INTO #enr
FROM (
    SELECT *, ROW_NUMBER() OVER (
            PARTITION BY LTRIM(RTRIM(source_record_id))
            ORDER BY ISNULL(enrollment_start_date,'1900-01-01') DESC
        ) AS rn
    FROM #sf_enrollment
    WHERE source_record_id IS NOT NULL AND LTRIM(RTRIM(source_record_id)) <> ''
      AND salesforce_id IS NOT NULL AND LTRIM(RTRIM(salesforce_id)) <> ''
) x
WHERE x.rn = 1;

DROP TABLE IF EXISTS #er;
SELECT
    s.*,
    dc.client_key                AS resolved_client_key,
    dc.client_version_key        AS resolved_client_version_key,
    dc.salesforce_id             AS resolved_client_source_id,
    cs.disability_status         AS resolved_disability_status,
    cs.mental_health_condition   AS resolved_mental_health_condition,
    cs.substance_abuse           AS resolved_substance_abuse,
    cs.times_homeless_3yr        AS resolved_times_homeless_3yr
INTO #er
FROM #enr s
LEFT JOIN dim_client dc
  ON dc.source_system = 'sf_cis'
 AND dc.is_current = 1
 AND dbo.fn_normalize_key(dc.salesforce_id) = s.salesforce_key
LEFT JOIN #client_staged cs
  ON cs.salesforce_key = s.salesforce_key;

-- A date outside dim_date (enrollment_start_date/enrollment_exit_date) would load a date key with no calendar row, breaking a
-- declared key and hiding the row from date filters: hold the record back and list it in v_dq_corrections.
DROP TABLE IF EXISTS #bad_er;
SELECT r.source_record_id_clean AS record_id,
       CASE
            WHEN CAST(r.enrollment_start_date AS DATE) NOT BETWEEN @cal_min AND @cal_max THEN 'enrollment_start_date'
            WHEN CAST(r.enrollment_exit_date AS DATE) NOT BETWEEN @cal_min AND @cal_max THEN 'enrollment_exit_date'
       END AS bad_column,
       r.salesforce_id_clean AS client_ref, r.program_code_clean AS program_ref
INTO #bad_er
FROM #er r
WHERE (r.enrollment_start_date IS NOT NULL AND r.resolved_client_key IS NOT NULL)
  AND (CAST(r.enrollment_start_date AS DATE) NOT BETWEEN @cal_min AND @cal_max OR CAST(r.enrollment_exit_date AS DATE) NOT BETWEEN @cal_min AND @cal_max);

INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, pipeline_run_id
)
SELECT @run_id, 'sf_cis', 'fact_enrollment', b.record_id, CONCAT('date_outside_calendar:', b.bad_column),
       b.client_ref, b.program_ref, @etl_time, @pipeline_run_id
FROM #bad_er b;

INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, pipeline_run_id
)
SELECT
    @run_id, 'sf_cis', 'fact_enrollment', r.source_record_id_clean,
    CASE WHEN r.enrollment_start_date IS NULL THEN 'missing_enrollment_start_date' ELSE 'orphan_client' END,
    r.salesforce_id_clean, r.program_code_clean, @etl_time, @pipeline_run_id
FROM #er r
WHERE r.enrollment_start_date IS NULL
   OR r.resolved_client_key IS NULL;

INSERT INTO fact_enrollment (
    client_key, client_version_key, program_id, program_code, household_key,
    enrollment_start_date, enrollment_start_date_key,
    enrollment_exit_date,  enrollment_exit_date_key,
    placement_date,
    housing_move_in_date,  housing_move_in_date_key,
    enrollment_status, grant_name, hvrp_participant_type, is_co_enrollment,
    service_type, is_carry_over,
    referral_agency, referred_to_workforce, workforce_referral_name,
    is_placed, starting_hourly_wage,
    disability_status, mental_health_condition, substance_abuse, times_homeless_3yr,
    source_system, source_record_id, source_enrollment_id, source_client_id,
    is_deleted, load_datetime
)
SELECT
    r.resolved_client_key, r.resolved_client_version_key,
    r.program_id, r.program_code_clean, NULL,
    r.enrollment_start_date,
    CAST(CONVERT(VARCHAR(8), r.enrollment_start_date, 112) AS INT),
    r.enrollment_exit_date,
    CASE WHEN r.enrollment_exit_date IS NULL THEN NULL
         ELSE CAST(CONVERT(VARCHAR(8), r.enrollment_exit_date, 112) AS INT) END,
    r.placement_date,
    NULL, NULL,
    r.enrollment_status, r.grant_name, r.hvrp_participant_type_clean,
    ISNULL(r.is_co_enrollment, 0),
    r.service_type, r.is_carry_over,
    r.referral_agency, r.referred_to_workforce, r.workforce_referral_name,
    r.is_placed, r.starting_hourly_wage,
    r.resolved_disability_status, r.resolved_mental_health_condition,
    r.resolved_substance_abuse, r.resolved_times_homeless_3yr,
    'sf_cis', r.source_record_id_clean, NULL, r.salesforce_id_clean,
    0, @etl_time
FROM #er r
WHERE NOT EXISTS (SELECT 1 FROM #bad_er bd WHERE bd.record_id = r.source_record_id_clean)
  AND r.enrollment_start_date IS NOT NULL
  AND r.resolved_client_key IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM fact_enrollment f
    WHERE f.source_system = 'sf_cis'
      AND dbo.fn_normalize_key(f.source_record_id) = dbo.fn_normalize_key(r.source_record_id_clean)
  );

UPDATE f
SET f.client_key            = r.resolved_client_key,
    f.client_version_key    = CASE WHEN f.client_version_key IS NULL OR f.client_key <> r.resolved_client_key
                                   THEN r.resolved_client_version_key ELSE f.client_version_key END,
    f.program_id            = r.program_id,
    f.program_code          = r.program_code_clean,
    f.enrollment_start_date     = r.enrollment_start_date,
    f.enrollment_start_date_key = CAST(CONVERT(VARCHAR(8), r.enrollment_start_date, 112) AS INT),
    f.enrollment_exit_date      = r.enrollment_exit_date,
    f.enrollment_exit_date_key  = CASE WHEN r.enrollment_exit_date IS NULL THEN NULL
                                       ELSE CAST(CONVERT(VARCHAR(8), r.enrollment_exit_date, 112) AS INT) END,
    f.placement_date        = r.placement_date,
    f.enrollment_status     = r.enrollment_status,
    f.grant_name            = r.grant_name,
    f.hvrp_participant_type = COALESCE(r.hvrp_participant_type_clean, f.hvrp_participant_type),
    f.is_co_enrollment      = COALESCE(r.is_co_enrollment, f.is_co_enrollment),
    f.service_type          = r.service_type,
    f.is_carry_over         = r.is_carry_over,
    f.referral_agency       = r.referral_agency,
    f.referred_to_workforce = r.referred_to_workforce,
    f.workforce_referral_name = r.workforce_referral_name,
    f.is_placed             = r.is_placed,
    f.starting_hourly_wage  = r.starting_hourly_wage,
    f.source_client_id      = COALESCE(r.resolved_client_source_id, f.source_client_id),
    f.disability_status       = COALESCE(r.resolved_disability_status,       f.disability_status),
    f.mental_health_condition = COALESCE(r.resolved_mental_health_condition, f.mental_health_condition),
    f.substance_abuse         = COALESCE(r.resolved_substance_abuse,         f.substance_abuse),
    f.times_homeless_3yr      = COALESCE(r.resolved_times_homeless_3yr,      f.times_homeless_3yr),
    f.is_deleted            = 0,
    f.deleted_detected_datetime = NULL,
    f.load_datetime         = @etl_time
FROM fact_enrollment f
JOIN #er r
  ON f.source_system = 'sf_cis'
 AND dbo.fn_normalize_key(f.source_record_id) = dbo.fn_normalize_key(r.source_record_id_clean)
WHERE NOT EXISTS (SELECT 1 FROM #bad_er bd WHERE bd.record_id = r.source_record_id_clean)
  AND r.enrollment_start_date IS NOT NULL
  AND r.resolved_client_key IS NOT NULL
  AND NOT EXISTS (
    SELECT f.client_key, f.client_version_key, f.program_id, f.program_code,
           f.enrollment_start_date, f.enrollment_exit_date, f.placement_date,
           f.enrollment_status, f.grant_name, f.hvrp_participant_type, f.is_co_enrollment,
           f.service_type, f.is_carry_over, f.referral_agency, f.referred_to_workforce, f.workforce_referral_name,
           f.is_placed, f.starting_hourly_wage, f.source_client_id,
           f.disability_status, f.mental_health_condition, f.substance_abuse, f.times_homeless_3yr,
           ISNULL(f.is_deleted, 0)
    INTERSECT
    SELECT r.resolved_client_key,
           CASE WHEN f.client_version_key IS NULL OR f.client_key <> r.resolved_client_key
                THEN r.resolved_client_version_key ELSE f.client_version_key END,
           r.program_id, r.program_code_clean,
           r.enrollment_start_date, r.enrollment_exit_date, r.placement_date,
           r.enrollment_status, r.grant_name, COALESCE(r.hvrp_participant_type_clean, f.hvrp_participant_type),
           COALESCE(r.is_co_enrollment, f.is_co_enrollment),
           r.service_type, r.is_carry_over, r.referral_agency, r.referred_to_workforce, r.workforce_referral_name,
           r.is_placed, r.starting_hourly_wage, COALESCE(r.resolved_client_source_id, f.source_client_id),
           COALESCE(r.resolved_disability_status, f.disability_status),
           COALESCE(r.resolved_mental_health_condition, f.mental_health_condition),
           COALESCE(r.resolved_substance_abuse, f.substance_abuse),
           COALESCE(r.resolved_times_homeless_3yr, f.times_homeless_3yr),
           0);

UPDATE fact_enrollment
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = 'sf_cis' AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #enr e WHERE dbo.fn_normalize_key(e.source_record_id_clean) = dbo.fn_normalize_key(fact_enrollment.source_record_id));

-- 4b. fact_assessment: one row per Salesforce contact assessment ('<Contact Id>|assessment')
DROP TABLE IF EXISTS #asm;
SELECT
    x.*,
    LTRIM(RTRIM(x.source_record_id))      AS source_record_id_clean,
    LTRIM(RTRIM(x.salesforce_id))         AS salesforce_id_clean,
    dbo.fn_normalize_key(x.salesforce_id) AS salesforce_key
INTO #asm
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY LTRIM(RTRIM(source_record_id))
                                 ORDER BY source_last_updated DESC) AS rn
    FROM #sf_assessment
    WHERE source_record_id IS NOT NULL AND LTRIM(RTRIM(source_record_id)) <> ''
      AND salesforce_id IS NOT NULL AND LTRIM(RTRIM(salesforce_id)) <> ''
) x
WHERE x.rn = 1;

DROP TABLE IF EXISTS #ar;
SELECT
    a.*,
    dc.client_key         AS resolved_client_key,
    dc.client_version_key AS resolved_client_version_key,
    CASE WHEN a.assessment_outcome = 'Enrolled' THEN pe.enrollment_key END AS resolved_enrollment_key,
    CASE WHEN a.assessment_date IS NULL THEN NULL
         ELSE CAST(CONVERT(VARCHAR(8), a.assessment_date, 112) AS INT) END AS assessment_date_key
INTO #ar
FROM #asm a
LEFT JOIN dim_client dc
  ON dc.source_system = 'sf_cis'
 AND dc.is_current = 1
 AND dbo.fn_normalize_key(dc.salesforce_id) = a.salesforce_key
LEFT JOIN (SELECT client_key, MAX(enrollment_key) AS enrollment_key
           FROM fact_enrollment
           WHERE source_system = 'sf_cis' AND ISNULL(is_co_enrollment, 0) = 0 AND ISNULL(is_deleted, 0) = 0
           GROUP BY client_key) pe
  ON pe.client_key = dc.client_key;

-- A date outside dim_date (assessment_date) would load a date key with no calendar row, breaking a
-- declared key and hiding the row from date filters: hold the record back and list it in v_dq_corrections.
DROP TABLE IF EXISTS #bad_ar;
SELECT r.source_record_id_clean AS record_id,
       CASE
            WHEN CAST(r.assessment_date AS DATE) NOT BETWEEN @cal_min AND @cal_max THEN 'assessment_date'
       END AS bad_column,
       r.salesforce_id_clean AS client_ref, r.program_code AS program_ref
INTO #bad_ar
FROM #ar r
WHERE (r.resolved_client_key IS NOT NULL)
  AND (CAST(r.assessment_date AS DATE) NOT BETWEEN @cal_min AND @cal_max);

INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, pipeline_run_id
)
SELECT @run_id, 'sf_cis', 'fact_assessment', b.record_id, CONCAT('date_outside_calendar:', b.bad_column),
       b.client_ref, b.program_ref, @etl_time, @pipeline_run_id
FROM #bad_ar b;

INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, pipeline_run_id
)
SELECT @run_id, 'sf_cis', 'fact_assessment', r.source_record_id_clean, 'orphan_client',
       r.salesforce_id_clean, r.program_code, @etl_time, @pipeline_run_id
FROM #ar r
WHERE r.resolved_client_key IS NULL;

INSERT INTO fact_assessment (
    client_key, client_version_key, enrollment_key, program_id,
    assessment_date, assessment_date_key, assessment_type, assessing_program, assessment_status,
    assessment_source, source_system, source_record_id, source_assessment_id, source_client_id,
    source_added_date, source_last_updated, is_deleted, load_datetime
)
SELECT
    r.resolved_client_key, r.resolved_client_version_key, r.resolved_enrollment_key, r.program_id,
    r.assessment_date, r.assessment_date_key, r.assessment_type, r.program_code, r.assessment_outcome,
    r.assessment_source, 'sf_cis', r.source_record_id_clean, r.source_assessment_id, r.salesforce_id_clean,
    r.source_added_date, r.source_last_updated, 0, @etl_time
FROM #ar r
WHERE NOT EXISTS (SELECT 1 FROM #bad_ar bd WHERE bd.record_id = r.source_record_id_clean)
  AND r.resolved_client_key IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM fact_assessment f
    WHERE f.source_system = 'sf_cis'
      AND dbo.fn_normalize_key(f.source_record_id) = dbo.fn_normalize_key(r.source_record_id_clean)
  );

-- source_last_updated changes whenever anything on the contact is edited, so it is not compared:
-- it is refreshed only when the assessment itself changed.
UPDATE f
SET f.client_key          = r.resolved_client_key,
    f.client_version_key  = CASE WHEN f.client_version_key IS NULL OR f.client_key <> r.resolved_client_key
                                 THEN r.resolved_client_version_key ELSE f.client_version_key END,
    f.enrollment_key      = r.resolved_enrollment_key,
    f.program_id          = r.program_id,
    f.assessment_date     = r.assessment_date,
    f.assessment_date_key = r.assessment_date_key,
    f.assessment_type     = r.assessment_type,
    f.assessing_program   = r.program_code,
    f.assessment_status   = r.assessment_outcome,
    f.assessment_source   = r.assessment_source,
    f.source_assessment_id = r.source_assessment_id,
    f.source_client_id    = r.salesforce_id_clean,
    f.source_added_date   = r.source_added_date,
    f.source_last_updated = r.source_last_updated,
    f.is_deleted          = 0,
    f.deleted_detected_datetime = NULL,
    f.load_datetime       = @etl_time
FROM fact_assessment f
JOIN #ar r
  ON f.source_system = 'sf_cis'
 AND dbo.fn_normalize_key(f.source_record_id) = dbo.fn_normalize_key(r.source_record_id_clean)
WHERE NOT EXISTS (SELECT 1 FROM #bad_ar bd WHERE bd.record_id = r.source_record_id_clean)
  AND r.resolved_client_key IS NOT NULL
  AND NOT EXISTS (
    SELECT f.client_key, f.client_version_key, f.enrollment_key, f.program_id, f.assessment_date,
           f.assessment_type, f.assessing_program, f.assessment_status, f.assessment_source,
           f.source_assessment_id, f.source_client_id, ISNULL(f.is_deleted, 0)
    INTERSECT
    SELECT r.resolved_client_key,
           CASE WHEN f.client_version_key IS NULL OR f.client_key <> r.resolved_client_key
                THEN r.resolved_client_version_key ELSE f.client_version_key END,
           r.resolved_enrollment_key, r.program_id, r.assessment_date,
           r.assessment_type, r.program_code, r.assessment_outcome, r.assessment_source,
           r.source_assessment_id, r.salesforce_id_clean, 0);

UPDATE fact_assessment
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = 'sf_cis' AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #ar a WHERE a.resolved_client_key IS NOT NULL
                  AND dbo.fn_normalize_key(a.source_record_id_clean) = dbo.fn_normalize_key(fact_assessment.source_record_id));

-- 5. dim_household: one synthetic single-person household per Salesforce client with an enrollment
DROP TABLE IF EXISTS #synthetic_households_needed;
SELECT DISTINCT e.client_key
INTO #synthetic_households_needed
FROM fact_enrollment e
WHERE e.household_key IS NULL AND e.client_key IS NOT NULL
  AND e.source_system = 'sf_cis';

INSERT INTO dim_household (
    household_name, household_type, household_size,
    household_origin, synthetic_for_client_key,
    source_system, source_agency, is_deleted, load_datetime
)
SELECT
    CONCAT('Synthetic household for client_key ', s.client_key),
    'Single Person', 1, 'synthetic', s.client_key, 'sf_cis', NULL, 0, @etl_time
FROM #synthetic_households_needed s
WHERE NOT EXISTS (
    SELECT 1 FROM dim_household h
    WHERE h.household_origin = 'synthetic'
      AND h.synthetic_for_client_key = s.client_key
      AND h.source_system = 'sf_cis'
);

UPDATE e
SET e.household_key = h.household_key,
    e.load_datetime = @etl_time
FROM fact_enrollment e
JOIN dim_household h
  ON h.household_origin = 'synthetic'
 AND h.synthetic_for_client_key = e.client_key
 AND h.source_system = 'sf_cis'
WHERE e.household_key IS NULL
  AND e.source_system = 'sf_cis';

UPDATE dc
SET dc.current_household_key = h.household_key,
    dc.load_datetime = @etl_time
FROM dim_client dc
JOIN dim_household h
  ON h.household_origin = 'synthetic'
 AND h.synthetic_for_client_key = dc.client_key
 AND h.source_system = 'sf_cis'
WHERE dc.is_current = 1 AND dc.current_household_key IS NULL
  AND dc.source_system = 'sf_cis';

-- A synthetic household is live while any live enrollment uses it
UPDATE h
SET h.is_deleted = 0, h.deleted_detected_datetime = NULL, h.load_datetime = @etl_time
FROM dim_household h
WHERE h.household_origin = 'synthetic' AND h.source_system = 'sf_cis' AND h.is_deleted = 1
  AND EXISTS (SELECT 1 FROM fact_enrollment e WHERE e.household_key = h.household_key AND ISNULL(e.is_deleted, 0) = 0);

UPDATE h
SET h.is_deleted = 1, h.deleted_detected_datetime = @etl_time, h.load_datetime = @etl_time
FROM dim_household h
WHERE h.household_origin = 'synthetic' AND h.source_system = 'sf_cis' AND ISNULL(h.is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM fact_enrollment e WHERE e.household_key = h.household_key AND ISNULL(e.is_deleted, 0) = 0);

-- 6. bridge_household_client: one row per live enrollment
SET @del_hh_links = (SELECT COUNT(*) FROM bridge_household_client b WHERE b.source_system = 'sf_cis'
    AND EXISTS (SELECT 1 FROM fact_enrollment e WHERE e.enrollment_key = b.enrollment_key AND e.is_deleted = 1));
DELETE FROM bridge_household_client
WHERE source_system = 'sf_cis'
  AND EXISTS (SELECT 1 FROM fact_enrollment e
              WHERE e.enrollment_key = bridge_household_client.enrollment_key AND e.is_deleted = 1);

INSERT INTO bridge_household_client (
    household_key, client_key, enrollment_key,
    relationship_to_hoh, is_head_of_household,
    start_date, end_date, is_current,
    source_system, source_enrollment_id, source_household_id,
    source_client_id, source_personal_id, load_datetime
)
SELECT
    e.household_key, e.client_key, e.enrollment_key,
    e.relationship_to_hoh, e.is_head_of_household,
    e.enrollment_start_date, e.enrollment_exit_date,
    CASE WHEN e.enrollment_exit_date IS NULL THEN 1 ELSE 0 END,
    e.source_system, e.source_enrollment_id, e.source_household_id,
    e.source_client_id, e.source_personal_id, @etl_time
FROM fact_enrollment e
WHERE e.household_key IS NOT NULL AND e.client_key IS NOT NULL
  AND e.source_system = 'sf_cis'
  AND ISNULL(e.is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM bridge_household_client b WHERE b.enrollment_key = e.enrollment_key);

UPDATE b
SET b.household_key = e.household_key,
    b.client_key    = e.client_key,
    b.start_date    = e.enrollment_start_date,
    b.end_date      = e.enrollment_exit_date,
    b.is_current    = CASE WHEN e.enrollment_exit_date IS NULL THEN 1 ELSE 0 END,
    b.load_datetime = @etl_time
FROM bridge_household_client b
JOIN fact_enrollment e ON e.enrollment_key = b.enrollment_key
WHERE e.source_system = 'sf_cis'
  AND NOT EXISTS (
    SELECT b.household_key, b.client_key, b.start_date, b.end_date, b.is_current
    INTERSECT
    SELECT e.household_key, e.client_key, e.enrollment_start_date, e.enrollment_exit_date,
           CASE WHEN e.enrollment_exit_date IS NULL THEN 1 ELSE 0 END);

-- 7. Audit: one stage_load_log row per table
INSERT INTO stage_load_log (
    source_system, entity_name, source_file_name, source_file_date,
    bronze_load_datetime, silver_load_datetime, warehouse_load_datetime,
    row_count_source, row_count_loaded, load_status, error_message,
    run_id, pipeline_run_id, rows_changed, rows_deleted
)
SELECT 'sf_cis', t.entity_name, NULL, NULL, NULL, t.silver_time, @etl_time,
       t.n_source, t.n_live, 'SUCCESS', NULL, @run_id, @pipeline_run_id, t.n_changed, t.n_deleted
FROM (
    SELECT 'dim_program' AS entity_name, (SELECT MAX(load_datetime) FROM #sf_program) AS silver_time,
           (SELECT COUNT(*) FROM #prog) AS n_source,
           (SELECT COUNT(*) FROM dim_program WHERE source_system = 'sf_cis' AND ISNULL(is_deleted, 0) = 0) AS n_live,
           (SELECT COUNT(*) FROM dim_program WHERE source_system = 'sf_cis' AND load_datetime = @etl_time) AS n_changed,
           (SELECT COUNT(*) FROM dim_program WHERE source_system = 'sf_cis' AND deleted_detected_datetime = @etl_time) AS n_deleted
    UNION ALL SELECT 'dim_client', (SELECT MAX(load_datetime) FROM #sf_client), (SELECT COUNT(*) FROM #client_staged),
           (SELECT COUNT(*) FROM dim_client WHERE source_system = 'sf_cis' AND is_current = 1 AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM dim_client WHERE source_system = 'sf_cis' AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM dim_client WHERE source_system = 'sf_cis' AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'fact_enrollment', (SELECT MAX(load_datetime) FROM #sf_enrollment), (SELECT COUNT(*) FROM #enr),
           (SELECT COUNT(*) FROM fact_enrollment WHERE source_system = 'sf_cis' AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM fact_enrollment WHERE source_system = 'sf_cis' AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM fact_enrollment WHERE source_system = 'sf_cis' AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'fact_assessment', (SELECT MAX(load_datetime) FROM #sf_assessment), (SELECT COUNT(*) FROM #asm),
           (SELECT COUNT(*) FROM fact_assessment WHERE source_system = 'sf_cis' AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM fact_assessment WHERE source_system = 'sf_cis' AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM fact_assessment WHERE source_system = 'sf_cis' AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'dim_program_history', NULL, NULL,
           (SELECT COUNT(*) FROM dim_program_history WHERE source_system = 'sf_cis' AND is_current = 1),
           (SELECT COUNT(*) FROM dim_program_history WHERE source_system = 'sf_cis' AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM dim_program_history WHERE source_system = 'sf_cis' AND valid_to = @etl_time)
    UNION ALL SELECT 'dim_household', NULL, NULL,
           (SELECT COUNT(*) FROM dim_household WHERE source_system = 'sf_cis' AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM dim_household WHERE source_system = 'sf_cis' AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM dim_household WHERE source_system = 'sf_cis' AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'bridge_household_client', NULL, NULL,
           (SELECT COUNT(*) FROM bridge_household_client WHERE source_system = 'sf_cis'),
           (SELECT COUNT(*) FROM bridge_household_client WHERE source_system = 'sf_cis' AND load_datetime = @etl_time),
           @del_hh_links
) t;

-- 8. Integrity checks. Any problem rolls the whole run back.
DECLARE @problems VARCHAR(4000) = NULL;
SELECT @problems = STRING_AGG(CAST(CONCAT(check_name, ' (', problem_rows, ')') AS VARCHAR(MAX)), '; ')
FROM (
    SELECT 'enrollment source_record_id duplicated' AS check_name, COUNT(*) AS problem_rows
    FROM (SELECT source_record_id FROM fact_enrollment WHERE source_system = 'sf_cis'
          GROUP BY source_record_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'assessment source_record_id duplicated', COUNT(*)
    FROM (SELECT source_record_id FROM fact_assessment WHERE source_system = 'sf_cis'
          GROUP BY source_record_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'fact_assessment pointing at a missing client version', COUNT(*)
    FROM fact_assessment f WHERE f.source_system = 'sf_cis' AND f.client_version_key IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM dim_client d WHERE d.client_version_key = f.client_version_key)
    UNION ALL SELECT 'assessment pointing at a deleted or missing enrollment', COUNT(*)
    FROM fact_assessment a WHERE a.source_system = 'sf_cis' AND a.enrollment_key IS NOT NULL AND ISNULL(a.is_deleted, 0) = 0
      AND NOT EXISTS (SELECT 1 FROM fact_enrollment e WHERE e.enrollment_key = a.enrollment_key AND ISNULL(e.is_deleted, 0) = 0)
    UNION ALL SELECT 'client_key with more than one current version', COUNT(*)
    FROM (SELECT client_key FROM dim_client WHERE source_system = 'sf_cis' AND is_current = 1
          GROUP BY client_key HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'client_key with no current version', COUNT(*)
    FROM (SELECT client_key FROM dim_client WHERE source_system = 'sf_cis'
          GROUP BY client_key HAVING SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END) = 0) x
    UNION ALL SELECT 'Salesforce Id on more than one client_key', COUNT(*)
    FROM (SELECT dbo.fn_normalize_key(salesforce_id) AS k FROM dim_client WHERE source_system = 'sf_cis'
          GROUP BY dbo.fn_normalize_key(salesforce_id) HAVING COUNT(DISTINCT client_key) > 1) x
    UNION ALL SELECT 'overlapping client versions', COUNT(*)
    FROM dim_client a
    JOIN dim_client b
      ON a.client_key = b.client_key AND a.client_version_key < b.client_version_key
     AND a.source_system = 'sf_cis' AND b.source_system = 'sf_cis'
    WHERE a.valid_from < ISNULL(b.valid_to, CAST('9999-12-31' AS DATETIME2(6)))
      AND b.valid_from < ISNULL(a.valid_to, CAST('9999-12-31' AS DATETIME2(6)))
    UNION ALL SELECT 'closed client version with no next version', COUNT(*)
    FROM dim_client a
    WHERE a.source_system = 'sf_cis' AND a.valid_to IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM dim_client b WHERE b.client_key = a.client_key AND b.valid_from = a.valid_to)
    UNION ALL SELECT 'fact_enrollment pointing at a missing client version', COUNT(*)
    FROM fact_enrollment f WHERE f.source_system = 'sf_cis' AND f.client_version_key IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM dim_client d WHERE d.client_version_key = f.client_version_key)
    UNION ALL SELECT 'bridge_household_client duplicated', COUNT(*)
    FROM (SELECT enrollment_key FROM bridge_household_client WHERE source_system = 'sf_cis'
          GROUP BY enrollment_key HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'synthetic household duplicated', COUNT(*)
    FROM (SELECT synthetic_for_client_key FROM dim_household WHERE source_system = 'sf_cis' AND household_origin = 'synthetic'
          GROUP BY synthetic_for_client_key HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'dim_program program_id duplicated', COUNT(*)
    FROM (SELECT program_id FROM dim_program GROUP BY program_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'Salesforce program_id outside 10001-19999', COUNT(*)
    FROM dim_program WHERE source_system = 'sf_cis' AND program_id NOT BETWEEN 10001 AND 19999
    UNION ALL SELECT 'program with no current history row', COUNT(*)
    FROM dim_program p WHERE p.source_system = 'sf_cis'
      AND NOT EXISTS (SELECT 1 FROM dim_program_history h WHERE h.program_id = p.program_id AND h.is_current = 1)
    UNION ALL SELECT 'program with more than one current history row', COUNT(*)
    FROM (SELECT program_id FROM dim_program_history WHERE source_system = 'sf_cis' AND is_current = 1
          GROUP BY program_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'program history versions overlap', COUNT(*)
    FROM dim_program_history a JOIN dim_program_history b
      ON b.program_id = a.program_id AND b.program_history_key <> a.program_history_key
    WHERE a.source_system = 'sf_cis'
      AND a.valid_from < ISNULL(b.valid_to, CAST('9999-12-31' AS DATETIME2(6)))
      AND b.valid_from < ISNULL(a.valid_to, CAST('9999-12-31' AS DATETIME2(6)))
    UNION ALL SELECT 'required column empty in fact_enrollment', COUNT(*)
    FROM fact_enrollment WHERE source_system = 'sf_cis' AND (source_record_id IS NULL OR client_key IS NULL OR client_version_key IS NULL OR load_datetime IS NULL)
    UNION ALL SELECT 'required column empty in fact_assessment', COUNT(*)
    FROM fact_assessment WHERE source_system = 'sf_cis' AND (source_record_id IS NULL OR client_key IS NULL OR client_version_key IS NULL OR load_datetime IS NULL)
    UNION ALL SELECT 'required column empty in dim_program', COUNT(*)
    FROM dim_program WHERE source_system = 'sf_cis' AND (program_code IS NULL OR load_datetime IS NULL)
    -- BEGIN declared-key checks (generated from tools/key_checks.py; do not edit by hand)
    UNION ALL SELECT 'FK_bridge_client_source_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.bridge_client_source c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_bridge_client_source: duplicate client_source_key', (SELECT COUNT(*) FROM (SELECT client_source_key FROM dbo.bridge_client_source WHERE client_source_key IS NOT NULL GROUP BY client_source_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_bridge_client_source: empty client_source_key', (SELECT COUNT(*) FROM dbo.bridge_client_source WHERE client_source_key IS NULL)
    UNION ALL SELECT 'FK_bridge_household_client_enrollment: enrollment_key not in fact_enrollment', (SELECT COUNT(*) FROM dbo.bridge_household_client c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key))
    UNION ALL SELECT 'FK_bridge_household_client_household: household_key not in dim_household', (SELECT COUNT(*) FROM dbo.bridge_household_client c WHERE c.household_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_household p WHERE p.household_key = c.household_key))
    UNION ALL SELECT 'FK_bridge_household_client_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.bridge_household_client c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_bridge_household_client: duplicate household_client_key', (SELECT COUNT(*) FROM (SELECT household_client_key FROM dbo.bridge_household_client WHERE household_client_key IS NOT NULL GROUP BY household_client_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_bridge_household_client: empty household_client_key', (SELECT COUNT(*) FROM dbo.bridge_household_client WHERE household_client_key IS NULL)
    UNION ALL SELECT 'FK_dim_client_current_household: current_household_key not in dim_household', (SELECT COUNT(*) FROM dbo.dim_client c WHERE c.current_household_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_household p WHERE p.household_key = c.current_household_key))
    UNION ALL SELECT 'FK_dim_client_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.dim_client c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_dim_client: duplicate client_version_key', (SELECT COUNT(*) FROM (SELECT client_version_key FROM dbo.dim_client WHERE client_version_key IS NOT NULL GROUP BY client_version_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_dim_client: empty client_version_key', (SELECT COUNT(*) FROM dbo.dim_client WHERE client_version_key IS NULL)
    UNION ALL SELECT 'UQ_dim_client_version_start: duplicate client_key, valid_from', (SELECT COUNT(*) FROM (SELECT client_key, valid_from FROM dbo.dim_client WHERE client_key IS NOT NULL AND valid_from IS NOT NULL GROUP BY client_key, valid_from HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'FK_dim_household_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.dim_household c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_dim_household: duplicate household_key', (SELECT COUNT(*) FROM (SELECT household_key FROM dbo.dim_household WHERE household_key IS NOT NULL GROUP BY household_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_dim_household: empty household_key', (SELECT COUNT(*) FROM dbo.dim_household WHERE household_key IS NULL)
    UNION ALL SELECT 'FK_dim_program_organization: organization_key not in dim_organization', (SELECT COUNT(*) FROM dbo.dim_program c WHERE c.organization_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_organization p WHERE p.organization_key = c.organization_key))
    UNION ALL SELECT 'FK_dim_program_site: site_key not in dim_site', (SELECT COUNT(*) FROM dbo.dim_program c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key))
    UNION ALL SELECT 'FK_dim_program_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.dim_program c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_dim_program: duplicate program_id', (SELECT COUNT(*) FROM (SELECT program_id FROM dbo.dim_program WHERE program_id IS NOT NULL GROUP BY program_id HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_dim_program: empty program_id', (SELECT COUNT(*) FROM dbo.dim_program WHERE program_id IS NULL)
    UNION ALL SELECT 'UQ_dim_program_code: duplicate program_code', (SELECT COUNT(*) FROM (SELECT program_code FROM dbo.dim_program WHERE program_code IS NOT NULL GROUP BY program_code HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'FK_dim_program_history_organization: organization_key not in dim_organization', (SELECT COUNT(*) FROM dbo.dim_program_history c WHERE c.organization_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_organization p WHERE p.organization_key = c.organization_key))
    UNION ALL SELECT 'FK_dim_program_history_program: program_id not in dim_program', (SELECT COUNT(*) FROM dbo.dim_program_history c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id))
    UNION ALL SELECT 'PK_dim_program_history: duplicate program_history_key', (SELECT COUNT(*) FROM (SELECT program_history_key FROM dbo.dim_program_history WHERE program_history_key IS NOT NULL GROUP BY program_history_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_dim_program_history: empty program_history_key', (SELECT COUNT(*) FROM dbo.dim_program_history WHERE program_history_key IS NULL)
    UNION ALL SELECT 'UQ_dim_program_history_start: duplicate program_id, valid_from', (SELECT COUNT(*) FROM (SELECT program_id, valid_from FROM dbo.dim_program_history WHERE program_id IS NOT NULL AND valid_from IS NOT NULL GROUP BY program_id, valid_from HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'FK_fact_assessment_client_version: client_version_key not in dim_client', (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key))
    UNION ALL SELECT 'FK_fact_assessment_date: assessment_date_key not in dim_date', (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.assessment_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.assessment_date_key))
    UNION ALL SELECT 'FK_fact_assessment_enrollment: enrollment_key not in fact_enrollment', (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key))
    UNION ALL SELECT 'FK_fact_assessment_household: household_key not in dim_household', (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.household_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_household p WHERE p.household_key = c.household_key))
    UNION ALL SELECT 'FK_fact_assessment_program: program_id not in dim_program', (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id))
    UNION ALL SELECT 'FK_fact_assessment_site: site_key not in dim_site', (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key))
    UNION ALL SELECT 'FK_fact_assessment_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'FK_fact_assessment_staff: staff_key not in dim_staff', (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.staff_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_staff p WHERE p.staff_key = c.staff_key))
    UNION ALL SELECT 'PK_fact_assessment: duplicate assessment_key', (SELECT COUNT(*) FROM (SELECT assessment_key FROM dbo.fact_assessment WHERE assessment_key IS NOT NULL GROUP BY assessment_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_fact_assessment: empty assessment_key', (SELECT COUNT(*) FROM dbo.fact_assessment WHERE assessment_key IS NULL)
    UNION ALL SELECT 'UQ_fact_assessment_source_record: duplicate source_system, source_record_id', (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_assessment WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'FK_fact_enrollment_client_version: client_version_key not in dim_client', (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key))
    UNION ALL SELECT 'FK_fact_enrollment_engagement_date: date_of_engagement_key not in dim_date', (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.date_of_engagement_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.date_of_engagement_key))
    UNION ALL SELECT 'FK_fact_enrollment_exit_date: enrollment_exit_date_key not in dim_date', (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.enrollment_exit_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.enrollment_exit_date_key))
    UNION ALL SELECT 'FK_fact_enrollment_grant: grant_key not in dim_grant', (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.grant_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_grant p WHERE p.grant_key = c.grant_key))
    UNION ALL SELECT 'FK_fact_enrollment_household: household_key not in dim_household', (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.household_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_household p WHERE p.household_key = c.household_key))
    UNION ALL SELECT 'FK_fact_enrollment_move_in_date: housing_move_in_date_key not in dim_date', (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.housing_move_in_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.housing_move_in_date_key))
    UNION ALL SELECT 'FK_fact_enrollment_program: program_id not in dim_program', (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id))
    UNION ALL SELECT 'FK_fact_enrollment_site: site_key not in dim_site', (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key))
    UNION ALL SELECT 'FK_fact_enrollment_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'FK_fact_enrollment_staff: staff_key not in dim_staff', (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.staff_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_staff p WHERE p.staff_key = c.staff_key))
    UNION ALL SELECT 'FK_fact_enrollment_start_date: enrollment_start_date_key not in dim_date', (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.enrollment_start_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.enrollment_start_date_key))
    UNION ALL SELECT 'PK_fact_enrollment: duplicate enrollment_key', (SELECT COUNT(*) FROM (SELECT enrollment_key FROM dbo.fact_enrollment WHERE enrollment_key IS NOT NULL GROUP BY enrollment_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_fact_enrollment: empty enrollment_key', (SELECT COUNT(*) FROM dbo.fact_enrollment WHERE enrollment_key IS NULL)
    UNION ALL SELECT 'UQ_fact_enrollment_source_record: duplicate source_system, source_record_id', (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_enrollment WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'FK_dim_program_history_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.dim_program_history c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'UQ_bridge_household_client_enrollment: duplicate enrollment_key', (SELECT COUNT(*) FROM (SELECT enrollment_key FROM dbo.bridge_household_client WHERE enrollment_key IS NOT NULL GROUP BY enrollment_key HAVING COUNT(*) > 1) d)
    -- END declared-key checks
) c
WHERE problem_rows > 0;

IF @problems IS NOT NULL
BEGIN
    DECLARE @int_msg NVARCHAR(4000) = CONCAT('Integrity check failed for sf_cis, nothing was saved: ', @problems);
    THROW 50012, @int_msg, 1;
END;

-- 9. LOG RUN END (inside the transaction)
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'SUCCESS'
WHERE script_name = '03a_load_sf_cis' AND run_start = @etl_time;

UPDATE etl_source_config
SET last_success_datetime = SYSDATETIME(),
    updated_by            = USER_NAME(),
    updated_datetime      = SYSDATETIME()
WHERE source_system = 'sf_cis';

COMMIT TRANSACTION;

-- 10. What changed this run
SELECT entity_name, row_count_source, row_count_loaded AS live_rows, rows_changed, rows_deleted
FROM stage_load_log
WHERE run_id = @run_id
ORDER BY entity_name;

SELECT reject_reason, COUNT(*) AS cnt
FROM stage_load_reject
WHERE run_id = @run_id
GROUP BY reject_reason
ORDER BY cnt DESC;

SELECT fact_table, column_name, unresolved_rows
FROM v_data_quality_unresolved
WHERE unresolved_rows > 0
ORDER BY unresolved_rows DESC;

END TRY
BEGIN CATCH

    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    INSERT INTO stage_load_log (
        source_system, entity_name, source_file_name, source_file_date,
        bronze_load_datetime, silver_load_datetime, warehouse_load_datetime,
        row_count_source, row_count_loaded, load_status, error_message,
        run_id, pipeline_run_id, rows_changed, rows_deleted
    )
    VALUES ('sf_cis', 'ALL', NULL, NULL, NULL, NULL, SYSDATETIME(),
            NULL, NULL, 'FAILED', ERROR_MESSAGE(), @run_id, @pipeline_run_id, 0, 0);

    UPDATE dbo.warehouse_run_log
    SET run_end = SYSDATETIME(), run_status = 'FAILED', error_message = ERROR_MESSAGE()
    WHERE script_name = '03a_load_sf_cis' AND run_start = @etl_time;

    THROW;

END CATCH;

END;
GO

-- EXEC dbo.usp_load_sf_cis;
-- After checking a large real removal in Salesforce:
-- EXEC dbo.usp_load_sf_cis @allow_mass_delete = 1;
