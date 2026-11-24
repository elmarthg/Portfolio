-- 03b_load_hmis_clarity v3.3: installs dbo.usp_load_hmis_clarity
-- Loads one Clarity county from lh_silver.dbo.conf_hmis_clarity_* into a PERSISTENT warehouse.
-- Pipeline: EXEC dbo.usp_load_hmis_clarity @src = 'hmis_lac',
--               @min_silver_load_datetime = '<TriggerTime>', @pipeline_run_id = '<pipeline().RunId>';
--
-- v3.3 (schema v8.19, needs migrations/0006): a record with a date outside dim_date (2000 to 2040) is held back and
--   listed in v_dq_corrections (reason 'date_outside_calendar:<column>'): enrollments, services, case notes, bed
--   status, bed nights and ROIs. Every declared key on the tables this procedure writes is checked before COMMIT
--   (generated list), including the new business keys on organizations, grants, funding sources, service items and beds.
--
-- v3.2 (schema v8.18, needs migrations/0005): bed nights that leave the export are flagged (is_deleted) and
--   restored if they come back, like every other fact, instead of deleted; dim_bed no longer writes is_current
--   (beds are updated in place, never versioned); dim_client.current_household_key (renamed from
--   household_key); program history in dim_program_history; required columns checked before commit.
--
-- v3.1 (schema v8.16, needs migrations/0003):
--   Clarity merges become links. When Clarity merges two clients it keeps one Personal ID and moves
--   the other's enrollments to it. The loader sees an enrollment it holds under Personal ID X arrive
--   under Y, with X gone from the client export and Y in it, and records an exact-rule 'match'
--   decision for X and Y (rule_name 'clarity_merge'). usp_refresh_person_links then links the two
--   profiles; X keeps its history, flagged deleted. A reviewer's decision on the pair is never
--   replaced. Logged in stage_load_log as entity 'clarity_merge'.
--
-- v3.0 (schema v8.14, needs migrations/0001):
--   1. Persistent: nothing is wiped. Each run inserts new rows, updates only rows whose values
--      changed, and flags rows that left the export (is_deleted = 1 + deleted_detected_datetime).
--      A row that comes back is restored. Running twice on the same Silver changes nothing.
--   2. One transaction. All writes commit together or not at all; on any error the warehouse
--      is exactly as it was before the run.
--   3. One timestamp per run (@etl_time) for load_datetime, version boundaries and delete flags.
--      Client versions are half-open: an old version's valid_to equals the next version's valid_from.
--   4. A fact keeps the client version it was loaded with. It is only repointed if the fact
--      moves to a different client_key.
--   5. client_key is permanent: it is found from any earlier version of the person, not only
--      the current one, so a key is never reissued.
--   6. Derived tables (enrollment-grant links, household links of deleted enrollments, the
--      restricted SSN table) are kept in step by insert/update/delete instead
--      of being emptied and rebuilt, so their keys stay stable too.
--   7. Lineage columns (source_file_name, source_file_date) and load_datetime are not compared,
--      so a new export file alone does not count as a change. length_of_stay_days is stored only
--      once an enrollment has exited (for open enrollments it would change every day).
--   8. Integrity checks run before COMMIT and roll the run back if anything is wrong.
--   9. stage_load_log gets one row per table: rows changed and rows flagged deleted this run.

DROP PROCEDURE IF EXISTS dbo.usp_load_hmis_clarity;
GO

CREATE PROCEDURE dbo.usp_load_hmis_clarity
    @src VARCHAR(50),
    @min_silver_load_datetime DATETIME2(6) = NULL,
    @allow_mass_delete BIT = 0,
    @pipeline_run_id VARCHAR(100) = NULL
AS
BEGIN

DECLARE @etl_time DATETIME2(6) = SYSDATETIME();
DECLARE @run_id VARCHAR(60) = CONCAT(@src, '_', CONVERT(VARCHAR(30), SYSDATETIME(), 126));
DECLARE @cal_min DATE = (SELECT MIN(full_date) FROM dim_date);
DECLARE @cal_max DATE = (SELECT MAX(full_date) FROM dim_date);

IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.19')
    THROW 50011, 'The warehouse schema is older than v8.19. Run migrations/0006_key_checks_and_dates.sql first.', 1;

BEGIN TRY

-- 0. LOG RUN START (outside the transaction, so a failed run still leaves a record)
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'ABANDONED'
WHERE script_name = '03b_load_hmis_clarity' AND run_status = 'RUNNING';

INSERT INTO dbo.warehouse_run_log (
    script_name, script_version, run_start, run_status, run_by, change_summary, pipeline_run_id
)
VALUES (
    '03b_load_hmis_clarity', 'v3.3', @etl_time, 'RUNNING', USER_NAME(),
    CONCAT('Persistent Clarity load for ', @src), @pipeline_run_id
);

-- 0b. Preflight (reads only)
IF NOT EXISTS (SELECT 1 FROM etl_source_config WHERE source_system = @src)
BEGIN
    DECLARE @cfg_msg NVARCHAR(2048) = CONCAT(@src, ' has no row in etl_source_config. Add one only after the CoC confirms in writing.');
    THROW 50005, @cfg_msg, 1;
END;

IF EXISTS (SELECT 1 FROM etl_source_config WHERE source_system = @src AND block_reason IS NOT NULL)
BEGIN
    DECLARE @block_msg NVARCHAR(2048) = CONCAT(@src, ' is blocked in etl_source_config: ',
        (SELECT block_reason FROM etl_source_config WHERE source_system = @src));
    THROW 50003, @block_msg, 1;
END;

IF NOT EXISTS (SELECT 1 FROM dim_source WHERE source_system = @src AND platform = 'Clarity')
BEGIN
    DECLARE @src_msg NVARCHAR(2048) = CONCAT(@src, ' is not a Clarity source in dim_source. Check the spelling or run 02_seed_reference_data.');
    THROW 50002, @src_msg, 1;
END;

DROP TABLE IF EXISTS #a;
DROP TABLE IF EXISTS #p;
DROP TABLE IF EXISTS #c;
DROP TABLE IF EXISTS #e;
DROP TABLE IF EXISTS #g;
DROP TABLE IF EXISTS #fs;
DROP TABLE IF EXISTS #s;
DROP TABLE IF EXISTS #si;
DROP TABLE IF EXISTS #n;
DROP TABLE IF EXISTS #hm;
DROP TABLE IF EXISTS #b;
DROP TABLE IF EXISTS #bs;
DROP TABLE IF EXISTS #bn;
DROP TABLE IF EXISTS #roi;
DROP TABLE IF EXISTS #eg;
SELECT * INTO #a   FROM lh_silver.dbo.conf_hmis_clarity_agency           WHERE source_system = @src;
SELECT * INTO #p   FROM lh_silver.dbo.conf_hmis_clarity_program          WHERE source_system = @src;
SELECT * INTO #c   FROM lh_silver.dbo.conf_hmis_clarity_client           WHERE source_system = @src;
SELECT * INTO #e   FROM lh_silver.dbo.conf_hmis_clarity_enrollment       WHERE source_system = @src;
SELECT * INTO #g   FROM lh_silver.dbo.conf_hmis_clarity_grant            WHERE source_system = @src;
SELECT * INTO #fs  FROM lh_silver.dbo.conf_hmis_clarity_funding_source   WHERE source_system = @src;
SELECT * INTO #s   FROM lh_silver.dbo.conf_hmis_clarity_service          WHERE source_system = @src;
SELECT * INTO #si  FROM lh_silver.dbo.conf_hmis_clarity_service_item     WHERE source_system = @src;
SELECT * INTO #n   FROM lh_silver.dbo.conf_hmis_clarity_case_note        WHERE source_system = @src;
SELECT * INTO #hm  FROM lh_silver.dbo.conf_hmis_clarity_household_member WHERE source_system = @src;
SELECT * INTO #b   FROM lh_silver.dbo.conf_hmis_clarity_bed              WHERE source_system = @src;
SELECT * INTO #bs  FROM lh_silver.dbo.conf_hmis_clarity_bed_status       WHERE source_system = @src;
SELECT * INTO #bn  FROM lh_silver.dbo.conf_hmis_clarity_bed_night        WHERE source_system = @src;
SELECT * INTO #roi FROM lh_silver.dbo.conf_hmis_clarity_roi              WHERE source_system = @src;
SELECT * INTO #eg  FROM lh_silver.dbo.conf_hmis_clarity_enrollment_grant WHERE source_system = @src;

DECLARE @n_a INT = (SELECT COUNT(*) FROM #a);
DECLARE @n_p INT = (SELECT COUNT(*) FROM #p);
DECLARE @n_c INT = (SELECT COUNT(*) FROM #c);
DECLARE @n_e INT = (SELECT COUNT(*) FROM #e);

IF @n_a = 0 OR @n_p = 0 OR @n_c = 0 OR @n_e = 0
BEGIN
    DECLARE @empty_msg NVARCHAR(2048) = CONCAT('No Silver rows for ', @src, '. Run the notebook for this county and let the SQL endpoint sync.');
    THROW 50001, @empty_msg, 1;
END;

IF @min_silver_load_datetime IS NOT NULL
BEGIN
    DECLARE @stale VARCHAR(400) = NULL;
    SELECT @stale = STRING_AGG(t, ', ') FROM (
        SELECT 'agency' AS t WHERE (SELECT MIN(load_datetime) FROM #a) < @min_silver_load_datetime
        UNION ALL SELECT 'program'    WHERE (SELECT MIN(load_datetime) FROM #p) < @min_silver_load_datetime
        UNION ALL SELECT 'client'     WHERE (SELECT MIN(load_datetime) FROM #c) < @min_silver_load_datetime
        UNION ALL SELECT 'enrollment' WHERE (SELECT MIN(load_datetime) FROM #e) < @min_silver_load_datetime
        UNION ALL SELECT 'grant'      WHERE (SELECT MIN(load_datetime) FROM #g) < @min_silver_load_datetime
        UNION ALL SELECT 'service'    WHERE (SELECT MIN(load_datetime) FROM #s) < @min_silver_load_datetime
        UNION ALL SELECT 'case_note'  WHERE (SELECT MIN(load_datetime) FROM #n) < @min_silver_load_datetime
        UNION ALL SELECT 'bed'        WHERE (SELECT MIN(load_datetime) FROM #b) < @min_silver_load_datetime
        UNION ALL SELECT 'funding'    WHERE (SELECT MIN(load_datetime) FROM #fs) < @min_silver_load_datetime
        UNION ALL SELECT 'roi'        WHERE (SELECT MIN(load_datetime) FROM #roi) < @min_silver_load_datetime
        UNION ALL SELECT 'enrollment_grant' WHERE (SELECT MIN(load_datetime) FROM #eg) < @min_silver_load_datetime
    ) x;
    IF @stale IS NOT NULL
    BEGIN
        DECLARE @stale_msg NVARCHAR(2048) = CONCAT('Silver for ', @src, ' is older than this run (', @stale,
            '). The notebook did not finish or the SQL endpoint has not synced yet. Wait and rerun.');
        THROW 50004, @stale_msg, 1;
    END;
END;

-- 0c. Records gone from this export, counted before anything changes
DECLARE @max_delete_pct DECIMAL(5,2) = 5.0;
DECLARE @live_c INT = (SELECT COUNT(*) FROM dim_client WHERE source_system = @src AND is_current = 1 AND ISNULL(is_deleted, 0) = 0);
DECLARE @gone_c INT = (SELECT COUNT(*) FROM dim_client d WHERE d.source_system = @src AND d.is_current = 1 AND ISNULL(d.is_deleted, 0) = 0
    AND NOT EXISTS (SELECT 1 FROM #c c WHERE dbo.fn_normalize_key(c.personal_id) = dbo.fn_normalize_key(d.hmis_personal_id)));
DECLARE @live_e INT = (SELECT COUNT(*) FROM fact_enrollment WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0);
DECLARE @gone_e INT = (SELECT COUNT(*) FROM fact_enrollment f WHERE f.source_system = @src AND ISNULL(f.is_deleted, 0) = 0
    AND NOT EXISTS (SELECT 1 FROM #e e WHERE dbo.fn_normalize_key(e.enrollment_id) = dbo.fn_normalize_key(f.source_record_id)));
DECLARE @live_s INT = (SELECT COUNT(*) FROM fact_service WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0);
DECLARE @gone_s INT = (SELECT COUNT(*) FROM fact_service f WHERE f.source_system = @src AND ISNULL(f.is_deleted, 0) = 0
    AND NOT EXISTS (SELECT 1 FROM #s x WHERE dbo.fn_normalize_key(x.service_id) = dbo.fn_normalize_key(f.source_record_id)));
DECLARE @live_n INT = (SELECT COUNT(*) FROM fact_case_note WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0);
DECLARE @gone_n INT = (SELECT COUNT(*) FROM fact_case_note f WHERE f.source_system = @src AND ISNULL(f.is_deleted, 0) = 0
    AND NOT EXISTS (SELECT 1 FROM #n x WHERE dbo.fn_normalize_key(x.note_id) = dbo.fn_normalize_key(f.source_record_id)));
DECLARE @live_r INT = (SELECT COUNT(*) FROM fact_consent WHERE source_system = @src AND consent_type = 'HMIS ROI' AND ISNULL(is_deleted, 0) = 0);
DECLARE @gone_r INT = (SELECT COUNT(*) FROM fact_consent f WHERE f.source_system = @src AND f.consent_type = 'HMIS ROI' AND ISNULL(f.is_deleted, 0) = 0
    AND NOT EXISTS (SELECT 1 FROM #roi x WHERE dbo.fn_normalize_key(x.roi_id) = dbo.fn_normalize_key(f.source_record_id)));

IF @allow_mass_delete = 0 AND (
       (@live_c > 0 AND @gone_c * 100.0 / @live_c > @max_delete_pct)
    OR (@live_e > 0 AND @gone_e * 100.0 / @live_e > @max_delete_pct)
    OR (@live_s > 0 AND @gone_s * 100.0 / @live_s > @max_delete_pct)
    OR (@live_n > 0 AND @gone_n * 100.0 / @live_n > @max_delete_pct)
    OR (@live_r > 0 AND @gone_r * 100.0 / @live_r > @max_delete_pct))
BEGIN
    DECLARE @del_msg NVARCHAR(2048) = CONCAT('More than ', @max_delete_pct, '% of ', @src,
        ' records would be marked deleted (clients ', @gone_c, '/', @live_c, ', enrollments ', @gone_e, '/', @live_e,
        ', services ', @gone_s, '/', @live_s, ', case notes ', @gone_n, '/', @live_n, ', ROIs ', @gone_r, '/', @live_r,
        '). Check the export. If it is real, rerun with @allow_mass_delete = 1.');
    THROW 50006, @del_msg, 1;
END;

DECLARE @agency_name VARCHAR(255) = (SELECT TOP 1 agency_name FROM #a ORDER BY agency_id);
DECLARE @coc_code VARCHAR(20) = (SELECT TOP 1 coc_code FROM #a ORDER BY agency_id);
DECLARE @del_ident INT = 0;
DECLARE @del_hh_links INT = 0;
DECLARE @del_bed_nights INT = 0;
DECLARE @del_eg_links INT = 0;
DECLARE @n_merge INT = 0;

-- =====================================================================
BEGIN TRANSACTION;
-- =====================================================================

-- 1. dim_organization
INSERT INTO dim_organization (
    hmis_organization_id, organization_name, organization_type,
    address_line1, city, state, postal_code, county, coc_code,
    source_system, source_agency, is_deleted, load_datetime
)
SELECT
    LTRIM(RTRIM(a.agency_id)), a.agency_name, a.organization_type,
    a.address_line1, a.city, a.state, a.postal_code, a.county, a.coc_code,
    @src, a.agency_name, 0, @etl_time
FROM #a a
WHERE NOT EXISTS (
    SELECT 1 FROM dim_organization o
    WHERE o.source_system = @src
      AND dbo.fn_normalize_key(o.hmis_organization_id) = dbo.fn_normalize_key(a.agency_id)
);

UPDATE o
SET o.organization_name = a.agency_name,
    o.organization_type = a.organization_type,
    o.address_line1     = a.address_line1,
    o.city              = a.city,
    o.state             = a.state,
    o.postal_code       = a.postal_code,
    o.county            = a.county,
    o.coc_code          = a.coc_code,
    o.source_agency     = a.agency_name,
    o.is_deleted        = 0,
    o.deleted_detected_datetime = NULL,
    o.load_datetime     = @etl_time
FROM dim_organization o
JOIN #a a
  ON o.source_system = @src
 AND dbo.fn_normalize_key(o.hmis_organization_id) = dbo.fn_normalize_key(a.agency_id)
WHERE NOT EXISTS (
    SELECT o.organization_name, o.organization_type, o.address_line1, o.city, o.state, o.postal_code,
           o.county, o.coc_code, o.source_agency, ISNULL(o.is_deleted, 0)
    INTERSECT
    SELECT a.agency_name, a.organization_type, a.address_line1, a.city, a.state, a.postal_code,
           a.county, a.coc_code, a.agency_name, 0);

UPDATE dim_organization
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #a a WHERE dbo.fn_normalize_key(a.agency_id) = dbo.fn_normalize_key(dim_organization.hmis_organization_id));

-- 2. dim_program: HMIS block 20001-29999. An existing program keeps its program_id forever.
DECLARE @prog_base INT = (
    SELECT ISNULL(MAX(program_id), 20000) FROM dim_program
    WHERE program_id BETWEEN 20001 AND 29999
);

INSERT INTO dim_program (
    program_id, program_code, sf_picklist_value, program_name,
    organization_key, hmis_project_id, project_type, target_population,
    coc_code, state, program_status,
    operating_start_date, operating_end_date, source_added_date,
    source_system, is_deleted, load_datetime
)
SELECT
    @prog_base + ROW_NUMBER() OVER (ORDER BY TRY_CAST(n.hmis_program_id AS INT), n.hmis_program_id),
    CONCAT(UPPER(@src), '_', n.hmis_program_id),
    NULL,
    n.program_name,
    o.organization_key,
    n.hmis_program_id,
    n.project_type_code,
    n.target_population,
    n.coc_code,
    n.state,
    CASE WHEN n.is_active = 1 THEN 'Active' WHEN n.is_active = 0 THEN 'Inactive' END,
    n.operating_start_date,
    n.operating_end_date,
    n.added_date,
    @src,
    0,
    @etl_time
FROM (
    SELECT p.*
    FROM #p p
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_program d
        WHERE d.source_system = @src
          AND dbo.fn_normalize_key(d.hmis_project_id) = dbo.fn_normalize_key(p.hmis_program_id)
    )
) n
LEFT JOIN dim_organization o
  ON o.source_system = @src
 AND dbo.fn_normalize_key(o.organization_name) = dbo.fn_normalize_key(n.agency_name);

UPDATE d
SET d.program_name         = p.program_name,
    d.organization_key     = COALESCE(o.organization_key, d.organization_key),
    d.project_type         = p.project_type_code,
    d.target_population    = p.target_population,
    d.coc_code             = p.coc_code,
    d.state                = p.state,
    d.program_status       = COALESCE(CASE WHEN p.is_active = 1 THEN 'Active'
                                           WHEN p.is_active = 0 THEN 'Inactive' END, d.program_status),
    d.operating_start_date = p.operating_start_date,
    d.operating_end_date   = p.operating_end_date,
    d.source_added_date    = p.added_date,
    d.is_deleted           = 0,
    d.deleted_detected_datetime = NULL,
    d.load_datetime        = @etl_time
FROM dim_program d
JOIN #p p
  ON d.source_system = @src
 AND dbo.fn_normalize_key(d.hmis_project_id) = dbo.fn_normalize_key(p.hmis_program_id)
LEFT JOIN dim_organization o
  ON o.source_system = @src
 AND dbo.fn_normalize_key(o.organization_name) = dbo.fn_normalize_key(p.agency_name)
WHERE NOT EXISTS (
    SELECT d.program_name, d.organization_key, d.project_type, d.target_population, d.coc_code, d.state,
           d.program_status, d.operating_start_date, d.operating_end_date, d.source_added_date, ISNULL(d.is_deleted, 0)
    INTERSECT
    SELECT p.program_name, COALESCE(o.organization_key, d.organization_key), p.project_type_code, p.target_population,
           p.coc_code, p.state,
           COALESCE(CASE WHEN p.is_active = 1 THEN 'Active' WHEN p.is_active = 0 THEN 'Inactive' END, d.program_status),
           p.operating_start_date, p.operating_end_date, p.added_date, 0);

UPDATE dim_program
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #p p WHERE dbo.fn_normalize_key(p.hmis_program_id) = dbo.fn_normalize_key(dim_program.hmis_project_id));

-- 2b. dim_program_history: a new version whenever a tracked attribute changes (schema v8.18).
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
WHERE p.source_system = @src;

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

-- 3. dim_grant: every Grant ID in the enrollment export or the funding export.
--    Amounts and dates come from the funding export; when that export is missing they keep their last value.
DROP TABLE IF EXISTS #gsrc;
SELECT
    gid.grant_id,
    COALESCE(gi.grant_identifier, fx.grant_identifier) AS grant_identifier,
    fx.grant_amount, fx.original_amount, fx.grant_start_date, fx.grant_end_date,
    fx.is_active, fx.source_added_date, fx.source_last_updated
INTO #gsrc
FROM (
    SELECT DISTINCT LTRIM(RTRIM(grant_id)) AS grant_id FROM #g WHERE grant_id IS NOT NULL
    UNION
    SELECT DISTINCT LTRIM(RTRIM(grant_id)) FROM #fs WHERE grant_id IS NOT NULL
) gid
LEFT JOIN (
    SELECT LTRIM(RTRIM(grant_id)) AS grant_id, MAX(grant_identifier) AS grant_identifier
    FROM #g WHERE grant_id IS NOT NULL GROUP BY LTRIM(RTRIM(grant_id))
) gi ON gi.grant_id = gid.grant_id
LEFT JOIN (
    SELECT LTRIM(RTRIM(grant_id))      AS grant_id,
           MAX(grant_identifier)       AS grant_identifier,
           MAX(grant_amount)           AS grant_amount,
           MAX(original_amount)        AS original_amount,
           MIN(grant_start_date)       AS grant_start_date,
           MAX(grant_end_date)         AS grant_end_date,
           MAX(CAST(is_active AS INT)) AS is_active,
           MIN(source_added_date)      AS source_added_date,
           MAX(source_last_updated)    AS source_last_updated
    FROM #fs WHERE grant_id IS NOT NULL GROUP BY LTRIM(RTRIM(grant_id))
) fx ON fx.grant_id = gid.grant_id;

INSERT INTO dim_grant (
    hmis_grant_id, grant_identifier, grant_name, coc_code,
    award_amount, original_award_amount, grant_start_date, grant_end_date, is_active,
    source_added_date, source_last_updated, source_system, is_deleted, load_datetime
)
SELECT
    s.grant_id, s.grant_identifier, s.grant_identifier, @coc_code,
    s.grant_amount, s.original_amount, s.grant_start_date, s.grant_end_date, s.is_active,
    s.source_added_date, s.source_last_updated, @src, 0, @etl_time
FROM #gsrc s
WHERE NOT EXISTS (
    SELECT 1 FROM dim_grant d
    WHERE d.source_system = @src
      AND dbo.fn_normalize_key(d.hmis_grant_id) = dbo.fn_normalize_key(s.grant_id)
);

UPDATE d
SET d.grant_identifier      = COALESCE(s.grant_identifier, d.grant_identifier),
    d.grant_name            = COALESCE(d.grant_name, s.grant_identifier),
    d.award_amount          = COALESCE(s.grant_amount, d.award_amount),
    d.original_award_amount = COALESCE(s.original_amount, d.original_award_amount),
    d.grant_start_date      = COALESCE(s.grant_start_date, d.grant_start_date),
    d.grant_end_date        = COALESCE(s.grant_end_date, d.grant_end_date),
    d.is_active             = COALESCE(s.is_active, d.is_active),
    d.source_added_date     = COALESCE(s.source_added_date, d.source_added_date),
    d.source_last_updated   = COALESCE(s.source_last_updated, d.source_last_updated),
    d.is_deleted            = 0,
    d.deleted_detected_datetime = NULL,
    d.load_datetime         = @etl_time
FROM dim_grant d
JOIN #gsrc s
  ON d.source_system = @src
 AND dbo.fn_normalize_key(d.hmis_grant_id) = dbo.fn_normalize_key(s.grant_id)
WHERE NOT EXISTS (
    SELECT d.grant_identifier, d.grant_name, d.award_amount, d.original_award_amount, d.grant_start_date,
           d.grant_end_date, d.is_active, d.source_added_date, d.source_last_updated, ISNULL(d.is_deleted, 0)
    INTERSECT
    SELECT COALESCE(s.grant_identifier, d.grant_identifier), COALESCE(d.grant_name, s.grant_identifier),
           COALESCE(s.grant_amount, d.award_amount), COALESCE(s.original_amount, d.original_award_amount),
           COALESCE(s.grant_start_date, d.grant_start_date), COALESCE(s.grant_end_date, d.grant_end_date),
           COALESCE(s.is_active, d.is_active), COALESCE(s.source_added_date, d.source_added_date),
           COALESCE(s.source_last_updated, d.source_last_updated), 0);

UPDATE dim_grant
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #gsrc s WHERE dbo.fn_normalize_key(s.grant_id) = dbo.fn_normalize_key(dim_grant.hmis_grant_id));

-- 3c. dim_funding_source: one row per Clarity program funding source
DROP TABLE IF EXISTS #fsr;
SELECT
    LTRIM(RTRIM(f.funding_source_id))                      AS funding_source_id,
    g.grant_key                                            AS resolved_grant_key,
    p.program_id                                           AS resolved_program_id,
    f.fund_name, f.funding_source,
    LEFT(f.funding_source_code, 20)                        AS funding_source_code,
    f.amount,
    COALESCE(f.fs_start_date, f.grant_start_date)          AS start_date,
    COALESCE(f.fs_end_date, f.grant_end_date)              AS end_date,
    f.is_active, f.is_direct_expense, f.is_eligibility,
    f.source_added_date, f.source_last_updated
INTO #fsr
FROM #fs f
LEFT JOIN dim_grant g
  ON g.source_system = @src
 AND dbo.fn_normalize_key(g.hmis_grant_id) = dbo.fn_normalize_key(f.grant_id)
LEFT JOIN dim_program p
  ON p.source_system = @src
 AND dbo.fn_normalize_key(p.hmis_project_id) = dbo.fn_normalize_key(f.hmis_program_id)
WHERE f.funding_source_id IS NOT NULL;

INSERT INTO dim_funding_source (
    grant_key, hmis_funding_source_id, program_id,
    funding_source_name, funding_source_type, funding_source_code,
    funding_source_amount, start_date, end_date,
    is_active, is_direct_expense, is_eligibility,
    source_system, source_added_date, source_last_updated, is_deleted, load_datetime
)
SELECT
    r.resolved_grant_key, r.funding_source_id, r.resolved_program_id,
    r.fund_name, r.funding_source, r.funding_source_code,
    r.amount, r.start_date, r.end_date,
    r.is_active, r.is_direct_expense, r.is_eligibility,
    @src, r.source_added_date, r.source_last_updated, 0, @etl_time
FROM #fsr r
WHERE NOT EXISTS (
    SELECT 1 FROM dim_funding_source d
    WHERE d.source_system = @src
      AND dbo.fn_normalize_key(d.hmis_funding_source_id) = dbo.fn_normalize_key(r.funding_source_id)
);

UPDATE d
SET d.grant_key             = COALESCE(r.resolved_grant_key, d.grant_key),
    d.program_id            = COALESCE(r.resolved_program_id, d.program_id),
    d.funding_source_name   = r.fund_name,
    d.funding_source_type   = r.funding_source,
    d.funding_source_code   = r.funding_source_code,
    d.funding_source_amount = r.amount,
    d.start_date            = r.start_date,
    d.end_date              = r.end_date,
    d.is_active             = r.is_active,
    d.is_direct_expense     = r.is_direct_expense,
    d.is_eligibility        = r.is_eligibility,
    d.source_added_date     = r.source_added_date,
    d.source_last_updated   = r.source_last_updated,
    d.is_deleted            = 0,
    d.deleted_detected_datetime = NULL,
    d.load_datetime         = @etl_time
FROM dim_funding_source d
JOIN #fsr r
  ON d.source_system = @src
 AND dbo.fn_normalize_key(d.hmis_funding_source_id) = dbo.fn_normalize_key(r.funding_source_id)
WHERE NOT EXISTS (
    SELECT d.grant_key, d.program_id, d.funding_source_name, d.funding_source_type, d.funding_source_code,
           d.funding_source_amount, d.start_date, d.end_date, d.is_active, d.is_direct_expense, d.is_eligibility,
           d.source_added_date, d.source_last_updated, ISNULL(d.is_deleted, 0)
    INTERSECT
    SELECT COALESCE(r.resolved_grant_key, d.grant_key), COALESCE(r.resolved_program_id, d.program_id),
           r.fund_name, r.funding_source, r.funding_source_code, r.amount, r.start_date, r.end_date,
           r.is_active, r.is_direct_expense, r.is_eligibility, r.source_added_date, r.source_last_updated, 0);

UPDATE dim_funding_source
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #fsr r WHERE dbo.fn_normalize_key(r.funding_source_id) = dbo.fn_normalize_key(dim_funding_source.hmis_funding_source_id));

-- 3d. dim_service_item: the catalog of service types
INSERT INTO dim_service_item (
    service_item_code, service_item_name, service_category, delivery_type,
    is_active, source_system, is_deleted, load_datetime
)
SELECT
    LTRIM(RTRIM(i.service_item_code)), i.service_item_name, i.service_category, i.delivery_type,
    1, @src, 0, @etl_time
FROM #si i
WHERE NOT EXISTS (
    SELECT 1 FROM dim_service_item d
    WHERE d.source_system = @src
      AND dbo.fn_normalize_key(d.service_item_code) = dbo.fn_normalize_key(i.service_item_code)
);

UPDATE d
SET d.service_item_name = i.service_item_name,
    d.service_category  = i.service_category,
    d.delivery_type     = i.delivery_type,
    d.is_deleted        = 0,
    d.deleted_detected_datetime = NULL,
    d.load_datetime     = @etl_time
FROM dim_service_item d
JOIN #si i
  ON d.source_system = @src
 AND dbo.fn_normalize_key(d.service_item_code) = dbo.fn_normalize_key(i.service_item_code)
WHERE NOT EXISTS (
    SELECT d.service_item_name, d.service_category, d.delivery_type, ISNULL(d.is_deleted, 0)
    INTERSECT
    SELECT i.service_item_name, i.service_category, i.delivery_type, 0);

UPDATE dim_service_item
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #si i WHERE dbo.fn_normalize_key(i.service_item_code) = dbo.fn_normalize_key(dim_service_item.service_item_code));

-- 3b. Clarity merges (before any client or enrollment row changes). An old Personal ID counts as
--     merged only when every moved enrollment goes to the same new Personal ID.
DROP TABLE IF EXISTS #mv;
SELECT DISTINCT LTRIM(RTRIM(f.source_personal_id)) AS old_id, LTRIM(RTRIM(e.personal_id)) AS new_id
INTO #mv
FROM fact_enrollment f
JOIN #e e ON dbo.fn_normalize_key(e.enrollment_id) = dbo.fn_normalize_key(f.source_record_id)
WHERE f.source_system = @src
  AND ISNULL(f.is_deleted, 0) = 0
  AND f.source_personal_id IS NOT NULL AND e.personal_id IS NOT NULL
  AND dbo.fn_normalize_key(f.source_personal_id) <> dbo.fn_normalize_key(e.personal_id)
  AND NOT EXISTS (SELECT 1 FROM #c c WHERE dbo.fn_normalize_key(c.personal_id) = dbo.fn_normalize_key(f.source_personal_id))
  AND EXISTS (SELECT 1 FROM #c c WHERE dbo.fn_normalize_key(c.personal_id) = dbo.fn_normalize_key(e.personal_id));

DROP TABLE IF EXISTS #merge;
SELECT CASE WHEN dbo.fn_normalize_key(m.old_id) <= dbo.fn_normalize_key(m.new_id) THEN m.old_id ELSE m.new_id END AS id_a,
       CASE WHEN dbo.fn_normalize_key(m.old_id) <= dbo.fn_normalize_key(m.new_id) THEN m.new_id ELSE m.old_id END AS id_b,
       m.old_id, m.new_id
INTO #merge
FROM #mv m
WHERE NOT EXISTS (SELECT 1 FROM #mv m2
                  WHERE dbo.fn_normalize_key(m2.old_id) = dbo.fn_normalize_key(m.old_id)
                    AND dbo.fn_normalize_key(m2.new_id) <> dbo.fn_normalize_key(m.new_id));

-- leave pairs a reviewer decided, and pairs already recorded as a Clarity merge
DELETE FROM #merge
WHERE EXISTS (SELECT 1 FROM match_decision d
              WHERE d.is_active = 1
                AND (d.decision_method = 'reviewer' OR d.rule_name = 'clarity_merge')
                AND d.source_system_a = @src AND dbo.fn_normalize_key(d.source_id_a) = dbo.fn_normalize_key(#merge.id_a)
                AND d.source_system_b = @src AND dbo.fn_normalize_key(d.source_id_b) = dbo.fn_normalize_key(#merge.id_b));

UPDATE match_decision
SET is_active = 0, revoked_by = '03b_load_hmis_clarity', revoked_datetime = @etl_time
WHERE is_active = 1
  AND EXISTS (SELECT 1 FROM #merge m
              WHERE match_decision.source_system_a = @src AND dbo.fn_normalize_key(match_decision.source_id_a) = dbo.fn_normalize_key(m.id_a)
                AND match_decision.source_system_b = @src AND dbo.fn_normalize_key(match_decision.source_id_b) = dbo.fn_normalize_key(m.id_b));

INSERT INTO match_decision (
    source_system_a, source_id_a, source_system_b, source_id_b,
    decision, decision_method, match_probability, decided_by, decided_datetime, notes, is_active, rule_name
)
SELECT @src, m.id_a, @src, m.id_b, 'match', 'exact_rule', NULL, '03b_load_hmis_clarity', @etl_time,
       CONCAT('Clarity merge: enrollments of Personal ID ', m.old_id, ' moved to ', m.new_id,
              ' and ', m.old_id, ' left the client export'), 1, 'clarity_merge'
FROM #merge m;

SET @n_merge = (SELECT COUNT(*) FROM #merge);

-- 4. dim_client: SCD type 2 on hmis_personal_id + source_system
DROP TABLE IF EXISTS #client_staged;

SELECT
    dbo.fn_normalize_key(c.personal_id) AS personal_key,
    CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONCAT(
        ISNULL(LTRIM(RTRIM(c.unique_identifier)),''),   '|',
        ISNULL(LTRIM(RTRIM(c.first_name)),''),          '|',
        ISNULL(LTRIM(RTRIM(c.middle_name)),''),         '|',
        ISNULL(LTRIM(RTRIM(c.last_name)),''),           '|',
        ISNULL(CONVERT(VARCHAR(20), c.date_of_birth, 120),''), '|',
        ISNULL(LTRIM(RTRIM(c.sex)),''),                 '|',
        ISNULL(LTRIM(RTRIM(c.gender)),''),              '|',
        ISNULL(LTRIM(RTRIM(c.race)),''),                '|',
        ISNULL(LTRIM(RTRIM(c.ethnicity)),''),           '|',
        ISNULL(LTRIM(RTRIM(c.marital_status)),''),      '|',
        ISNULL(LTRIM(RTRIM(c.primary_language)),''),    '|',
        ISNULL(c.ssn_hash,''),                          '|',
        ISNULL(c.ssn_last4,''),                         '|',
        ISNULL(LTRIM(RTRIM(c.name_data_quality)),''),   '|',
        ISNULL(LTRIM(RTRIM(c.ssn_data_quality)),''),    '|',
        ISNULL(LTRIM(RTRIM(c.dob_data_quality)),''),    '|',
        ISNULL(LTRIM(RTRIM(c.email)),''),               '|',
        ISNULL(LTRIM(RTRIM(c.home_phone)),''),          '|',
        ISNULL(LTRIM(RTRIM(c.mobile_phone)),''),        '|',
        ISNULL(LTRIM(RTRIM(c.address_line1)),''),       '|',
        ISNULL(LTRIM(RTRIM(c.city)),''),                '|',
        ISNULL(LTRIM(RTRIM(c.state)),''),               '|',
        ISNULL(LTRIM(RTRIM(c.postal_code)),''),         '|',
        ISNULL(LTRIM(RTRIM(c.veteran_status)),''),      '|',
        ISNULL(LTRIM(RTRIM(c.military_branch)),''),     '|',
        ISNULL(LTRIM(RTRIM(c.discharge_status)),'')
    )), 2) AS row_hash,
    c.*
INTO #client_staged
FROM #c c
WHERE c.personal_id IS NOT NULL AND LTRIM(RTRIM(c.personal_id)) <> '';

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
                  OVER (ORDER BY s.personal_key ROWS UNBOUNDED PRECEDING)
    ) AS resolved_client_key
INTO #client_keyed
FROM #client_staged s
LEFT JOIN dim_client cur
  ON cur.source_system = @src
 AND cur.is_current = 1
 AND dbo.fn_normalize_key(cur.hmis_personal_id) = s.personal_key
LEFT JOIN (
    SELECT dbo.fn_normalize_key(hmis_personal_id) AS personal_key, MAX(client_key) AS client_key
    FROM dim_client
    WHERE source_system = @src
    GROUP BY dbo.fn_normalize_key(hmis_personal_id)
) anyv ON anyv.personal_key = s.personal_key;

-- Close the current version of every client whose values changed
UPDATE dc
SET dc.is_current    = 0,
    dc.valid_to      = @etl_time,
    dc.load_datetime = @etl_time
FROM dim_client dc
JOIN #client_keyed k
  ON dc.client_key = k.current_client_key
 AND dc.is_current = 1
WHERE k.existing_row_hash <> k.row_hash;

-- New clients (valid from the start of time) and new versions of changed clients (valid from this run)
INSERT INTO dim_client (
    client_key, is_current, valid_from, valid_to, row_hash, current_household_key,
    hmis_client_id, hmis_personal_id,
    first_name, middle_name, last_name, date_of_birth,
    sex, gender, race, race_detail, ethnicity,
    marital_status, primary_language,
    ssn_hash, ssn_last4,
    name_data_quality, ssn_data_quality, dob_data_quality,
    email, home_phone, mobile_phone,
    address_line1, city, state, postal_code,
    veteran_status, military_branch, discharge_status,
    source_system, source_agency, source_added_date, source_last_updated,
    created_by_id, last_modified_by_id,
    created_date, last_modified_date, is_deleted, deleted_detected_datetime, load_datetime
)
SELECT
    k.resolved_client_key, 1,
    CASE WHEN k.current_client_key IS NULL AND k.any_client_key IS NULL
         THEN CAST('1900-01-01' AS DATETIME2(6))
         ELSE @etl_time END,
    NULL, k.row_hash, k.existing_household_key,
    LTRIM(RTRIM(k.unique_identifier)), LTRIM(RTRIM(k.personal_id)),
    k.first_name, k.middle_name, k.last_name, k.date_of_birth,
    k.sex, k.gender, k.race, k.race_detail, k.ethnicity,
    k.marital_status, k.primary_language,
    k.ssn_hash, k.ssn_last4,
    k.name_data_quality, k.ssn_data_quality, k.dob_data_quality,
    k.email, k.home_phone, k.mobile_phone,
    k.address_line1, k.city, k.state, k.postal_code,
    k.veteran_status, k.military_branch, k.discharge_status,
    @src, @agency_name, k.source_added_date, k.source_last_updated,
    k.created_by, k.updated_by,
    k.source_added_date, k.source_last_updated, 0, NULL, @etl_time
FROM #client_keyed k
WHERE k.current_client_key IS NULL
   OR k.existing_row_hash <> k.row_hash;

-- Back in the export but still flagged deleted (unchanged values): restore
UPDATE dim_client
SET is_deleted = 0, deleted_detected_datetime = NULL, load_datetime = @etl_time
WHERE source_system = @src AND is_current = 1 AND is_deleted = 1
  AND EXISTS (SELECT 1 FROM #c c WHERE dbo.fn_normalize_key(c.personal_id) = dbo.fn_normalize_key(dim_client.hmis_personal_id));

-- Gone from the export: flag the current version
UPDATE dim_client
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = @src AND is_current = 1 AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #c c WHERE dbo.fn_normalize_key(c.personal_id) = dbo.fn_normalize_key(dim_client.hmis_personal_id));

-- 4b. dim_client_identifier: full SSN, restricted table, one row per client with an SSN.
--     Kept in step row by row; an SSN removed at the source is removed here too.
DROP TABLE IF EXISTS #ident;
SELECT k.resolved_client_key AS client_key, LTRIM(RTRIM(k.personal_id)) AS source_client_id,
       k.ssn_raw, k.ssn_data_quality
INTO #ident
FROM #client_keyed k
WHERE k.ssn_raw IS NOT NULL;

UPDATE i
SET i.client_key       = x.client_key,
    i.ssn_raw          = x.ssn_raw,
    i.ssn_data_quality = x.ssn_data_quality,
    i.load_datetime    = @etl_time
FROM dim_client_identifier i
JOIN #ident x
  ON i.source_system = @src AND i.source_client_id = x.source_client_id
WHERE NOT EXISTS (
    SELECT i.client_key, i.ssn_raw, i.ssn_data_quality
    INTERSECT
    SELECT x.client_key, x.ssn_raw, x.ssn_data_quality);

INSERT INTO dim_client_identifier (client_key, source_system, source_client_id, ssn_raw, ssn_data_quality, load_datetime)
SELECT x.client_key, @src, x.source_client_id, x.ssn_raw, x.ssn_data_quality, @etl_time
FROM #ident x
WHERE NOT EXISTS (
    SELECT 1 FROM dim_client_identifier i
    WHERE i.source_system = @src AND i.source_client_id = x.source_client_id
);

SET @del_ident = (SELECT COUNT(*) FROM dim_client_identifier i WHERE i.source_system = @src
    AND NOT EXISTS (SELECT 1 FROM #ident x WHERE x.source_client_id = i.source_client_id));
DELETE FROM dim_client_identifier
WHERE source_system = @src
  AND NOT EXISTS (SELECT 1 FROM #ident x WHERE x.source_client_id = dim_client_identifier.source_client_id);

-- 5. bridge_client_source (one row per source person; Silver matching will fill this properly)
INSERT INTO bridge_client_source (
    client_key, source_system, source_client_id, source_personal_id,
    match_method, match_confidence, matched_by, matched_date,
    is_active, load_datetime
)
SELECT DISTINCT
    dc.client_key, @src, dc.hmis_client_id, dc.hmis_personal_id,
    'single_source', 1.0000, 'system', @etl_time, 1, @etl_time
FROM dim_client dc
WHERE dc.is_current = 1
  AND dc.source_system = @src
  AND dc.hmis_personal_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM bridge_client_source b
    WHERE b.source_system = @src
      AND b.is_active = 1
      AND dbo.fn_normalize_key(b.source_personal_id) = dbo.fn_normalize_key(dc.hmis_personal_id)
  );

-- 6. dim_household: real Clarity households (household_origin = 'source')
DROP TABLE IF EXISTS #hh;
SELECT
    LTRIM(RTRIM(household_id))  AS household_id,
    MAX(household_type)         AS household_type,
    COUNT(DISTINCT personal_id) AS household_size,
    MIN(source_added_date)      AS source_added_date,
    MAX(source_last_updated)    AS source_last_updated
INTO #hh
FROM #e
WHERE household_id IS NOT NULL AND LTRIM(RTRIM(household_id)) <> ''
GROUP BY LTRIM(RTRIM(household_id));

INSERT INTO dim_household (
    hmis_household_id, household_name, household_type, household_size,
    household_origin, synthetic_for_client_key,
    source_system, source_agency, source_added_date, source_last_updated,
    is_deleted, load_datetime
)
SELECT
    h.household_id, CONCAT(UPPER(@src), ' household ', h.household_id),
    h.household_type, h.household_size,
    'source', NULL,
    @src, @agency_name, h.source_added_date, h.source_last_updated,
    0, @etl_time
FROM #hh h
WHERE NOT EXISTS (
    SELECT 1 FROM dim_household d
    WHERE d.source_system = @src
      AND d.household_origin = 'source'
      AND dbo.fn_normalize_key(d.hmis_household_id) = dbo.fn_normalize_key(h.household_id)
);

UPDATE d
SET d.household_type      = h.household_type,
    d.household_size      = h.household_size,
    d.source_added_date   = h.source_added_date,
    d.source_last_updated = h.source_last_updated,
    d.is_deleted          = 0,
    d.deleted_detected_datetime = NULL,
    d.load_datetime       = @etl_time
FROM dim_household d
JOIN #hh h
  ON d.source_system = @src
 AND d.household_origin = 'source'
 AND dbo.fn_normalize_key(d.hmis_household_id) = dbo.fn_normalize_key(h.household_id)
WHERE NOT EXISTS (
    SELECT d.household_type, d.household_size, d.source_added_date, d.source_last_updated, ISNULL(d.is_deleted, 0)
    INTERSECT
    SELECT h.household_type, h.household_size, h.source_added_date, h.source_last_updated, 0);

UPDATE dim_household
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = @src AND household_origin = 'source' AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #hh h WHERE dbo.fn_normalize_key(h.household_id) = dbo.fn_normalize_key(dim_household.hmis_household_id));

-- 7. fact_enrollment: resolve keys.
--    NULL strategy: unresolved program, household or grant stay NULL (see v_data_quality_unresolved).
--    Only a missing client or start date blocks the row.
DROP TABLE IF EXISTS #er;

SELECT
    e.*,
    LTRIM(RTRIM(e.enrollment_id))  AS enrollment_id_clean,
    dc.client_key                  AS resolved_client_key,
    dc.client_version_key          AS resolved_client_version_key,
    p.program_id                   AS resolved_program_id,
    p.program_code                 AS resolved_program_code,
    h.household_key                AS resolved_household_key,
    g.grant_key                    AS resolved_grant_key,
    c.disability_status            AS resolved_disability_status,
    c.mental_health_condition      AS resolved_mental_health_condition,
    c.substance_abuse              AS resolved_substance_abuse,
    CASE WHEN e.enrollment_exit_date IS NOT NULL THEN e.length_of_stay_days END AS resolved_length_of_stay_days
INTO #er
FROM #e e
LEFT JOIN dim_client dc
  ON dc.source_system = @src
 AND dc.is_current = 1
 AND dbo.fn_normalize_key(dc.hmis_personal_id) = dbo.fn_normalize_key(e.personal_id)
LEFT JOIN dim_program p
  ON p.source_system = @src
 AND dbo.fn_normalize_key(p.hmis_project_id) = dbo.fn_normalize_key(e.hmis_program_id)
LEFT JOIN dim_household h
  ON h.source_system = @src
 AND h.household_origin = 'source'
 AND dbo.fn_normalize_key(h.hmis_household_id) = dbo.fn_normalize_key(e.household_id)
LEFT JOIN dim_grant g
  ON g.source_system = @src
 AND dbo.fn_normalize_key(g.hmis_grant_id) = dbo.fn_normalize_key(e.source_grant_id)
LEFT JOIN #c c
  ON dbo.fn_normalize_key(c.personal_id) = dbo.fn_normalize_key(e.personal_id)
WHERE e.enrollment_id IS NOT NULL AND LTRIM(RTRIM(e.enrollment_id)) <> '';

-- A date outside dim_date (enrollment_start_date/enrollment_exit_date/housing_move_in_date) would load a date key with no calendar row, breaking a
-- declared key and hiding the row from date filters: hold the record back and list it in v_dq_corrections.
DROP TABLE IF EXISTS #bad_er;
SELECT r.enrollment_id_clean AS record_id,
       CASE
            WHEN CAST(r.enrollment_start_date AS DATE) NOT BETWEEN @cal_min AND @cal_max THEN 'enrollment_start_date'
            WHEN CAST(r.enrollment_exit_date AS DATE) NOT BETWEEN @cal_min AND @cal_max THEN 'enrollment_exit_date'
            WHEN CAST(r.housing_move_in_date AS DATE) NOT BETWEEN @cal_min AND @cal_max THEN 'housing_move_in_date'
       END AS bad_column,
       r.personal_id AS client_ref, r.hmis_program_id AS program_ref
INTO #bad_er
FROM #er r
WHERE (r.enrollment_start_date IS NOT NULL AND r.resolved_client_key IS NOT NULL)
  AND (CAST(r.enrollment_start_date AS DATE) NOT BETWEEN @cal_min AND @cal_max OR CAST(r.enrollment_exit_date AS DATE) NOT BETWEEN @cal_min AND @cal_max OR CAST(r.housing_move_in_date AS DATE) NOT BETWEEN @cal_min AND @cal_max);

INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, pipeline_run_id
)
SELECT @run_id, @src, 'fact_enrollment', b.record_id, CONCAT('date_outside_calendar:', b.bad_column),
       b.client_ref, b.program_ref, @etl_time, @pipeline_run_id
FROM #bad_er b;

INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, pipeline_run_id
)
SELECT
    @run_id, @src, 'fact_enrollment', r.enrollment_id_clean,
    CASE WHEN r.enrollment_start_date IS NULL THEN 'missing_enrollment_start_date'
         ELSE 'orphan_client' END,
    r.personal_id, r.hmis_program_id, @etl_time, @pipeline_run_id
FROM #er r
WHERE r.enrollment_start_date IS NULL
   OR r.resolved_client_key IS NULL;

INSERT INTO fact_enrollment (
    client_key, client_version_key, program_id, program_code, household_key, grant_key,
    enrollment_start_date, enrollment_start_date_key,
    enrollment_exit_date,  enrollment_exit_date_key,
    housing_move_in_date,  housing_move_in_date_key,
    enrollment_status, grant_name, is_co_enrollment,
    relationship_to_hoh, is_head_of_household,
    prior_living_category, length_of_stay_days,
    destination_at_exit, destination_category, exit_reason,
    household_composition_type,
    project_type_code, continuum_code,
    disability_status, mental_health_condition, substance_abuse,
    source_system, source_record_id, source_enrollment_id,
    source_client_id, source_household_id, source_personal_id,
    source_file_name, source_file_date, source_grant_id,
    source_added_date, source_last_updated, source_created_by, source_updated_by,
    is_deleted, load_datetime
)
SELECT
    r.resolved_client_key, r.resolved_client_version_key,
    r.resolved_program_id, r.resolved_program_code,
    r.resolved_household_key, r.resolved_grant_key,
    r.enrollment_start_date,
    CAST(CONVERT(VARCHAR(8), r.enrollment_start_date, 112) AS INT),
    r.enrollment_exit_date,
    CASE WHEN r.enrollment_exit_date IS NULL THEN NULL
         ELSE CAST(CONVERT(VARCHAR(8), r.enrollment_exit_date, 112) AS INT) END,
    r.housing_move_in_date,
    CASE WHEN r.housing_move_in_date IS NULL THEN NULL
         ELSE CAST(CONVERT(VARCHAR(8), r.housing_move_in_date, 112) AS INT) END,
    r.enrollment_status, r.grant_name, 0,
    r.relationship_to_hoh, r.is_head_of_household,
    r.prior_living_category, r.resolved_length_of_stay_days,
    r.destination_at_exit, r.destination_category, r.exit_reason,
    r.household_composition_type,
    r.project_type_code, r.continuum_code,
    r.resolved_disability_status, r.resolved_mental_health_condition, r.resolved_substance_abuse,
    @src, r.enrollment_id_clean, r.enrollment_id_clean,
    LTRIM(RTRIM(r.unique_identifier)), LTRIM(RTRIM(r.household_id)), LTRIM(RTRIM(r.personal_id)),
    r.source_file_name, r.source_file_date, r.source_grant_id,
    r.source_added_date, r.source_last_updated, r.created_by, r.updated_by,
    0, @etl_time
FROM #er r
WHERE NOT EXISTS (SELECT 1 FROM #bad_er bd WHERE bd.record_id = r.enrollment_id_clean)
  AND r.enrollment_start_date IS NOT NULL
  AND r.resolved_client_key IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM fact_enrollment f
    WHERE f.source_system = @src
      AND dbo.fn_normalize_key(f.source_record_id) = dbo.fn_normalize_key(r.enrollment_id_clean)
  );

UPDATE f
SET f.client_key                = r.resolved_client_key,
    f.client_version_key        = CASE WHEN f.client_version_key IS NULL OR f.client_key <> r.resolved_client_key
                                       THEN r.resolved_client_version_key ELSE f.client_version_key END,
    f.program_id                = r.resolved_program_id,
    f.program_code              = r.resolved_program_code,
    f.household_key             = COALESCE(r.resolved_household_key, f.household_key),
    f.grant_key                 = r.resolved_grant_key,
    f.enrollment_start_date     = r.enrollment_start_date,
    f.enrollment_start_date_key = CAST(CONVERT(VARCHAR(8), r.enrollment_start_date, 112) AS INT),
    f.enrollment_exit_date      = r.enrollment_exit_date,
    f.enrollment_exit_date_key  = CASE WHEN r.enrollment_exit_date IS NULL THEN NULL
                                       ELSE CAST(CONVERT(VARCHAR(8), r.enrollment_exit_date, 112) AS INT) END,
    f.housing_move_in_date      = r.housing_move_in_date,
    f.housing_move_in_date_key  = CASE WHEN r.housing_move_in_date IS NULL THEN NULL
                                       ELSE CAST(CONVERT(VARCHAR(8), r.housing_move_in_date, 112) AS INT) END,
    f.enrollment_status         = r.enrollment_status,
    f.grant_name                = r.grant_name,
    f.relationship_to_hoh       = r.relationship_to_hoh,
    f.is_head_of_household      = r.is_head_of_household,
    f.prior_living_category     = r.prior_living_category,
    f.length_of_stay_days       = r.resolved_length_of_stay_days,
    f.destination_at_exit       = r.destination_at_exit,
    f.destination_category      = r.destination_category,
    f.exit_reason               = r.exit_reason,
    f.household_composition_type = r.household_composition_type,
    f.project_type_code         = r.project_type_code,
    f.continuum_code            = r.continuum_code,
    f.disability_status         = COALESCE(r.resolved_disability_status,       f.disability_status),
    f.mental_health_condition   = COALESCE(r.resolved_mental_health_condition, f.mental_health_condition),
    f.substance_abuse           = COALESCE(r.resolved_substance_abuse,         f.substance_abuse),
    f.source_client_id          = LTRIM(RTRIM(r.unique_identifier)),
    f.source_household_id       = LTRIM(RTRIM(r.household_id)),
    f.source_personal_id        = LTRIM(RTRIM(r.personal_id)),
    f.source_grant_id           = r.source_grant_id,
    f.source_added_date         = r.source_added_date,
    f.source_last_updated       = r.source_last_updated,
    f.source_created_by         = r.created_by,
    f.source_updated_by         = r.updated_by,
    f.source_file_name          = r.source_file_name,
    f.source_file_date          = r.source_file_date,
    f.is_deleted                = 0,
    f.deleted_detected_datetime = NULL,
    f.load_datetime             = @etl_time
FROM fact_enrollment f
JOIN #er r
  ON f.source_system = @src
 AND dbo.fn_normalize_key(f.source_record_id) = dbo.fn_normalize_key(r.enrollment_id_clean)
WHERE NOT EXISTS (SELECT 1 FROM #bad_er bd WHERE bd.record_id = r.enrollment_id_clean)
  AND r.enrollment_start_date IS NOT NULL
  AND r.resolved_client_key IS NOT NULL
  AND NOT EXISTS (
    SELECT f.client_key, f.client_version_key, f.program_id, f.program_code, f.household_key, f.grant_key,
           f.enrollment_start_date, f.enrollment_exit_date, f.housing_move_in_date,
           f.enrollment_status, f.grant_name, f.relationship_to_hoh, f.is_head_of_household,
           f.prior_living_category, f.length_of_stay_days, f.destination_at_exit, f.destination_category,
           f.exit_reason, f.household_composition_type, f.project_type_code, f.continuum_code,
           f.disability_status, f.mental_health_condition, f.substance_abuse,
           f.source_client_id, f.source_household_id, f.source_personal_id, f.source_grant_id,
           f.source_added_date, f.source_last_updated, f.source_created_by, f.source_updated_by,
           ISNULL(f.is_deleted, 0)
    INTERSECT
    SELECT r.resolved_client_key,
           CASE WHEN f.client_version_key IS NULL OR f.client_key <> r.resolved_client_key
                THEN r.resolved_client_version_key ELSE f.client_version_key END,
           r.resolved_program_id, r.resolved_program_code,
           COALESCE(r.resolved_household_key, f.household_key), r.resolved_grant_key,
           r.enrollment_start_date, r.enrollment_exit_date, r.housing_move_in_date,
           r.enrollment_status, r.grant_name, r.relationship_to_hoh, r.is_head_of_household,
           r.prior_living_category, r.resolved_length_of_stay_days, r.destination_at_exit, r.destination_category,
           r.exit_reason, r.household_composition_type, r.project_type_code, r.continuum_code,
           COALESCE(r.resolved_disability_status, f.disability_status),
           COALESCE(r.resolved_mental_health_condition, f.mental_health_condition),
           COALESCE(r.resolved_substance_abuse, f.substance_abuse),
           LTRIM(RTRIM(r.unique_identifier)), LTRIM(RTRIM(r.household_id)), LTRIM(RTRIM(r.personal_id)),
           r.source_grant_id, r.source_added_date, r.source_last_updated, r.created_by, r.updated_by,
           0);

UPDATE fact_enrollment
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #e e WHERE dbo.fn_normalize_key(e.enrollment_id) = dbo.fn_normalize_key(fact_enrollment.source_record_id));

-- 8. dim_client.current_household_key: household of the client's latest live enrollment in this county
UPDATE dc
SET dc.current_household_key = x.household_key,
    dc.load_datetime = @etl_time
FROM dim_client dc
JOIN (
    SELECT client_key, household_key,
           ROW_NUMBER() OVER (PARTITION BY client_key
                              ORDER BY enrollment_start_date DESC, enrollment_key DESC) AS rn
    FROM fact_enrollment
    WHERE source_system = @src AND household_key IS NOT NULL AND ISNULL(is_deleted, 0) = 0
) x ON x.client_key = dc.client_key AND x.rn = 1
WHERE dc.is_current = 1
  AND dc.source_system = @src
  AND ISNULL(dc.current_household_key, -1) <> x.household_key;

-- 9. bridge_household_client: one row per live enrollment in a household
SET @del_hh_links = (SELECT COUNT(*) FROM bridge_household_client b WHERE b.source_system = @src
    AND EXISTS (SELECT 1 FROM fact_enrollment e WHERE e.enrollment_key = b.enrollment_key AND e.is_deleted = 1));
DELETE FROM bridge_household_client
WHERE source_system = @src
  AND EXISTS (SELECT 1 FROM fact_enrollment e
              WHERE e.enrollment_key = bridge_household_client.enrollment_key AND e.is_deleted = 1);

INSERT INTO bridge_household_client (
    household_key, client_key, enrollment_key,
    relationship_to_hoh, member_type, is_head_of_household,
    start_date, end_date, is_current,
    source_system, source_enrollment_id, source_household_id,
    source_client_id, source_personal_id,
    source_file_name, source_file_date, load_datetime
)
SELECT
    e.household_key, e.client_key, e.enrollment_key,
    e.relationship_to_hoh, NULL, e.is_head_of_household,
    e.enrollment_start_date, e.enrollment_exit_date,
    CASE WHEN e.enrollment_exit_date IS NULL THEN 1 ELSE 0 END,
    e.source_system, e.source_enrollment_id, e.source_household_id,
    e.source_client_id, e.source_personal_id,
    e.source_file_name, e.source_file_date, @etl_time
FROM fact_enrollment e
WHERE e.source_system = @src
  AND e.household_key IS NOT NULL AND e.client_key IS NOT NULL
  AND ISNULL(e.is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM bridge_household_client b WHERE b.enrollment_key = e.enrollment_key);

UPDATE b
SET b.household_key        = e.household_key,
    b.client_key           = e.client_key,
    b.relationship_to_hoh  = e.relationship_to_hoh,
    b.is_head_of_household = e.is_head_of_household,
    b.start_date           = e.enrollment_start_date,
    b.end_date             = e.enrollment_exit_date,
    b.is_current           = CASE WHEN e.enrollment_exit_date IS NULL THEN 1 ELSE 0 END,
    b.source_household_id  = e.source_household_id,
    b.load_datetime        = @etl_time
FROM bridge_household_client b
JOIN fact_enrollment e ON e.enrollment_key = b.enrollment_key
WHERE e.source_system = @src
  AND NOT EXISTS (
    SELECT b.household_key, b.client_key, b.relationship_to_hoh, b.is_head_of_household,
           b.start_date, b.end_date, b.is_current, b.source_household_id
    INTERSECT
    SELECT e.household_key, e.client_key, e.relationship_to_hoh, e.is_head_of_household,
           e.enrollment_start_date, e.enrollment_exit_date,
           CASE WHEN e.enrollment_exit_date IS NULL THEN 1 ELSE 0 END, e.source_household_id);

-- 9b. member_type from the household export
UPDATE b
SET b.member_type   = hm.member_type,
    b.load_datetime = @etl_time
FROM bridge_household_client b
JOIN #hm hm
  ON b.source_system = @src
 AND dbo.fn_normalize_key(b.source_household_id) = dbo.fn_normalize_key(hm.household_id)
 AND dbo.fn_normalize_key(b.source_client_id)    = dbo.fn_normalize_key(hm.unique_identifier)
WHERE ISNULL(b.member_type, '') <> ISNULL(hm.member_type, '');

-- 10. fact_service: one row per Clarity service.
--     No matching client = rejected. A service whose enrollment isn't found still loads, enrollment_key NULL.
DROP TABLE IF EXISTS #sr;
SELECT
    s.*,
    LTRIM(RTRIM(s.service_id))                      AS service_id_clean,
    COALESCE(s.service_item_id, s.service_item_name) AS resolved_service_item_code,
    dc.client_key                                   AS resolved_client_key,
    dc.client_version_key                           AS resolved_client_version_key,
    fe.enrollment_key                               AS resolved_enrollment_key,
    fe.program_id                                   AS resolved_program_id,
    si.service_item_key                             AS resolved_service_item_key
INTO #sr
FROM #s s
LEFT JOIN dim_client dc
  ON dc.source_system = @src
 AND dc.is_current = 1
 AND dbo.fn_normalize_key(dc.hmis_personal_id) = dbo.fn_normalize_key(s.personal_id)
LEFT JOIN fact_enrollment fe
  ON fe.source_system = @src
 AND dbo.fn_normalize_key(fe.source_record_id) = dbo.fn_normalize_key(s.enrollment_id)
LEFT JOIN dim_service_item si
  ON si.source_system = @src
 AND dbo.fn_normalize_key(si.service_item_code) = dbo.fn_normalize_key(COALESCE(s.service_item_id, s.service_item_name))
WHERE s.service_id IS NOT NULL;

-- A date outside dim_date (service_start_date/service_end_date) would load a date key with no calendar row, breaking a
-- declared key and hiding the row from date filters: hold the record back and list it in v_dq_corrections.
DROP TABLE IF EXISTS #bad_sr;
SELECT r.service_id_clean AS record_id,
       CASE
            WHEN CAST(r.service_start_date AS DATE) NOT BETWEEN @cal_min AND @cal_max THEN 'service_start_date'
            WHEN CAST(r.service_end_date AS DATE) NOT BETWEEN @cal_min AND @cal_max THEN 'service_end_date'
       END AS bad_column,
       r.personal_id AS client_ref, CAST(NULL AS VARCHAR(255)) AS program_ref
INTO #bad_sr
FROM #sr r
WHERE (r.resolved_client_key IS NOT NULL)
  AND (CAST(r.service_start_date AS DATE) NOT BETWEEN @cal_min AND @cal_max OR CAST(r.service_end_date AS DATE) NOT BETWEEN @cal_min AND @cal_max);

INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, pipeline_run_id
)
SELECT @run_id, @src, 'fact_service', b.record_id, CONCAT('date_outside_calendar:', b.bad_column),
       b.client_ref, b.program_ref, @etl_time, @pipeline_run_id
FROM #bad_sr b;

INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, pipeline_run_id
)
SELECT @run_id, @src, 'fact_service', r.service_id_clean, 'orphan_client',
       r.personal_id, NULL, @etl_time, @pipeline_run_id
FROM #sr r
WHERE r.resolved_client_key IS NULL;

INSERT INTO fact_service (
    client_key, client_version_key, program_id, enrollment_key, service_item_key,
    source_system, source_record_id, source_service_id, source_enrollment_id,
    source_client_id, source_personal_id,
    service_item_code, service_item_name, service_category, delivery_type,
    service_start_date, service_start_date_key, service_end_date, service_end_date_key,
    financial_assistance_start_date, financial_assistance_end_date,
    service_amount, service_notes, aftercare_project,
    source_file_name, source_file_date, source_added_date, source_last_updated,
    source_created_by, source_updated_by, is_deleted, load_datetime
)
SELECT
    r.resolved_client_key, r.resolved_client_version_key, r.resolved_program_id,
    r.resolved_enrollment_key, r.resolved_service_item_key,
    @src, r.service_id_clean, r.service_id_clean, LTRIM(RTRIM(r.enrollment_id)),
    LTRIM(RTRIM(r.unique_identifier)), LTRIM(RTRIM(r.personal_id)),
    r.resolved_service_item_code, r.service_item_name, r.service_category, r.delivery_type,
    r.service_start_date,
    CASE WHEN r.service_start_date IS NULL THEN NULL ELSE CAST(CONVERT(VARCHAR(8), r.service_start_date, 112) AS INT) END,
    r.service_end_date,
    CASE WHEN r.service_end_date IS NULL THEN NULL ELSE CAST(CONVERT(VARCHAR(8), r.service_end_date, 112) AS INT) END,
    r.fa_start_date, r.fa_end_date,
    r.service_amount, r.service_notes, r.aftercare_project,
    r.source_file_name, r.source_file_date, r.source_added_date, r.source_last_updated,
    r.created_by, r.updated_by, 0, @etl_time
FROM #sr r
WHERE NOT EXISTS (SELECT 1 FROM #bad_sr bd WHERE bd.record_id = r.service_id_clean)
  AND r.resolved_client_key IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM fact_service f
    WHERE f.source_system = @src
      AND dbo.fn_normalize_key(f.source_record_id) = dbo.fn_normalize_key(r.service_id_clean)
  );

UPDATE f
SET f.client_key             = r.resolved_client_key,
    f.client_version_key     = CASE WHEN f.client_version_key IS NULL OR f.client_key <> r.resolved_client_key
                                    THEN r.resolved_client_version_key ELSE f.client_version_key END,
    f.program_id             = r.resolved_program_id,
    f.enrollment_key         = r.resolved_enrollment_key,
    f.service_item_key       = r.resolved_service_item_key,
    f.source_enrollment_id   = LTRIM(RTRIM(r.enrollment_id)),
    f.service_item_code      = r.resolved_service_item_code,
    f.service_item_name      = r.service_item_name,
    f.service_category       = r.service_category,
    f.delivery_type          = r.delivery_type,
    f.service_start_date     = r.service_start_date,
    f.service_start_date_key = CASE WHEN r.service_start_date IS NULL THEN NULL ELSE CAST(CONVERT(VARCHAR(8), r.service_start_date, 112) AS INT) END,
    f.service_end_date       = r.service_end_date,
    f.service_end_date_key   = CASE WHEN r.service_end_date IS NULL THEN NULL ELSE CAST(CONVERT(VARCHAR(8), r.service_end_date, 112) AS INT) END,
    f.financial_assistance_start_date = r.fa_start_date,
    f.financial_assistance_end_date   = r.fa_end_date,
    f.service_amount         = r.service_amount,
    f.service_notes          = r.service_notes,
    f.aftercare_project      = r.aftercare_project,
    f.source_added_date      = r.source_added_date,
    f.source_last_updated    = r.source_last_updated,
    f.source_created_by      = r.created_by,
    f.source_updated_by      = r.updated_by,
    f.source_file_name       = r.source_file_name,
    f.source_file_date       = r.source_file_date,
    f.is_deleted             = 0,
    f.deleted_detected_datetime = NULL,
    f.load_datetime          = @etl_time
FROM fact_service f
JOIN #sr r
  ON f.source_system = @src
 AND dbo.fn_normalize_key(f.source_record_id) = dbo.fn_normalize_key(r.service_id_clean)
WHERE NOT EXISTS (SELECT 1 FROM #bad_sr bd WHERE bd.record_id = r.service_id_clean)
  AND r.resolved_client_key IS NOT NULL
  AND NOT EXISTS (
    SELECT f.client_key, f.client_version_key, f.program_id, f.enrollment_key, f.service_item_key,
           f.source_enrollment_id, f.service_item_code, f.service_item_name, f.service_category, f.delivery_type,
           f.service_start_date, f.service_end_date, f.financial_assistance_start_date, f.financial_assistance_end_date,
           f.service_amount, f.service_notes, f.aftercare_project,
           f.source_added_date, f.source_last_updated, f.source_created_by, f.source_updated_by, ISNULL(f.is_deleted, 0)
    INTERSECT
    SELECT r.resolved_client_key,
           CASE WHEN f.client_version_key IS NULL OR f.client_key <> r.resolved_client_key
                THEN r.resolved_client_version_key ELSE f.client_version_key END,
           r.resolved_program_id, r.resolved_enrollment_key, r.resolved_service_item_key,
           LTRIM(RTRIM(r.enrollment_id)), r.resolved_service_item_code, r.service_item_name, r.service_category, r.delivery_type,
           r.service_start_date, r.service_end_date, r.fa_start_date, r.fa_end_date,
           r.service_amount, r.service_notes, r.aftercare_project,
           r.source_added_date, r.source_last_updated, r.created_by, r.updated_by, 0);

UPDATE fact_service
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #s x WHERE dbo.fn_normalize_key(x.service_id) = dbo.fn_normalize_key(fact_service.source_record_id));

-- 11. fact_case_note: one row per case note. Title and text are masked for anyone without full access.
DROP TABLE IF EXISTS #nr;
SELECT
    n.*,
    LTRIM(RTRIM(n.note_id))  AS note_id_clean,
    LEFT(n.note_title, 500)  AS note_title_clean,
    LEFT(n.note_category, 100) AS note_category_clean,
    dc.client_key            AS resolved_client_key,
    dc.client_version_key    AS resolved_client_version_key,
    fe.enrollment_key        AS resolved_enrollment_key,
    fe.program_id            AS resolved_program_id
INTO #nr
FROM #n n
LEFT JOIN dim_client dc
  ON dc.source_system = @src
 AND dc.is_current = 1
 AND dbo.fn_normalize_key(dc.hmis_personal_id) = dbo.fn_normalize_key(n.personal_id)
LEFT JOIN fact_enrollment fe
  ON fe.source_system = @src
 AND dbo.fn_normalize_key(fe.source_record_id) = dbo.fn_normalize_key(n.enrollment_id)
WHERE n.note_id IS NOT NULL;

-- A date outside dim_date (note_date) would load a date key with no calendar row, breaking a
-- declared key and hiding the row from date filters: hold the record back and list it in v_dq_corrections.
DROP TABLE IF EXISTS #bad_nr;
SELECT r.note_id_clean AS record_id,
       CASE
            WHEN CAST(r.note_date AS DATE) NOT BETWEEN @cal_min AND @cal_max THEN 'note_date'
       END AS bad_column,
       r.personal_id AS client_ref, CAST(NULL AS VARCHAR(255)) AS program_ref
INTO #bad_nr
FROM #nr r
WHERE (r.resolved_client_key IS NOT NULL)
  AND (CAST(r.note_date AS DATE) NOT BETWEEN @cal_min AND @cal_max);

INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, pipeline_run_id
)
SELECT @run_id, @src, 'fact_case_note', b.record_id, CONCAT('date_outside_calendar:', b.bad_column),
       b.client_ref, b.program_ref, @etl_time, @pipeline_run_id
FROM #bad_nr b;

INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, pipeline_run_id
)
SELECT @run_id, @src, 'fact_case_note', r.note_id_clean, 'orphan_client',
       r.personal_id, NULL, @etl_time, @pipeline_run_id
FROM #nr r
WHERE r.resolved_client_key IS NULL;

INSERT INTO fact_case_note (
    client_key, client_version_key, enrollment_key, program_id,
    source_system, source_record_id, source_note_id, source_enrollment_id,
    source_client_id, source_personal_id,
    note_title, note_content, note_date, note_date_key, note_author, note_category,
    time_tracked_minutes, is_private,
    source_file_name, source_file_date, source_added_date, source_last_updated, is_deleted, load_datetime
)
SELECT
    r.resolved_client_key, r.resolved_client_version_key, r.resolved_enrollment_key, r.resolved_program_id,
    @src, r.note_id_clean, r.note_id_clean, LTRIM(RTRIM(r.enrollment_id)),
    LTRIM(RTRIM(r.unique_identifier)), LTRIM(RTRIM(r.personal_id)),
    r.note_title_clean, r.note_content, r.note_date,
    CASE WHEN r.note_date IS NULL THEN NULL ELSE CAST(CONVERT(VARCHAR(8), r.note_date, 112) AS INT) END,
    r.note_author, r.note_category_clean,
    r.time_tracked_minutes, r.is_private,
    r.source_file_name, r.source_file_date, r.source_added_date, r.source_last_updated, 0, @etl_time
FROM #nr r
WHERE NOT EXISTS (SELECT 1 FROM #bad_nr bd WHERE bd.record_id = r.note_id_clean)
  AND r.resolved_client_key IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM fact_case_note f
    WHERE f.source_system = @src
      AND dbo.fn_normalize_key(f.source_record_id) = dbo.fn_normalize_key(r.note_id_clean)
  );

UPDATE f
SET f.client_key           = r.resolved_client_key,
    f.client_version_key   = CASE WHEN f.client_version_key IS NULL OR f.client_key <> r.resolved_client_key
                                  THEN r.resolved_client_version_key ELSE f.client_version_key END,
    f.enrollment_key       = r.resolved_enrollment_key,
    f.program_id           = r.resolved_program_id,
    f.source_enrollment_id = LTRIM(RTRIM(r.enrollment_id)),
    f.note_title           = r.note_title_clean,
    f.note_content         = r.note_content,
    f.note_date            = r.note_date,
    f.note_date_key        = CASE WHEN r.note_date IS NULL THEN NULL ELSE CAST(CONVERT(VARCHAR(8), r.note_date, 112) AS INT) END,
    f.note_author          = r.note_author,
    f.note_category        = r.note_category_clean,
    f.time_tracked_minutes = r.time_tracked_minutes,
    f.is_private           = r.is_private,
    f.source_added_date    = r.source_added_date,
    f.source_last_updated  = r.source_last_updated,
    f.source_file_name     = r.source_file_name,
    f.source_file_date     = r.source_file_date,
    f.is_deleted           = 0,
    f.deleted_detected_datetime = NULL,
    f.load_datetime        = @etl_time
FROM fact_case_note f
JOIN #nr r
  ON f.source_system = @src
 AND dbo.fn_normalize_key(f.source_record_id) = dbo.fn_normalize_key(r.note_id_clean)
WHERE NOT EXISTS (SELECT 1 FROM #bad_nr bd WHERE bd.record_id = r.note_id_clean)
  AND r.resolved_client_key IS NOT NULL
  AND NOT EXISTS (
    SELECT f.client_key, f.client_version_key, f.enrollment_key, f.program_id, f.source_enrollment_id,
           f.note_title, f.note_content, f.note_date, f.note_author, f.note_category,
           f.time_tracked_minutes, f.is_private, f.source_added_date, f.source_last_updated, ISNULL(f.is_deleted, 0)
    INTERSECT
    SELECT r.resolved_client_key,
           CASE WHEN f.client_version_key IS NULL OR f.client_key <> r.resolved_client_key
                THEN r.resolved_client_version_key ELSE f.client_version_key END,
           r.resolved_enrollment_key, r.resolved_program_id, LTRIM(RTRIM(r.enrollment_id)),
           r.note_title_clean, r.note_content, r.note_date, r.note_author, r.note_category_clean,
           r.time_tracked_minutes, r.is_private, r.source_added_date, r.source_last_updated, 0);

UPDATE fact_case_note
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #n x WHERE dbo.fn_normalize_key(x.note_id) = dbo.fn_normalize_key(fact_case_note.source_record_id));

-- 12. dim_bed: one row per Clarity bed
DROP TABLE IF EXISTS #bd;
SELECT
    LTRIM(RTRIM(b.bed_id)) AS bed_id, p.program_id AS resolved_program_id,
    LTRIM(RTRIM(b.unit_id)) AS unit_id, LEFT(b.bed_name, 50) AS bed_number, b.unit_name,
    LEFT(b.household_type, 50) AS household_type, b.inventory_start_date, b.added_date, b.last_updated_date
INTO #bd
FROM #b b
LEFT JOIN dim_program p
  ON p.source_system = @src
 AND dbo.fn_normalize_key(p.hmis_project_id) = dbo.fn_normalize_key(b.hmis_program_id)
WHERE b.bed_id IS NOT NULL;

INSERT INTO dim_bed (
    program_id, hmis_bed_id, hmis_unit_id, bed_number, unit_name, household_type,
    inventory_start_date, added_date, last_updated_date,
    source_system, is_deleted, load_datetime
)
SELECT
    x.resolved_program_id, x.bed_id, x.unit_id, x.bed_number, x.unit_name, x.household_type,
    x.inventory_start_date, x.added_date, x.last_updated_date,
    @src, 0, @etl_time
FROM #bd x
WHERE NOT EXISTS (
    SELECT 1 FROM dim_bed d
    WHERE d.source_system = @src
      AND dbo.fn_normalize_key(d.hmis_bed_id) = dbo.fn_normalize_key(x.bed_id)
);

UPDATE d
SET d.program_id           = COALESCE(x.resolved_program_id, d.program_id),
    d.hmis_unit_id         = x.unit_id,
    d.bed_number           = x.bed_number,
    d.unit_name            = x.unit_name,
    d.household_type       = x.household_type,
    d.inventory_start_date = x.inventory_start_date,
    d.added_date           = x.added_date,
    d.last_updated_date    = x.last_updated_date,
    d.is_deleted           = 0,
    d.deleted_detected_datetime = NULL,
    d.load_datetime        = @etl_time
FROM dim_bed d
JOIN #bd x
  ON d.source_system = @src
 AND dbo.fn_normalize_key(d.hmis_bed_id) = dbo.fn_normalize_key(x.bed_id)
WHERE NOT EXISTS (
    SELECT d.program_id, d.hmis_unit_id, d.bed_number, d.unit_name, d.household_type,
           d.inventory_start_date, d.added_date, d.last_updated_date, ISNULL(d.is_deleted, 0)
    INTERSECT
    SELECT COALESCE(x.resolved_program_id, d.program_id), x.unit_id, x.bed_number, x.unit_name, x.household_type,
           x.inventory_start_date, x.added_date, x.last_updated_date, 0);

UPDATE dim_bed
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #bd x WHERE dbo.fn_normalize_key(x.bed_id) = dbo.fn_normalize_key(dim_bed.hmis_bed_id));

-- 13. fact_bed_availability: one row per bed status period
DROP TABLE IF EXISTS #bsr;
SELECT
    LEFT(bs.status_record_id, 255) AS status_record_id,
    d.bed_key AS resolved_bed_key, d.program_id AS resolved_program_id,
    LEFT(bs.availability_status, 50) AS availability_status, LEFT(bs.bed_status, 50) AS bed_status,
    bs.offline_reason, bs.bed_status_start, bs.bed_status_end, LEFT(bs.unit_status, 50) AS unit_status,
    bs.availability_start, bs.availability_end, bs.is_current,
    bs.source_file_name, bs.source_file_date
INTO #bsr
FROM #bs bs
LEFT JOIN dim_bed d
  ON d.source_system = @src
 AND dbo.fn_normalize_key(d.hmis_bed_id) = dbo.fn_normalize_key(bs.bed_id)
WHERE bs.status_record_id IS NOT NULL;

-- A date outside dim_date (status_start_date) would load a date key with no calendar row, breaking a
-- declared key and hiding the row from date filters: hold the record back and list it in v_dq_corrections.
DROP TABLE IF EXISTS #bad_bsr;
SELECT r.status_record_id AS record_id,
       CASE
            WHEN CAST(r.availability_start AS DATE) NOT BETWEEN @cal_min AND @cal_max THEN 'status_start_date'
       END AS bad_column,
       CAST(NULL AS VARCHAR(255)) AS client_ref, CAST(NULL AS VARCHAR(255)) AS program_ref
INTO #bad_bsr
FROM #bsr r
WHERE (1 = 1)
  AND (CAST(r.availability_start AS DATE) NOT BETWEEN @cal_min AND @cal_max);

INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, pipeline_run_id
)
SELECT @run_id, @src, 'fact_bed_availability', b.record_id, CONCAT('date_outside_calendar:', b.bad_column),
       b.client_ref, b.program_ref, @etl_time, @pipeline_run_id
FROM #bad_bsr b;

INSERT INTO fact_bed_availability (
    bed_key, program_id, availability_status, bed_status, offline_reason,
    bed_status_start_date, bed_status_end_date, unit_status,
    status_start_date, status_start_date_key, status_end_date, is_current,
    source_system, source_record_id, source_file_name, source_file_date, is_deleted, load_datetime
)
SELECT
    r.resolved_bed_key, r.resolved_program_id, r.availability_status, r.bed_status, r.offline_reason,
    r.bed_status_start, r.bed_status_end, r.unit_status,
    r.availability_start,
    CASE WHEN r.availability_start IS NULL THEN NULL ELSE CAST(CONVERT(VARCHAR(8), r.availability_start, 112) AS INT) END,
    r.availability_end, r.is_current,
    @src, r.status_record_id, r.source_file_name, r.source_file_date, 0, @etl_time
FROM #bsr r
WHERE NOT EXISTS (SELECT 1 FROM #bad_bsr bd WHERE bd.record_id = r.status_record_id)
  AND NOT EXISTS (
    SELECT 1 FROM fact_bed_availability f
    WHERE f.source_system = @src AND f.source_record_id = r.status_record_id
);

UPDATE f
SET f.bed_key               = r.resolved_bed_key,
    f.program_id            = r.resolved_program_id,
    f.availability_status   = r.availability_status,
    f.bed_status            = r.bed_status,
    f.offline_reason        = r.offline_reason,
    f.bed_status_start_date = r.bed_status_start,
    f.bed_status_end_date   = r.bed_status_end,
    f.unit_status           = r.unit_status,
    f.status_start_date     = r.availability_start,
    f.status_start_date_key = CASE WHEN r.availability_start IS NULL THEN NULL ELSE CAST(CONVERT(VARCHAR(8), r.availability_start, 112) AS INT) END,
    f.status_end_date       = r.availability_end,
    f.is_current            = r.is_current,
    f.source_file_name      = r.source_file_name,
    f.source_file_date      = r.source_file_date,
    f.is_deleted            = 0,
    f.deleted_detected_datetime = NULL,
    f.load_datetime         = @etl_time
FROM fact_bed_availability f
JOIN #bsr r
  ON f.source_system = @src AND f.source_record_id = r.status_record_id
WHERE NOT EXISTS (SELECT 1 FROM #bad_bsr bd WHERE bd.record_id = r.status_record_id)
  AND NOT EXISTS (
    SELECT f.bed_key, f.program_id, f.availability_status, f.bed_status, f.offline_reason,
           f.bed_status_start_date, f.bed_status_end_date, f.unit_status, f.status_start_date,
           f.status_end_date, f.is_current, ISNULL(f.is_deleted, 0)
    INTERSECT
    SELECT r.resolved_bed_key, r.resolved_program_id, r.availability_status, r.bed_status, r.offline_reason,
           r.bed_status_start, r.bed_status_end, r.unit_status, r.availability_start,
           r.availability_end, r.is_current, 0);

UPDATE fact_bed_availability
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #bsr r WHERE r.status_record_id = fact_bed_availability.source_record_id);

-- 14. fact_bed_night: one row per occupied bed per night, derived from occupancy. A night that is no
--     longer in Silver (occupancy shortened or removed) is flagged deleted, and restored if it comes back.
DROP TABLE IF EXISTS #bnr;
SELECT
    CONCAT(LTRIM(RTRIM(bn.occupancy_id)), '|', CONVERT(VARCHAR(10), bn.bed_night_date, 120)) AS night_record_id,
    dc.client_key AS resolved_client_key, dc.client_version_key AS resolved_client_version_key,
    fe.enrollment_key AS resolved_enrollment_key, d.bed_key AS resolved_bed_key,
    COALESCE(d.program_id, fe.program_id) AS resolved_program_id,
    bn.bed_night_date, bn.occupation_start, bn.occupation_end, bn.assignment_date, bn.pending_end,
    LEFT(bn.occupancy_type, 50) AS occupancy_type,
    LTRIM(RTRIM(bn.occupancy_id)) AS occupancy_id, LTRIM(RTRIM(bn.enrollment_id)) AS enrollment_id,
    LTRIM(RTRIM(bn.unique_identifier)) AS unique_identifier, LTRIM(RTRIM(bn.personal_id)) AS personal_id,
    bn.source_file_name, bn.source_file_date
INTO #bnr
FROM #bn bn
LEFT JOIN dim_bed d
  ON d.source_system = @src
 AND dbo.fn_normalize_key(d.hmis_bed_id) = dbo.fn_normalize_key(bn.bed_id)
LEFT JOIN dim_client dc
  ON dc.source_system = @src
 AND dc.is_current = 1
 AND dbo.fn_normalize_key(dc.hmis_personal_id) = dbo.fn_normalize_key(bn.personal_id)
LEFT JOIN fact_enrollment fe
  ON fe.source_system = @src
 AND dbo.fn_normalize_key(fe.source_record_id) = dbo.fn_normalize_key(bn.enrollment_id)
WHERE bn.occupancy_id IS NOT NULL AND bn.bed_night_date IS NOT NULL;

-- A date outside dim_date (bed_night_date) would load a date key with no calendar row, breaking a
-- declared key and hiding the row from date filters: hold the record back and list it in v_dq_corrections.
DROP TABLE IF EXISTS #bad_bnr;
SELECT r.night_record_id AS record_id,
       CASE
            WHEN CAST(r.bed_night_date AS DATE) NOT BETWEEN @cal_min AND @cal_max THEN 'bed_night_date'
       END AS bad_column,
       r.personal_id AS client_ref, CAST(NULL AS VARCHAR(255)) AS program_ref
INTO #bad_bnr
FROM #bnr r
WHERE (1 = 1)
  AND (CAST(r.bed_night_date AS DATE) NOT BETWEEN @cal_min AND @cal_max);

INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, pipeline_run_id
)
SELECT @run_id, @src, 'fact_bed_night', b.record_id, CONCAT('date_outside_calendar:', b.bad_column),
       b.client_ref, b.program_ref, @etl_time, @pipeline_run_id
FROM #bad_bnr b;

SET @del_bed_nights = (SELECT COUNT(*) FROM fact_bed_night f WHERE f.source_system = @src AND ISNULL(f.is_deleted, 0) = 0
    AND NOT EXISTS (SELECT 1 FROM #bnr r WHERE r.night_record_id = f.source_record_id));
UPDATE fact_bed_night
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #bnr r WHERE r.night_record_id = fact_bed_night.source_record_id);

UPDATE f
SET f.client_key          = r.resolved_client_key,
    f.client_version_key  = CASE WHEN f.client_version_key IS NULL OR ISNULL(f.client_key, -1) <> ISNULL(r.resolved_client_key, -1)
                                 THEN r.resolved_client_version_key ELSE f.client_version_key END,
    f.enrollment_key      = r.resolved_enrollment_key,
    f.bed_key             = r.resolved_bed_key,
    f.program_id          = r.resolved_program_id,
    f.check_in_datetime   = r.occupation_start,
    f.check_out_datetime  = r.occupation_end,
    f.assignment_date     = r.assignment_date,
    f.pending_occupation_end_date = r.pending_end,
    f.occupancy_type      = r.occupancy_type,
    f.source_enrollment_id = r.enrollment_id,
    f.source_client_id    = r.unique_identifier,
    f.source_personal_id  = r.personal_id,
    f.source_file_name    = r.source_file_name,
    f.source_file_date    = r.source_file_date,
    f.is_deleted          = 0,
    f.deleted_detected_datetime = NULL,
    f.load_datetime       = @etl_time
FROM fact_bed_night f
JOIN #bnr r
  ON f.source_system = @src AND f.source_record_id = r.night_record_id
WHERE NOT EXISTS (SELECT 1 FROM #bad_bnr bd WHERE bd.record_id = r.night_record_id)
  AND NOT EXISTS (
    SELECT f.client_key, f.client_version_key, f.enrollment_key, f.bed_key, f.program_id,
           f.check_in_datetime, f.check_out_datetime, f.assignment_date, f.pending_occupation_end_date,
           f.occupancy_type, f.source_enrollment_id, f.source_client_id, f.source_personal_id, ISNULL(f.is_deleted, 0)
    INTERSECT
    SELECT r.resolved_client_key,
           CASE WHEN f.client_version_key IS NULL OR ISNULL(f.client_key, -1) <> ISNULL(r.resolved_client_key, -1)
                THEN r.resolved_client_version_key ELSE f.client_version_key END,
           r.resolved_enrollment_key, r.resolved_bed_key, r.resolved_program_id,
           r.occupation_start, r.occupation_end, r.assignment_date, r.pending_end,
           r.occupancy_type, r.enrollment_id, r.unique_identifier, r.personal_id, 0);

INSERT INTO fact_bed_night (
    client_key, client_version_key, enrollment_key, bed_key, program_id,
    bed_night_date, bed_night_date_key, check_in_datetime, check_out_datetime,
    assignment_date, pending_occupation_end_date, occupancy_type,
    source_system, source_record_id, source_occupancy_id, source_enrollment_id,
    source_client_id, source_personal_id, source_file_name, source_file_date, is_deleted, load_datetime
)
SELECT
    r.resolved_client_key, r.resolved_client_version_key, r.resolved_enrollment_key, r.resolved_bed_key,
    r.resolved_program_id,
    r.bed_night_date, CAST(CONVERT(VARCHAR(8), r.bed_night_date, 112) AS INT),
    r.occupation_start, r.occupation_end,
    r.assignment_date, r.pending_end, r.occupancy_type,
    @src, r.night_record_id, r.occupancy_id,
    r.enrollment_id, r.unique_identifier, r.personal_id,
    r.source_file_name, r.source_file_date, 0, @etl_time
FROM #bnr r
WHERE NOT EXISTS (SELECT 1 FROM #bad_bnr bd WHERE bd.record_id = r.night_record_id)
  AND NOT EXISTS (
    SELECT 1 FROM fact_bed_night f
    WHERE f.source_system = @src AND f.source_record_id = r.night_record_id
);

-- 15. fact_consent: releases of information (consent_type 'HMIS ROI')
DROP TABLE IF EXISTS #rr;
SELECT
    r.*,
    LTRIM(RTRIM(r.roi_id))   AS roi_id_clean,
    dc.client_key            AS resolved_client_key,
    dc.client_version_key    AS resolved_client_version_key,
    CASE WHEN r.documentation = 'Electronic Signature' THEN 1
         WHEN r.documentation = 'Verbal Consent' THEN 0 END AS resolved_signature_captured
INTO #rr
FROM #roi r
LEFT JOIN dim_client dc
  ON dc.source_system = @src
 AND dc.is_current = 1
 AND (   dbo.fn_normalize_key(dc.hmis_client_id)   = dbo.fn_normalize_key(r.unique_identifier)
      OR dbo.fn_normalize_key(dc.hmis_personal_id) = dbo.fn_normalize_key(r.personal_id))
WHERE r.roi_id IS NOT NULL;

-- A date outside dim_date (signed_date) would load a date key with no calendar row, breaking a
-- declared key and hiding the row from date filters: hold the record back and list it in v_dq_corrections.
DROP TABLE IF EXISTS #bad_rr;
SELECT r.roi_id_clean AS record_id,
       CASE
            WHEN CAST(r.start_date AS DATE) NOT BETWEEN @cal_min AND @cal_max THEN 'signed_date'
       END AS bad_column,
       r.unique_identifier AS client_ref, CAST(NULL AS VARCHAR(255)) AS program_ref
INTO #bad_rr
FROM #rr r
WHERE (r.resolved_client_key IS NOT NULL)
  AND (CAST(r.start_date AS DATE) NOT BETWEEN @cal_min AND @cal_max);

INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, pipeline_run_id
)
SELECT @run_id, @src, 'fact_consent', b.record_id, CONCAT('date_outside_calendar:', b.bad_column),
       b.client_ref, b.program_ref, @etl_time, @pipeline_run_id
FROM #bad_rr b;

INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, pipeline_run_id
)
SELECT @run_id, @src, 'fact_consent', r.roi_id_clean, 'orphan_client',
       r.unique_identifier, NULL, @etl_time, @pipeline_run_id
FROM #rr r
WHERE r.resolved_client_key IS NULL;

INSERT INTO fact_consent (
    client_key, client_version_key, enrollment_key, program_id,
    consent_type, consent_scope, permission_granted, consent_status, documentation_type,
    signed_date, signed_date_key, effective_start_date, effective_end_date,
    client_signature_captured, is_current,
    source_system, source_record_id, source_consent_id, source_client_id, source_personal_id,
    source_file_name, source_file_date, source_added_date, source_last_updated,
    source_created_by, source_updated_by, is_deleted, load_datetime
)
SELECT
    r.resolved_client_key, r.resolved_client_version_key, NULL, NULL,
    'HMIS ROI', r.agency_name, r.permission_granted, r.consent_status, r.documentation,
    r.start_date,
    CASE WHEN r.start_date IS NULL THEN NULL ELSE CAST(CONVERT(VARCHAR(8), r.start_date, 112) AS INT) END,
    r.start_date, r.end_date,
    r.resolved_signature_captured, r.is_current,
    @src, r.roi_id_clean, r.roi_id_clean, LTRIM(RTRIM(r.unique_identifier)), LTRIM(RTRIM(r.personal_id)),
    r.source_file_name, r.source_file_date, r.source_added_date, r.source_last_updated,
    r.created_by, r.updated_by, 0, @etl_time
FROM #rr r
WHERE NOT EXISTS (SELECT 1 FROM #bad_rr bd WHERE bd.record_id = r.roi_id_clean)
  AND r.resolved_client_key IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM fact_consent f
    WHERE f.source_system = @src
      AND f.consent_type = 'HMIS ROI'
      AND dbo.fn_normalize_key(f.source_record_id) = dbo.fn_normalize_key(r.roi_id_clean)
  );

UPDATE f
SET f.client_key           = r.resolved_client_key,
    f.client_version_key   = CASE WHEN f.client_version_key IS NULL OR f.client_key <> r.resolved_client_key
                                  THEN r.resolved_client_version_key ELSE f.client_version_key END,
    f.consent_scope        = r.agency_name,
    f.permission_granted   = r.permission_granted,
    f.consent_status       = r.consent_status,
    f.documentation_type   = r.documentation,
    f.signed_date          = r.start_date,
    f.signed_date_key      = CASE WHEN r.start_date IS NULL THEN NULL ELSE CAST(CONVERT(VARCHAR(8), r.start_date, 112) AS INT) END,
    f.effective_start_date = r.start_date,
    f.effective_end_date   = r.end_date,
    f.client_signature_captured = r.resolved_signature_captured,
    f.is_current           = r.is_current,
    f.source_client_id     = LTRIM(RTRIM(r.unique_identifier)),
    f.source_personal_id   = LTRIM(RTRIM(r.personal_id)),
    f.source_added_date    = r.source_added_date,
    f.source_last_updated  = r.source_last_updated,
    f.source_created_by    = r.created_by,
    f.source_updated_by    = r.updated_by,
    f.source_file_name     = r.source_file_name,
    f.source_file_date     = r.source_file_date,
    f.is_deleted           = 0,
    f.deleted_detected_datetime = NULL,
    f.load_datetime        = @etl_time
FROM fact_consent f
JOIN #rr r
  ON f.source_system = @src
 AND f.consent_type = 'HMIS ROI'
 AND dbo.fn_normalize_key(f.source_record_id) = dbo.fn_normalize_key(r.roi_id_clean)
WHERE NOT EXISTS (SELECT 1 FROM #bad_rr bd WHERE bd.record_id = r.roi_id_clean)
  AND r.resolved_client_key IS NOT NULL
  AND NOT EXISTS (
    SELECT f.client_key, f.client_version_key, f.consent_scope, f.permission_granted, f.consent_status,
           f.documentation_type, f.signed_date, f.effective_start_date, f.effective_end_date,
           f.client_signature_captured, f.is_current, f.source_client_id, f.source_personal_id,
           f.source_added_date, f.source_last_updated, f.source_created_by, f.source_updated_by, ISNULL(f.is_deleted, 0)
    INTERSECT
    SELECT r.resolved_client_key,
           CASE WHEN f.client_version_key IS NULL OR f.client_key <> r.resolved_client_key
                THEN r.resolved_client_version_key ELSE f.client_version_key END,
           r.agency_name, r.permission_granted, r.consent_status,
           r.documentation, r.start_date, r.start_date, r.end_date,
           r.resolved_signature_captured, r.is_current, LTRIM(RTRIM(r.unique_identifier)), LTRIM(RTRIM(r.personal_id)),
           r.source_added_date, r.source_last_updated, r.created_by, r.updated_by, 0);

UPDATE fact_consent
SET is_deleted = 1, deleted_detected_datetime = @etl_time, load_datetime = @etl_time
WHERE source_system = @src AND consent_type = 'HMIS ROI' AND ISNULL(is_deleted, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM #roi x WHERE dbo.fn_normalize_key(x.roi_id) = dbo.fn_normalize_key(fact_consent.source_record_id));

-- 16. bridge_enrollment_grant: grants whose period overlaps each live enrollment (derived; kept in step)
DROP TABLE IF EXISTS #egr;
SELECT
    fe.enrollment_key, g.grant_key, fs.funding_source_key,
    CASE WHEN COUNT(*) OVER (PARTITION BY fe.enrollment_key) = 1 THEN 1 ELSE 0 END AS is_primary,
    x.link_start, x.link_end,
    LTRIM(RTRIM(x.enrollment_id)) AS source_enrollment_id, LTRIM(RTRIM(x.grant_id)) AS source_grant_id
INTO #egr
FROM #eg x
JOIN fact_enrollment fe
  ON fe.source_system = @src
 AND ISNULL(fe.is_deleted, 0) = 0
 AND dbo.fn_normalize_key(fe.source_record_id) = dbo.fn_normalize_key(x.enrollment_id)
LEFT JOIN dim_grant g
  ON g.source_system = @src
 AND dbo.fn_normalize_key(g.hmis_grant_id) = dbo.fn_normalize_key(x.grant_id)
LEFT JOIN dim_funding_source fs
  ON fs.source_system = @src
 AND dbo.fn_normalize_key(fs.hmis_funding_source_id) = dbo.fn_normalize_key(x.funding_source_id)
WHERE x.grant_id IS NOT NULL;

SET @del_eg_links = (SELECT COUNT(*) FROM bridge_enrollment_grant b WHERE b.source_system = @src
    AND NOT EXISTS (SELECT 1 FROM #egr x WHERE x.enrollment_key = b.enrollment_key AND x.source_grant_id = b.source_grant_id));
DELETE FROM bridge_enrollment_grant
WHERE source_system = @src
  AND NOT EXISTS (SELECT 1 FROM #egr x
                  WHERE x.enrollment_key = bridge_enrollment_grant.enrollment_key
                    AND x.source_grant_id = bridge_enrollment_grant.source_grant_id);

UPDATE b
SET b.grant_key            = x.grant_key,
    b.funding_source_key   = x.funding_source_key,
    b.is_primary           = x.is_primary,
    b.start_date           = x.link_start,
    b.end_date             = x.link_end,
    b.source_enrollment_id = x.source_enrollment_id,
    b.load_datetime        = @etl_time
FROM bridge_enrollment_grant b
JOIN #egr x
  ON b.source_system = @src AND b.enrollment_key = x.enrollment_key AND b.source_grant_id = x.source_grant_id
WHERE NOT EXISTS (
    SELECT b.grant_key, b.funding_source_key, b.is_primary, b.start_date, b.end_date, b.source_enrollment_id
    INTERSECT
    SELECT x.grant_key, x.funding_source_key, x.is_primary, x.link_start, x.link_end, x.source_enrollment_id);

INSERT INTO bridge_enrollment_grant (
    enrollment_key, grant_key, funding_source_key, is_primary, allocation_share,
    start_date, end_date, source_system, source_enrollment_id, source_grant_id,
    source_file_name, source_file_date, load_datetime
)
SELECT
    x.enrollment_key, x.grant_key, x.funding_source_key, x.is_primary, NULL,
    x.link_start, x.link_end, @src, x.source_enrollment_id, x.source_grant_id,
    NULL, NULL, @etl_time
FROM #egr x
WHERE NOT EXISTS (
    SELECT 1 FROM bridge_enrollment_grant b
    WHERE b.source_system = @src AND b.enrollment_key = x.enrollment_key AND b.source_grant_id = x.source_grant_id
);

-- 17. Audit: one stage_load_log row per table (rows written this run, rows flagged or removed this run)
INSERT INTO stage_load_log (
    source_system, entity_name, source_file_name, source_file_date,
    bronze_load_datetime, silver_load_datetime, warehouse_load_datetime,
    row_count_source, row_count_loaded, load_status, error_message,
    run_id, pipeline_run_id, rows_changed, rows_deleted
)
SELECT @src, t.entity_name, NULL, t.file_date, NULL, t.silver_time, @etl_time,
       t.n_source, t.n_live, 'SUCCESS', NULL, @run_id, @pipeline_run_id, t.n_changed, t.n_deleted
FROM (
    SELECT 'dim_organization' AS entity_name, CAST(NULL AS DATE) AS file_date, (SELECT MAX(load_datetime) FROM #a) AS silver_time,
           @n_a AS n_source,
           (SELECT COUNT(*) FROM dim_organization WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0) AS n_live,
           (SELECT COUNT(*) FROM dim_organization WHERE source_system = @src AND load_datetime = @etl_time) AS n_changed,
           (SELECT COUNT(*) FROM dim_organization WHERE source_system = @src AND deleted_detected_datetime = @etl_time) AS n_deleted
    UNION ALL SELECT 'dim_program_history', NULL, NULL, NULL,
           (SELECT COUNT(*) FROM dim_program_history WHERE source_system = @src AND is_current = 1),
           (SELECT COUNT(*) FROM dim_program_history WHERE source_system = @src AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM dim_program_history WHERE source_system = @src AND valid_to = @etl_time)
    UNION ALL SELECT 'dim_program', NULL, (SELECT MAX(load_datetime) FROM #p), @n_p,
           (SELECT COUNT(*) FROM dim_program WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM dim_program WHERE source_system = @src AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM dim_program WHERE source_system = @src AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'dim_grant', NULL, (SELECT MAX(load_datetime) FROM #g), (SELECT COUNT(*) FROM #gsrc),
           (SELECT COUNT(*) FROM dim_grant WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM dim_grant WHERE source_system = @src AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM dim_grant WHERE source_system = @src AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'dim_funding_source', NULL, (SELECT MAX(load_datetime) FROM #fs), (SELECT COUNT(*) FROM #fsr),
           (SELECT COUNT(*) FROM dim_funding_source WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM dim_funding_source WHERE source_system = @src AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM dim_funding_source WHERE source_system = @src AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'dim_service_item', NULL, (SELECT MAX(load_datetime) FROM #si), (SELECT COUNT(*) FROM #si),
           (SELECT COUNT(*) FROM dim_service_item WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM dim_service_item WHERE source_system = @src AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM dim_service_item WHERE source_system = @src AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'dim_client', (SELECT MAX(source_file_date) FROM #c), (SELECT MAX(load_datetime) FROM #c), @n_c,
           (SELECT COUNT(*) FROM dim_client WHERE source_system = @src AND is_current = 1 AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM dim_client WHERE source_system = @src AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM dim_client WHERE source_system = @src AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'dim_client_identifier', NULL, (SELECT MAX(load_datetime) FROM #c), (SELECT COUNT(*) FROM #ident),
           (SELECT COUNT(*) FROM dim_client_identifier WHERE source_system = @src),
           (SELECT COUNT(*) FROM dim_client_identifier WHERE source_system = @src AND load_datetime = @etl_time),
           @del_ident
    UNION ALL SELECT 'clarity_merge', NULL, (SELECT MAX(load_datetime) FROM #e), @n_merge,
           (SELECT COUNT(*) FROM match_decision WHERE is_active = 1 AND rule_name = 'clarity_merge' AND source_system_a = @src),
           @n_merge, 0
    UNION ALL SELECT 'dim_household', NULL, (SELECT MAX(load_datetime) FROM #e), (SELECT COUNT(*) FROM #hh),
           (SELECT COUNT(*) FROM dim_household WHERE source_system = @src AND household_origin = 'source' AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM dim_household WHERE source_system = @src AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM dim_household WHERE source_system = @src AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'fact_enrollment', (SELECT MAX(source_file_date) FROM #e), (SELECT MAX(load_datetime) FROM #e), @n_e,
           (SELECT COUNT(*) FROM fact_enrollment WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM fact_enrollment WHERE source_system = @src AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM fact_enrollment WHERE source_system = @src AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'bridge_household_client', NULL, NULL, NULL,
           (SELECT COUNT(*) FROM bridge_household_client WHERE source_system = @src),
           (SELECT COUNT(*) FROM bridge_household_client WHERE source_system = @src AND load_datetime = @etl_time),
           @del_hh_links
    UNION ALL SELECT 'fact_service', (SELECT MAX(source_file_date) FROM #s), (SELECT MAX(load_datetime) FROM #s), (SELECT COUNT(*) FROM #s),
           (SELECT COUNT(*) FROM fact_service WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM fact_service WHERE source_system = @src AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM fact_service WHERE source_system = @src AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'fact_case_note', (SELECT MAX(source_file_date) FROM #n), (SELECT MAX(load_datetime) FROM #n), (SELECT COUNT(*) FROM #n),
           (SELECT COUNT(*) FROM fact_case_note WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM fact_case_note WHERE source_system = @src AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM fact_case_note WHERE source_system = @src AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'dim_bed', (SELECT MAX(source_file_date) FROM #b), (SELECT MAX(load_datetime) FROM #b), (SELECT COUNT(*) FROM #bd),
           (SELECT COUNT(*) FROM dim_bed WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM dim_bed WHERE source_system = @src AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM dim_bed WHERE source_system = @src AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'fact_bed_availability', NULL, (SELECT MAX(load_datetime) FROM #bs), (SELECT COUNT(*) FROM #bsr),
           (SELECT COUNT(*) FROM fact_bed_availability WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM fact_bed_availability WHERE source_system = @src AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM fact_bed_availability WHERE source_system = @src AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'fact_bed_night', NULL, (SELECT MAX(load_datetime) FROM #bn), (SELECT COUNT(*) FROM #bnr),
           (SELECT COUNT(*) FROM fact_bed_night WHERE source_system = @src AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM fact_bed_night WHERE source_system = @src AND load_datetime = @etl_time),
           @del_bed_nights
    UNION ALL SELECT 'fact_consent', (SELECT MAX(source_file_date) FROM #roi), (SELECT MAX(load_datetime) FROM #roi), (SELECT COUNT(*) FROM #roi),
           (SELECT COUNT(*) FROM fact_consent WHERE source_system = @src AND consent_type = 'HMIS ROI' AND ISNULL(is_deleted, 0) = 0),
           (SELECT COUNT(*) FROM fact_consent WHERE source_system = @src AND consent_type = 'HMIS ROI' AND load_datetime = @etl_time),
           (SELECT COUNT(*) FROM fact_consent WHERE source_system = @src AND consent_type = 'HMIS ROI' AND deleted_detected_datetime = @etl_time)
    UNION ALL SELECT 'bridge_enrollment_grant', NULL, (SELECT MAX(load_datetime) FROM #eg), (SELECT COUNT(*) FROM #egr),
           (SELECT COUNT(*) FROM bridge_enrollment_grant WHERE source_system = @src),
           (SELECT COUNT(*) FROM bridge_enrollment_grant WHERE source_system = @src AND load_datetime = @etl_time),
           @del_eg_links
) t;

-- 18. Integrity checks. Any problem rolls the whole run back.
DECLARE @problems VARCHAR(4000) = NULL;
SELECT @problems = STRING_AGG(CAST(CONCAT(check_name, ' (', problem_rows, ')') AS VARCHAR(MAX)), '; ')
FROM (
    SELECT 'enrollment source_record_id duplicated' AS check_name, COUNT(*) AS problem_rows
    FROM (SELECT source_record_id FROM fact_enrollment WHERE source_system = @src
          GROUP BY source_record_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'client_key with more than one current version', COUNT(*)
    FROM (SELECT client_key FROM dim_client WHERE source_system = @src AND is_current = 1
          GROUP BY client_key HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'client_key with no current version', COUNT(*)
    FROM (SELECT client_key FROM dim_client WHERE source_system = @src
          GROUP BY client_key HAVING SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END) = 0) x
    UNION ALL SELECT 'personal ID on more than one client_key', COUNT(*)
    FROM (SELECT dbo.fn_normalize_key(hmis_personal_id) AS k FROM dim_client WHERE source_system = @src
          GROUP BY dbo.fn_normalize_key(hmis_personal_id) HAVING COUNT(DISTINCT client_key) > 1) x
    UNION ALL SELECT 'overlapping client versions', COUNT(*)
    FROM dim_client a
    JOIN dim_client b
      ON a.client_key = b.client_key AND a.client_version_key < b.client_version_key
     AND a.source_system = @src AND b.source_system = @src
    WHERE a.valid_from < ISNULL(b.valid_to, CAST('9999-12-31' AS DATETIME2(6)))
      AND b.valid_from < ISNULL(a.valid_to, CAST('9999-12-31' AS DATETIME2(6)))
    UNION ALL SELECT 'closed client version with no next version', COUNT(*)
    FROM dim_client a
    WHERE a.source_system = @src AND a.valid_to IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM dim_client b WHERE b.client_key = a.client_key AND b.valid_from = a.valid_to)
    UNION ALL SELECT 'fact_enrollment pointing at a missing client version', COUNT(*)
    FROM fact_enrollment f WHERE f.source_system = @src AND f.client_version_key IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM dim_client d WHERE d.client_version_key = f.client_version_key)
    UNION ALL SELECT 'fact_service source_record_id duplicated', COUNT(*)
    FROM (SELECT source_record_id FROM fact_service WHERE source_system = @src GROUP BY source_record_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'fact_case_note source_record_id duplicated', COUNT(*)
    FROM (SELECT source_record_id FROM fact_case_note WHERE source_system = @src GROUP BY source_record_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'fact_bed_night source_record_id duplicated', COUNT(*)
    FROM (SELECT source_record_id FROM fact_bed_night WHERE source_system = @src GROUP BY source_record_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'fact_consent ROI duplicated', COUNT(*)
    FROM (SELECT source_record_id FROM fact_consent WHERE source_system = @src AND consent_type = 'HMIS ROI'
          GROUP BY source_record_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'fact_bed_availability duplicated', COUNT(*)
    FROM (SELECT source_record_id FROM fact_bed_availability WHERE source_system = @src GROUP BY source_record_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'bridge_enrollment_grant duplicated', COUNT(*)
    FROM (SELECT enrollment_key, source_grant_id FROM bridge_enrollment_grant WHERE source_system = @src
          GROUP BY enrollment_key, source_grant_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'bridge_household_client duplicated', COUNT(*)
    FROM (SELECT enrollment_key FROM bridge_household_client WHERE source_system = @src
          GROUP BY enrollment_key HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'dim_client_identifier duplicated', COUNT(*)
    FROM (SELECT source_client_id FROM dim_client_identifier WHERE source_system = @src
          GROUP BY source_client_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'dim_bed duplicated', COUNT(*)
    FROM (SELECT hmis_bed_id FROM dim_bed WHERE source_system = @src GROUP BY hmis_bed_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'dim_grant duplicated', COUNT(*)
    FROM (SELECT hmis_grant_id FROM dim_grant WHERE source_system = @src GROUP BY hmis_grant_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'dim_program program_id duplicated', COUNT(*)
    FROM (SELECT program_id FROM dim_program GROUP BY program_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'hmis program_id outside 20001-29999', COUNT(*)
    FROM dim_program WHERE source_system = @src AND program_id NOT BETWEEN 20001 AND 29999
    UNION ALL SELECT 'program with no current history row', COUNT(*)
    FROM dim_program p WHERE p.source_system = @src
      AND NOT EXISTS (SELECT 1 FROM dim_program_history h WHERE h.program_id = p.program_id AND h.is_current = 1)
    UNION ALL SELECT 'program with more than one current history row', COUNT(*)
    FROM (SELECT program_id FROM dim_program_history WHERE source_system = @src AND is_current = 1
          GROUP BY program_id HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'program history versions overlap', COUNT(*)
    FROM dim_program_history a JOIN dim_program_history b
      ON b.program_id = a.program_id AND b.program_history_key <> a.program_history_key
    WHERE a.source_system = @src
      AND a.valid_from < ISNULL(b.valid_to, CAST('9999-12-31' AS DATETIME2(6)))
      AND b.valid_from < ISNULL(a.valid_to, CAST('9999-12-31' AS DATETIME2(6)))
    UNION ALL SELECT 'required column empty in fact_enrollment', COUNT(*)
    FROM fact_enrollment WHERE source_system = @src AND (source_record_id IS NULL OR client_key IS NULL OR client_version_key IS NULL OR load_datetime IS NULL)
    UNION ALL SELECT 'required column empty in fact_service', COUNT(*)
    FROM fact_service WHERE source_system = @src AND (source_record_id IS NULL OR client_key IS NULL OR client_version_key IS NULL OR load_datetime IS NULL)
    UNION ALL SELECT 'required column empty in fact_case_note', COUNT(*)
    FROM fact_case_note WHERE source_system = @src AND (source_record_id IS NULL OR client_key IS NULL OR client_version_key IS NULL OR load_datetime IS NULL)
    UNION ALL SELECT 'required column empty in fact_bed_night', COUNT(*)
    FROM fact_bed_night WHERE source_system = @src AND (source_record_id IS NULL OR client_key IS NULL OR client_version_key IS NULL OR load_datetime IS NULL)
    UNION ALL SELECT 'required column empty in fact_consent', COUNT(*)
    FROM fact_consent WHERE source_system = @src AND (source_record_id IS NULL OR client_key IS NULL OR client_version_key IS NULL OR load_datetime IS NULL)
    UNION ALL SELECT 'required column empty in fact_bed_availability', COUNT(*)
    FROM fact_bed_availability WHERE source_system = @src AND (source_record_id IS NULL OR load_datetime IS NULL)
    UNION ALL SELECT 'required column empty in dim_program', COUNT(*)
    FROM dim_program WHERE source_system = @src AND (program_code IS NULL OR load_datetime IS NULL)
    -- BEGIN declared-key checks (generated from tools/key_checks.py; do not edit by hand)
    UNION ALL SELECT 'FK_bridge_client_source_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.bridge_client_source c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_bridge_client_source: duplicate client_source_key', (SELECT COUNT(*) FROM (SELECT client_source_key FROM dbo.bridge_client_source WHERE client_source_key IS NOT NULL GROUP BY client_source_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_bridge_client_source: empty client_source_key', (SELECT COUNT(*) FROM dbo.bridge_client_source WHERE client_source_key IS NULL)
    UNION ALL SELECT 'FK_bridge_enrollment_grant_enrollment: enrollment_key not in fact_enrollment', (SELECT COUNT(*) FROM dbo.bridge_enrollment_grant c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key))
    UNION ALL SELECT 'FK_bridge_enrollment_grant_funding_source: funding_source_key not in dim_funding_source', (SELECT COUNT(*) FROM dbo.bridge_enrollment_grant c WHERE c.funding_source_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_funding_source p WHERE p.funding_source_key = c.funding_source_key))
    UNION ALL SELECT 'FK_bridge_enrollment_grant_grant: grant_key not in dim_grant', (SELECT COUNT(*) FROM dbo.bridge_enrollment_grant c WHERE c.grant_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_grant p WHERE p.grant_key = c.grant_key))
    UNION ALL SELECT 'FK_bridge_enrollment_grant_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.bridge_enrollment_grant c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_bridge_enrollment_grant: duplicate enrollment_grant_key', (SELECT COUNT(*) FROM (SELECT enrollment_grant_key FROM dbo.bridge_enrollment_grant WHERE enrollment_grant_key IS NOT NULL GROUP BY enrollment_grant_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_bridge_enrollment_grant: empty enrollment_grant_key', (SELECT COUNT(*) FROM dbo.bridge_enrollment_grant WHERE enrollment_grant_key IS NULL)
    UNION ALL SELECT 'FK_bridge_household_client_enrollment: enrollment_key not in fact_enrollment', (SELECT COUNT(*) FROM dbo.bridge_household_client c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key))
    UNION ALL SELECT 'FK_bridge_household_client_household: household_key not in dim_household', (SELECT COUNT(*) FROM dbo.bridge_household_client c WHERE c.household_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_household p WHERE p.household_key = c.household_key))
    UNION ALL SELECT 'FK_bridge_household_client_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.bridge_household_client c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_bridge_household_client: duplicate household_client_key', (SELECT COUNT(*) FROM (SELECT household_client_key FROM dbo.bridge_household_client WHERE household_client_key IS NOT NULL GROUP BY household_client_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_bridge_household_client: empty household_client_key', (SELECT COUNT(*) FROM dbo.bridge_household_client WHERE household_client_key IS NULL)
    UNION ALL SELECT 'FK_dim_bed_program: program_id not in dim_program', (SELECT COUNT(*) FROM dbo.dim_bed c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id))
    UNION ALL SELECT 'FK_dim_bed_site: site_key not in dim_site', (SELECT COUNT(*) FROM dbo.dim_bed c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key))
    UNION ALL SELECT 'FK_dim_bed_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.dim_bed c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_dim_bed: duplicate bed_key', (SELECT COUNT(*) FROM (SELECT bed_key FROM dbo.dim_bed WHERE bed_key IS NOT NULL GROUP BY bed_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_dim_bed: empty bed_key', (SELECT COUNT(*) FROM dbo.dim_bed WHERE bed_key IS NULL)
    UNION ALL SELECT 'FK_dim_client_current_household: current_household_key not in dim_household', (SELECT COUNT(*) FROM dbo.dim_client c WHERE c.current_household_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_household p WHERE p.household_key = c.current_household_key))
    UNION ALL SELECT 'FK_dim_client_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.dim_client c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_dim_client: duplicate client_version_key', (SELECT COUNT(*) FROM (SELECT client_version_key FROM dbo.dim_client WHERE client_version_key IS NOT NULL GROUP BY client_version_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_dim_client: empty client_version_key', (SELECT COUNT(*) FROM dbo.dim_client WHERE client_version_key IS NULL)
    UNION ALL SELECT 'UQ_dim_client_version_start: duplicate client_key, valid_from', (SELECT COUNT(*) FROM (SELECT client_key, valid_from FROM dbo.dim_client WHERE client_key IS NOT NULL AND valid_from IS NOT NULL GROUP BY client_key, valid_from HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'FK_dim_client_identifier_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.dim_client_identifier c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_dim_client_identifier: duplicate source_system, source_client_id', (SELECT COUNT(*) FROM (SELECT source_system, source_client_id FROM dbo.dim_client_identifier WHERE source_system IS NOT NULL AND source_client_id IS NOT NULL GROUP BY source_system, source_client_id HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_dim_client_identifier: empty source_system, source_client_id', (SELECT COUNT(*) FROM dbo.dim_client_identifier WHERE source_system IS NULL OR source_client_id IS NULL)
    UNION ALL SELECT 'FK_dim_funding_source_grant: grant_key not in dim_grant', (SELECT COUNT(*) FROM dbo.dim_funding_source c WHERE c.grant_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_grant p WHERE p.grant_key = c.grant_key))
    UNION ALL SELECT 'FK_dim_funding_source_program: program_id not in dim_program', (SELECT COUNT(*) FROM dbo.dim_funding_source c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id))
    UNION ALL SELECT 'FK_dim_funding_source_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.dim_funding_source c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_dim_funding_source: duplicate funding_source_key', (SELECT COUNT(*) FROM (SELECT funding_source_key FROM dbo.dim_funding_source WHERE funding_source_key IS NOT NULL GROUP BY funding_source_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_dim_funding_source: empty funding_source_key', (SELECT COUNT(*) FROM dbo.dim_funding_source WHERE funding_source_key IS NULL)
    UNION ALL SELECT 'FK_dim_grant_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.dim_grant c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_dim_grant: duplicate grant_key', (SELECT COUNT(*) FROM (SELECT grant_key FROM dbo.dim_grant WHERE grant_key IS NOT NULL GROUP BY grant_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_dim_grant: empty grant_key', (SELECT COUNT(*) FROM dbo.dim_grant WHERE grant_key IS NULL)
    UNION ALL SELECT 'FK_dim_household_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.dim_household c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_dim_household: duplicate household_key', (SELECT COUNT(*) FROM (SELECT household_key FROM dbo.dim_household WHERE household_key IS NOT NULL GROUP BY household_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_dim_household: empty household_key', (SELECT COUNT(*) FROM dbo.dim_household WHERE household_key IS NULL)
    UNION ALL SELECT 'FK_dim_organization_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.dim_organization c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_dim_organization: duplicate organization_key', (SELECT COUNT(*) FROM (SELECT organization_key FROM dbo.dim_organization WHERE organization_key IS NOT NULL GROUP BY organization_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_dim_organization: empty organization_key', (SELECT COUNT(*) FROM dbo.dim_organization WHERE organization_key IS NULL)
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
    UNION ALL SELECT 'FK_dim_service_item_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.dim_service_item c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_dim_service_item: duplicate service_item_key', (SELECT COUNT(*) FROM (SELECT service_item_key FROM dbo.dim_service_item WHERE service_item_key IS NOT NULL GROUP BY service_item_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_dim_service_item: empty service_item_key', (SELECT COUNT(*) FROM dbo.dim_service_item WHERE service_item_key IS NULL)
    UNION ALL SELECT 'FK_fact_bed_availability_bed: bed_key not in dim_bed', (SELECT COUNT(*) FROM dbo.fact_bed_availability c WHERE c.bed_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_bed p WHERE p.bed_key = c.bed_key))
    UNION ALL SELECT 'FK_fact_bed_availability_program: program_id not in dim_program', (SELECT COUNT(*) FROM dbo.fact_bed_availability c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id))
    UNION ALL SELECT 'FK_fact_bed_availability_site: site_key not in dim_site', (SELECT COUNT(*) FROM dbo.fact_bed_availability c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key))
    UNION ALL SELECT 'FK_fact_bed_availability_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.fact_bed_availability c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'FK_fact_bed_availability_start_date: status_start_date_key not in dim_date', (SELECT COUNT(*) FROM dbo.fact_bed_availability c WHERE c.status_start_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.status_start_date_key))
    UNION ALL SELECT 'PK_fact_bed_availability: duplicate availability_key', (SELECT COUNT(*) FROM (SELECT availability_key FROM dbo.fact_bed_availability WHERE availability_key IS NOT NULL GROUP BY availability_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_fact_bed_availability: empty availability_key', (SELECT COUNT(*) FROM dbo.fact_bed_availability WHERE availability_key IS NULL)
    UNION ALL SELECT 'UQ_fact_bed_availability_source_record: duplicate source_system, source_record_id', (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_bed_availability WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'FK_fact_bed_night_bed: bed_key not in dim_bed', (SELECT COUNT(*) FROM dbo.fact_bed_night c WHERE c.bed_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_bed p WHERE p.bed_key = c.bed_key))
    UNION ALL SELECT 'FK_fact_bed_night_client_version: client_version_key not in dim_client', (SELECT COUNT(*) FROM dbo.fact_bed_night c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key))
    UNION ALL SELECT 'FK_fact_bed_night_date: bed_night_date_key not in dim_date', (SELECT COUNT(*) FROM dbo.fact_bed_night c WHERE c.bed_night_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.bed_night_date_key))
    UNION ALL SELECT 'FK_fact_bed_night_enrollment: enrollment_key not in fact_enrollment', (SELECT COUNT(*) FROM dbo.fact_bed_night c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key))
    UNION ALL SELECT 'FK_fact_bed_night_program: program_id not in dim_program', (SELECT COUNT(*) FROM dbo.fact_bed_night c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id))
    UNION ALL SELECT 'FK_fact_bed_night_site: site_key not in dim_site', (SELECT COUNT(*) FROM dbo.fact_bed_night c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key))
    UNION ALL SELECT 'FK_fact_bed_night_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.fact_bed_night c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_fact_bed_night: duplicate bed_night_key', (SELECT COUNT(*) FROM (SELECT bed_night_key FROM dbo.fact_bed_night WHERE bed_night_key IS NOT NULL GROUP BY bed_night_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_fact_bed_night: empty bed_night_key', (SELECT COUNT(*) FROM dbo.fact_bed_night WHERE bed_night_key IS NULL)
    UNION ALL SELECT 'UQ_fact_bed_night_source_record: duplicate source_system, source_record_id', (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_bed_night WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'FK_fact_case_note_client_version: client_version_key not in dim_client', (SELECT COUNT(*) FROM dbo.fact_case_note c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key))
    UNION ALL SELECT 'FK_fact_case_note_date: note_date_key not in dim_date', (SELECT COUNT(*) FROM dbo.fact_case_note c WHERE c.note_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.note_date_key))
    UNION ALL SELECT 'FK_fact_case_note_enrollment: enrollment_key not in fact_enrollment', (SELECT COUNT(*) FROM dbo.fact_case_note c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key))
    UNION ALL SELECT 'FK_fact_case_note_program: program_id not in dim_program', (SELECT COUNT(*) FROM dbo.fact_case_note c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id))
    UNION ALL SELECT 'FK_fact_case_note_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.fact_case_note c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'FK_fact_case_note_staff: staff_key not in dim_staff', (SELECT COUNT(*) FROM dbo.fact_case_note c WHERE c.staff_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_staff p WHERE p.staff_key = c.staff_key))
    UNION ALL SELECT 'PK_fact_case_note: duplicate note_key', (SELECT COUNT(*) FROM (SELECT note_key FROM dbo.fact_case_note WHERE note_key IS NOT NULL GROUP BY note_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_fact_case_note: empty note_key', (SELECT COUNT(*) FROM dbo.fact_case_note WHERE note_key IS NULL)
    UNION ALL SELECT 'UQ_fact_case_note_source_record: duplicate source_system, source_record_id', (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_case_note WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'FK_fact_consent_client_version: client_version_key not in dim_client', (SELECT COUNT(*) FROM dbo.fact_consent c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key))
    UNION ALL SELECT 'FK_fact_consent_document: document_key not in fact_document', (SELECT COUNT(*) FROM dbo.fact_consent c WHERE c.document_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_document p WHERE p.document_key = c.document_key))
    UNION ALL SELECT 'FK_fact_consent_enrollment: enrollment_key not in fact_enrollment', (SELECT COUNT(*) FROM dbo.fact_consent c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key))
    UNION ALL SELECT 'FK_fact_consent_program: program_id not in dim_program', (SELECT COUNT(*) FROM dbo.fact_consent c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id))
    UNION ALL SELECT 'FK_fact_consent_signed_date: signed_date_key not in dim_date', (SELECT COUNT(*) FROM dbo.fact_consent c WHERE c.signed_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.signed_date_key))
    UNION ALL SELECT 'FK_fact_consent_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.fact_consent c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'FK_fact_consent_staff: staff_key not in dim_staff', (SELECT COUNT(*) FROM dbo.fact_consent c WHERE c.staff_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_staff p WHERE p.staff_key = c.staff_key))
    UNION ALL SELECT 'PK_fact_consent: duplicate consent_key', (SELECT COUNT(*) FROM (SELECT consent_key FROM dbo.fact_consent WHERE consent_key IS NOT NULL GROUP BY consent_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_fact_consent: empty consent_key', (SELECT COUNT(*) FROM dbo.fact_consent WHERE consent_key IS NULL)
    UNION ALL SELECT 'UQ_fact_consent_source_record: duplicate source_system, source_record_id', (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_consent WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d)
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
    UNION ALL SELECT 'FK_fact_service_client_version: client_version_key not in dim_client', (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key))
    UNION ALL SELECT 'FK_fact_service_end_date: service_end_date_key not in dim_date', (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.service_end_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.service_end_date_key))
    UNION ALL SELECT 'FK_fact_service_enrollment: enrollment_key not in fact_enrollment', (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key))
    UNION ALL SELECT 'FK_fact_service_funding_source: funding_source_key not in dim_funding_source', (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.funding_source_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_funding_source p WHERE p.funding_source_key = c.funding_source_key))
    UNION ALL SELECT 'FK_fact_service_grant: grant_key not in dim_grant', (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.grant_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_grant p WHERE p.grant_key = c.grant_key))
    UNION ALL SELECT 'FK_fact_service_program: program_id not in dim_program', (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id))
    UNION ALL SELECT 'FK_fact_service_service_item: service_item_key not in dim_service_item', (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.service_item_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_service_item p WHERE p.service_item_key = c.service_item_key))
    UNION ALL SELECT 'FK_fact_service_site: site_key not in dim_site', (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key))
    UNION ALL SELECT 'FK_fact_service_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'FK_fact_service_staff: staff_key not in dim_staff', (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.staff_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_staff p WHERE p.staff_key = c.staff_key))
    UNION ALL SELECT 'FK_fact_service_start_date: service_start_date_key not in dim_date', (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.service_start_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.service_start_date_key))
    UNION ALL SELECT 'PK_fact_service: duplicate service_key', (SELECT COUNT(*) FROM (SELECT service_key FROM dbo.fact_service WHERE service_key IS NOT NULL GROUP BY service_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_fact_service: empty service_key', (SELECT COUNT(*) FROM dbo.fact_service WHERE service_key IS NULL)
    UNION ALL SELECT 'UQ_fact_service_source_record: duplicate source_system, source_record_id', (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_service WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'FK_match_decision_source_a: source_system_a not in dim_source', (SELECT COUNT(*) FROM dbo.match_decision c WHERE c.source_system_a IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system_a))
    UNION ALL SELECT 'FK_match_decision_source_b: source_system_b not in dim_source', (SELECT COUNT(*) FROM dbo.match_decision c WHERE c.source_system_b IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system_b))
    UNION ALL SELECT 'PK_match_decision: duplicate decision_key', (SELECT COUNT(*) FROM (SELECT decision_key FROM dbo.match_decision WHERE decision_key IS NOT NULL GROUP BY decision_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_match_decision: empty decision_key', (SELECT COUNT(*) FROM dbo.match_decision WHERE decision_key IS NULL)
    UNION ALL SELECT 'FK_dim_program_history_source: source_system not in dim_source', (SELECT COUNT(*) FROM dbo.dim_program_history c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'UQ_dim_organization_source_id: duplicate source_system, hmis_organization_id', (SELECT COUNT(*) FROM (SELECT source_system, hmis_organization_id FROM dbo.dim_organization WHERE source_system IS NOT NULL AND hmis_organization_id IS NOT NULL GROUP BY source_system, hmis_organization_id HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'UQ_dim_grant_source_id: duplicate source_system, hmis_grant_id', (SELECT COUNT(*) FROM (SELECT source_system, hmis_grant_id FROM dbo.dim_grant WHERE source_system IS NOT NULL AND hmis_grant_id IS NOT NULL GROUP BY source_system, hmis_grant_id HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'UQ_dim_funding_source_source_id: duplicate source_system, hmis_funding_source_id', (SELECT COUNT(*) FROM (SELECT source_system, hmis_funding_source_id FROM dbo.dim_funding_source WHERE source_system IS NOT NULL AND hmis_funding_source_id IS NOT NULL GROUP BY source_system, hmis_funding_source_id HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'UQ_dim_service_item_source_code: duplicate source_system, service_item_code', (SELECT COUNT(*) FROM (SELECT source_system, service_item_code FROM dbo.dim_service_item WHERE source_system IS NOT NULL AND service_item_code IS NOT NULL GROUP BY source_system, service_item_code HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'UQ_dim_bed_source_id: duplicate source_system, hmis_bed_id', (SELECT COUNT(*) FROM (SELECT source_system, hmis_bed_id FROM dbo.dim_bed WHERE source_system IS NOT NULL AND hmis_bed_id IS NOT NULL GROUP BY source_system, hmis_bed_id HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'UQ_bridge_household_client_enrollment: duplicate enrollment_key', (SELECT COUNT(*) FROM (SELECT enrollment_key FROM dbo.bridge_household_client WHERE enrollment_key IS NOT NULL GROUP BY enrollment_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'UQ_bridge_enrollment_grant_link: duplicate enrollment_key, source_grant_id', (SELECT COUNT(*) FROM (SELECT enrollment_key, source_grant_id FROM dbo.bridge_enrollment_grant WHERE enrollment_key IS NOT NULL AND source_grant_id IS NOT NULL GROUP BY enrollment_key, source_grant_id HAVING COUNT(*) > 1) d)
    -- END declared-key checks
) c
WHERE problem_rows > 0;

IF @problems IS NOT NULL
BEGIN
    DECLARE @int_msg NVARCHAR(4000) = CONCAT('Integrity check failed for ', @src, ', nothing was saved: ', @problems);
    THROW 50012, @int_msg, 1;
END;

-- 19. LOG RUN END (inside the transaction, so success is recorded only if the data commits)
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'SUCCESS'
WHERE script_name = '03b_load_hmis_clarity' AND run_start = @etl_time;

UPDATE etl_source_config
SET last_success_datetime = SYSDATETIME(),
    last_file_date        = COALESCE((SELECT MAX(source_file_date) FROM #e), last_file_date),
    updated_by            = USER_NAME(),
    updated_datetime      = SYSDATETIME()
WHERE source_system = @src;

COMMIT TRANSACTION;

-- 20. What changed this run
SELECT entity_name, row_count_source, row_count_loaded AS live_rows, rows_changed, rows_deleted
FROM stage_load_log
WHERE run_id = @run_id
ORDER BY entity_name;

SELECT reject_reason, entity_name, COUNT(*) AS cnt
FROM stage_load_reject
WHERE run_id = @run_id
GROUP BY reject_reason, entity_name
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
    VALUES (@src, 'ALL', NULL, NULL, NULL, NULL, SYSDATETIME(),
            NULL, NULL, 'FAILED', ERROR_MESSAGE(), @run_id, @pipeline_run_id, 0, 0);

    UPDATE dbo.warehouse_run_log
    SET run_end = SYSDATETIME(), run_status = 'FAILED', error_message = ERROR_MESSAGE()
    WHERE script_name = '03b_load_hmis_clarity' AND run_start = @etl_time;

    THROW;

END CATCH;

END;
GO

-- Load LA County:
-- EXEC dbo.usp_load_hmis_clarity @src = 'hmis_lac';
-- After checking a large real removal in the export:
-- EXEC dbo.usp_load_hmis_clarity @src = 'hmis_lac', @allow_mass_delete = 1;
