-- migrations/0003_matching.sql  (warehouse schema v8.15 -> v8.16)
--
-- Adds what the matching engine, the review notebook and the pipeline health dashboard need.
-- Run once, after 0002 and before installing 06_proc_person_links.sql v1.1 and 03b v3.1. The Silver
-- notebooks must have run at least once (the health views read lh_silver.dbo.etl_quality_log).
-- Safe to rerun: tables are created only if missing; views are dropped and recreated.
-- Never drops a table or a column. Make a user-defined restore point first.
--
-- What it adds:
--   1. dbo.match_review_queue: pairs the matching notebook scored as possible matches that no
--      reviewer has decided yet. Rebuilt by usp_apply_match_suggestions after every matching run.
--      Holds IDs, scores and plain-language evidence only; names and dates stay in dim_client.
--   2. dbo.apricot_profile_map: which Apricot profile each source profile went to. Written by the
--      Apricot import (one row per source profile it creates or updates). Keyed on source IDs.
--   3. dbo.v_apricot_worklist: merges and splits staff must do by hand in Apricot, worked out
--      from the current links and the map. A task disappears by itself once the Apricot import
--      records the fix, so there is no "mark done" to forget. People with a score-only link
--      (rpt.person.has_unconfirmed_link = 1) get no merge task until a reviewer confirms them.
--   4. dbo.v_match_review_pairs: the review queue side by side with both profiles, for the
--      review notebook. Leaves out the SSN hash, SSN last 4, gender identity and orientation.
--   5. Health views for the dashboard: v_health_run_steps, v_health_runs,
--      v_health_table_changes (this run against the median of the 8 before), v_health_issues,
--      v_health_people.
--   Note: Fabric has no ownership chaining, so a view never grants access to the tables behind it.
--   04_security v1.9 grants the match_reviewer role what the review notebook needs.

-- 0. LOG RUN START
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'ABANDONED'
WHERE script_name = 'migration_0003' AND run_status = 'RUNNING';

INSERT INTO dbo.warehouse_run_log (script_name, script_version, run_start, run_status, run_by, change_summary)
VALUES ('migration_0003', 'v8.16', SYSDATETIME(), 'RUNNING', USER_NAME(),
        'Matching: match_review_queue, apricot_profile_map, v_apricot_worklist, v_match_review_pairs, health views');
GO

IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.15')
    THROW 50011, 'The warehouse schema is older than v8.15. Run migrations/0002_person_links.sql first.', 1;
IF NOT EXISTS (SELECT 1 FROM lh_silver.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'etl_quality_log')
    THROW 50011, 'lh_silver.dbo.etl_quality_log was not found. Run a Silver notebook once (it creates the table), then rerun this migration.', 1;
GO

-- 0b. Which automatic rule made a decision ('clarity_merge', 'name_dob_contact', 'same_ssn_dob',
--     'score'). Automatic decisions from the matching notebook are retired when their evidence goes
--     away; a Clarity merge is kept. Reviewer decisions leave it empty.
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.match_decision') AND name = 'rule_name')
    ALTER TABLE dbo.match_decision ADD rule_name VARCHAR(50);
GO

-- 1. REVIEW QUEUE (replaced as a whole by usp_apply_match_suggestions)
IF OBJECT_ID('dbo.match_review_queue', 'U') IS NULL
CREATE TABLE dbo.match_review_queue (
    source_system_a           VARCHAR(50)  NOT NULL,
    source_id_a               VARCHAR(100) NOT NULL,
    source_system_b           VARCHAR(50)  NOT NULL,
    source_id_b               VARCHAR(100) NOT NULL,
    client_key_a              BIGINT,
    client_key_b              BIGINT,
    match_probability         DECIMAL(9,8),
    match_weight              DECIMAL(9,4),
    queue_status              VARCHAR(20)  NOT NULL,
    evidence                  VARCHAR(1000),
    comparison_levels         VARCHAR(4000),
    first_suggested_datetime  DATETIME2(6) NOT NULL,
    match_run_id              VARCHAR(60)  NOT NULL,
    refreshed_datetime        DATETIME2(6) NOT NULL
);
GO

-- 2. APRICOT PROFILE MAP (written by the Apricot import)
IF OBJECT_ID('dbo.apricot_profile_map', 'U') IS NULL
CREATE TABLE dbo.apricot_profile_map (
    source_system             VARCHAR(50)  NOT NULL,
    source_id                 VARCHAR(100) NOT NULL,
    apricot_record_id         VARCHAR(50)  NOT NULL,
    person_key_at_export      BIGINT,
    exported_datetime         DATETIME2(6) NOT NULL,
    updated_datetime          DATETIME2(6) NOT NULL,
    is_current                BIT          NOT NULL
);
GO

-- 3. APRICOT WORKLIST: one row per profile involved in a task
DROP VIEW IF EXISTS dbo.v_apricot_worklist;
GO

CREATE VIEW dbo.v_apricot_worklist AS
WITH src AS (
    SELECT b.source_system,
           dbo.fn_normalize_key(COALESCE(b.source_personal_id, b.source_client_id)) AS source_key,
           MIN(b.client_key) AS client_key
    FROM dbo.bridge_client_source b
    WHERE b.is_active = 1 AND COALESCE(b.source_personal_id, b.source_client_id) IS NOT NULL
    GROUP BY b.source_system, dbo.fn_normalize_key(COALESCE(b.source_personal_id, b.source_client_id))
),
m AS (
    SELECT a.source_system, a.source_id, a.apricot_record_id, a.exported_datetime,
           s.client_key, l.person_key, l.valid_from AS linked_since
    FROM dbo.apricot_profile_map a
    JOIN src s ON s.source_system = a.source_system AND s.source_key = dbo.fn_normalize_key(a.source_id)
    JOIN dbo.person_client_link l ON l.client_key = s.client_key AND l.is_current = 1
    WHERE a.is_current = 1
),
merge_people AS (
    -- only confirmed people: a link made by score alone is not merged in Apricot until a reviewer confirms it
    SELECT m.person_key FROM m
    WHERE NOT EXISTS (SELECT 1 FROM rpt.person p WHERE p.person_key = m.person_key AND p.has_unconfirmed_link = 1)
    GROUP BY m.person_key HAVING COUNT(DISTINCT m.apricot_record_id) > 1
),
split_ids AS (
    SELECT apricot_record_id FROM m GROUP BY apricot_record_id HAVING COUNT(DISTINCT person_key) > 1
),
tasks AS (
    SELECT CONCAT('merge-', m.person_key) AS task_id, 'merge' AS task_type, m.*,
           FIRST_VALUE(m.apricot_record_id) OVER (PARTITION BY m.person_key
                ORDER BY m.exported_datetime, m.apricot_record_id) AS keep_apricot_record_id,
           NULL AS stays_person_key
    FROM m JOIN merge_people mp ON mp.person_key = m.person_key
    UNION ALL
    SELECT CONCAT('split-', m.apricot_record_id) AS task_id, 'split' AS task_type, m.*,
           m.apricot_record_id AS keep_apricot_record_id,
           FIRST_VALUE(m.person_key) OVER (PARTITION BY m.apricot_record_id
                ORDER BY m.exported_datetime, m.person_key) AS stays_person_key
    FROM m JOIN split_ids si ON si.apricot_record_id = m.apricot_record_id
)
SELECT t.task_id, t.task_type, t.person_key, t.apricot_record_id, t.keep_apricot_record_id,
       t.source_system, t.source_id, t.client_key,
       CASE WHEN t.task_type = 'merge' AND t.apricot_record_id = t.keep_apricot_record_id THEN 'Keep this Apricot profile'
            WHEN t.task_type = 'merge' THEN CONCAT('Merge into Apricot profile ', t.keep_apricot_record_id)
            WHEN t.person_key = t.stays_person_key THEN 'Stays on this Apricot profile'
            ELSE 'Moves to a new Apricot profile' END AS action,
       MAX(t.linked_since) OVER (PARTITION BY t.task_id) AS since,
       (SELECT COUNT(*) FROM dbo.fact_enrollment e WHERE e.client_key = t.client_key AND ISNULL(e.is_deleted, 0) = 0) AS live_enrollments,
       (SELECT COUNT(*) FROM dbo.fact_service s WHERE s.client_key = t.client_key AND ISNULL(s.is_deleted, 0) = 0) AS live_services,
       (SELECT COUNT(*) FROM dbo.fact_case_note n WHERE n.client_key = t.client_key AND ISNULL(n.is_deleted, 0) = 0) AS live_case_notes
FROM tasks t;
GO

-- 4. REVIEW PAIRS: the queue with both profiles side by side (no SSN, gender identity or orientation)
DROP VIEW IF EXISTS dbo.v_match_review_pairs;
GO

CREATE VIEW dbo.v_match_review_pairs AS
WITH latest_enr AS (
    SELECT e.client_key, e.program_code, e.enrollment_start_date, e.enrollment_exit_date,
           ROW_NUMBER() OVER (PARTITION BY e.client_key ORDER BY e.enrollment_start_date DESC, e.enrollment_key DESC) AS rn,
           COUNT(*) OVER (PARTITION BY e.client_key) AS enrollments
    FROM dbo.fact_enrollment e
    WHERE ISNULL(e.is_deleted, 0) = 0
)
SELECT q.source_system_a, q.source_id_a, q.source_system_b, q.source_id_b,
       q.client_key_a, q.client_key_b, q.match_probability, q.match_weight, q.queue_status,
       q.evidence, q.comparison_levels, q.first_suggested_datetime, q.match_run_id,
       la.person_key AS person_key_a, lb.person_key AS person_key_b,
       a.first_name AS first_name_a, b.first_name AS first_name_b,
       a.middle_name AS middle_name_a, b.middle_name AS middle_name_b,
       a.last_name AS last_name_a, b.last_name AS last_name_b,
       a.date_of_birth AS date_of_birth_a, b.date_of_birth AS date_of_birth_b,
       a.dob_data_quality AS dob_data_quality_a, b.dob_data_quality AS dob_data_quality_b,
       a.sex AS sex_a, b.sex AS sex_b, a.gender AS gender_a, b.gender AS gender_b,
       a.mobile_phone AS mobile_phone_a, b.mobile_phone AS mobile_phone_b,
       a.home_phone AS home_phone_a, b.home_phone AS home_phone_b,
       a.email AS email_a, b.email AS email_b,
       a.address_line1 AS address_line1_a, b.address_line1 AS address_line1_b,
       a.city AS city_a, b.city AS city_b, a.postal_code AS postal_code_a, b.postal_code AS postal_code_b,
       a.veteran_status AS veteran_status_a, b.veteran_status AS veteran_status_b,
       a.military_branch AS military_branch_a, b.military_branch AS military_branch_b,
       a.discharge_status AS discharge_status_a, b.discharge_status AS discharge_status_b,
       ea.enrollments AS enrollments_a, eb.enrollments AS enrollments_b,
       ea.program_code AS latest_program_a, eb.program_code AS latest_program_b,
       ea.enrollment_start_date AS latest_start_a, eb.enrollment_start_date AS latest_start_b
FROM dbo.match_review_queue q
LEFT JOIN dbo.dim_client a ON a.client_key = q.client_key_a AND a.is_current = 1
LEFT JOIN dbo.dim_client b ON b.client_key = q.client_key_b AND b.is_current = 1
LEFT JOIN dbo.person_client_link la ON la.client_key = q.client_key_a AND la.is_current = 1
LEFT JOIN dbo.person_client_link lb ON lb.client_key = q.client_key_b AND lb.is_current = 1
LEFT JOIN latest_enr ea ON ea.client_key = q.client_key_a AND ea.rn = 1
LEFT JOIN latest_enr eb ON eb.client_key = q.client_key_b AND eb.rn = 1
-- a pair a reviewer decided since the last pipeline run drops out at once, not at the next rebuild of the queue
WHERE NOT EXISTS (SELECT 1 FROM dbo.match_decision d
                  WHERE d.is_active = 1 AND d.decision_method = 'reviewer' AND d.decision IN ('match', 'no_match')
                    AND d.source_system_a = q.source_system_a AND dbo.fn_normalize_key(d.source_id_a) = dbo.fn_normalize_key(q.source_id_a)
                    AND d.source_system_b = q.source_system_b AND dbo.fn_normalize_key(d.source_id_b) = dbo.fn_normalize_key(q.source_id_b));
GO

-- 5. HEALTH VIEWS
DROP VIEW IF EXISTS dbo.v_health_run_steps;
GO

-- One row per warehouse step. A pipeline run groups its steps by pipeline_run_id; a manual run
-- of one script is a run of its own.
CREATE VIEW dbo.v_health_run_steps AS
SELECT COALESCE(pipeline_run_id, CONCAT('manual ', script_name, ' ', CONVERT(VARCHAR(30), run_start, 126))) AS run_ref,
       pipeline_run_id, script_name, script_version, run_start, run_end,
       DATEDIFF(second, run_start, run_end) AS seconds,
       run_status, LEFT(error_message, 1000) AS error_message
FROM dbo.warehouse_run_log
WHERE script_name IN ('03a_load_sf_cis', '03b_load_hmis_clarity', '06_apply_match_suggestions', '06_refresh_person_links');
GO

DROP VIEW IF EXISTS dbo.v_health_runs;
GO

CREATE VIEW dbo.v_health_runs AS
SELECT run_ref, MAX(pipeline_run_id) AS pipeline_run_id,
       MIN(run_start) AS started, MAX(run_end) AS ended,
       DATEDIFF(second, MIN(run_start), MAX(run_end)) AS seconds,
       COUNT(*) AS steps,
       SUM(CASE WHEN run_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_steps,
       CASE WHEN SUM(CASE WHEN run_status = 'FAILED' THEN 1 ELSE 0 END) > 0 THEN 'Failed'
            WHEN SUM(CASE WHEN run_status = 'RUNNING' THEN 1 ELSE 0 END) > 0 THEN 'Running'
            WHEN SUM(CASE WHEN run_status = 'ABANDONED' THEN 1 ELSE 0 END) > 0 THEN 'Abandoned'
            ELSE 'Succeeded' END AS status,
       MAX(CASE WHEN run_status = 'FAILED' THEN error_message END) AS error_message
FROM dbo.v_health_run_steps
GROUP BY run_ref;
GO

DROP VIEW IF EXISTS dbo.v_health_table_changes;
GO

-- The latest successful load of each table against the median of the 8 loads before it.
CREATE VIEW dbo.v_health_table_changes AS
WITH s AS (
    SELECT source_system, entity_name, warehouse_load_datetime, row_count_loaded, rows_changed, rows_deleted,
           ROW_NUMBER() OVER (PARTITION BY source_system, entity_name
                              ORDER BY warehouse_load_datetime DESC, load_log_key DESC) AS age
    FROM dbo.stage_load_log
    WHERE load_status = 'SUCCESS' AND rows_changed IS NOT NULL
),
prev AS (
    SELECT source_system, entity_name, rows_changed, ISNULL(rows_deleted, 0) AS rows_deleted,
           ROW_NUMBER() OVER (PARTITION BY source_system, entity_name ORDER BY rows_changed) AS rn_c,
           ROW_NUMBER() OVER (PARTITION BY source_system, entity_name ORDER BY ISNULL(rows_deleted, 0)) AS rn_d,
           COUNT(*) OVER (PARTITION BY source_system, entity_name) AS cnt
    FROM s
    WHERE age BETWEEN 2 AND 9
),
med_c AS (
    SELECT source_system, entity_name, MAX(cnt) AS history_runs, AVG(CAST(rows_changed AS DECIMAL(18,2))) AS median_changed
    FROM prev
    WHERE rn_c IN (CAST(FLOOR((cnt + 1) / 2.0) AS INT), CAST(FLOOR((cnt + 2) / 2.0) AS INT))
    GROUP BY source_system, entity_name
),
med_d AS (
    SELECT source_system, entity_name, AVG(CAST(rows_deleted AS DECIMAL(18,2))) AS median_deleted
    FROM prev
    WHERE rn_d IN (CAST(FLOOR((cnt + 1) / 2.0) AS INT), CAST(FLOOR((cnt + 2) / 2.0) AS INT))
    GROUP BY source_system, entity_name
),
j AS (
    SELECT c.source_system, c.entity_name, c.warehouse_load_datetime, c.row_count_loaded, c.rows_changed,
           ISNULL(c.rows_deleted, 0) AS rows_deleted,
           p1.row_count_loaded AS previous_live_rows, p1.rows_changed AS previous_rows_changed,
           ISNULL(mc.history_runs, 0) AS history_runs, mc.median_changed, md.median_deleted
    FROM s c
    LEFT JOIN s p1 ON p1.source_system = c.source_system AND p1.entity_name = c.entity_name AND p1.age = 2
    LEFT JOIN med_c mc ON mc.source_system = c.source_system AND mc.entity_name = c.entity_name
    LEFT JOIN med_d md ON md.source_system = c.source_system AND md.entity_name = c.entity_name
    WHERE c.age = 1
)
SELECT j.*,
       CASE WHEN history_runs < 3 THEN 'new'
            WHEN previous_live_rows > 0 AND row_count_loaded < previous_live_rows * 0.9 THEN 'unusual'
            WHEN rows_changed >= median_changed * 3 AND rows_changed - median_changed >= 25 THEN 'unusual'
            WHEN rows_deleted >= median_deleted * 3 AND rows_deleted - median_deleted >= 10 THEN 'unusual'
            WHEN rows_changed = 0 AND previous_rows_changed = 0 AND median_changed > 0 THEN 'quiet'
            ELSE 'normal' END AS flag_level,
       CASE WHEN history_runs < 3 THEN 'Not enough history yet'
            WHEN previous_live_rows > 0 AND row_count_loaded < previous_live_rows * 0.9
                 THEN CONCAT('Live rows dropped from ', previous_live_rows, ' to ', row_count_loaded)
            WHEN rows_changed >= median_changed * 3 AND rows_changed - median_changed >= 25
                 THEN CONCAT(rows_changed, ' changes, usually about ', CAST(ROUND(median_changed, 0) AS INT))
            WHEN rows_deleted >= median_deleted * 3 AND rows_deleted - median_deleted >= 10
                 THEN CONCAT(rows_deleted, ' flagged deleted, usually about ', CAST(ROUND(median_deleted, 0) AS INT))
            WHEN rows_changed = 0 AND previous_rows_changed = 0 AND median_changed > 0
                 THEN 'No changes in the last 2 runs'
            ELSE 'Normal' END AS flag_text
FROM j;
GO

DROP VIEW IF EXISTS dbo.v_health_issues;
GO

-- Everything from the last 8 days that needs a look: notebook stops and warnings, load rejects
-- (including contradicting match decisions) and failed steps.
CREATE VIEW dbo.v_health_issues AS
SELECT q.logged_at AS detected_datetime, 'Notebook check' AS area, q.source_system,
       CASE q.status WHEN 'stop' THEN 'Stop' WHEN 'warn' THEN 'Warning' ELSE 'Override' END AS level,
       CONCAT(q.notebook, ': ', q.check_name, CASE WHEN q.table_name IS NOT NULL THEN CONCAT(' (', q.table_name, ')') ELSE '' END) AS title,
       q.detail, q.value AS item_count
FROM lh_silver.dbo.etl_quality_log q
WHERE q.status IN ('stop', 'warn', 'override')
  AND q.logged_at >= DATEADD(day, -8, SYSDATETIME())
UNION ALL
SELECT MAX(r.detected_datetime), 'Load reject', r.source_system,
       CASE WHEN r.entity_name = 'person_link' THEN 'Review' ELSE 'Warning' END,
       CONCAT(r.entity_name, ': ', r.reject_reason), NULL, COUNT(*)
FROM dbo.stage_load_reject r
WHERE r.detected_datetime >= DATEADD(day, -8, SYSDATETIME())
GROUP BY r.run_id, r.source_system, r.entity_name, r.reject_reason
UNION ALL
SELECT w.run_start, 'Run failure', NULL, 'Failure', CONCAT(w.script_name, ' failed'), LEFT(w.error_message, 1000), 1
FROM dbo.warehouse_run_log w
WHERE w.run_status = 'FAILED' AND w.run_start >= DATEADD(day, -8, SYSDATETIME());
GO

DROP VIEW IF EXISTS dbo.v_health_people;
GO

CREATE VIEW dbo.v_health_people AS
SELECT
    (SELECT COUNT(*) FROM dbo.person_client_link WHERE is_current = 1) AS profiles,
    (SELECT COUNT(DISTINCT person_key) FROM dbo.person_client_link WHERE is_current = 1) AS people,
    (SELECT COUNT(*) FROM (SELECT person_key FROM dbo.person_client_link WHERE is_current = 1
                           GROUP BY person_key HAVING COUNT(*) > 1) x) AS people_with_linked_profiles,
    (SELECT COUNT(*) FROM rpt.person WHERE has_unconfirmed_link = 1) AS people_with_unconfirmed_link,
    (SELECT COUNT(*) FROM dbo.match_review_queue) AS review_queue,
    (SELECT COUNT(*) FROM dbo.match_decision
     WHERE decision_method = 'reviewer' AND decided_datetime >= DATEADD(day, -7, SYSDATETIME())) AS reviewer_decisions_7_days,
    (SELECT COUNT(*) FROM dbo.person_client_link
     WHERE link_method <> 'self' AND valid_from >= DATEADD(day, -7, SYSDATETIME())) AS links_made_7_days,
    (SELECT COUNT(*) FROM dbo.person_client_link l
     WHERE l.link_method = 'self' AND l.valid_from >= DATEADD(day, -7, SYSDATETIME())
       AND EXISTS (SELECT 1 FROM dbo.person_client_link p
                   WHERE p.client_key = l.client_key AND p.valid_to = l.valid_from AND p.link_method <> 'self')) AS unlinked_7_days,
    (SELECT COUNT(*) FROM dbo.stage_load_reject r
     WHERE r.entity_name = 'person_link'
       AND r.run_id = (SELECT MAX(g.run_id) FROM dbo.stage_load_log g
                       WHERE g.entity_name = 'person_client_link' AND g.load_status = 'SUCCESS')) AS open_conflicts,
    (SELECT COUNT(DISTINCT task_id) FROM dbo.v_apricot_worklist) AS apricot_tasks;
GO

-- Dropping a view drops the permissions granted on it. Give back what 04_security grants on the views above, so a
-- rerun of this migration never locks the health dashboard out.
IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'pipeline_monitor' AND type = 'R')
BEGIN
    GRANT SELECT ON dbo.v_health_run_steps     TO pipeline_monitor;
    GRANT SELECT ON dbo.v_health_runs          TO pipeline_monitor;
    GRANT SELECT ON dbo.v_health_table_changes TO pipeline_monitor;
    GRANT SELECT ON dbo.v_health_issues        TO pipeline_monitor;
    GRANT SELECT ON dbo.v_health_people        TO pipeline_monitor;
    GRANT SELECT ON dbo.v_apricot_worklist     TO pipeline_monitor;
END;
GO

-- 6. SCHEMA VERSION + LOG RUN END
IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.16')
    INSERT INTO dbo.warehouse_schema_version (version, applied_date, applied_by, summary, details)
    VALUES ('v8.16', SYSDATETIME(), USER_NAME(),
            'Matching: match_review_queue, apricot_profile_map, v_apricot_worklist, v_match_review_pairs, v_health_* views',
            'migrations/0003_matching.sql');

UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'SUCCESS'
WHERE script_name = 'migration_0003' AND run_status = 'RUNNING';
GO

-- 7. CHECK: every row should say 1. Then install 06_proc_person_links.sql v1.1 and 03b v3.1,
--    and rerun 04_security v1.9.
-- Each check is worked out on its own: Fabric cannot mix catalog views or metadata functions with a
-- warehouse table in one query (Msg 15816), so the result is built from variables only.
DECLARE @ok1 INT = CASE WHEN EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.16') THEN 1 ELSE 0 END;
DECLARE @ok2 INT = CASE WHEN OBJECT_ID('dbo.match_review_queue') IS NOT NULL THEN 1 ELSE 0 END;
DECLARE @ok3 INT = CASE WHEN OBJECT_ID('dbo.apricot_profile_map') IS NOT NULL THEN 1 ELSE 0 END;
DECLARE @ok4 INT = CASE WHEN OBJECT_ID('dbo.v_apricot_worklist') IS NOT NULL THEN 1 ELSE 0 END;
DECLARE @ok5 INT = CASE WHEN OBJECT_ID('dbo.v_match_review_pairs') IS NOT NULL THEN 1 ELSE 0 END;
DECLARE @ok6 INT = CASE WHEN OBJECT_ID('dbo.v_health_runs') IS NOT NULL THEN 1 ELSE 0 END;
DECLARE @ok7 INT = CASE WHEN OBJECT_ID('dbo.v_health_table_changes') IS NOT NULL THEN 1 ELSE 0 END;
DECLARE @ok8 INT = CASE WHEN OBJECT_ID('dbo.v_health_issues') IS NOT NULL THEN 1 ELSE 0 END;
DECLARE @ok9 INT = CASE WHEN OBJECT_ID('dbo.v_health_people') IS NOT NULL THEN 1 ELSE 0 END;
SELECT check_name, ok FROM (VALUES
    ('v8.16 recorded', @ok1),
    ('dbo.match_review_queue', @ok2),
    ('dbo.apricot_profile_map', @ok3),
    ('dbo.v_apricot_worklist', @ok4),
    ('dbo.v_match_review_pairs', @ok5),
    ('dbo.v_health_runs', @ok6),
    ('dbo.v_health_table_changes', @ok7),
    ('dbo.v_health_issues', @ok8),
    ('dbo.v_health_people', @ok9)
) v (check_name, ok);
GO
