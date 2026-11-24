-- migrations/0002_person_links.sql  (warehouse schema v8.14 -> v8.15)
--
-- Adds the person layer: one person can have several source profiles (a Salesforce contact,
-- a Clarity client), and the warehouse LINKS them instead of merging them. Every source profile
-- stays exactly as its system sent it. A wrong link is undone by changing one decision.
--
-- Run once, after 0001 and before installing 06_proc_person_links.sql.
-- Safe to rerun: every change checks first. Never drops a table or a column.
-- Create a user-defined restore point first (Warehouse > Settings > Restore points > Add).
--
-- What it adds:
--   1. Schema rpt: the reporting layer. It holds TABLES, refreshed by usp_refresh_person_links after
--      every load, not views. In Fabric a view does not hide the tables behind it (no ownership
--      chaining), so report users would need access to the core tables for a view to work.
--   2. dbo.match_decision: every "same person" / "not the same person" decision, keyed on SOURCE
--      IDs (Salesforce Contact Id, Clarity Personal ID), so decisions survive reloads and even an
--      admin clear. Written by reviewers and, later, the matching notebook, through
--      dbo.usp_record_match_decision. Old decisions are never deleted, only made inactive.
--   3. dbo.person_client_link: which person each source profile (client_key) belongs to, with
--      history (valid_from / valid_to, half-open, same rule as dim_client). A report can be rerun
--      "as of" the date it was submitted and give the same unique count.
--   4. rpt.person: one row per person, the merged ("golden") profile, built from the linked
--      profiles by the survivorship rule in dbo.v_person_golden.
--      rpt.person_client: each source profile with its person_key, for joining facts.
--   5. dbo.v_person_golden: the survivorship logic (internal; rpt.person is a stored copy of it).
--      Rule (docs/design_decisions.md, decision 7): for each field, the newest non-blank value, preferring
--      profiles not flagged deleted; address fields come together from one profile; a date of birth
--      marked approximate, partial, unknown or refused loses to a full or unrated one even if newer.
--      Sensitive columns (ssn_hash, ssn_last4, gender_identity, sexual_orientation) are left out.

-- 0. LOG RUN START
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'ABANDONED'
WHERE script_name = 'migration_0002' AND run_status = 'RUNNING';

INSERT INTO dbo.warehouse_run_log (script_name, script_version, run_start, run_status, run_by, change_summary)
VALUES ('migration_0002', 'v8.15', SYSDATETIME(), 'RUNNING', USER_NAME(),
        'Person layer: rpt schema, match_decision, person_client_link, rpt.person, rpt.person_client');
GO

-- 1. REPORTING SCHEMA
IF SCHEMA_ID('rpt') IS NULL
    EXEC('CREATE SCHEMA rpt');
GO

-- 2. MATCH DECISIONS (keyed on source IDs; pair stored in a fixed order, a < b)
IF OBJECT_ID('dbo.match_decision', 'U') IS NULL
CREATE TABLE dbo.match_decision (
    decision_key          BIGINT IDENTITY NOT NULL,
    source_system_a       VARCHAR(50)  NOT NULL,
    source_id_a           VARCHAR(100) NOT NULL,
    source_system_b       VARCHAR(50)  NOT NULL,
    source_id_b           VARCHAR(100) NOT NULL,
    decision              VARCHAR(10)  NOT NULL,
    decision_method       VARCHAR(20)  NOT NULL,
    match_probability     DECIMAL(9,8),
    decided_by            VARCHAR(255) NOT NULL,
    decided_datetime      DATETIME2(6) NOT NULL,
    notes                 VARCHAR(500),
    is_active             BIT NOT NULL,
    revoked_by            VARCHAR(255),
    revoked_datetime      DATETIME2(6)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_match_decision')
    ALTER TABLE dbo.match_decision ADD CONSTRAINT PK_match_decision PRIMARY KEY NONCLUSTERED (decision_key) NOT ENFORCED;
GO

-- 3. PERSON LINKS (history kept; exactly one current row per client_key)
IF OBJECT_ID('dbo.person_client_link', 'U') IS NULL
CREATE TABLE dbo.person_client_link (
    link_key              BIGINT IDENTITY NOT NULL,
    client_key            BIGINT       NOT NULL,
    person_key            BIGINT       NOT NULL,
    link_method           VARCHAR(20)  NOT NULL,
    decision_key          BIGINT,
    valid_from            DATETIME2(6) NOT NULL,
    valid_to              DATETIME2(6),
    is_current            BIT          NOT NULL,
    run_id                VARCHAR(60),
    pipeline_run_id       VARCHAR(100)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_person_client_link')
    ALTER TABLE dbo.person_client_link ADD CONSTRAINT PK_person_client_link PRIMARY KEY NONCLUSTERED (link_key) NOT ENFORCED;
GO

-- 4. REPORTING TABLES (contents replaced by usp_refresh_person_links inside its transaction)
IF OBJECT_ID('rpt.person', 'U') IS NULL
CREATE TABLE rpt.person (
    person_key            BIGINT       NOT NULL,
    profile_count         INT          NOT NULL,
    active_profile_count  INT          NOT NULL,
    salesforce_profiles   INT          NOT NULL,
    hmis_profiles         INT          NOT NULL,
    is_active             BIT          NOT NULL,
    has_unconfirmed_link  BIT          NOT NULL,
    salesforce_id         VARCHAR(18),
    hmis_personal_id      VARCHAR(50),
    first_name            VARCHAR(100),
    middle_name           VARCHAR(100),
    last_name             VARCHAR(100),
    date_of_birth         DATE,
    dob_data_quality      VARCHAR(50),
    sex                   VARCHAR(50),
    gender                VARCHAR(50),
    race                  VARCHAR(255),
    ethnicity             VARCHAR(50),
    veteran_status        VARCHAR(50),
    military_branch       VARCHAR(50),
    discharge_status      VARCHAR(50),
    email                 VARCHAR(255),
    home_phone            VARCHAR(30),
    mobile_phone          VARCHAR(30),
    address_line1         VARCHAR(255),
    city                  VARCHAR(100),
    state                 VARCHAR(10),
    postal_code           VARCHAR(20),
    county                VARCHAR(100),
    first_seen_date       DATETIME2(6),
    last_updated          DATETIME2(6),
    refreshed_datetime    DATETIME2(6) NOT NULL
);
GO

IF OBJECT_ID('rpt.person_client', 'U') IS NULL
CREATE TABLE rpt.person_client (
    client_key            BIGINT       NOT NULL,
    person_key            BIGINT       NOT NULL,
    source_system         VARCHAR(50),
    salesforce_id         VARCHAR(18),
    hmis_personal_id      VARCHAR(50),
    link_method           VARCHAR(20)  NOT NULL,
    linked_since          DATETIME2(6) NOT NULL,
    is_deleted            BIT          NOT NULL,
    refreshed_datetime    DATETIME2(6) NOT NULL
);
GO

-- 5. SURVIVORSHIP VIEW (internal: report users read rpt.person, which is a stored copy)
DROP VIEW IF EXISTS dbo.v_person_golden;
GO

CREATE VIEW dbo.v_person_golden AS
WITH p AS (
    SELECT l.person_key, l.link_method, c.client_key, c.source_system,
           c.salesforce_id, c.hmis_personal_id,
           c.first_name, c.middle_name, c.last_name, c.date_of_birth, c.dob_data_quality,
           c.sex, c.gender, c.race, c.ethnicity, c.veteran_status, c.military_branch, c.discharge_status,
           c.email, c.home_phone, c.mobile_phone, c.address_line1, c.city, c.state, c.postal_code, c.county,
           c.source_added_date,
           CASE WHEN ISNULL(c.is_deleted, 0) = 1 THEN 1 ELSE 0 END AS deleted_flag,
           -- HUD DOB quality: 1 full, 2 approximate or partial, 8 doesn't know, 9 refused, 99 not collected.
           -- Salesforce has no quality field, so its dates count as full.
           CASE WHEN LTRIM(RTRIM(ISNULL(c.dob_data_quality, ''))) IN ('2', '8', '9', '99')
                  OR c.dob_data_quality LIKE 'Approximate%'
                  OR c.dob_data_quality LIKE 'Client%'
                  OR c.dob_data_quality LIKE 'Data not collected%'
                THEN 1 ELSE 0 END AS dob_weak,
           COALESCE(c.source_last_updated, c.last_modified_date, c.source_added_date, c.valid_from) AS recency
    FROM dbo.person_client_link l
    JOIN dbo.dim_client c ON c.client_key = l.client_key AND c.is_current = 1
    WHERE l.is_current = 1
),
v AS (
    SELECT person_key,
        ROW_NUMBER() OVER (PARTITION BY person_key ORDER BY client_key) AS rn,
        FIRST_VALUE(salesforce_id)     OVER (PARTITION BY person_key ORDER BY CASE WHEN salesforce_id     IS NULL OR salesforce_id     = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS salesforce_id,
        FIRST_VALUE(hmis_personal_id)  OVER (PARTITION BY person_key ORDER BY CASE WHEN hmis_personal_id  IS NULL OR hmis_personal_id  = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS hmis_personal_id,
        FIRST_VALUE(first_name)        OVER (PARTITION BY person_key ORDER BY CASE WHEN first_name        IS NULL OR first_name        = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS first_name,
        FIRST_VALUE(middle_name)       OVER (PARTITION BY person_key ORDER BY CASE WHEN middle_name       IS NULL OR middle_name       = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS middle_name,
        FIRST_VALUE(last_name)         OVER (PARTITION BY person_key ORDER BY CASE WHEN last_name         IS NULL OR last_name         = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS last_name,
        FIRST_VALUE(date_of_birth)     OVER (PARTITION BY person_key ORDER BY CASE WHEN date_of_birth     IS NULL THEN 1 ELSE 0 END, dob_weak,                     deleted_flag, recency DESC, client_key) AS date_of_birth,
        FIRST_VALUE(dob_data_quality)  OVER (PARTITION BY person_key ORDER BY CASE WHEN date_of_birth     IS NULL THEN 1 ELSE 0 END, dob_weak,                     deleted_flag, recency DESC, client_key) AS dob_data_quality,
        FIRST_VALUE(sex)               OVER (PARTITION BY person_key ORDER BY CASE WHEN sex               IS NULL OR sex               = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS sex,
        FIRST_VALUE(gender)            OVER (PARTITION BY person_key ORDER BY CASE WHEN gender            IS NULL OR gender            = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS gender,
        FIRST_VALUE(race)              OVER (PARTITION BY person_key ORDER BY CASE WHEN race              IS NULL OR race              = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS race,
        FIRST_VALUE(ethnicity)         OVER (PARTITION BY person_key ORDER BY CASE WHEN ethnicity         IS NULL OR ethnicity         = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS ethnicity,
        FIRST_VALUE(veteran_status)    OVER (PARTITION BY person_key ORDER BY CASE WHEN veteran_status    IS NULL OR veteran_status    = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS veteran_status,
        FIRST_VALUE(military_branch)   OVER (PARTITION BY person_key ORDER BY CASE WHEN military_branch   IS NULL OR military_branch   = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS military_branch,
        FIRST_VALUE(discharge_status)  OVER (PARTITION BY person_key ORDER BY CASE WHEN discharge_status  IS NULL OR discharge_status  = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS discharge_status,
        FIRST_VALUE(email)             OVER (PARTITION BY person_key ORDER BY CASE WHEN email             IS NULL OR email             = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS email,
        FIRST_VALUE(home_phone)        OVER (PARTITION BY person_key ORDER BY CASE WHEN home_phone        IS NULL OR home_phone        = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS home_phone,
        FIRST_VALUE(mobile_phone)      OVER (PARTITION BY person_key ORDER BY CASE WHEN mobile_phone      IS NULL OR mobile_phone      = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS mobile_phone,
        FIRST_VALUE(address_line1)     OVER (PARTITION BY person_key ORDER BY CASE WHEN address_line1     IS NULL OR address_line1     = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS address_line1,
        FIRST_VALUE(city)              OVER (PARTITION BY person_key ORDER BY CASE WHEN address_line1     IS NULL OR address_line1     = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS city,
        FIRST_VALUE(state)             OVER (PARTITION BY person_key ORDER BY CASE WHEN address_line1     IS NULL OR address_line1     = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS state,
        FIRST_VALUE(postal_code)       OVER (PARTITION BY person_key ORDER BY CASE WHEN address_line1     IS NULL OR address_line1     = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS postal_code,
        FIRST_VALUE(county)            OVER (PARTITION BY person_key ORDER BY CASE WHEN address_line1     IS NULL OR address_line1     = '' THEN 1 ELSE 0 END, deleted_flag, recency DESC, client_key) AS county
    FROM p
),
agg AS (
    SELECT person_key,
           COUNT(*)                                                       AS profile_count,
           SUM(CASE WHEN deleted_flag = 0 THEN 1 ELSE 0 END)              AS active_profile_count,
           SUM(CASE WHEN source_system = 'sf_cis' THEN 1 ELSE 0 END)      AS salesforce_profiles,
           SUM(CASE WHEN source_system LIKE 'hmis%' THEN 1 ELSE 0 END)    AS hmis_profiles,
           MAX(CASE WHEN link_method = 'auto_score' THEN 1 ELSE 0 END)    AS has_unconfirmed_link,
           MIN(source_added_date)                                         AS first_seen_date,
           MAX(recency)                                                   AS last_updated
    FROM p
    GROUP BY person_key
)
SELECT v.person_key, agg.profile_count, agg.active_profile_count, agg.salesforce_profiles, agg.hmis_profiles,
       CASE WHEN agg.active_profile_count > 0 THEN 1 ELSE 0 END AS is_active,
       agg.has_unconfirmed_link,
       v.salesforce_id, v.hmis_personal_id,
       v.first_name, v.middle_name, v.last_name, v.date_of_birth, v.dob_data_quality,
       v.sex, v.gender, v.race, v.ethnicity, v.veteran_status, v.military_branch, v.discharge_status,
       v.email, v.home_phone, v.mobile_phone, v.address_line1, v.city, v.state, v.postal_code, v.county,
       agg.first_seen_date, agg.last_updated
FROM v
JOIN agg ON agg.person_key = v.person_key
WHERE v.rn = 1;
GO

-- 6. SCHEMA VERSION + LOG RUN END
IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.15')
    INSERT INTO dbo.warehouse_schema_version (version, applied_date, applied_by, summary, details)
    VALUES ('v8.15', SYSDATETIME(), USER_NAME(),
            'Person layer: rpt schema, match_decision, person_client_link, rpt.person, rpt.person_client, v_person_golden',
            'migrations/0002_person_links.sql');

UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'SUCCESS'
WHERE script_name = 'migration_0002' AND run_status = 'RUNNING';
GO

-- 7. CHECK: every row should say 1. Then install 06_proc_person_links.sql, rerun 04_security,
--    and run EXEC dbo.usp_refresh_person_links; once to give every existing profile its person.
-- Each check is worked out on its own: Fabric cannot mix catalog views or metadata functions with a
-- warehouse table in one query (Msg 15816), so the result is built from variables only.
DECLARE @ok1 INT = CASE WHEN EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.15') THEN 1 ELSE 0 END;
DECLARE @ok2 INT = CASE WHEN SCHEMA_ID('rpt') IS NOT NULL THEN 1 ELSE 0 END;
DECLARE @ok3 INT = CASE WHEN OBJECT_ID('dbo.match_decision') IS NOT NULL THEN 1 ELSE 0 END;
DECLARE @ok4 INT = CASE WHEN OBJECT_ID('dbo.person_client_link') IS NOT NULL THEN 1 ELSE 0 END;
DECLARE @ok5 INT = CASE WHEN OBJECT_ID('rpt.person') IS NOT NULL THEN 1 ELSE 0 END;
DECLARE @ok6 INT = CASE WHEN OBJECT_ID('rpt.person_client') IS NOT NULL THEN 1 ELSE 0 END;
DECLARE @ok7 INT = CASE WHEN OBJECT_ID('dbo.v_person_golden') IS NOT NULL THEN 1 ELSE 0 END;
SELECT check_name, ok FROM (VALUES
    ('v8.15 recorded', @ok1),
    ('schema rpt', @ok2),
    ('dbo.match_decision', @ok3),
    ('dbo.person_client_link', @ok4),
    ('rpt.person', @ok5),
    ('rpt.person_client', @ok6),
    ('dbo.v_person_golden', @ok7)
) v (check_name, ok);
GO
