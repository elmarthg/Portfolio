-- Fabric Warehouse DDL v8.13 (baseline, guarded 2026-11-13)
-- The BASELINE schema. Run it once on an empty warehouse, then apply migrations/ in number order.
-- Fresh build order: 01 DDL, migrations/0000 (all PASS), 0001 and 0002, 02 seed, 02b seed, run one Silver notebook, migrations/0003, 04 security, then install 03a, 03b, 05 and 06.
-- Guard: every batch that drops or creates objects refuses to run (error 50020) once dim_client or
-- fact_enrollment holds data, so this file can never wipe a live warehouse. Schema changes after the
-- first load are migrations (ALTER TABLE ... ADD), never a rerun of this file.

IF OBJECT_ID('dbo.warehouse_run_log', 'U') IS NULL
CREATE TABLE dbo.warehouse_run_log (
    run_key          BIGINT IDENTITY NOT NULL,
    script_name      VARCHAR(100) NOT NULL,
    script_version   VARCHAR(20),
    run_start        DATETIME2(6) NOT NULL,
    run_end          DATETIME2(6),
    run_status       VARCHAR(20),
    run_by           VARCHAR(255),
    change_summary   VARCHAR(500),
    error_message    VARCHAR(MAX),
    notes            VARCHAR(MAX)
);
GO

IF OBJECT_ID('dbo.warehouse_schema_version', 'U') IS NULL
CREATE TABLE dbo.warehouse_schema_version (
    version_key     BIGINT IDENTITY NOT NULL,
    version         VARCHAR(20) NOT NULL,
    applied_date    DATETIME2(6) NOT NULL,
    applied_by      VARCHAR(255),
    summary         VARCHAR(500),
    details         VARCHAR(MAX)
);
GO

-- -0. LOG RUN START
IF OBJECT_ID('dbo.dim_client', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.dim_client)
        THROW 50020, '01_ddl stopped: the warehouse holds loaded data. Schema changes go in migrations/. To rebuild from empty on purpose, run dbo.usp_admin_clear_warehouse_data first.', 1;
END;
IF OBJECT_ID('dbo.fact_enrollment', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.fact_enrollment)
        THROW 50020, '01_ddl stopped: the warehouse holds loaded data. Schema changes go in migrations/. To rebuild from empty on purpose, run dbo.usp_admin_clear_warehouse_data first.', 1;
END;
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'ABANDONED'
WHERE script_name = '01_ddl' AND run_status = 'RUNNING';

INSERT INTO dbo.warehouse_run_log (
    script_name, script_version, run_start, run_status, run_by, change_summary
)
VALUES (
    '01_ddl', 'v8.13', SYSDATETIME(), 'RUNNING', USER_NAME(),
    'DDL rebuild: dim_client_identifier holds full SSN, ssn_last4 on dim_client, soft-delete columns on client, enrollment, service, case note'
);
GO

-- 0. DROP EXISTING OBJECTS
IF OBJECT_ID('dbo.dim_client', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.dim_client)
        THROW 50020, '01_ddl stopped: the warehouse holds loaded data. Schema changes go in migrations/. To rebuild from empty on purpose, run dbo.usp_admin_clear_warehouse_data first.', 1;
END;
IF OBJECT_ID('dbo.fact_enrollment', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.fact_enrollment)
        THROW 50020, '01_ddl stopped: the warehouse holds loaded data. Schema changes go in migrations/. To rebuild from empty on purpose, run dbo.usp_admin_clear_warehouse_data first.', 1;
END;
DROP SECURITY POLICY IF EXISTS dbo.ClientProgramAccessPolicy;
DROP FUNCTION IF EXISTS dbo.fn_rls_client_program_access;

DROP VIEW IF EXISTS dbo.v_data_quality_unresolved;
DROP VIEW IF EXISTS dbo.v_kpi_target;
DROP VIEW IF EXISTS dbo.v_fact_enrollment_primary;
DROP VIEW IF EXISTS dbo.v_fact_enrollment_all;

DROP FUNCTION IF EXISTS dbo.fn_normalize_key;

DECLARE @drop_fk_sql NVARCHAR(MAX) = N'';

SELECT @drop_fk_sql = @drop_fk_sql
    + N'ALTER TABLE dbo.[' + OBJECT_NAME(fk.parent_object_id)
    + N'] DROP CONSTRAINT [' + fk.name + N'];' + CHAR(10)
FROM sys.foreign_keys fk
WHERE OBJECT_NAME(fk.parent_object_id) IN (
    'dim_site','dim_grant','dim_funding_source','dim_household','dim_organization','dim_source',
    'dim_client','dim_client_identifier','dim_program','dim_service_item','dim_bed','dim_date','dim_staff',
    'bridge_household_client','bridge_client_source',
    'fact_enrollment','fact_service','fact_case_note','fact_assessment',
    'fact_assessment_response','fact_document','fact_bed_night','fact_bed_availability',
    'fact_financial_assistance','fact_ce_event','fact_consent',
    'fact_enrollment_stage','fact_current_living_situation',
    'bridge_enrollment_grant','bridge_program_site',
    'dim_hud_code','kpi_target','etl_source_config',
    'stage_load_log','stage_load_reject','security_user_program_access'
);

IF LEN(@drop_fk_sql) > 0
BEGIN
    PRINT @drop_fk_sql;
    EXEC sp_executesql @drop_fk_sql;
END
GO

IF OBJECT_ID('dbo.dim_client', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.dim_client)
        THROW 50020, '01_ddl stopped: the warehouse holds loaded data. Schema changes go in migrations/. To rebuild from empty on purpose, run dbo.usp_admin_clear_warehouse_data first.', 1;
END;
IF OBJECT_ID('dbo.fact_enrollment', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.fact_enrollment)
        THROW 50020, '01_ddl stopped: the warehouse holds loaded data. Schema changes go in migrations/. To rebuild from empty on purpose, run dbo.usp_admin_clear_warehouse_data first.', 1;
END;
DROP TABLE IF EXISTS stage_load_reject;
DROP TABLE IF EXISTS stage_load_log;

DROP TABLE IF EXISTS fact_current_living_situation;
DROP TABLE IF EXISTS fact_enrollment_stage;
DROP TABLE IF EXISTS fact_consent;
DROP TABLE IF EXISTS fact_ce_event;
DROP TABLE IF EXISTS fact_financial_assistance;
DROP TABLE IF EXISTS fact_assessment_response;
DROP TABLE IF EXISTS fact_assessment;
DROP TABLE IF EXISTS fact_document;
DROP TABLE IF EXISTS fact_bed_availability;
DROP TABLE IF EXISTS fact_bed_night;
DROP TABLE IF EXISTS fact_case_note;
DROP TABLE IF EXISTS fact_service;

DROP TABLE IF EXISTS bridge_household_client;
DROP TABLE IF EXISTS bridge_client_source;
DROP TABLE IF EXISTS bridge_enrollment_grant;
DROP TABLE IF EXISTS bridge_program_site;

DROP TABLE IF EXISTS fact_enrollment;

DROP TABLE IF EXISTS dim_bed;
DROP TABLE IF EXISTS dim_staff;
DROP TABLE IF EXISTS dim_client_identifier;
DROP TABLE IF EXISTS dim_client;
DROP TABLE IF EXISTS dim_program;

DROP TABLE IF EXISTS dim_service_item;
DROP TABLE IF EXISTS dim_funding_source;
DROP TABLE IF EXISTS dim_grant;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_site;
DROP TABLE IF EXISTS dim_organization;
DROP TABLE IF EXISTS dim_household;
DROP TABLE IF EXISTS dim_source;
GO

-- 1. DIMENSIONS
IF OBJECT_ID('dbo.dim_client', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.dim_client)
        THROW 50020, '01_ddl stopped: the warehouse holds loaded data. Schema changes go in migrations/. To rebuild from empty on purpose, run dbo.usp_admin_clear_warehouse_data first.', 1;
END;
IF OBJECT_ID('dbo.fact_enrollment', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.fact_enrollment)
        THROW 50020, '01_ddl stopped: the warehouse holds loaded data. Schema changes go in migrations/. To rebuild from empty on purpose, run dbo.usp_admin_clear_warehouse_data first.', 1;
END;

CREATE TABLE dim_site (
    site_key                BIGINT IDENTITY NOT NULL,
    salesforce_id           VARCHAR(18),
    apricot_organization_id VARCHAR(50),
    wellsky_site_id         VARCHAR(50),
    hmis_site_id            VARCHAR(50),
    site_name               VARCHAR(255),
    organization_name       VARCHAR(255),
    address_line1           VARCHAR(255),
    address_line2           VARCHAR(255),
    city                    VARCHAR(100),
    state                   VARCHAR(10),
    postal_code             VARCHAR(20),
    county                  VARCHAR(100),
    region                  VARCHAR(100),
    source_system           VARCHAR(50),
    load_datetime           DATETIME2(6)
);

CREATE TABLE dim_grant (
    grant_key               BIGINT IDENTITY NOT NULL,
    salesforce_id           VARCHAR(18),
    apricot_grant_id        VARCHAR(50),
    hmis_grant_id           VARCHAR(50),
    wellsky_grant_id        VARCHAR(50),
    grant_identifier        VARCHAR(100),
    grant_name              VARCHAR(255),
    grant_description       VARCHAR(MAX),
    funder_name             VARCHAR(255),
    funder_type             VARCHAR(100),
    funder_code             VARCHAR(20),
    grant_type              VARCHAR(100),
    coc_code                VARCHAR(20),
    grant_start_date        DATE,
    grant_end_date          DATE,
    award_date              DATE,
    fiscal_year             INT,
    original_award_amount   DECIMAL(18,2),
    award_amount            DECIMAL(18,2),
    adjustment_amount       DECIMAL(18,2),
    match_required_amount   DECIMAL(18,2),
    grant_status            VARCHAR(50),
    is_active               BIT,
    administering_agency    VARCHAR(255),
    grant_manager           VARCHAR(255),
    reporting_frequency     VARCHAR(50),
    source_system           VARCHAR(50),
    source_added_date       DATETIME2(6),
    source_last_updated     DATETIME2(6),
    is_deleted              BIT,
    load_datetime           DATETIME2(6)
);

CREATE TABLE dim_funding_source (
    funding_source_key      BIGINT IDENTITY NOT NULL,
    grant_key               BIGINT,
    salesforce_id           VARCHAR(18),
    apricot_funding_id      VARCHAR(50),
    hmis_funding_source_id  VARCHAR(50),
    wellsky_funding_id      VARCHAR(50),
    program_id              INT,
    funding_source_name     VARCHAR(255),
    funding_source_type     VARCHAR(255),
    funding_source_code     VARCHAR(20),
    funder_category         VARCHAR(100),
    funding_source_amount   DECIMAL(18,2),
    amount_expended         DECIMAL(18,2),
    amount_remaining        DECIMAL(18,2),
    start_date              DATE,
    end_date                DATE,
    is_active               BIT,
    is_direct_expense       BIT,
    is_eligibility          BIT,
    is_match_source         BIT,
    eligibility_notes       VARCHAR(MAX),
    source_system           VARCHAR(50),
    source_added_date       DATETIME2(6),
    source_last_updated     DATETIME2(6),
    is_deleted              BIT,
    load_datetime           DATETIME2(6)
);

CREATE TABLE dim_household (
    household_key            BIGINT IDENTITY NOT NULL,
    salesforce_id            VARCHAR(18),
    apricot_record_id        VARCHAR(50),
    wellsky_household_id     VARCHAR(50),
    hmis_household_id        VARCHAR(50),
    household_name           VARCHAR(255),
    household_type           VARCHAR(50),
    household_size           INT,
    address_line1            VARCHAR(255),
    city                     VARCHAR(100),
    state                    VARCHAR(10),
    postal_code              VARCHAR(20),
    county                   VARCHAR(100),
    household_origin         VARCHAR(50),
    synthetic_for_client_key BIGINT,
    source_system            VARCHAR(50),
    source_agency            VARCHAR(255),
    source_added_date        DATETIME2(6),
    source_last_updated      DATETIME2(6),
    created_date             DATETIME2(6),
    last_modified_date       DATETIME2(6),
    is_deleted               BIT,
    load_datetime            DATETIME2(6)
);

CREATE TABLE dim_organization (
    organization_key        BIGINT IDENTITY NOT NULL,
    salesforce_id           VARCHAR(18),
    apricot_record_id       VARCHAR(50),
    wellsky_organization_id VARCHAR(50),
    hmis_organization_id    VARCHAR(50),
    organization_name       VARCHAR(255),
    organization_type       VARCHAR(100),
    organization_phone      VARCHAR(30),
    address_line1           VARCHAR(255),
    city                    VARCHAR(100),
    state                   VARCHAR(10),
    postal_code             VARCHAR(20),
    county                  VARCHAR(100),
    coc_code                VARCHAR(20),
    victim_service_provider BIT,
    source_system           VARCHAR(50),
    source_agency           VARCHAR(255),
    source_added_date       DATETIME2(6),
    source_last_updated     DATETIME2(6),
    is_deleted              BIT,
    load_datetime           DATETIME2(6)
);

CREATE TABLE dim_source (
    source_system           VARCHAR(50)  NOT NULL,
    platform                VARCHAR(50)  NOT NULL,
    instance_name           VARCHAR(255),
    county                  VARCHAR(100),
    ingest_method           VARCHAR(20),
    is_active               BIT,
    load_datetime           DATETIME2(6)
);

CREATE TABLE dim_client (
    client_version_key    BIGINT IDENTITY NOT NULL,
    client_key            BIGINT NOT NULL,
    is_current            BIT NOT NULL,
    valid_from            DATETIME2(6) NOT NULL,
    valid_to              DATETIME2(6) NULL,
    row_hash              VARCHAR(64) NOT NULL,
    household_key         BIGINT,
    salesforce_id         VARCHAR(18),
    salesforce_account_id VARCHAR(18),
    external_id           VARCHAR(50),
    apricot_record_id     VARCHAR(50),
    wellsky_client_uid    VARCHAR(50),
    hmis_client_id        VARCHAR(50),
    hmis_personal_id      VARCHAR(50),
    champ_client_id       VARCHAR(50),
    first_name            VARCHAR(100),
    middle_name           VARCHAR(100),
    last_name             VARCHAR(100),
    date_of_birth         DATE,
    sex                   VARCHAR(50),
    gender                VARCHAR(50),
    race                  VARCHAR(255),
    race_detail           VARCHAR(500),
    ethnicity             VARCHAR(50),
    marital_status        VARCHAR(50),
    number_of_children    VARCHAR(50),
    education_level       VARCHAR(100),
    primary_language      VARCHAR(50),
    ssn_hash              VARCHAR(128) MASKED WITH (FUNCTION = 'default()'),
    ssn_last4             VARCHAR(4)   MASKED WITH (FUNCTION = 'default()'),
    gender_identity       VARCHAR(255) MASKED WITH (FUNCTION = 'default()'),
    sexual_orientation    VARCHAR(100) MASKED WITH (FUNCTION = 'default()'),
    name_data_quality     VARCHAR(50),
    ssn_data_quality      VARCHAR(50),
    dob_data_quality      VARCHAR(50),
    email                 VARCHAR(255),
    home_phone            VARCHAR(30),
    mobile_phone          VARCHAR(30),
    address_line1         VARCHAR(255),
    city                  VARCHAR(100),
    state                 VARCHAR(10),
    postal_code           VARCHAR(20),
    county                VARCHAR(100),
    military_branch            VARCHAR(50),
    military_service_component VARCHAR(50),
    discharge_status           VARCHAR(50),
    military_entry_date        DATE,
    military_discharge_date    DATE,
    is_post_9_11               VARCHAR(20),
    service_connected          VARCHAR(50),
    period_of_service          VARCHAR(255),
    veteran_status             VARCHAR(50),
    homeless_status        VARCHAR(50),
    housing_status         VARCHAR(100),
    length_of_homelessness VARCHAR(100),
    has_conviction                  VARCHAR(50),
    conviction_type                 VARCHAR(255),
    conviction_date                 DATE,
    probation_parole_status         VARCHAR(50),
    parole_end_date                 DATE,
    last_release_date               DATE,
    experience_during_incarceration VARCHAR(255),
    service_type          VARCHAR(100),
    lead_source           VARCHAR(100),
    source_system         VARCHAR(50),
    source_agency         VARCHAR(255),
    source_added_date     DATETIME2(6),
    source_last_updated   DATETIME2(6),
    owner_id              VARCHAR(18),
    created_by_id         VARCHAR(255),
    last_modified_by_id   VARCHAR(255),
    created_date          DATETIME2(6),
    last_modified_date    DATETIME2(6),
    is_deleted            BIT,
    deleted_detected_datetime DATETIME2(6),
    load_datetime         DATETIME2(6)
);

-- Full SSN lives only here. Apricot integration reads it; report users are denied (04_security).
CREATE TABLE dim_client_identifier (
    client_key            BIGINT NOT NULL,
    source_system         VARCHAR(50) NOT NULL,
    source_client_id      VARCHAR(50) NOT NULL,
    ssn_raw               VARCHAR(11)  MASKED WITH (FUNCTION = 'partial(0,"XXX-XX-",4)'),
    ssn_data_quality      VARCHAR(50),
    load_datetime         DATETIME2(6)
);

CREATE TABLE dim_program (
    program_id              INT NOT NULL,
    program_code            VARCHAR(50),
    sf_picklist_value       VARCHAR(255),
    program_name            VARCHAR(255),
    organization_key        BIGINT,
    site_key                BIGINT,
    hmis_project_id         VARCHAR(50),
    wellsky_program_id      VARCHAR(50),
    apricot_record_id       VARCHAR(50),
    salesforce_id           VARCHAR(18),
    salesforce_program_id   VARCHAR(18),
    project_type            INT,
    housing_type            VARCHAR(100),
    target_population       VARCHAR(100),
    coc_code                VARCHAR(20),
    city                    VARCHAR(100),
    state                   VARCHAR(10),
    county                  VARCHAR(100),
    program_status          VARCHAR(50),
    cross_agency            BIT,
    is_continuum_project    BIT,
    operating_start_date    DATE,
    operating_end_date      DATE,
    source_added_date       DATETIME2(6),
    availability_start_date DATE,
    availability_end_date   DATE,
    source_system           VARCHAR(50),
    load_datetime           DATETIME2(6)
);

CREATE TABLE dim_service_item (
    service_item_key      BIGINT IDENTITY NOT NULL,
    service_item_code     VARCHAR(50),
    service_item_name     VARCHAR(255),
    service_category      VARCHAR(255),
    service_subcategory   VARCHAR(255),
    delivery_type         VARCHAR(100),
    hmis_service_type     VARCHAR(255),
    is_active             BIT,
    source_system         VARCHAR(50),
    load_datetime         DATETIME2(6)
);

CREATE TABLE dim_bed (
    bed_key                 BIGINT IDENTITY NOT NULL,
    program_id              INT,
    site_key                BIGINT,
    apricot_bed_unit_id     VARCHAR(50),
    wellsky_bed_id          VARCHAR(50),
    hmis_bed_id             VARCHAR(50),
    hmis_unit_id            VARCHAR(50),
    bed_number              VARCHAR(50),
    unit_number             VARCHAR(50),
    unit_name               VARCHAR(255),
    bed_type                VARCHAR(50),
    bed_inventory_type      VARCHAR(50),
    availability_type       VARCHAR(50),
    household_type          VARCHAR(50),
    is_dedicated_bed        BIT,
    dedicated_population    VARCHAR(100),
    inventory_start_date    DATE,
    inventory_end_date      DATE,
    is_current              BIT,
    valid_from              DATE,
    valid_to                DATE,
    added_date              DATETIME2(6),
    last_updated_date       DATETIME2(6),
    source_system           VARCHAR(50),
    load_datetime           DATETIME2(6)
);

CREATE TABLE dim_staff (
    staff_key               BIGINT IDENTITY NOT NULL,
    salesforce_id           VARCHAR(18),
    apricot_record_id       VARCHAR(50),
    wellsky_user_id         VARCHAR(50),
    hmis_user_id            VARCHAR(50),
    champ_user_id           VARCHAR(50),
    staff_name              VARCHAR(255),
    first_name              VARCHAR(100),
    last_name               VARCHAR(100),
    email                   VARCHAR(255),
    phone                   VARCHAR(30),
    staff_type              VARCHAR(50),
    job_title               VARCHAR(255),
    role_name               VARCHAR(255),
    department              VARCHAR(255),
    organization_key        BIGINT,
    site_key                BIGINT,
    primary_program_id      INT,
    start_date              DATE,
    end_date                DATE,
    is_active               BIT,
    source_system           VARCHAR(50),
    source_added_date       DATETIME2(6),
    source_last_updated     DATETIME2(6),
    is_deleted              BIT,
    load_datetime           DATETIME2(6)
);

CREATE TABLE dim_date (
    date_key          INT NOT NULL,
    full_date         DATE NOT NULL,
    year              INT NOT NULL,
    quarter           INT NOT NULL,
    month             INT NOT NULL,
    month_name        VARCHAR(20) NOT NULL,
    day               INT NOT NULL,
    day_of_week       INT NOT NULL,
    day_name          VARCHAR(20) NOT NULL,
    is_weekend        BIT NOT NULL,
    fiscal_year       INT,
    fiscal_quarter    INT,
    load_datetime     DATETIME2(6)
);

-- 2. BRIDGES

CREATE TABLE bridge_household_client (
    household_client_key  BIGINT IDENTITY NOT NULL,
    household_key         BIGINT,
    client_key            BIGINT,
    enrollment_key        BIGINT,
    relationship_to_hoh   VARCHAR(100),
    member_type           VARCHAR(100),
    is_head_of_household  BIT,
    start_date            DATE,
    end_date              DATE,
    is_current            BIT,
    source_system         VARCHAR(50),
    source_enrollment_id  VARCHAR(50),
    source_household_id   VARCHAR(50),
    source_client_id      VARCHAR(50),
    source_personal_id    VARCHAR(50),
    source_file_name      VARCHAR(255),
    source_file_date      DATE,
    load_datetime         DATETIME2(6)
);

CREATE TABLE bridge_client_source (
    client_source_key    BIGINT IDENTITY NOT NULL,
    client_key           BIGINT,
    source_system        VARCHAR(50),
    source_client_id     VARCHAR(50),
    source_personal_id   VARCHAR(50),
    match_method         VARCHAR(50),
    match_confidence     DECIMAL(5,4),
    matched_by           VARCHAR(100),
    matched_date         DATETIME2(6),
    is_active            BIT,
    notes                VARCHAR(MAX),
    load_datetime        DATETIME2(6)
);

-- 3. FACTS

CREATE TABLE fact_enrollment (
    enrollment_key          BIGINT IDENTITY NOT NULL,
    client_key              BIGINT,
    client_version_key      BIGINT,
    program_id              INT,
    program_code            VARCHAR(50),
    household_key           BIGINT,
    site_key                BIGINT,
    grant_key               BIGINT,
    staff_key               BIGINT,
    enrollment_start_date       DATE,
    enrollment_start_date_key   INT,
    enrollment_exit_date        DATE,
    enrollment_exit_date_key    INT,
    placement_date              DATE,
    housing_move_in_date        DATE,
    housing_move_in_date_key    INT,
    date_of_engagement          DATE,
    date_of_engagement_key      INT,
    enrollment_status       VARCHAR(50),
    service_type            VARCHAR(100),
    is_carry_over           VARCHAR(20),
    referral_agency         VARCHAR(255),
    referred_to_workforce   VARCHAR(50),
    workforce_referral_name VARCHAR(255),
    is_placed               VARCHAR(20),
    grant_name              VARCHAR(255),
    hvrp_participant_type   VARCHAR(50),
    is_co_enrollment        BIT,
    relationship_to_hoh     VARCHAR(100),
    is_head_of_household    BIT,
    data_collection_stage   VARCHAR(50),
    in_permanent_housing    VARCHAR(20),
    prior_living_type       VARCHAR(255),
    prior_living_category   VARCHAR(100),
    prior_living_start_date DATE,
    prior_living_length     VARCHAR(100),
    times_homeless_3yr      VARCHAR(50),
    months_homeless_3yr     VARCHAR(50),
    rental_subsidy_type     VARCHAR(100),
    housing_status_at_entry VARCHAR(255),
    length_of_stay_days     INT,
    destination_at_exit     VARCHAR(255),
    destination_category    VARCHAR(100),
    exit_reason             VARCHAR(255),
    household_composition_type VARCHAR(50),
    project_type_code       INT,
    continuum_code          VARCHAR(20),
    starting_hourly_wage    DECIMAL(10,2),
    disability_status       VARCHAR(50),
    mental_health_condition VARCHAR(50),
    substance_abuse         VARCHAR(50),
    source_system           VARCHAR(50),
    source_record_id        VARCHAR(255),
    source_enrollment_id    VARCHAR(50),
    source_client_id        VARCHAR(50),
    source_household_id     VARCHAR(50),
    source_personal_id      VARCHAR(50),
    source_file_name        VARCHAR(255),
    source_file_date        DATE,
    source_grant_id         VARCHAR(50),
    source_added_date       DATETIME2(6),
    source_last_updated     DATETIME2(6),
    source_created_by       VARCHAR(255),
    source_updated_by       VARCHAR(255),
    is_deleted              BIT,
    deleted_detected_datetime DATETIME2(6),
    load_datetime           DATETIME2(6)
);

CREATE TABLE fact_service (
    service_key             BIGINT IDENTITY NOT NULL,
    client_key              BIGINT,
    client_version_key      BIGINT,
    program_id              INT,
    enrollment_key          BIGINT,
    service_item_key        BIGINT,
    site_key                BIGINT,
    staff_key               BIGINT,
    grant_key               BIGINT,
    funding_source_key      BIGINT,
    source_system           VARCHAR(50),
    source_record_id        VARCHAR(255),
    source_service_id       VARCHAR(50),
    source_enrollment_id    VARCHAR(50),
    source_client_id        VARCHAR(50),
    source_personal_id      VARCHAR(50),
    service_item_code       VARCHAR(50),
    service_item_name       VARCHAR(255),
    service_category        VARCHAR(255),
    service_subcategory     VARCHAR(255),
    delivery_type           VARCHAR(100),
    hud_service_type        VARCHAR(255),
    service_status          VARCHAR(50),
    service_start_date          DATE,
    service_start_date_key      INT,
    service_end_date            DATE,
    service_end_date_key        INT,
    service_result_date     DATE,
    financial_assistance_start_date DATE,
    financial_assistance_end_date   DATE,
    units_provided          DECIMAL(18,2),
    service_amount          DECIMAL(18,2),
    service_notes           VARCHAR(MAX),
    safe_alternative_housing VARCHAR(20),
    aftercare_project       VARCHAR(255),
    source_file_name        VARCHAR(255),
    source_file_date        DATE,
    source_added_date       DATETIME2(6),
    source_last_updated     DATETIME2(6),
    source_created_by       VARCHAR(255),
    source_updated_by       VARCHAR(255),
    is_deleted              BIT,
    deleted_detected_datetime DATETIME2(6),
    load_datetime           DATETIME2(6)
);

CREATE TABLE fact_case_note (
    note_key                BIGINT IDENTITY NOT NULL,
    client_key              BIGINT,
    client_version_key      BIGINT,
    enrollment_key          BIGINT,
    program_id              INT,
    staff_key               BIGINT,
    source_system           VARCHAR(50),
    source_record_id        VARCHAR(255),
    source_note_id          VARCHAR(50),
    source_enrollment_id    VARCHAR(50),
    source_client_id        VARCHAR(50),
    source_personal_id      VARCHAR(50),
    note_title              VARCHAR(500) MASKED WITH (FUNCTION = 'default()'),
    note_content            VARCHAR(MAX) MASKED WITH (FUNCTION = 'default()'),
    note_date               DATE,
    note_date_key           INT,
    note_author             VARCHAR(255),
    note_category           VARCHAR(100),
    time_tracked_minutes    INT,
    is_private              BIT,
    source_file_name        VARCHAR(255),
    source_file_date        DATE,
    source_added_date       DATETIME2(6),
    source_last_updated     DATETIME2(6),
    is_deleted              BIT,
    deleted_detected_datetime DATETIME2(6),
    load_datetime           DATETIME2(6)
);

CREATE TABLE fact_assessment (
    assessment_key              BIGINT IDENTITY NOT NULL,
    client_key                  BIGINT,
    client_version_key          BIGINT,
    enrollment_key              BIGINT,
    program_id                  INT,
    household_key               BIGINT,
    site_key                    BIGINT,
    staff_key                   BIGINT,
    assessment_date             DATE,
    assessment_date_key         INT,
    assessment_type             VARCHAR(100),
    assessment_level            VARCHAR(50),
    assessment_location         VARCHAR(255),
    prioritization_status       VARCHAR(100),
    assessing_program           VARCHAR(255),
    assessment_status           VARCHAR(50),
    assessment_score            DECIMAL(10,2),
    assessment_score_type       VARCHAR(50),
    client_signature_captured   BIT,
    source_system               VARCHAR(50),
    source_record_id            VARCHAR(255),
    source_assessment_id        VARCHAR(50),
    source_enrollment_id        VARCHAR(50),
    source_client_id            VARCHAR(50),
    source_personal_id          VARCHAR(50),
    source_household_id         VARCHAR(50),
    source_file_name            VARCHAR(255),
    source_file_date            DATE,
    source_added_date           DATETIME2(6),
    source_last_updated         DATETIME2(6),
    load_datetime               DATETIME2(6)
);

CREATE TABLE fact_assessment_response (
    response_key            BIGINT IDENTITY NOT NULL,
    assessment_key          BIGINT,
    question_id             VARCHAR(50),
    question_label          VARCHAR(500),
    question_response       VARCHAR(MAX),
    response_numeric        DECIMAL(18,4),
    response_date           DATE,
    response_boolean        BIT,
    source_system           VARCHAR(50),
    source_file_name        VARCHAR(255),
    source_file_date        DATE,
    load_datetime           DATETIME2(6)
);

CREATE TABLE fact_document (
    document_key            BIGINT IDENTITY NOT NULL,
    client_key              BIGINT,
    client_version_key      BIGINT,
    enrollment_key          BIGINT,
    program_id              INT,
    site_key                BIGINT,
    staff_key               BIGINT,
    document_type           VARCHAR(100),
    document_category       VARCHAR(100),
    document_subcategory    VARCHAR(100),
    upload_date             DATE,
    upload_date_key         INT,
    expiry_date             DATE,
    is_verified             BIT,
    verified_by             VARCHAR(255),
    verified_date           DATE,
    file_reference          VARCHAR(500),
    notes                   VARCHAR(MAX),
    source_system           VARCHAR(50),
    source_record_id        VARCHAR(255),
    source_enrollment_id    VARCHAR(50),
    source_client_id        VARCHAR(50),
    source_file_name        VARCHAR(255),
    source_file_date        DATE,
    load_datetime           DATETIME2(6)
);

CREATE TABLE fact_bed_night (
    bed_night_key               BIGINT IDENTITY NOT NULL,
    client_key                  BIGINT,
    client_version_key          BIGINT,
    enrollment_key              BIGINT,
    bed_key                     BIGINT,
    program_id                  INT,
    site_key                    BIGINT,
    bed_night_date              DATE,
    bed_night_date_key          INT,
    check_in_datetime           DATETIME2(6),
    check_out_datetime          DATETIME2(6),
    assignment_date             DATE,
    pending_occupation_end_date DATE,
    occupancy_type              VARCHAR(50),
    is_no_show                  BIT,
    source_system               VARCHAR(50),
    source_record_id            VARCHAR(255),
    source_occupancy_id         VARCHAR(50),
    source_enrollment_id        VARCHAR(50),
    source_client_id            VARCHAR(50),
    source_personal_id          VARCHAR(50),
    source_file_name            VARCHAR(255),
    source_file_date            DATE,
    load_datetime               DATETIME2(6)
);

CREATE TABLE fact_bed_availability (
    availability_key        BIGINT IDENTITY NOT NULL,
    bed_key                 BIGINT,
    program_id              INT,
    site_key                BIGINT,
    availability_status     VARCHAR(50),
    bed_status              VARCHAR(50),
    offline_reason          VARCHAR(255),
    bed_status_start_date   DATE,
    bed_status_end_date     DATE,
    unit_status             VARCHAR(50),
    status_start_date       DATE,
    status_start_date_key   INT,
    status_end_date         DATE,
    is_current              BIT,
    source_system           VARCHAR(50),
    source_record_id        VARCHAR(255),
    source_file_name        VARCHAR(255),
    source_file_date        DATE,
    load_datetime           DATETIME2(6)
);

CREATE TABLE fact_financial_assistance (
    expenditure_key         BIGINT IDENTITY NOT NULL,
    client_key              BIGINT,
    client_version_key      BIGINT,
    enrollment_key          BIGINT,
    program_id              INT,
    grant_key               BIGINT,
    funding_source_key      BIGINT,
    site_key                BIGINT,
    service_key             BIGINT,
    document_key            BIGINT,
    staff_key               BIGINT,
    requested_date          DATE,
    requested_date_key      INT,
    request_type            VARCHAR(50),
    expense_category        VARCHAR(255),
    requested_amount        DECIMAL(18,2),
    request_month           DATE,
    request_notes           VARCHAR(MAX),
    plan_month              DATE,
    amount_of_arrears       DECIMAL(18,2),
    needed_support          VARCHAR(MAX),
    participant_to_accomplish VARCHAR(MAX),
    outcome                 VARCHAR(MAX),
    client_signed           BIT,
    client_signed_date      DATE,
    staff_signed            BIT,
    staff_signed_date       DATE,
    expenditure_date        DATE,
    expenditure_date_key    INT,
    status                  VARCHAR(50),
    source_system           VARCHAR(50),
    source_record_id        VARCHAR(255),
    source_file_name        VARCHAR(255),
    source_file_date        DATE,
    load_datetime           DATETIME2(6)
);

CREATE TABLE fact_ce_event (
    ce_event_key                  BIGINT IDENTITY NOT NULL,
    client_key                    BIGINT,
    client_version_key            BIGINT,
    enrollment_key                BIGINT,
    program_id                    INT,
    household_key                 BIGINT,
    staff_key                     BIGINT,
    event_date                    DATE,
    event_date_key                INT,
    event_type_code               INT,
    event_type                    VARCHAR(255),
    problem_solving_result        VARCHAR(50),
    case_management_after_referral VARCHAR(50),
    referral_location             VARCHAR(255),
    referral_program_id           INT,
    referral_result               VARCHAR(100),
    result_date                   DATE,
    result_date_key               INT,
    source_system                 VARCHAR(50),
    source_record_id              VARCHAR(255),
    source_event_id               VARCHAR(50),
    source_enrollment_id          VARCHAR(50),
    source_client_id              VARCHAR(50),
    source_personal_id            VARCHAR(50),
    source_file_name              VARCHAR(255),
    source_file_date              DATE,
    source_added_date             DATETIME2(6),
    source_last_updated           DATETIME2(6),
    source_created_by             VARCHAR(255),
    source_updated_by             VARCHAR(255),
    load_datetime                 DATETIME2(6)
);

CREATE TABLE fact_consent (
    consent_key               BIGINT IDENTITY NOT NULL,
    client_key                BIGINT,
    client_version_key        BIGINT,
    enrollment_key            BIGINT,
    program_id                INT,
    staff_key                 BIGINT,
    document_key              BIGINT,
    consent_type              VARCHAR(100),
    consent_scope             VARCHAR(255),
    permission_granted        BIT,
    consent_status            VARCHAR(50),
    documentation_type        VARCHAR(100),
    signed_date               DATE,
    signed_date_key           INT,
    effective_start_date      DATE,
    effective_end_date        DATE,
    revoked_date              DATE,
    client_signature_captured BIT,
    staff_signature_captured  BIT,
    is_current                BIT,
    notes                     VARCHAR(MAX) MASKED WITH (FUNCTION = 'default()'),
    source_system             VARCHAR(50),
    source_record_id          VARCHAR(255),
    source_consent_id         VARCHAR(50),
    source_enrollment_id      VARCHAR(50),
    source_client_id          VARCHAR(50),
    source_personal_id        VARCHAR(50),
    source_file_name          VARCHAR(255),
    source_file_date          DATE,
    source_added_date         DATETIME2(6),
    source_last_updated       DATETIME2(6),
    source_created_by         VARCHAR(255),
    source_updated_by         VARCHAR(255),
    load_datetime             DATETIME2(6)
);

CREATE TABLE fact_enrollment_stage (
    enrollment_stage_key            BIGINT IDENTITY NOT NULL,
    enrollment_key                  BIGINT,
    client_key                      BIGINT,
    client_version_key              BIGINT,
    program_id                      INT,
    household_key                   BIGINT,
    staff_key                       BIGINT,
    data_collection_stage           VARCHAR(50),
    data_collection_stage_code      INT,
    information_date                DATE,
    information_date_key            INT,
    income_from_any_source          VARCHAR(50),
    total_monthly_income            DECIMAL(12,2),
    earned_income                   DECIMAL(12,2),
    unemployment_income             DECIMAL(12,2),
    ssi_income                      DECIMAL(12,2),
    ssdi_income                     DECIMAL(12,2),
    va_service_connected_income     DECIMAL(12,2),
    va_non_service_connected_income DECIMAL(12,2),
    private_disability_income       DECIMAL(12,2),
    workers_comp_income             DECIMAL(12,2),
    tanf_income                     DECIMAL(12,2),
    general_assistance_income       DECIMAL(12,2),
    social_security_retirement_income DECIMAL(12,2),
    pension_income                  DECIMAL(12,2),
    child_support_income            DECIMAL(12,2),
    alimony_income                  DECIMAL(12,2),
    other_income                    DECIMAL(12,2),
    other_income_source             VARCHAR(255),
    benefits_from_any_source        VARCHAR(50),
    snap                            BIT,
    wic                             BIT,
    tanf_child_care                 BIT,
    tanf_transportation             BIT,
    other_tanf                      BIT,
    other_benefits_source           VARCHAR(255),
    insurance_from_any_source       VARCHAR(50),
    medicaid                        BIT,
    medicare                        BIT,
    schip                           BIT,
    va_medical                      BIT,
    employer_insurance              BIT,
    cobra                           BIT,
    private_pay_insurance           BIT,
    state_adult_insurance           BIT,
    indian_health_services          BIT,
    other_insurance                 BIT,
    other_insurance_source          VARCHAR(255),
    disabling_condition             VARCHAR(50),
    physical_disability             VARCHAR(50),
    developmental_disability        VARCHAR(50),
    chronic_health_condition        VARCHAR(50),
    hiv_aids                        VARCHAR(50) MASKED WITH (FUNCTION = 'default()'),
    mental_health_disorder          VARCHAR(50),
    substance_use_disorder          VARCHAR(50),
    dv_survivor                     VARCHAR(50) MASKED WITH (FUNCTION = 'default()'),
    dv_when_occurred                VARCHAR(100) MASKED WITH (FUNCTION = 'default()'),
    dv_currently_fleeing            VARCHAR(50) MASKED WITH (FUNCTION = 'default()'),
    employed                        VARCHAR(50),
    employment_type                 VARCHAR(100),
    not_employed_reason             VARCHAR(100),
    source_system                   VARCHAR(50),
    source_record_id                VARCHAR(255),
    source_enrollment_id            VARCHAR(50),
    source_client_id                VARCHAR(50),
    source_personal_id              VARCHAR(50),
    source_file_name                VARCHAR(255),
    source_file_date                DATE,
    source_added_date               DATETIME2(6),
    source_last_updated             DATETIME2(6),
    load_datetime                   DATETIME2(6)
);

CREATE TABLE fact_current_living_situation (
    living_situation_key        BIGINT IDENTITY NOT NULL,
    client_key                  BIGINT,
    client_version_key          BIGINT,
    enrollment_key              BIGINT,
    program_id                  INT,
    staff_key                   BIGINT,
    contact_date                DATE,
    contact_date_key            INT,
    living_situation_code       INT,
    living_situation            VARCHAR(255),
    living_situation_category   VARCHAR(100),
    verified_by                 VARCHAR(255),
    leave_within_14_days        VARCHAR(50),
    subsequent_residence        VARCHAR(50),
    resources_to_obtain_housing VARCHAR(50),
    lease_own_60_days           VARCHAR(50),
    moved_2_or_more_times       VARCHAR(50),
    location_details            VARCHAR(500) MASKED WITH (FUNCTION = 'default()'),
    source_system               VARCHAR(50),
    source_record_id            VARCHAR(255),
    source_enrollment_id        VARCHAR(50),
    source_client_id            VARCHAR(50),
    source_personal_id          VARCHAR(50),
    source_file_name            VARCHAR(255),
    source_file_date            DATE,
    source_added_date           DATETIME2(6),
    source_last_updated         DATETIME2(6),
    load_datetime               DATETIME2(6)
);

CREATE TABLE bridge_enrollment_grant (
    enrollment_grant_key    BIGINT IDENTITY NOT NULL,
    enrollment_key          BIGINT,
    grant_key               BIGINT,
    funding_source_key      BIGINT,
    is_primary              BIT,
    allocation_share        DECIMAL(5,4),
    start_date              DATE,
    end_date                DATE,
    source_system           VARCHAR(50),
    source_enrollment_id    VARCHAR(50),
    source_grant_id         VARCHAR(50),
    source_file_name        VARCHAR(255),
    source_file_date        DATE,
    load_datetime           DATETIME2(6)
);

CREATE TABLE bridge_program_site (
    program_site_key        BIGINT IDENTITY NOT NULL,
    program_id              INT,
    site_key                BIGINT,
    is_primary              BIT,
    start_date              DATE,
    end_date                DATE,
    source_system           VARCHAR(50),
    source_program_id       VARCHAR(50),
    source_site_id          VARCHAR(50),
    load_datetime           DATETIME2(6)
);

IF OBJECT_ID('dbo.dim_hud_code', 'U') IS NULL
CREATE TABLE dim_hud_code (
    hud_code_key            BIGINT IDENTITY NOT NULL,
    list_name               VARCHAR(100) NOT NULL,
    code                    VARCHAR(20) NOT NULL,
    label                   VARCHAR(500),
    category                VARCHAR(100),
    sort_order              INT,
    spec_version            VARCHAR(20),
    is_active               BIT,
    load_datetime           DATETIME2(6)
);

IF OBJECT_ID('dbo.kpi_target', 'U') IS NULL
CREATE TABLE kpi_target (
    kpi_target_key          BIGINT IDENTITY NOT NULL,
    grant_source_system     VARCHAR(50),
    source_grant_id         VARCHAR(50),
    program_code            VARCHAR(100),
    measure_code            VARCHAR(50) NOT NULL,
    measure_name            VARCHAR(255),
    target_value            DECIMAL(14,4),
    target_unit             VARCHAR(20),
    comparison              VARCHAR(5),
    period_label            VARCHAR(50),
    period_start            DATE,
    period_end              DATE,
    source_document         VARCHAR(255),
    notes                   VARCHAR(500),
    entered_by              VARCHAR(255),
    entered_date            DATETIME2(6),
    is_active               BIT,
    load_datetime           DATETIME2(6)
);

IF OBJECT_ID('dbo.etl_source_config', 'U') IS NULL
CREATE TABLE etl_source_config (
    source_system           VARCHAR(50) NOT NULL,
    is_enabled              BIT,
    load_order              INT,
    silver_notebook         VARCHAR(128),
    loader_procedure        VARCHAR(128),
    bronze_prefix           VARCHAR(100),
    expected_frequency      VARCHAR(20),
    last_file_date          DATE,
    last_success_datetime   DATETIME2(6),
    block_reason            VARCHAR(500),
    notes                   VARCHAR(500),
    updated_by              VARCHAR(255),
    updated_datetime        DATETIME2(6)
);

-- 4. OBSERVABILITY

CREATE TABLE stage_load_log (
    load_log_key            BIGINT IDENTITY NOT NULL,
    source_system           VARCHAR(50),
    entity_name             VARCHAR(100),
    source_file_name        VARCHAR(255),
    source_file_date        DATE,
    bronze_load_datetime    DATETIME2(6),
    silver_load_datetime    DATETIME2(6),
    warehouse_load_datetime DATETIME2(6),
    row_count_source        INT,
    row_count_loaded        INT,
    load_status             VARCHAR(50),
    error_message           VARCHAR(MAX)
);

CREATE TABLE stage_load_reject (
    reject_key           BIGINT IDENTITY NOT NULL,
    run_id               VARCHAR(50),
    source_system        VARCHAR(50),
    entity_name          VARCHAR(100),
    source_record_id     VARCHAR(255),
    reject_reason        VARCHAR(100),
    referenced_client_id VARCHAR(255),
    referenced_program   VARCHAR(255),
    detected_datetime    DATETIME2(6),
    raw_payload          VARCHAR(MAX)
);
GO

-- 5. CONSTRAINTS
IF OBJECT_ID('dbo.dim_client', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.dim_client)
        THROW 50020, '01_ddl stopped: the warehouse holds loaded data. Schema changes go in migrations/. To rebuild from empty on purpose, run dbo.usp_admin_clear_warehouse_data first.', 1;
END;
IF OBJECT_ID('dbo.fact_enrollment', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.fact_enrollment)
        THROW 50020, '01_ddl stopped: the warehouse holds loaded data. Schema changes go in migrations/. To rebuild from empty on purpose, run dbo.usp_admin_clear_warehouse_data first.', 1;
END;

ALTER TABLE dim_site ADD CONSTRAINT PK_dim_site PRIMARY KEY NONCLUSTERED (site_key) NOT ENFORCED;
ALTER TABLE dim_grant ADD CONSTRAINT PK_dim_grant PRIMARY KEY NONCLUSTERED (grant_key) NOT ENFORCED;
ALTER TABLE dim_funding_source ADD CONSTRAINT PK_dim_funding_source PRIMARY KEY NONCLUSTERED (funding_source_key) NOT ENFORCED;
ALTER TABLE dim_household ADD CONSTRAINT PK_dim_household PRIMARY KEY NONCLUSTERED (household_key) NOT ENFORCED;
ALTER TABLE dim_organization ADD CONSTRAINT PK_dim_organization PRIMARY KEY NONCLUSTERED (organization_key) NOT ENFORCED;
ALTER TABLE dim_source ADD CONSTRAINT PK_dim_source PRIMARY KEY NONCLUSTERED (source_system) NOT ENFORCED;
ALTER TABLE dim_client ADD CONSTRAINT PK_dim_client PRIMARY KEY NONCLUSTERED (client_version_key) NOT ENFORCED;
ALTER TABLE dim_client_identifier ADD CONSTRAINT PK_dim_client_identifier PRIMARY KEY NONCLUSTERED (source_system, source_client_id) NOT ENFORCED;
ALTER TABLE dim_program ADD CONSTRAINT PK_dim_program PRIMARY KEY NONCLUSTERED (program_id) NOT ENFORCED;
ALTER TABLE dim_service_item ADD CONSTRAINT PK_dim_service_item PRIMARY KEY NONCLUSTERED (service_item_key) NOT ENFORCED;
ALTER TABLE dim_bed ADD CONSTRAINT PK_dim_bed PRIMARY KEY NONCLUSTERED (bed_key) NOT ENFORCED;
ALTER TABLE dim_date ADD CONSTRAINT PK_dim_date PRIMARY KEY NONCLUSTERED (date_key) NOT ENFORCED;
ALTER TABLE bridge_household_client ADD CONSTRAINT PK_bridge_household_client PRIMARY KEY NONCLUSTERED (household_client_key) NOT ENFORCED;
ALTER TABLE bridge_client_source ADD CONSTRAINT PK_bridge_client_source PRIMARY KEY NONCLUSTERED (client_source_key) NOT ENFORCED;
ALTER TABLE fact_enrollment ADD CONSTRAINT PK_fact_enrollment PRIMARY KEY NONCLUSTERED (enrollment_key) NOT ENFORCED;
ALTER TABLE fact_service ADD CONSTRAINT PK_fact_service PRIMARY KEY NONCLUSTERED (service_key) NOT ENFORCED;
ALTER TABLE fact_case_note ADD CONSTRAINT PK_fact_case_note PRIMARY KEY NONCLUSTERED (note_key) NOT ENFORCED;
ALTER TABLE fact_assessment ADD CONSTRAINT PK_fact_assessment PRIMARY KEY NONCLUSTERED (assessment_key) NOT ENFORCED;
ALTER TABLE fact_assessment_response ADD CONSTRAINT PK_fact_assessment_response PRIMARY KEY NONCLUSTERED (response_key) NOT ENFORCED;
ALTER TABLE fact_document ADD CONSTRAINT PK_fact_document PRIMARY KEY NONCLUSTERED (document_key) NOT ENFORCED;
ALTER TABLE fact_bed_night ADD CONSTRAINT PK_fact_bed_night PRIMARY KEY NONCLUSTERED (bed_night_key) NOT ENFORCED;
ALTER TABLE fact_bed_availability ADD CONSTRAINT PK_fact_bed_availability PRIMARY KEY NONCLUSTERED (availability_key) NOT ENFORCED;
ALTER TABLE fact_financial_assistance ADD CONSTRAINT PK_fact_financial_assistance PRIMARY KEY NONCLUSTERED (expenditure_key) NOT ENFORCED;
ALTER TABLE dim_staff ADD CONSTRAINT PK_dim_staff PRIMARY KEY NONCLUSTERED (staff_key) NOT ENFORCED;
ALTER TABLE fact_ce_event ADD CONSTRAINT PK_fact_ce_event PRIMARY KEY NONCLUSTERED (ce_event_key) NOT ENFORCED;
ALTER TABLE fact_consent ADD CONSTRAINT PK_fact_consent PRIMARY KEY NONCLUSTERED (consent_key) NOT ENFORCED;
ALTER TABLE fact_enrollment_stage ADD CONSTRAINT PK_fact_enrollment_stage PRIMARY KEY NONCLUSTERED (enrollment_stage_key) NOT ENFORCED;
ALTER TABLE fact_current_living_situation ADD CONSTRAINT PK_fact_current_living_situation PRIMARY KEY NONCLUSTERED (living_situation_key) NOT ENFORCED;
ALTER TABLE bridge_enrollment_grant ADD CONSTRAINT PK_bridge_enrollment_grant PRIMARY KEY NONCLUSTERED (enrollment_grant_key) NOT ENFORCED;
ALTER TABLE bridge_program_site ADD CONSTRAINT PK_bridge_program_site PRIMARY KEY NONCLUSTERED (program_site_key) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_dim_hud_code')
    ALTER TABLE dim_hud_code ADD CONSTRAINT PK_dim_hud_code PRIMARY KEY NONCLUSTERED (hud_code_key) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_kpi_target')
    ALTER TABLE kpi_target ADD CONSTRAINT PK_kpi_target PRIMARY KEY NONCLUSTERED (kpi_target_key) NOT ENFORCED;
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_etl_source_config')
    ALTER TABLE etl_source_config ADD CONSTRAINT PK_etl_source_config PRIMARY KEY NONCLUSTERED (source_system) NOT ENFORCED;
ALTER TABLE stage_load_log ADD CONSTRAINT PK_stage_load_log PRIMARY KEY NONCLUSTERED (load_log_key) NOT ENFORCED;
ALTER TABLE stage_load_reject ADD CONSTRAINT PK_stage_load_reject PRIMARY KEY NONCLUSTERED (reject_key) NOT ENFORCED;
GO

IF OBJECT_ID('dbo.dim_client', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.dim_client)
        THROW 50020, '01_ddl stopped: the warehouse holds loaded data. Schema changes go in migrations/. To rebuild from empty on purpose, run dbo.usp_admin_clear_warehouse_data first.', 1;
END;
IF OBJECT_ID('dbo.fact_enrollment', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.fact_enrollment)
        THROW 50020, '01_ddl stopped: the warehouse holds loaded data. Schema changes go in migrations/. To rebuild from empty on purpose, run dbo.usp_admin_clear_warehouse_data first.', 1;
END;
ALTER TABLE dim_funding_source ADD CONSTRAINT FK_dim_funding_source_grant
    FOREIGN KEY (grant_key) REFERENCES dim_grant(grant_key) NOT ENFORCED;
ALTER TABLE dim_funding_source ADD CONSTRAINT FK_dim_funding_source_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;

ALTER TABLE dim_client ADD CONSTRAINT FK_dim_client_household
    FOREIGN KEY (household_key) REFERENCES dim_household(household_key) NOT ENFORCED;
ALTER TABLE dim_program ADD CONSTRAINT FK_dim_program_organization
    FOREIGN KEY (organization_key) REFERENCES dim_organization(organization_key) NOT ENFORCED;
ALTER TABLE dim_program ADD CONSTRAINT FK_dim_program_site
    FOREIGN KEY (site_key) REFERENCES dim_site(site_key) NOT ENFORCED;
ALTER TABLE dim_bed ADD CONSTRAINT FK_dim_bed_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
ALTER TABLE dim_bed ADD CONSTRAINT FK_dim_bed_site
    FOREIGN KEY (site_key) REFERENCES dim_site(site_key) NOT ENFORCED;

ALTER TABLE bridge_household_client ADD CONSTRAINT FK_bridge_household_client_household
    FOREIGN KEY (household_key) REFERENCES dim_household(household_key) NOT ENFORCED;
ALTER TABLE bridge_household_client ADD CONSTRAINT FK_bridge_household_client_enrollment
    FOREIGN KEY (enrollment_key) REFERENCES fact_enrollment(enrollment_key) NOT ENFORCED;

ALTER TABLE fact_enrollment ADD CONSTRAINT FK_fact_enrollment_client_version
    FOREIGN KEY (client_version_key) REFERENCES dim_client(client_version_key) NOT ENFORCED;
ALTER TABLE fact_enrollment ADD CONSTRAINT FK_fact_enrollment_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
ALTER TABLE fact_enrollment ADD CONSTRAINT FK_fact_enrollment_household
    FOREIGN KEY (household_key) REFERENCES dim_household(household_key) NOT ENFORCED;
ALTER TABLE fact_enrollment ADD CONSTRAINT FK_fact_enrollment_site
    FOREIGN KEY (site_key) REFERENCES dim_site(site_key) NOT ENFORCED;
ALTER TABLE fact_enrollment ADD CONSTRAINT FK_fact_enrollment_grant
    FOREIGN KEY (grant_key) REFERENCES dim_grant(grant_key) NOT ENFORCED;
ALTER TABLE fact_enrollment ADD CONSTRAINT FK_fact_enrollment_start_date
    FOREIGN KEY (enrollment_start_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;
ALTER TABLE fact_enrollment ADD CONSTRAINT FK_fact_enrollment_exit_date
    FOREIGN KEY (enrollment_exit_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;
ALTER TABLE fact_enrollment ADD CONSTRAINT FK_fact_enrollment_move_in_date
    FOREIGN KEY (housing_move_in_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;

ALTER TABLE fact_service ADD CONSTRAINT FK_fact_service_enrollment
    FOREIGN KEY (enrollment_key) REFERENCES fact_enrollment(enrollment_key) NOT ENFORCED;
ALTER TABLE fact_service ADD CONSTRAINT FK_fact_service_client_version
    FOREIGN KEY (client_version_key) REFERENCES dim_client(client_version_key) NOT ENFORCED;
ALTER TABLE fact_service ADD CONSTRAINT FK_fact_service_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
ALTER TABLE fact_service ADD CONSTRAINT FK_fact_service_service_item
    FOREIGN KEY (service_item_key) REFERENCES dim_service_item(service_item_key) NOT ENFORCED;
ALTER TABLE fact_service ADD CONSTRAINT FK_fact_service_site
    FOREIGN KEY (site_key) REFERENCES dim_site(site_key) NOT ENFORCED;
ALTER TABLE fact_service ADD CONSTRAINT FK_fact_service_start_date
    FOREIGN KEY (service_start_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;
ALTER TABLE fact_service ADD CONSTRAINT FK_fact_service_end_date
    FOREIGN KEY (service_end_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;

ALTER TABLE fact_case_note ADD CONSTRAINT FK_fact_case_note_enrollment
    FOREIGN KEY (enrollment_key) REFERENCES fact_enrollment(enrollment_key) NOT ENFORCED;
ALTER TABLE fact_case_note ADD CONSTRAINT FK_fact_case_note_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
ALTER TABLE fact_case_note ADD CONSTRAINT FK_fact_case_note_client_version
    FOREIGN KEY (client_version_key) REFERENCES dim_client(client_version_key) NOT ENFORCED;
ALTER TABLE fact_case_note ADD CONSTRAINT FK_fact_case_note_date
    FOREIGN KEY (note_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;

ALTER TABLE fact_assessment ADD CONSTRAINT FK_fact_assessment_client_version
    FOREIGN KEY (client_version_key) REFERENCES dim_client(client_version_key) NOT ENFORCED;
ALTER TABLE fact_assessment ADD CONSTRAINT FK_fact_assessment_enrollment
    FOREIGN KEY (enrollment_key) REFERENCES fact_enrollment(enrollment_key) NOT ENFORCED;
ALTER TABLE fact_assessment ADD CONSTRAINT FK_fact_assessment_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
ALTER TABLE fact_assessment ADD CONSTRAINT FK_fact_assessment_household
    FOREIGN KEY (household_key) REFERENCES dim_household(household_key) NOT ENFORCED;
ALTER TABLE fact_assessment ADD CONSTRAINT FK_fact_assessment_site
    FOREIGN KEY (site_key) REFERENCES dim_site(site_key) NOT ENFORCED;
ALTER TABLE fact_assessment ADD CONSTRAINT FK_fact_assessment_date
    FOREIGN KEY (assessment_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;

ALTER TABLE fact_assessment_response ADD CONSTRAINT FK_fact_assessment_response_assessment
    FOREIGN KEY (assessment_key) REFERENCES fact_assessment(assessment_key) NOT ENFORCED;

ALTER TABLE fact_document ADD CONSTRAINT FK_fact_document_client_version
    FOREIGN KEY (client_version_key) REFERENCES dim_client(client_version_key) NOT ENFORCED;
ALTER TABLE fact_document ADD CONSTRAINT FK_fact_document_enrollment
    FOREIGN KEY (enrollment_key) REFERENCES fact_enrollment(enrollment_key) NOT ENFORCED;
ALTER TABLE fact_document ADD CONSTRAINT FK_fact_document_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
ALTER TABLE fact_document ADD CONSTRAINT FK_fact_document_site
    FOREIGN KEY (site_key) REFERENCES dim_site(site_key) NOT ENFORCED;
ALTER TABLE fact_document ADD CONSTRAINT FK_fact_document_upload_date
    FOREIGN KEY (upload_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;

ALTER TABLE fact_bed_night ADD CONSTRAINT FK_fact_bed_night_client_version
    FOREIGN KEY (client_version_key) REFERENCES dim_client(client_version_key) NOT ENFORCED;
ALTER TABLE fact_bed_night ADD CONSTRAINT FK_fact_bed_night_enrollment
    FOREIGN KEY (enrollment_key) REFERENCES fact_enrollment(enrollment_key) NOT ENFORCED;
ALTER TABLE fact_bed_night ADD CONSTRAINT FK_fact_bed_night_bed
    FOREIGN KEY (bed_key) REFERENCES dim_bed(bed_key) NOT ENFORCED;
ALTER TABLE fact_bed_night ADD CONSTRAINT FK_fact_bed_night_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
ALTER TABLE fact_bed_night ADD CONSTRAINT FK_fact_bed_night_site
    FOREIGN KEY (site_key) REFERENCES dim_site(site_key) NOT ENFORCED;
ALTER TABLE fact_bed_night ADD CONSTRAINT FK_fact_bed_night_date
    FOREIGN KEY (bed_night_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;

ALTER TABLE fact_bed_availability ADD CONSTRAINT FK_fact_bed_availability_bed
    FOREIGN KEY (bed_key) REFERENCES dim_bed(bed_key) NOT ENFORCED;
ALTER TABLE fact_bed_availability ADD CONSTRAINT FK_fact_bed_availability_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
ALTER TABLE fact_bed_availability ADD CONSTRAINT FK_fact_bed_availability_site
    FOREIGN KEY (site_key) REFERENCES dim_site(site_key) NOT ENFORCED;
ALTER TABLE fact_bed_availability ADD CONSTRAINT FK_fact_bed_availability_start_date
    FOREIGN KEY (status_start_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;

ALTER TABLE fact_financial_assistance ADD CONSTRAINT FK_fact_fa_client_version
    FOREIGN KEY (client_version_key) REFERENCES dim_client(client_version_key) NOT ENFORCED;
ALTER TABLE fact_financial_assistance ADD CONSTRAINT FK_fact_fa_enrollment
    FOREIGN KEY (enrollment_key) REFERENCES fact_enrollment(enrollment_key) NOT ENFORCED;
ALTER TABLE fact_financial_assistance ADD CONSTRAINT FK_fact_fa_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
ALTER TABLE fact_financial_assistance ADD CONSTRAINT FK_fact_fa_grant
    FOREIGN KEY (grant_key) REFERENCES dim_grant(grant_key) NOT ENFORCED;
ALTER TABLE fact_financial_assistance ADD CONSTRAINT FK_fact_fa_funding_source
    FOREIGN KEY (funding_source_key) REFERENCES dim_funding_source(funding_source_key) NOT ENFORCED;
ALTER TABLE fact_financial_assistance ADD CONSTRAINT FK_fact_fa_site
    FOREIGN KEY (site_key) REFERENCES dim_site(site_key) NOT ENFORCED;
ALTER TABLE fact_financial_assistance ADD CONSTRAINT FK_fact_fa_service
    FOREIGN KEY (service_key) REFERENCES fact_service(service_key) NOT ENFORCED;
ALTER TABLE fact_financial_assistance ADD CONSTRAINT FK_fact_fa_document
    FOREIGN KEY (document_key) REFERENCES fact_document(document_key) NOT ENFORCED;
ALTER TABLE fact_financial_assistance ADD CONSTRAINT FK_fact_fa_requested_date
    FOREIGN KEY (requested_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;
ALTER TABLE fact_financial_assistance ADD CONSTRAINT FK_fact_fa_expenditure_date
    FOREIGN KEY (expenditure_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;

ALTER TABLE dim_staff ADD CONSTRAINT FK_dim_staff_organization
    FOREIGN KEY (organization_key) REFERENCES dim_organization(organization_key) NOT ENFORCED;
ALTER TABLE dim_staff ADD CONSTRAINT FK_dim_staff_site
    FOREIGN KEY (site_key) REFERENCES dim_site(site_key) NOT ENFORCED;
ALTER TABLE dim_staff ADD CONSTRAINT FK_dim_staff_program
    FOREIGN KEY (primary_program_id) REFERENCES dim_program(program_id) NOT ENFORCED;

ALTER TABLE fact_enrollment ADD CONSTRAINT FK_fact_enrollment_staff
    FOREIGN KEY (staff_key) REFERENCES dim_staff(staff_key) NOT ENFORCED;
ALTER TABLE fact_service ADD CONSTRAINT FK_fact_service_staff
    FOREIGN KEY (staff_key) REFERENCES dim_staff(staff_key) NOT ENFORCED;
ALTER TABLE fact_case_note ADD CONSTRAINT FK_fact_case_note_staff
    FOREIGN KEY (staff_key) REFERENCES dim_staff(staff_key) NOT ENFORCED;
ALTER TABLE fact_assessment ADD CONSTRAINT FK_fact_assessment_staff
    FOREIGN KEY (staff_key) REFERENCES dim_staff(staff_key) NOT ENFORCED;
ALTER TABLE fact_document ADD CONSTRAINT FK_fact_document_staff
    FOREIGN KEY (staff_key) REFERENCES dim_staff(staff_key) NOT ENFORCED;
ALTER TABLE fact_financial_assistance ADD CONSTRAINT FK_fact_fa_staff
    FOREIGN KEY (staff_key) REFERENCES dim_staff(staff_key) NOT ENFORCED;

ALTER TABLE fact_ce_event ADD CONSTRAINT FK_fact_ce_event_client_version
    FOREIGN KEY (client_version_key) REFERENCES dim_client(client_version_key) NOT ENFORCED;
ALTER TABLE fact_ce_event ADD CONSTRAINT FK_fact_ce_event_enrollment
    FOREIGN KEY (enrollment_key) REFERENCES fact_enrollment(enrollment_key) NOT ENFORCED;
ALTER TABLE fact_ce_event ADD CONSTRAINT FK_fact_ce_event_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
ALTER TABLE fact_ce_event ADD CONSTRAINT FK_fact_ce_event_household
    FOREIGN KEY (household_key) REFERENCES dim_household(household_key) NOT ENFORCED;
ALTER TABLE fact_ce_event ADD CONSTRAINT FK_fact_ce_event_staff
    FOREIGN KEY (staff_key) REFERENCES dim_staff(staff_key) NOT ENFORCED;
ALTER TABLE fact_ce_event ADD CONSTRAINT FK_fact_ce_event_referral_program
    FOREIGN KEY (referral_program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
ALTER TABLE fact_ce_event ADD CONSTRAINT FK_fact_ce_event_date
    FOREIGN KEY (event_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;
ALTER TABLE fact_ce_event ADD CONSTRAINT FK_fact_ce_event_result_date
    FOREIGN KEY (result_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;

ALTER TABLE fact_consent ADD CONSTRAINT FK_fact_consent_client_version
    FOREIGN KEY (client_version_key) REFERENCES dim_client(client_version_key) NOT ENFORCED;
ALTER TABLE fact_consent ADD CONSTRAINT FK_fact_consent_enrollment
    FOREIGN KEY (enrollment_key) REFERENCES fact_enrollment(enrollment_key) NOT ENFORCED;
ALTER TABLE fact_consent ADD CONSTRAINT FK_fact_consent_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
ALTER TABLE fact_consent ADD CONSTRAINT FK_fact_consent_staff
    FOREIGN KEY (staff_key) REFERENCES dim_staff(staff_key) NOT ENFORCED;
ALTER TABLE fact_consent ADD CONSTRAINT FK_fact_consent_document
    FOREIGN KEY (document_key) REFERENCES fact_document(document_key) NOT ENFORCED;
ALTER TABLE fact_consent ADD CONSTRAINT FK_fact_consent_signed_date
    FOREIGN KEY (signed_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;

ALTER TABLE fact_service ADD CONSTRAINT FK_fact_service_grant
    FOREIGN KEY (grant_key) REFERENCES dim_grant(grant_key) NOT ENFORCED;
ALTER TABLE fact_service ADD CONSTRAINT FK_fact_service_funding_source
    FOREIGN KEY (funding_source_key) REFERENCES dim_funding_source(funding_source_key) NOT ENFORCED;
ALTER TABLE fact_enrollment ADD CONSTRAINT FK_fact_enrollment_engagement_date
    FOREIGN KEY (date_of_engagement_key) REFERENCES dim_date(date_key) NOT ENFORCED;

ALTER TABLE fact_enrollment_stage ADD CONSTRAINT FK_fact_enrollment_stage_enrollment
    FOREIGN KEY (enrollment_key) REFERENCES fact_enrollment(enrollment_key) NOT ENFORCED;
ALTER TABLE fact_enrollment_stage ADD CONSTRAINT FK_fact_enrollment_stage_client_version
    FOREIGN KEY (client_version_key) REFERENCES dim_client(client_version_key) NOT ENFORCED;
ALTER TABLE fact_enrollment_stage ADD CONSTRAINT FK_fact_enrollment_stage_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
ALTER TABLE fact_enrollment_stage ADD CONSTRAINT FK_fact_enrollment_stage_household
    FOREIGN KEY (household_key) REFERENCES dim_household(household_key) NOT ENFORCED;
ALTER TABLE fact_enrollment_stage ADD CONSTRAINT FK_fact_enrollment_stage_staff
    FOREIGN KEY (staff_key) REFERENCES dim_staff(staff_key) NOT ENFORCED;
ALTER TABLE fact_enrollment_stage ADD CONSTRAINT FK_fact_enrollment_stage_date
    FOREIGN KEY (information_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;

ALTER TABLE fact_current_living_situation ADD CONSTRAINT FK_fact_cls_client_version
    FOREIGN KEY (client_version_key) REFERENCES dim_client(client_version_key) NOT ENFORCED;
ALTER TABLE fact_current_living_situation ADD CONSTRAINT FK_fact_cls_enrollment
    FOREIGN KEY (enrollment_key) REFERENCES fact_enrollment(enrollment_key) NOT ENFORCED;
ALTER TABLE fact_current_living_situation ADD CONSTRAINT FK_fact_cls_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
ALTER TABLE fact_current_living_situation ADD CONSTRAINT FK_fact_cls_staff
    FOREIGN KEY (staff_key) REFERENCES dim_staff(staff_key) NOT ENFORCED;
ALTER TABLE fact_current_living_situation ADD CONSTRAINT FK_fact_cls_date
    FOREIGN KEY (contact_date_key) REFERENCES dim_date(date_key) NOT ENFORCED;

ALTER TABLE bridge_enrollment_grant ADD CONSTRAINT FK_bridge_enrollment_grant_enrollment
    FOREIGN KEY (enrollment_key) REFERENCES fact_enrollment(enrollment_key) NOT ENFORCED;
ALTER TABLE bridge_enrollment_grant ADD CONSTRAINT FK_bridge_enrollment_grant_grant
    FOREIGN KEY (grant_key) REFERENCES dim_grant(grant_key) NOT ENFORCED;
ALTER TABLE bridge_enrollment_grant ADD CONSTRAINT FK_bridge_enrollment_grant_funding_source
    FOREIGN KEY (funding_source_key) REFERENCES dim_funding_source(funding_source_key) NOT ENFORCED;
ALTER TABLE bridge_program_site ADD CONSTRAINT FK_bridge_program_site_program
    FOREIGN KEY (program_id) REFERENCES dim_program(program_id) NOT ENFORCED;
ALTER TABLE bridge_program_site ADD CONSTRAINT FK_bridge_program_site_site
    FOREIGN KEY (site_key) REFERENCES dim_site(site_key) NOT ENFORCED;

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_etl_source_config_source')
    ALTER TABLE etl_source_config ADD CONSTRAINT FK_etl_source_config_source
        FOREIGN KEY (source_system) REFERENCES dim_source(source_system) NOT ENFORCED;
GO

-- 6. HELPER FUNCTION
CREATE FUNCTION dbo.fn_normalize_key(@val VARCHAR(255))
RETURNS VARCHAR(255)
AS
BEGIN
    RETURN LOWER(
        REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(@val)),
            CHAR(9), ' '), CHAR(10), ' '), CHAR(13), ' ')
    );
END;
GO

-- 7. REPORTING VIEWS
CREATE VIEW dbo.v_fact_enrollment_primary AS
SELECT * FROM dbo.fact_enrollment WHERE is_co_enrollment = 0 AND ISNULL(is_deleted, 0) = 0;
GO

CREATE VIEW dbo.v_fact_enrollment_all AS
SELECT * FROM dbo.fact_enrollment WHERE ISNULL(is_deleted, 0) = 0;
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

CREATE VIEW dbo.v_data_quality_unresolved AS
SELECT 'fact_enrollment' AS fact_table, 'program_id' AS column_name,
       COUNT(*) AS unresolved_rows FROM dbo.fact_enrollment WHERE program_id IS NULL
UNION ALL SELECT 'fact_enrollment', 'client_version_key',
       COUNT(*) FROM dbo.fact_enrollment WHERE client_version_key IS NULL
UNION ALL SELECT 'fact_enrollment', 'household_key',
       COUNT(*) FROM dbo.fact_enrollment WHERE household_key IS NULL
UNION ALL SELECT 'fact_enrollment', 'grant_key',
       COUNT(*) FROM dbo.fact_enrollment WHERE grant_key IS NULL
UNION ALL SELECT 'fact_service', 'enrollment_key',
       COUNT(*) FROM dbo.fact_service WHERE enrollment_key IS NULL
UNION ALL SELECT 'fact_service', 'program_id',
       COUNT(*) FROM dbo.fact_service WHERE program_id IS NULL
UNION ALL SELECT 'fact_service', 'service_item_key',
       COUNT(*) FROM dbo.fact_service WHERE service_item_key IS NULL
UNION ALL SELECT 'fact_case_note', 'enrollment_key',
       COUNT(*) FROM dbo.fact_case_note WHERE enrollment_key IS NULL
UNION ALL SELECT 'fact_case_note', 'program_id',
       COUNT(*) FROM dbo.fact_case_note WHERE program_id IS NULL
UNION ALL SELECT 'fact_assessment', 'enrollment_key',
       COUNT(*) FROM dbo.fact_assessment WHERE enrollment_key IS NULL
UNION ALL SELECT 'fact_assessment', 'program_id',
       COUNT(*) FROM dbo.fact_assessment WHERE program_id IS NULL
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
       COUNT(*) FROM dbo.fact_bed_availability WHERE bed_key IS NULL
UNION ALL SELECT 'fact_bed_availability', 'program_id',
       COUNT(*) FROM dbo.fact_bed_availability WHERE program_id IS NULL
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
       COUNT(*) FROM dbo.fact_consent WHERE enrollment_key IS NULL
UNION ALL SELECT 'fact_consent', 'program_id',
       COUNT(*) FROM dbo.fact_consent WHERE program_id IS NULL
UNION ALL SELECT 'fact_enrollment_stage', 'enrollment_key',
       COUNT(*) FROM dbo.fact_enrollment_stage WHERE enrollment_key IS NULL
UNION ALL SELECT 'fact_current_living_situation', 'enrollment_key',
       COUNT(*) FROM dbo.fact_current_living_situation WHERE enrollment_key IS NULL
UNION ALL SELECT 'bridge_enrollment_grant', 'grant_key',
       COUNT(*) FROM dbo.bridge_enrollment_grant WHERE grant_key IS NULL
UNION ALL SELECT 'kpi_target', 'grant not found',
       COUNT(*) FROM dbo.v_kpi_target WHERE source_grant_id IS NOT NULL AND grant_key IS NULL;
GO

-- 8. LOG RUN END + SCHEMA VERSION
IF OBJECT_ID('dbo.dim_client', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.dim_client)
        THROW 50020, '01_ddl stopped: the warehouse holds loaded data. Schema changes go in migrations/. To rebuild from empty on purpose, run dbo.usp_admin_clear_warehouse_data first.', 1;
END;
IF OBJECT_ID('dbo.fact_enrollment', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM dbo.fact_enrollment)
        THROW 50020, '01_ddl stopped: the warehouse holds loaded data. Schema changes go in migrations/. To rebuild from empty on purpose, run dbo.usp_admin_clear_warehouse_data first.', 1;
END;
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'SUCCESS'
WHERE script_name = '01_ddl' AND run_status = 'RUNNING';

IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.13')
INSERT INTO dbo.warehouse_schema_version (version, applied_date, applied_by, summary, details)
VALUES (
    'v8.13',
    SYSDATETIME(),
    USER_NAME(),
    'dim_client_identifier (full SSN, restricted); dim_client.ssn_raw replaced by ssn_last4; soft-delete columns on dim_client, fact_enrollment, fact_service, fact_case_note; reporting views exclude deleted enrollments',
    'See DDL header for full change list.'
);
GO
