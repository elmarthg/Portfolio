-- checks/check_declared_keys.sql (schema v8.19): one row per check of every declared key, worst first.
-- Generated from tools/key_checks.py, the same key list the migrations and loaders use. Read-only.
-- Fabric does not enforce primary, unique or foreign keys and may rely on them when it plans a query, so every
-- problem_rows here should be 0. The loaders and person-link procedures run the checks for the tables they
-- write before every COMMIT; this script checks the whole warehouse at once.
SELECT check_name, problem_rows
FROM (
    SELECT 'FK_apricot_profile_map_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.apricot_profile_map c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_apricot_profile_map: duplicate source_system, source_id, exported_datetime' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_id, exported_datetime FROM dbo.apricot_profile_map WHERE source_system IS NOT NULL AND source_id IS NOT NULL AND exported_datetime IS NOT NULL GROUP BY source_system, source_id, exported_datetime HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_apricot_profile_map: empty source_system, source_id, exported_datetime' AS check_name, (SELECT COUNT(*) FROM dbo.apricot_profile_map WHERE source_system IS NULL OR source_id IS NULL OR exported_datetime IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_bridge_client_source_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_client_source c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_bridge_client_source: duplicate client_source_key' AS check_name, (SELECT COUNT(*) FROM (SELECT client_source_key FROM dbo.bridge_client_source WHERE client_source_key IS NOT NULL GROUP BY client_source_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_bridge_client_source: empty client_source_key' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_client_source WHERE client_source_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_bridge_enrollment_grant_enrollment: enrollment_key not in fact_enrollment' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_enrollment_grant c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_bridge_enrollment_grant_funding_source: funding_source_key not in dim_funding_source' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_enrollment_grant c WHERE c.funding_source_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_funding_source p WHERE p.funding_source_key = c.funding_source_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_bridge_enrollment_grant_grant: grant_key not in dim_grant' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_enrollment_grant c WHERE c.grant_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_grant p WHERE p.grant_key = c.grant_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_bridge_enrollment_grant_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_enrollment_grant c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_bridge_enrollment_grant: duplicate enrollment_grant_key' AS check_name, (SELECT COUNT(*) FROM (SELECT enrollment_grant_key FROM dbo.bridge_enrollment_grant WHERE enrollment_grant_key IS NOT NULL GROUP BY enrollment_grant_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_bridge_enrollment_grant: empty enrollment_grant_key' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_enrollment_grant WHERE enrollment_grant_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_bridge_household_client_enrollment: enrollment_key not in fact_enrollment' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_household_client c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_bridge_household_client_household: household_key not in dim_household' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_household_client c WHERE c.household_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_household p WHERE p.household_key = c.household_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_bridge_household_client_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_household_client c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_bridge_household_client: duplicate household_client_key' AS check_name, (SELECT COUNT(*) FROM (SELECT household_client_key FROM dbo.bridge_household_client WHERE household_client_key IS NOT NULL GROUP BY household_client_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_bridge_household_client: empty household_client_key' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_household_client WHERE household_client_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_bridge_program_site_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_program_site c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_bridge_program_site_site: site_key not in dim_site' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_program_site c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_bridge_program_site_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_program_site c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_bridge_program_site: duplicate program_site_key' AS check_name, (SELECT COUNT(*) FROM (SELECT program_site_key FROM dbo.bridge_program_site WHERE program_site_key IS NOT NULL GROUP BY program_site_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_bridge_program_site: empty program_site_key' AS check_name, (SELECT COUNT(*) FROM dbo.bridge_program_site WHERE program_site_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_bed_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.dim_bed c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_bed_site: site_key not in dim_site' AS check_name, (SELECT COUNT(*) FROM dbo.dim_bed c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_bed_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_bed c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_bed: duplicate bed_key' AS check_name, (SELECT COUNT(*) FROM (SELECT bed_key FROM dbo.dim_bed WHERE bed_key IS NOT NULL GROUP BY bed_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_bed: empty bed_key' AS check_name, (SELECT COUNT(*) FROM dbo.dim_bed WHERE bed_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_client_current_household: current_household_key not in dim_household' AS check_name, (SELECT COUNT(*) FROM dbo.dim_client c WHERE c.current_household_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_household p WHERE p.household_key = c.current_household_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_client_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_client c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_client: duplicate client_version_key' AS check_name, (SELECT COUNT(*) FROM (SELECT client_version_key FROM dbo.dim_client WHERE client_version_key IS NOT NULL GROUP BY client_version_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_client: empty client_version_key' AS check_name, (SELECT COUNT(*) FROM dbo.dim_client WHERE client_version_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_dim_client_version_start: duplicate client_key, valid_from' AS check_name, (SELECT COUNT(*) FROM (SELECT client_key, valid_from FROM dbo.dim_client WHERE client_key IS NOT NULL AND valid_from IS NOT NULL GROUP BY client_key, valid_from HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_client_identifier_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_client_identifier c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_client_identifier: duplicate source_system, source_client_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_client_id FROM dbo.dim_client_identifier WHERE source_system IS NOT NULL AND source_client_id IS NOT NULL GROUP BY source_system, source_client_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_client_identifier: empty source_system, source_client_id' AS check_name, (SELECT COUNT(*) FROM dbo.dim_client_identifier WHERE source_system IS NULL OR source_client_id IS NULL) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_date: duplicate date_key' AS check_name, (SELECT COUNT(*) FROM (SELECT date_key FROM dbo.dim_date WHERE date_key IS NOT NULL GROUP BY date_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_date: empty date_key' AS check_name, (SELECT COUNT(*) FROM dbo.dim_date WHERE date_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_dim_date_full_date: duplicate full_date' AS check_name, (SELECT COUNT(*) FROM (SELECT full_date FROM dbo.dim_date WHERE full_date IS NOT NULL GROUP BY full_date HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_funding_source_grant: grant_key not in dim_grant' AS check_name, (SELECT COUNT(*) FROM dbo.dim_funding_source c WHERE c.grant_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_grant p WHERE p.grant_key = c.grant_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_funding_source_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.dim_funding_source c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_funding_source_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_funding_source c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_funding_source: duplicate funding_source_key' AS check_name, (SELECT COUNT(*) FROM (SELECT funding_source_key FROM dbo.dim_funding_source WHERE funding_source_key IS NOT NULL GROUP BY funding_source_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_funding_source: empty funding_source_key' AS check_name, (SELECT COUNT(*) FROM dbo.dim_funding_source WHERE funding_source_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_grant_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_grant c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_grant: duplicate grant_key' AS check_name, (SELECT COUNT(*) FROM (SELECT grant_key FROM dbo.dim_grant WHERE grant_key IS NOT NULL GROUP BY grant_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_grant: empty grant_key' AS check_name, (SELECT COUNT(*) FROM dbo.dim_grant WHERE grant_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_household_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_household c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_household: duplicate household_key' AS check_name, (SELECT COUNT(*) FROM (SELECT household_key FROM dbo.dim_household WHERE household_key IS NOT NULL GROUP BY household_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_household: empty household_key' AS check_name, (SELECT COUNT(*) FROM dbo.dim_household WHERE household_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_hud_code: duplicate hud_code_key' AS check_name, (SELECT COUNT(*) FROM (SELECT hud_code_key FROM dbo.dim_hud_code WHERE hud_code_key IS NOT NULL GROUP BY hud_code_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_hud_code: empty hud_code_key' AS check_name, (SELECT COUNT(*) FROM dbo.dim_hud_code WHERE hud_code_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_organization_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_organization c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_organization: duplicate organization_key' AS check_name, (SELECT COUNT(*) FROM (SELECT organization_key FROM dbo.dim_organization WHERE organization_key IS NOT NULL GROUP BY organization_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_organization: empty organization_key' AS check_name, (SELECT COUNT(*) FROM dbo.dim_organization WHERE organization_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_program_organization: organization_key not in dim_organization' AS check_name, (SELECT COUNT(*) FROM dbo.dim_program c WHERE c.organization_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_organization p WHERE p.organization_key = c.organization_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_program_site: site_key not in dim_site' AS check_name, (SELECT COUNT(*) FROM dbo.dim_program c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_program_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_program c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_program: duplicate program_id' AS check_name, (SELECT COUNT(*) FROM (SELECT program_id FROM dbo.dim_program WHERE program_id IS NOT NULL GROUP BY program_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_program: empty program_id' AS check_name, (SELECT COUNT(*) FROM dbo.dim_program WHERE program_id IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_dim_program_code: duplicate program_code' AS check_name, (SELECT COUNT(*) FROM (SELECT program_code FROM dbo.dim_program WHERE program_code IS NOT NULL GROUP BY program_code HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_program_history_organization: organization_key not in dim_organization' AS check_name, (SELECT COUNT(*) FROM dbo.dim_program_history c WHERE c.organization_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_organization p WHERE p.organization_key = c.organization_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_program_history_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.dim_program_history c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_program_history: duplicate program_history_key' AS check_name, (SELECT COUNT(*) FROM (SELECT program_history_key FROM dbo.dim_program_history WHERE program_history_key IS NOT NULL GROUP BY program_history_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_program_history: empty program_history_key' AS check_name, (SELECT COUNT(*) FROM dbo.dim_program_history WHERE program_history_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_dim_program_history_start: duplicate program_id, valid_from' AS check_name, (SELECT COUNT(*) FROM (SELECT program_id, valid_from FROM dbo.dim_program_history WHERE program_id IS NOT NULL AND valid_from IS NOT NULL GROUP BY program_id, valid_from HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_service_item_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_service_item c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_service_item: duplicate service_item_key' AS check_name, (SELECT COUNT(*) FROM (SELECT service_item_key FROM dbo.dim_service_item WHERE service_item_key IS NOT NULL GROUP BY service_item_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_service_item: empty service_item_key' AS check_name, (SELECT COUNT(*) FROM dbo.dim_service_item WHERE service_item_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_site_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_site c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_site: duplicate site_key' AS check_name, (SELECT COUNT(*) FROM (SELECT site_key FROM dbo.dim_site WHERE site_key IS NOT NULL GROUP BY site_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_site: empty site_key' AS check_name, (SELECT COUNT(*) FROM dbo.dim_site WHERE site_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_source: duplicate source_system' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system FROM dbo.dim_source WHERE source_system IS NOT NULL GROUP BY source_system HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_source: empty source_system' AS check_name, (SELECT COUNT(*) FROM dbo.dim_source WHERE source_system IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_staff_organization: organization_key not in dim_organization' AS check_name, (SELECT COUNT(*) FROM dbo.dim_staff c WHERE c.organization_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_organization p WHERE p.organization_key = c.organization_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_staff_program: primary_program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.dim_staff c WHERE c.primary_program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.primary_program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_staff_site: site_key not in dim_site' AS check_name, (SELECT COUNT(*) FROM dbo.dim_staff c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_staff_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_staff c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_staff: duplicate staff_key' AS check_name, (SELECT COUNT(*) FROM (SELECT staff_key FROM dbo.dim_staff WHERE staff_key IS NOT NULL GROUP BY staff_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_dim_staff: empty staff_key' AS check_name, (SELECT COUNT(*) FROM dbo.dim_staff WHERE staff_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_etl_source_config_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.etl_source_config c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_etl_source_config: duplicate source_system' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system FROM dbo.etl_source_config WHERE source_system IS NOT NULL GROUP BY source_system HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_etl_source_config: empty source_system' AS check_name, (SELECT COUNT(*) FROM dbo.etl_source_config WHERE source_system IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_assessment_client_version: client_version_key not in dim_client' AS check_name, (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_assessment_date: assessment_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.assessment_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.assessment_date_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_assessment_enrollment: enrollment_key not in fact_enrollment' AS check_name, (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_assessment_household: household_key not in dim_household' AS check_name, (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.household_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_household p WHERE p.household_key = c.household_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_assessment_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_assessment_site: site_key not in dim_site' AS check_name, (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_assessment_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_assessment_staff: staff_key not in dim_staff' AS check_name, (SELECT COUNT(*) FROM dbo.fact_assessment c WHERE c.staff_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_staff p WHERE p.staff_key = c.staff_key)) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_assessment: duplicate assessment_key' AS check_name, (SELECT COUNT(*) FROM (SELECT assessment_key FROM dbo.fact_assessment WHERE assessment_key IS NOT NULL GROUP BY assessment_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_assessment: empty assessment_key' AS check_name, (SELECT COUNT(*) FROM dbo.fact_assessment WHERE assessment_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_assessment_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_assessment WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_assessment_response_assessment: assessment_key not in fact_assessment' AS check_name, (SELECT COUNT(*) FROM dbo.fact_assessment_response c WHERE c.assessment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_assessment p WHERE p.assessment_key = c.assessment_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_assessment_response_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_assessment_response c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_assessment_response: duplicate response_key' AS check_name, (SELECT COUNT(*) FROM (SELECT response_key FROM dbo.fact_assessment_response WHERE response_key IS NOT NULL GROUP BY response_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_assessment_response: empty response_key' AS check_name, (SELECT COUNT(*) FROM dbo.fact_assessment_response WHERE response_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_bed_availability_bed: bed_key not in dim_bed' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_availability c WHERE c.bed_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_bed p WHERE p.bed_key = c.bed_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_bed_availability_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_availability c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_bed_availability_site: site_key not in dim_site' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_availability c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_bed_availability_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_availability c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_bed_availability_start_date: status_start_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_availability c WHERE c.status_start_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.status_start_date_key)) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_bed_availability: duplicate availability_key' AS check_name, (SELECT COUNT(*) FROM (SELECT availability_key FROM dbo.fact_bed_availability WHERE availability_key IS NOT NULL GROUP BY availability_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_bed_availability: empty availability_key' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_availability WHERE availability_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_bed_availability_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_bed_availability WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_bed_night_bed: bed_key not in dim_bed' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_night c WHERE c.bed_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_bed p WHERE p.bed_key = c.bed_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_bed_night_client_version: client_version_key not in dim_client' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_night c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_bed_night_date: bed_night_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_night c WHERE c.bed_night_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.bed_night_date_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_bed_night_enrollment: enrollment_key not in fact_enrollment' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_night c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_bed_night_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_night c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_bed_night_site: site_key not in dim_site' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_night c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_bed_night_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_night c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_bed_night: duplicate bed_night_key' AS check_name, (SELECT COUNT(*) FROM (SELECT bed_night_key FROM dbo.fact_bed_night WHERE bed_night_key IS NOT NULL GROUP BY bed_night_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_bed_night: empty bed_night_key' AS check_name, (SELECT COUNT(*) FROM dbo.fact_bed_night WHERE bed_night_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_bed_night_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_bed_night WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_case_note_client_version: client_version_key not in dim_client' AS check_name, (SELECT COUNT(*) FROM dbo.fact_case_note c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_case_note_date: note_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_case_note c WHERE c.note_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.note_date_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_case_note_enrollment: enrollment_key not in fact_enrollment' AS check_name, (SELECT COUNT(*) FROM dbo.fact_case_note c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_case_note_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.fact_case_note c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_case_note_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_case_note c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_case_note_staff: staff_key not in dim_staff' AS check_name, (SELECT COUNT(*) FROM dbo.fact_case_note c WHERE c.staff_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_staff p WHERE p.staff_key = c.staff_key)) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_case_note: duplicate note_key' AS check_name, (SELECT COUNT(*) FROM (SELECT note_key FROM dbo.fact_case_note WHERE note_key IS NOT NULL GROUP BY note_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_case_note: empty note_key' AS check_name, (SELECT COUNT(*) FROM dbo.fact_case_note WHERE note_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_case_note_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_case_note WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_ce_event_client_version: client_version_key not in dim_client' AS check_name, (SELECT COUNT(*) FROM dbo.fact_ce_event c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_ce_event_date: event_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_ce_event c WHERE c.event_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.event_date_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_ce_event_enrollment: enrollment_key not in fact_enrollment' AS check_name, (SELECT COUNT(*) FROM dbo.fact_ce_event c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_ce_event_household: household_key not in dim_household' AS check_name, (SELECT COUNT(*) FROM dbo.fact_ce_event c WHERE c.household_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_household p WHERE p.household_key = c.household_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_ce_event_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.fact_ce_event c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_ce_event_referral_program: referral_program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.fact_ce_event c WHERE c.referral_program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.referral_program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_ce_event_result_date: result_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_ce_event c WHERE c.result_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.result_date_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_ce_event_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_ce_event c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_ce_event_staff: staff_key not in dim_staff' AS check_name, (SELECT COUNT(*) FROM dbo.fact_ce_event c WHERE c.staff_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_staff p WHERE p.staff_key = c.staff_key)) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_ce_event: duplicate ce_event_key' AS check_name, (SELECT COUNT(*) FROM (SELECT ce_event_key FROM dbo.fact_ce_event WHERE ce_event_key IS NOT NULL GROUP BY ce_event_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_ce_event: empty ce_event_key' AS check_name, (SELECT COUNT(*) FROM dbo.fact_ce_event WHERE ce_event_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_ce_event_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_ce_event WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_consent_client_version: client_version_key not in dim_client' AS check_name, (SELECT COUNT(*) FROM dbo.fact_consent c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_consent_document: document_key not in fact_document' AS check_name, (SELECT COUNT(*) FROM dbo.fact_consent c WHERE c.document_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_document p WHERE p.document_key = c.document_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_consent_enrollment: enrollment_key not in fact_enrollment' AS check_name, (SELECT COUNT(*) FROM dbo.fact_consent c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_consent_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.fact_consent c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_consent_signed_date: signed_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_consent c WHERE c.signed_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.signed_date_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_consent_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_consent c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_consent_staff: staff_key not in dim_staff' AS check_name, (SELECT COUNT(*) FROM dbo.fact_consent c WHERE c.staff_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_staff p WHERE p.staff_key = c.staff_key)) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_consent: duplicate consent_key' AS check_name, (SELECT COUNT(*) FROM (SELECT consent_key FROM dbo.fact_consent WHERE consent_key IS NOT NULL GROUP BY consent_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_consent: empty consent_key' AS check_name, (SELECT COUNT(*) FROM dbo.fact_consent WHERE consent_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_consent_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_consent WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_cls_client_version: client_version_key not in dim_client' AS check_name, (SELECT COUNT(*) FROM dbo.fact_current_living_situation c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_cls_date: contact_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_current_living_situation c WHERE c.contact_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.contact_date_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_cls_enrollment: enrollment_key not in fact_enrollment' AS check_name, (SELECT COUNT(*) FROM dbo.fact_current_living_situation c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_cls_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.fact_current_living_situation c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_cls_staff: staff_key not in dim_staff' AS check_name, (SELECT COUNT(*) FROM dbo.fact_current_living_situation c WHERE c.staff_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_staff p WHERE p.staff_key = c.staff_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_current_living_situation_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_current_living_situation c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_current_living_situation: duplicate living_situation_key' AS check_name, (SELECT COUNT(*) FROM (SELECT living_situation_key FROM dbo.fact_current_living_situation WHERE living_situation_key IS NOT NULL GROUP BY living_situation_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_current_living_situation: empty living_situation_key' AS check_name, (SELECT COUNT(*) FROM dbo.fact_current_living_situation WHERE living_situation_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_current_living_situation_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_current_living_situation WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_document_client_version: client_version_key not in dim_client' AS check_name, (SELECT COUNT(*) FROM dbo.fact_document c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_document_enrollment: enrollment_key not in fact_enrollment' AS check_name, (SELECT COUNT(*) FROM dbo.fact_document c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_document_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.fact_document c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_document_site: site_key not in dim_site' AS check_name, (SELECT COUNT(*) FROM dbo.fact_document c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_document_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_document c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_document_staff: staff_key not in dim_staff' AS check_name, (SELECT COUNT(*) FROM dbo.fact_document c WHERE c.staff_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_staff p WHERE p.staff_key = c.staff_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_document_upload_date: upload_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_document c WHERE c.upload_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.upload_date_key)) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_document: duplicate document_key' AS check_name, (SELECT COUNT(*) FROM (SELECT document_key FROM dbo.fact_document WHERE document_key IS NOT NULL GROUP BY document_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_document: empty document_key' AS check_name, (SELECT COUNT(*) FROM dbo.fact_document WHERE document_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_document_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_document WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_client_version: client_version_key not in dim_client' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_engagement_date: date_of_engagement_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.date_of_engagement_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.date_of_engagement_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_exit_date: enrollment_exit_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.enrollment_exit_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.enrollment_exit_date_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_grant: grant_key not in dim_grant' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.grant_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_grant p WHERE p.grant_key = c.grant_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_household: household_key not in dim_household' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.household_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_household p WHERE p.household_key = c.household_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_move_in_date: housing_move_in_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.housing_move_in_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.housing_move_in_date_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_site: site_key not in dim_site' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_staff: staff_key not in dim_staff' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.staff_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_staff p WHERE p.staff_key = c.staff_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_start_date: enrollment_start_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment c WHERE c.enrollment_start_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.enrollment_start_date_key)) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_enrollment: duplicate enrollment_key' AS check_name, (SELECT COUNT(*) FROM (SELECT enrollment_key FROM dbo.fact_enrollment WHERE enrollment_key IS NOT NULL GROUP BY enrollment_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_enrollment: empty enrollment_key' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment WHERE enrollment_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_enrollment_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_enrollment WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_stage_client_version: client_version_key not in dim_client' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment_stage c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_stage_date: information_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment_stage c WHERE c.information_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.information_date_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_stage_enrollment: enrollment_key not in fact_enrollment' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment_stage c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_stage_household: household_key not in dim_household' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment_stage c WHERE c.household_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_household p WHERE p.household_key = c.household_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_stage_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment_stage c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_stage_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment_stage c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_enrollment_stage_staff: staff_key not in dim_staff' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment_stage c WHERE c.staff_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_staff p WHERE p.staff_key = c.staff_key)) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_enrollment_stage: duplicate enrollment_stage_key' AS check_name, (SELECT COUNT(*) FROM (SELECT enrollment_stage_key FROM dbo.fact_enrollment_stage WHERE enrollment_stage_key IS NOT NULL GROUP BY enrollment_stage_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_enrollment_stage: empty enrollment_stage_key' AS check_name, (SELECT COUNT(*) FROM dbo.fact_enrollment_stage WHERE enrollment_stage_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_enrollment_stage_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_enrollment_stage WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_fa_client_version: client_version_key not in dim_client' AS check_name, (SELECT COUNT(*) FROM dbo.fact_financial_assistance c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_fa_document: document_key not in fact_document' AS check_name, (SELECT COUNT(*) FROM dbo.fact_financial_assistance c WHERE c.document_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_document p WHERE p.document_key = c.document_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_fa_enrollment: enrollment_key not in fact_enrollment' AS check_name, (SELECT COUNT(*) FROM dbo.fact_financial_assistance c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_fa_expenditure_date: expenditure_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_financial_assistance c WHERE c.expenditure_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.expenditure_date_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_fa_funding_source: funding_source_key not in dim_funding_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_financial_assistance c WHERE c.funding_source_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_funding_source p WHERE p.funding_source_key = c.funding_source_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_fa_grant: grant_key not in dim_grant' AS check_name, (SELECT COUNT(*) FROM dbo.fact_financial_assistance c WHERE c.grant_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_grant p WHERE p.grant_key = c.grant_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_fa_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.fact_financial_assistance c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_fa_requested_date: requested_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_financial_assistance c WHERE c.requested_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.requested_date_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_fa_service: service_key not in fact_service' AS check_name, (SELECT COUNT(*) FROM dbo.fact_financial_assistance c WHERE c.service_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_service p WHERE p.service_key = c.service_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_fa_site: site_key not in dim_site' AS check_name, (SELECT COUNT(*) FROM dbo.fact_financial_assistance c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_fa_staff: staff_key not in dim_staff' AS check_name, (SELECT COUNT(*) FROM dbo.fact_financial_assistance c WHERE c.staff_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_staff p WHERE p.staff_key = c.staff_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_financial_assistance_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_financial_assistance c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_financial_assistance: duplicate expenditure_key' AS check_name, (SELECT COUNT(*) FROM (SELECT expenditure_key FROM dbo.fact_financial_assistance WHERE expenditure_key IS NOT NULL GROUP BY expenditure_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_financial_assistance: empty expenditure_key' AS check_name, (SELECT COUNT(*) FROM dbo.fact_financial_assistance WHERE expenditure_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_financial_assistance_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_financial_assistance WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_service_client_version: client_version_key not in dim_client' AS check_name, (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.client_version_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_client p WHERE p.client_version_key = c.client_version_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_service_end_date: service_end_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.service_end_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.service_end_date_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_service_enrollment: enrollment_key not in fact_enrollment' AS check_name, (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.enrollment_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.fact_enrollment p WHERE p.enrollment_key = c.enrollment_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_service_funding_source: funding_source_key not in dim_funding_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.funding_source_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_funding_source p WHERE p.funding_source_key = c.funding_source_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_service_grant: grant_key not in dim_grant' AS check_name, (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.grant_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_grant p WHERE p.grant_key = c.grant_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_service_program: program_id not in dim_program' AS check_name, (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.program_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_program p WHERE p.program_id = c.program_id)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_service_service_item: service_item_key not in dim_service_item' AS check_name, (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.service_item_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_service_item p WHERE p.service_item_key = c.service_item_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_service_site: site_key not in dim_site' AS check_name, (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.site_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_site p WHERE p.site_key = c.site_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_service_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_service_staff: staff_key not in dim_staff' AS check_name, (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.staff_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_staff p WHERE p.staff_key = c.staff_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_fact_service_start_date: service_start_date_key not in dim_date' AS check_name, (SELECT COUNT(*) FROM dbo.fact_service c WHERE c.service_start_date_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_date p WHERE p.date_key = c.service_start_date_key)) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_service: duplicate service_key' AS check_name, (SELECT COUNT(*) FROM (SELECT service_key FROM dbo.fact_service WHERE service_key IS NOT NULL GROUP BY service_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_fact_service: empty service_key' AS check_name, (SELECT COUNT(*) FROM dbo.fact_service WHERE service_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_fact_service_source_record: duplicate source_system, source_record_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, source_record_id FROM dbo.fact_service WHERE source_system IS NOT NULL AND source_record_id IS NOT NULL GROUP BY source_system, source_record_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_kpi_target: duplicate kpi_target_key' AS check_name, (SELECT COUNT(*) FROM (SELECT kpi_target_key FROM dbo.kpi_target WHERE kpi_target_key IS NOT NULL GROUP BY kpi_target_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_kpi_target: empty kpi_target_key' AS check_name, (SELECT COUNT(*) FROM dbo.kpi_target WHERE kpi_target_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_match_decision_source_a: source_system_a not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.match_decision c WHERE c.source_system_a IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system_a)) AS problem_rows
    UNION ALL
    SELECT 'FK_match_decision_source_b: source_system_b not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.match_decision c WHERE c.source_system_b IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system_b)) AS problem_rows
    UNION ALL
    SELECT 'PK_match_decision: duplicate decision_key' AS check_name, (SELECT COUNT(*) FROM (SELECT decision_key FROM dbo.match_decision WHERE decision_key IS NOT NULL GROUP BY decision_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_match_decision: empty decision_key' AS check_name, (SELECT COUNT(*) FROM dbo.match_decision WHERE decision_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_match_review_queue_source_a: source_system_a not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.match_review_queue c WHERE c.source_system_a IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system_a)) AS problem_rows
    UNION ALL
    SELECT 'FK_match_review_queue_source_b: source_system_b not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.match_review_queue c WHERE c.source_system_b IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system_b)) AS problem_rows
    UNION ALL
    SELECT 'PK_match_review_queue: duplicate source_system_a, source_id_a, source_system_b, source_id_b' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system_a, source_id_a, source_system_b, source_id_b FROM dbo.match_review_queue WHERE source_system_a IS NOT NULL AND source_id_a IS NOT NULL AND source_system_b IS NOT NULL AND source_id_b IS NOT NULL GROUP BY source_system_a, source_id_a, source_system_b, source_id_b HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_match_review_queue: empty source_system_a, source_id_a, source_system_b, source_id_b' AS check_name, (SELECT COUNT(*) FROM dbo.match_review_queue WHERE source_system_a IS NULL OR source_id_a IS NULL OR source_system_b IS NULL OR source_id_b IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_person_client_link_decision: decision_key not in match_decision' AS check_name, (SELECT COUNT(*) FROM dbo.person_client_link c WHERE c.decision_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.match_decision p WHERE p.decision_key = c.decision_key)) AS problem_rows
    UNION ALL
    SELECT 'PK_person_client_link: duplicate link_key' AS check_name, (SELECT COUNT(*) FROM (SELECT link_key FROM dbo.person_client_link WHERE link_key IS NOT NULL GROUP BY link_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_person_client_link: empty link_key' AS check_name, (SELECT COUNT(*) FROM dbo.person_client_link WHERE link_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_person_client_link_start: duplicate client_key, valid_from' AS check_name, (SELECT COUNT(*) FROM (SELECT client_key, valid_from FROM dbo.person_client_link WHERE client_key IS NOT NULL AND valid_from IS NOT NULL GROUP BY client_key, valid_from HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_rpt_person: duplicate person_key' AS check_name, (SELECT COUNT(*) FROM (SELECT person_key FROM rpt.person WHERE person_key IS NOT NULL GROUP BY person_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_rpt_person: empty person_key' AS check_name, (SELECT COUNT(*) FROM rpt.person WHERE person_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'FK_rpt_person_client_person: person_key not in rpt.person' AS check_name, (SELECT COUNT(*) FROM rpt.person_client c WHERE c.person_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM rpt.person p WHERE p.person_key = c.person_key)) AS problem_rows
    UNION ALL
    SELECT 'FK_rpt_person_client_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM rpt.person_client c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'PK_rpt_person_client: duplicate client_key' AS check_name, (SELECT COUNT(*) FROM (SELECT client_key FROM rpt.person_client WHERE client_key IS NOT NULL GROUP BY client_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_rpt_person_client: empty client_key' AS check_name, (SELECT COUNT(*) FROM rpt.person_client WHERE client_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'PK_stage_load_log: duplicate load_log_key' AS check_name, (SELECT COUNT(*) FROM (SELECT load_log_key FROM dbo.stage_load_log WHERE load_log_key IS NOT NULL GROUP BY load_log_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_stage_load_log: empty load_log_key' AS check_name, (SELECT COUNT(*) FROM dbo.stage_load_log WHERE load_log_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'PK_stage_load_reject: duplicate reject_key' AS check_name, (SELECT COUNT(*) FROM (SELECT reject_key FROM dbo.stage_load_reject WHERE reject_key IS NOT NULL GROUP BY reject_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_stage_load_reject: empty reject_key' AS check_name, (SELECT COUNT(*) FROM dbo.stage_load_reject WHERE reject_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'PK_warehouse_run_log: duplicate run_key' AS check_name, (SELECT COUNT(*) FROM (SELECT run_key FROM dbo.warehouse_run_log WHERE run_key IS NOT NULL GROUP BY run_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_warehouse_run_log: empty run_key' AS check_name, (SELECT COUNT(*) FROM dbo.warehouse_run_log WHERE run_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'PK_warehouse_schema_version: duplicate version_key' AS check_name, (SELECT COUNT(*) FROM (SELECT version_key FROM dbo.warehouse_schema_version WHERE version_key IS NOT NULL GROUP BY version_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'PK_warehouse_schema_version: empty version_key' AS check_name, (SELECT COUNT(*) FROM dbo.warehouse_schema_version WHERE version_key IS NULL) AS problem_rows
    UNION ALL
    SELECT 'UQ_warehouse_schema_version_version: duplicate version' AS check_name, (SELECT COUNT(*) FROM (SELECT version FROM dbo.warehouse_schema_version WHERE version IS NOT NULL GROUP BY version HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'FK_dim_program_history_source: source_system not in dim_source' AS check_name, (SELECT COUNT(*) FROM dbo.dim_program_history c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system)) AS problem_rows
    UNION ALL
    SELECT 'UQ_dim_organization_source_id: duplicate source_system, hmis_organization_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, hmis_organization_id FROM dbo.dim_organization WHERE source_system IS NOT NULL AND hmis_organization_id IS NOT NULL GROUP BY source_system, hmis_organization_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_dim_grant_source_id: duplicate source_system, hmis_grant_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, hmis_grant_id FROM dbo.dim_grant WHERE source_system IS NOT NULL AND hmis_grant_id IS NOT NULL GROUP BY source_system, hmis_grant_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_dim_funding_source_source_id: duplicate source_system, hmis_funding_source_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, hmis_funding_source_id FROM dbo.dim_funding_source WHERE source_system IS NOT NULL AND hmis_funding_source_id IS NOT NULL GROUP BY source_system, hmis_funding_source_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_dim_service_item_source_code: duplicate source_system, service_item_code' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, service_item_code FROM dbo.dim_service_item WHERE source_system IS NOT NULL AND service_item_code IS NOT NULL GROUP BY source_system, service_item_code HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_dim_bed_source_id: duplicate source_system, hmis_bed_id' AS check_name, (SELECT COUNT(*) FROM (SELECT source_system, hmis_bed_id FROM dbo.dim_bed WHERE source_system IS NOT NULL AND hmis_bed_id IS NOT NULL GROUP BY source_system, hmis_bed_id HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_bridge_household_client_enrollment: duplicate enrollment_key' AS check_name, (SELECT COUNT(*) FROM (SELECT enrollment_key FROM dbo.bridge_household_client WHERE enrollment_key IS NOT NULL GROUP BY enrollment_key HAVING COUNT(*) > 1) d) AS problem_rows
    UNION ALL
    SELECT 'UQ_bridge_enrollment_grant_link: duplicate enrollment_key, source_grant_id' AS check_name, (SELECT COUNT(*) FROM (SELECT enrollment_key, source_grant_id FROM dbo.bridge_enrollment_grant WHERE enrollment_key IS NOT NULL AND source_grant_id IS NOT NULL GROUP BY enrollment_key, source_grant_id HAVING COUNT(*) > 1) d) AS problem_rows
) x
ORDER BY problem_rows DESC, check_name;
