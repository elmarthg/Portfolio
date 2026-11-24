SELECT * FROM (
SELECT 'salesforce_id' AS warehouse_column, 18 AS max_allowed, MAX(LEN(salesforce_id)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'salesforce_account_id' AS warehouse_column, 18 AS max_allowed, MAX(LEN(salesforce_account_id)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'external_id' AS warehouse_column, 50 AS max_allowed, MAX(LEN(external_id)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'first_name' AS warehouse_column, 100 AS max_allowed, MAX(LEN(first_name)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'middle_name' AS warehouse_column, 100 AS max_allowed, MAX(LEN(middle_name)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'last_name' AS warehouse_column, 100 AS max_allowed, MAX(LEN(last_name)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'sex' AS warehouse_column, 50 AS max_allowed, MAX(LEN(sex)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'gender' AS warehouse_column, 50 AS max_allowed, MAX(LEN(gender)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'race' AS warehouse_column, 255 AS max_allowed, MAX(LEN(race)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'race_detail' AS warehouse_column, 500 AS max_allowed, MAX(LEN(race_detail)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'ethnicity' AS warehouse_column, 50 AS max_allowed, MAX(LEN(ethnicity)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'marital_status' AS warehouse_column, 50 AS max_allowed, MAX(LEN(marital_status)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'number_of_children' AS warehouse_column, 50 AS max_allowed, MAX(LEN(number_of_children)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'education_level' AS warehouse_column, 100 AS max_allowed, MAX(LEN(education_level)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'email' AS warehouse_column, 255 AS max_allowed, MAX(LEN(email)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'home_phone' AS warehouse_column, 30 AS max_allowed, MAX(LEN(home_phone)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'mobile_phone' AS warehouse_column, 30 AS max_allowed, MAX(LEN(mobile_phone)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'address_line1' AS warehouse_column, 255 AS max_allowed, MAX(LEN(address_line1)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'city' AS warehouse_column, 100 AS max_allowed, MAX(LEN(city)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'state' AS warehouse_column, 10 AS max_allowed, MAX(LEN(state)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'postal_code' AS warehouse_column, 20 AS max_allowed, MAX(LEN(postal_code)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'county' AS warehouse_column, 100 AS max_allowed, MAX(LEN(county_name)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'veteran_status' AS warehouse_column, 50 AS max_allowed, MAX(LEN(veteran_status)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'military_branch' AS warehouse_column, 50 AS max_allowed, MAX(LEN(military_branch)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'military_service_component' AS warehouse_column, 50 AS max_allowed, MAX(LEN(military_service_component)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'discharge_status' AS warehouse_column, 50 AS max_allowed, MAX(LEN(discharge_status)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'is_post_9_11' AS warehouse_column, 20 AS max_allowed, MAX(LEN(is_post_9_11)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'service_connected' AS warehouse_column, 50 AS max_allowed, MAX(LEN(service_connected)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'period_of_service' AS warehouse_column, 255 AS max_allowed, MAX(LEN(period_of_service)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'homeless_status' AS warehouse_column, 50 AS max_allowed, MAX(LEN(homeless_status)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'housing_status' AS warehouse_column, 100 AS max_allowed, MAX(LEN(housing_status)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'length_of_homelessness' AS warehouse_column, 100 AS max_allowed, MAX(LEN(length_of_homelessness)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'has_conviction' AS warehouse_column, 50 AS max_allowed, MAX(LEN(has_conviction)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'conviction_type' AS warehouse_column, 255 AS max_allowed, MAX(LEN(conviction_type)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'probation_parole_status' AS warehouse_column, 50 AS max_allowed, MAX(LEN(probation_parole_status)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'experience_during_incarceration' AS warehouse_column, 255 AS max_allowed, MAX(LEN(experience_during_incarceration)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'service_type' AS warehouse_column, 100 AS max_allowed, MAX(LEN(service_type)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'lead_source' AS warehouse_column, 100 AS max_allowed, MAX(LEN(lead_source)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'owner_id' AS warehouse_column, 18 AS max_allowed, MAX(LEN(owner_id)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
UNION ALL SELECT 'created_by_id' AS warehouse_column, 255 AS max_allowed, MAX(LEN(created_by_id)) AS longest_in_silver FROM lh_silver.dbo.conf_sf_cis_client
) x
WHERE longest_in_silver > max_allowed
ORDER BY warehouse_column;
