-- 02b_seed_etl_source_config v1.1
-- Adds missing source rows only, so later edits are never overwritten. Run after 02_seed.

INSERT INTO etl_source_config (source_system, is_enabled, load_order, silver_notebook, loader_procedure,
                               bronze_prefix, expected_frequency, block_reason, notes, updated_by, updated_datetime)
SELECT v.source_system, v.is_enabled, v.load_order, v.silver_notebook, v.loader_procedure,
       v.bronze_prefix, v.expected_frequency, v.block_reason, v.notes, USER_NAME(), SYSDATETIME()
FROM (
    SELECT 'sf_cis' AS source_system, 1 AS is_enabled, 1 AS load_order,
           CAST('nb_silver_sf_cis_conform' AS VARCHAR(128)) AS silver_notebook, 'usp_load_sf_cis' AS loader_procedure,
           CAST('lh_bronze.dbo.sf_cis_' AS VARCHAR(100)) AS bronze_prefix, 'weekly' AS expected_frequency,
           CAST(NULL AS VARCHAR(500)) AS block_reason,
           'Salesforce CIS. Bronze from the Dataflow (sf_cis_contact); Silver from the Python notebook.' AS notes
    UNION ALL SELECT 'hmis_lac', 1, 2, 'nb_silver_hmis_clarity_conform', 'usp_load_hmis_clarity',
           'lh_bronze.dbo.hmis_lac_', 'weekly', NULL,
           'LA County Clarity. Data-sharing approval on file.'
    UNION ALL SELECT 'hmis_sandiego', 0, 3, 'nb_silver_hmis_clarity_conform', 'usp_load_hmis_clarity',
           'lh_bronze.dbo.hmis_sandiego_', 'weekly', NULL,
           'San Diego Clarity. Approved; enable once its exports land in Bronze.'
    UNION ALL SELECT 'hmis_riverside', 0, NULL, NULL, NULL, NULL, NULL,
           'Example of a blocked source: the county has not approved client-level extraction to an external database.',
           'Do not load.'
    UNION ALL SELECT 'hmis_ventura', 0, NULL, NULL, NULL, NULL, NULL,
           'Listed as WellSky in dim_source, not Clarity. Verify the platform before using the Clarity notebook.',
           NULL
) v
WHERE NOT EXISTS (SELECT 1 FROM etl_source_config c WHERE c.source_system = v.source_system);

UPDATE etl_source_config
SET silver_notebook  = 'nb_silver_sf_cis_conform',
    bronze_prefix    = 'lh_bronze.dbo.sf_cis_',
    notes            = 'Salesforce CIS. Bronze from the Dataflow (sf_cis_contact); Silver from the Python notebook.',
    updated_by       = USER_NAME(),
    updated_datetime = SYSDATETIME()
WHERE source_system = 'sf_cis' AND silver_notebook IS NULL
  AND notes = 'Salesforce CIS. Silver still in Dataflow Gen2 until the Python notebook replaces it.';

SELECT source_system, is_enabled, load_order, silver_notebook, loader_procedure, block_reason, notes
FROM etl_source_config
ORDER BY ISNULL(load_order, 99), source_system;
