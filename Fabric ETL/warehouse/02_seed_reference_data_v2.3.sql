-- 02_seed_reference_data v2.3 (warehouse schema v8.14): seeds dim_date and dim_source.
-- Run after 01_ddl and migrations/0001. Safe to rerun on a live warehouse: it only adds dates
-- and sources that are missing and corrects changed source details. Nothing is deleted.
-- v2.3: no longer clears and rebuilds the two tables (needed now that the warehouse persists).
-- Fiscal year starts October 1 and is named for the year it ends in (FY2026 = 2025-10-01 to 2026-09-30).

UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'ABANDONED'
WHERE script_name = '02_seed_reference_data' AND run_status = 'RUNNING';

INSERT INTO dbo.warehouse_run_log (
    script_name, script_version, run_start, run_status, run_by, change_summary
)
VALUES (
    '02_seed_reference_data', 'v2.3', SYSDATETIME(), 'RUNNING', USER_NAME(),
    'dim_date 2000-01-01 to 2040-12-31 and dim_source: add missing rows only'
);
GO

-- 1. dim_date: 2000-01-01 through 2040-12-31 (14,976 rows) so older HMIS enrollments get a date row.
--    Only dates not already present are added.

WITH digits AS (
    SELECT d FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS t(d)
),
nums AS (
    SELECT a.d + b.d*10 + c.d*100 + d.d*1000 + e.d*10000 AS n
    FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d CROSS JOIN digits e
),
date_range AS (
    SELECT DATEADD(day, n, CAST('2000-01-01' AS DATE)) AS d
    FROM nums
    WHERE n BETWEEN 0 AND DATEDIFF(day, CAST('2000-01-01' AS DATE), CAST('2040-12-31' AS DATE))
)
INSERT INTO dim_date (
    date_key, full_date, year, quarter, month, month_name,
    day, day_of_week, day_name, is_weekend,
    fiscal_year, fiscal_quarter, load_datetime
)
SELECT
    CAST(CONVERT(VARCHAR(8), d, 112) AS INT)                     AS date_key,
    d                                                            AS full_date,
    YEAR(d)                                                      AS year,
    DATEPART(quarter, d)                                         AS quarter,
    MONTH(d)                                                     AS month,
    DATENAME(month, d)                                           AS month_name,
    DAY(d)                                                       AS day,
    DATEPART(weekday, d)                                         AS day_of_week,
    DATENAME(weekday, d)                                         AS day_name,
    CASE WHEN DATEPART(weekday, d) IN (1, 7) THEN 1 ELSE 0 END   AS is_weekend,
    CASE WHEN MONTH(d) >= 10 THEN YEAR(d) + 1 ELSE YEAR(d) END   AS fiscal_year,
    CASE
        WHEN MONTH(d) IN (10, 11, 12) THEN 1
        WHEN MONTH(d) IN (1, 2, 3)    THEN 2
        WHEN MONTH(d) IN (4, 5, 6)    THEN 3
        ELSE 4
    END                                                          AS fiscal_quarter,
    SYSDATETIME()                                                AS load_datetime
FROM date_range r
WHERE NOT EXISTS (SELECT 1 FROM dim_date x WHERE x.full_date = r.d);
GO

-- 2. dim_source: one row per source, including ones that don't load yet. etl_source_config decides what loads.
--    Missing sources are added; existing ones keep their row and get any corrected details.
DROP TABLE IF EXISTS #src;
SELECT * INTO #src FROM (VALUES
  ('sf_cis',         'Salesforce', 'Salesforce CIS',         NULL,             'connector', 1),
  ('hmis_lac',       'Clarity',    'LA County HMIS',         'Los Angeles',    'manual',    1),
  ('hmis_orange',    'Clarity',    'Orange County HMIS',     'Orange',         'manual',    1),
  ('hmis_riverside', 'Clarity',    'Riverside County HMIS',  'Riverside',      'manual',    1),
  ('hmis_sanbern',   'Clarity',    'San Bernardino HMIS',    'San Bernardino', 'manual',    1),
  ('hmis_ventura',   'WellSky',    'Ventura County HMIS',    'Ventura',        'manual',    1),
  ('hmis_sandiego',  'Clarity',    'San Diego County HMIS',  'San Diego',      'manual',    1),
  ('hmis_kern',      'Clarity',    'Kern County HMIS',       'Kern',           'manual',    1),
  ('wellsky',        'WellSky',    'WellSky',                NULL,             'manual',    1),
  ('champ',          'CaseWorthy', 'CHAMP',                  NULL,             'manual',    1),
  ('apricot',        'Apricot',    'Bonterra Apricot',       NULL,             'manual',    1)
) v (source_system, platform, instance_name, county, ingest_method, is_active);

INSERT INTO dim_source (source_system, platform, instance_name, county, ingest_method, is_active, load_datetime)
SELECT v.source_system, v.platform, v.instance_name, v.county, v.ingest_method, v.is_active, SYSDATETIME()
FROM #src v
WHERE NOT EXISTS (SELECT 1 FROM dim_source d WHERE d.source_system = v.source_system);

UPDATE d
SET d.platform = v.platform, d.instance_name = v.instance_name, d.county = v.county,
    d.ingest_method = v.ingest_method, d.load_datetime = SYSDATETIME()
FROM dim_source d
JOIN #src v ON v.source_system = d.source_system
WHERE NOT EXISTS (SELECT d.platform, d.instance_name, d.county, d.ingest_method
                  INTERSECT
                  SELECT v.platform, v.instance_name, v.county, v.ingest_method);
GO

-- 3. Verify
SELECT COUNT(*) AS date_rows, MIN(full_date) AS min_date, MAX(full_date) AS max_date FROM dim_date;

SELECT full_date, fiscal_year, fiscal_quarter
FROM dim_date
WHERE full_date IN ('2025-09-30', '2025-10-01', '2026-03-15', '2026-09-30')
ORDER BY full_date;

SELECT platform, COUNT(*) AS instances FROM dim_source GROUP BY platform ORDER BY platform;
GO

UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'SUCCESS'
WHERE script_name = '02_seed_reference_data' AND run_status = 'RUNNING';
GO
