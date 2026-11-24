-- 00_clean_warehouse v1.4: drops every user object in the warehouse so 01 to 06 start clean.
-- Run once with @dry_run = 1 to see the list, then again with @dry_run = 0 to drop.
-- Afterwards follow docs/deploy_to_fabric.md: 01, migrations 0000 to 0002, 02, 02b, migrations 0003 to 0006, 04, then install 03a, 03b, 05, 06 and 07.
-- Erases every table, including client history. On a warehouse that matters, use a restore point instead.

DECLARE @dry_run BIT = 1;
DECLARE @keep_hand_maintained BIT = 1;   -- 1 keeps security_user_program_access, dim_hud_code, kpi_target, etl_source_config, match_decision (reviewer decisions), apricot_profile_map (what Apricot holds)
DECLARE @keep_old_stage BIT = 0;         -- 1 keeps old stg_% tables (needed for the Dataflow vs notebook comparison)

DROP TABLE IF EXISTS #to_drop;
CREATE TABLE #to_drop (seq INT, kind VARCHAR(20), obj VARCHAR(300), stmt VARCHAR(1000));

INSERT INTO #to_drop (seq, kind, obj, stmt)
SELECT 1, 'security policy', s.name + '.' + p.name,
       'DROP SECURITY POLICY [' + s.name + '].[' + p.name + '];'
FROM sys.security_policies p
JOIN sys.schemas s ON s.schema_id = p.schema_id;

INSERT INTO #to_drop (seq, kind, obj, stmt)
SELECT 2, 'view', s.name + '.' + v.name,
       'DROP VIEW [' + s.name + '].[' + v.name + '];'
FROM sys.views v
JOIN sys.schemas s ON s.schema_id = v.schema_id
WHERE v.is_ms_shipped = 0;

INSERT INTO #to_drop (seq, kind, obj, stmt)
SELECT 3, 'procedure', s.name + '.' + p.name,
       'DROP PROCEDURE [' + s.name + '].[' + p.name + '];'
FROM sys.procedures p
JOIN sys.schemas s ON s.schema_id = p.schema_id
WHERE p.is_ms_shipped = 0;

INSERT INTO #to_drop (seq, kind, obj, stmt)
SELECT 4, 'function', s.name + '.' + o.name,
       'DROP FUNCTION [' + s.name + '].[' + o.name + '];'
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.type IN ('FN', 'IF', 'TF') AND o.is_ms_shipped = 0;

INSERT INTO #to_drop (seq, kind, obj, stmt)
SELECT 5, 'foreign key', s.name + '.' + t.name + '.' + fk.name,
       'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + fk.name + '];'
FROM sys.foreign_keys fk
JOIN sys.tables t ON t.object_id = fk.parent_object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id;

INSERT INTO #to_drop (seq, kind, obj, stmt)
SELECT 6, 'table', s.name + '.' + t.name,
       'DROP TABLE [' + s.name + '].[' + t.name + '];'
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE t.is_ms_shipped = 0
  AND NOT (@keep_hand_maintained = 1
           AND t.name IN ('security_user_program_access', 'dim_hud_code', 'kpi_target', 'etl_source_config', 'match_decision', 'apricot_profile_map'))
  AND NOT (@keep_old_stage = 1 AND t.name LIKE 'stg[_]%');

SELECT seq, kind, obj FROM #to_drop ORDER BY seq, obj;

SELECT t.name AS kept_table
FROM sys.tables t
WHERE t.is_ms_shipped = 0
  AND ((@keep_hand_maintained = 1
        AND t.name IN ('security_user_program_access', 'dim_hud_code', 'kpi_target', 'etl_source_config', 'match_decision', 'apricot_profile_map'))
       OR (@keep_old_stage = 1 AND t.name LIKE 'stg[_]%'))
ORDER BY t.name;

IF @dry_run = 1
BEGIN
    PRINT 'Dry run: nothing was dropped. Check both lists, then set @dry_run = 0 and run again.';
END
ELSE
BEGIN
    DROP TABLE IF EXISTS #run;
    CREATE TABLE #run (rn INT, stmt VARCHAR(1000));
    INSERT INTO #run (rn, stmt)
    SELECT ROW_NUMBER() OVER (ORDER BY seq, obj), stmt FROM #to_drop;

    DECLARE @i INT = 1;
    DECLARE @n INT = (SELECT COUNT(*) FROM #run);
    DECLARE @sql NVARCHAR(1000);

    WHILE @i <= @n
    BEGIN
        SELECT @sql = CAST(stmt AS NVARCHAR(1000)) FROM #run WHERE rn = @i;
        PRINT @sql;
        EXEC sp_executesql @sql;
        SET @i = @i + 1;
    END;

    PRINT CONCAT('Dropped ', @n, ' objects. Now follow docs/deploy_to_fabric.md: 01, migrations 0000 to 0002, 02, 02b, migrations 0003 to 0006, 04, then install 03a, 03b, 05, 06 and 07.');
END;
