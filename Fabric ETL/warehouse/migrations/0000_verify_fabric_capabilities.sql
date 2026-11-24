-- migrations/0000_verify_fabric_capabilities.sql
-- Run ONCE in the Fabric warehouse before migration 0001 and the v3.0 / v2.0 loaders (tests 7 and 8 were
-- added for migration 0002 and 06_proc_person_links; on a warehouse that already passed 1 to 6, run it again).
-- It checks the warehouse features the persistent loaders rely on, using a scratch table that it
-- creates and drops itself. It never touches your real tables. Every row of the last result
-- should say PASS. If any says FAIL, stop and send the result before going further.

DROP TABLE IF EXISTS dbo.zz_capability_check;
CREATE TABLE dbo.zz_capability_check (id INT, val VARCHAR(20), changed_at DATETIME2(6));
INSERT INTO dbo.zz_capability_check VALUES (1, 'a', NULL), (2, 'b', NULL);

DROP TABLE IF EXISTS #results;
-- Distributed (Parquet-backed) temp table: Fabric's default #temp table is MDF-backed and cannot take rows
-- selected from a warehouse table (Msg 15816). A temp table made by SELECT ... INTO is always distributed.
CREATE TABLE #results (test_name VARCHAR(100), result VARCHAR(10), detail VARCHAR(400)) WITH (DISTRIBUTION = ROUND_ROBIN);

-- 1. A temp table created inside a transaction, then ROLLBACK: nothing kept
BEGIN TRY
    BEGIN TRANSACTION;
    DROP TABLE IF EXISTS #t1;
    SELECT 3 AS id, 'c' AS val INTO #t1;
    INSERT INTO dbo.zz_capability_check (id, val) SELECT id, val FROM #t1;
    ROLLBACK TRANSACTION;
    INSERT INTO #results SELECT '1 temp table inside a transaction + rollback',
        CASE WHEN COUNT(*) = 2 THEN 'PASS' ELSE 'FAIL' END, CONCAT(COUNT(*), ' rows after rollback (expect 2)')
    FROM dbo.zz_capability_check;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    INSERT INTO #results VALUES ('1 temp table inside a transaction + rollback', 'FAIL', LEFT(ERROR_MESSAGE(), 400));
END CATCH;

-- 2. Same, with COMMIT: the row is kept
BEGIN TRY
    BEGIN TRANSACTION;
    DROP TABLE IF EXISTS #t2;
    SELECT 3 AS id, 'c' AS val INTO #t2;
    INSERT INTO dbo.zz_capability_check (id, val) SELECT id, val FROM #t2;
    COMMIT TRANSACTION;
    INSERT INTO #results SELECT '2 temp table inside a transaction + commit',
        CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END, CONCAT(COUNT(*), ' rows after commit (expect 3)')
    FROM dbo.zz_capability_check;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    INSERT INTO #results VALUES ('2 temp table inside a transaction + commit', 'FAIL', LEFT(ERROR_MESSAGE(), 400));
END CATCH;

-- 3. An error inside the transaction: CATCH sees it, rolls back, nothing kept
DECLARE @tc_in_catch INT = NULL;
BEGIN TRY
    BEGIN TRANSACTION;
    INSERT INTO dbo.zz_capability_check (id, val) VALUES (4, 'd');
    THROW 50099, 'deliberate test error', 1;
END TRY
BEGIN CATCH
    SET @tc_in_catch = @@TRANCOUNT;
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
END CATCH;
INSERT INTO #results SELECT '3 error inside a transaction rolls back',
    CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END,
    CONCAT(COUNT(*), ' rows (expect 3); @@TRANCOUNT in CATCH was ', ISNULL(CAST(@tc_in_catch AS VARCHAR(5)), 'NULL'))
FROM dbo.zz_capability_check;

-- 4. UPDATE ... FROM with a null-safe change test (NOT EXISTS ... INTERSECT): only changed rows are touched
BEGIN TRY
    DROP TABLE IF EXISTS #src;
    SELECT * INTO #src FROM (VALUES (1, 'a'), (2, 'B'), (3, NULL)) v (id, val);
    UPDATE z
    SET z.val = s.val, z.changed_at = SYSDATETIME()
    FROM dbo.zz_capability_check z
    JOIN #src s ON s.id = z.id
    WHERE NOT EXISTS (SELECT z.val INTERSECT SELECT s.val);
    INSERT INTO #results SELECT '4 change detection with INTERSECT',
        CASE WHEN SUM(CASE WHEN changed_at IS NOT NULL THEN 1 ELSE 0 END) = 2
              AND MAX(CASE WHEN id = 1 THEN changed_at END) IS NULL THEN 'PASS' ELSE 'FAIL' END,
        CONCAT(SUM(CASE WHEN changed_at IS NOT NULL THEN 1 ELSE 0 END), ' rows changed (expect 2: ids 2 and 3)')
    FROM dbo.zz_capability_check;
END TRY
BEGIN CATCH
    INSERT INTO #results VALUES ('4 change detection with INTERSECT', 'FAIL', LEFT(ERROR_MESSAGE(), 400));
END CATCH;

-- 5. Pacific date for length_of_stay_to_date
BEGIN TRY
    DECLARE @pacific DATE = CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'Pacific Standard Time' AS DATE);
    INSERT INTO #results VALUES ('5 AT TIME ZONE Pacific', CASE WHEN @pacific IS NOT NULL THEN 'PASS' ELSE 'FAIL' END,
                                 CONCAT('Pacific date today: ', CONVERT(VARCHAR(10), @pacific, 120)));
END TRY
BEGIN CATCH
    INSERT INTO #results VALUES ('5 AT TIME ZONE Pacific', 'FAIL', LEFT(ERROR_MESSAGE(), 400));
END CATCH;

-- 6. Reading Silver (lakehouse SQL endpoint) inside a transaction
BEGIN TRY
    BEGIN TRANSACTION;
    DROP TABLE IF EXISTS #silver_peek;
    SELECT TOP 1 program_id INTO #silver_peek FROM lh_silver.dbo.conf_sf_cis_program;
    ROLLBACK TRANSACTION;
    INSERT INTO #results VALUES ('6 Silver read inside a transaction', 'PASS', 'lh_silver.dbo.conf_sf_cis_program readable');
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    INSERT INTO #results VALUES ('6 Silver read inside a transaction', 'FAIL', LEFT(ERROR_MESSAGE(), 400));
END CATCH;

-- 7. A WHILE loop that rebuilds a temp table and updates another, inside a transaction
--    (06_proc_person_links groups profiles this way)
BEGIN TRY
    DECLARE @loop_changed INT = 1;
    DECLARE @loop_rounds INT = 0;
    BEGIN TRANSACTION;
    DROP TABLE IF EXISTS #lab7;
    SELECT * INTO #lab7 FROM (VALUES (1, 1), (2, 2), (3, 3)) v (k, grp);
    DROP TABLE IF EXISTS #edge7;
    SELECT * INTO #edge7 FROM (VALUES (1, 2), (2, 1), (2, 3), (3, 2)) v (a, b);
    WHILE @loop_changed > 0
    BEGIN
        SET @loop_rounds = @loop_rounds + 1;
        DROP TABLE IF EXISTS #nb7;
        SELECT l.k, MIN(n.grp) AS new_grp
        INTO #nb7
        FROM #lab7 l JOIN #edge7 e ON e.a = l.k JOIN #lab7 n ON n.k = e.b
        GROUP BY l.k;
        SELECT @loop_changed = COUNT(*) FROM #nb7 x JOIN #lab7 l ON l.k = x.k WHERE x.new_grp < l.grp;
        UPDATE l SET l.grp = x.new_grp FROM #lab7 l JOIN #nb7 x ON x.k = l.k WHERE x.new_grp < l.grp;
    END;
    COMMIT TRANSACTION;
    INSERT INTO #results SELECT '7 WHILE loop with temp tables inside a transaction',
        CASE WHEN MAX(grp) = 1 THEN 'PASS' ELSE 'FAIL' END,
        CONCAT('all three grouped as ', MAX(grp), ' (expect 1) in ', @loop_rounds, ' rounds')
    FROM #lab7;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    INSERT INTO #results VALUES ('7 WHILE loop with temp tables inside a transaction', 'FAIL', LEFT(ERROR_MESSAGE(), 400));
END CATCH;

-- 8. FIRST_VALUE picking the newest non-blank value (rpt.person survivorship)
BEGIN TRY
    DROP TABLE IF EXISTS #fv;
    SELECT DISTINCT
        FIRST_VALUE(v) OVER (PARTITION BY p ORDER BY CASE WHEN v IS NULL THEN 1 ELSE 0 END, d DESC) AS picked
    INTO #fv
    FROM (VALUES (1, 'old', CAST('2025-01-01' AS DATE)), (1, NULL, CAST('2026-01-01' AS DATE)),
                 (1, 'new', CAST('2025-06-01' AS DATE))) x (p, v, d);
    INSERT INTO #results SELECT '8 FIRST_VALUE survivorship',
        CASE WHEN MAX(picked) = 'new' AND COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END,
        CONCAT('picked ', MAX(picked), ' (expect new)')
    FROM #fv;
END TRY
BEGIN CATCH
    INSERT INTO #results VALUES ('8 FIRST_VALUE survivorship', 'FAIL', LEFT(ERROR_MESSAGE(), 400));
END CATCH;

DROP TABLE IF EXISTS dbo.zz_capability_check;

SELECT test_name, result, detail FROM #results ORDER BY test_name;
