-- 06_proc_person_links v1.3: installs dbo.usp_record_match_decision, dbo.usp_apply_match_suggestions
-- and dbo.usp_refresh_person_links. Needs migrations/0003_matching.sql (schema v8.16).
--
-- v1.3 (schema v8.19): both procedures check every declared key on the tables they write before COMMIT
--   (generated list: match_decision and match_review_queue; person_client_link, rpt.person and rpt.person_client).
-- v1.2: the review queue's temp table declares queue_status as VARCHAR(20). Fabric sized it from the first
--       values ('new', 'unsure') and refused 'evidence_changed' (Msg 2628, first Fabric run 2026-11-22).
--
-- v1.1:
--   1. A reviewer can record 'unsure' (the Withdraw button in the review notebook). It replaces an
--      earlier 'match' or 'no_match' on the pair, joins nothing and separates nothing, and stops the
--      automatic rules from deciding that pair again. The pair goes back to the review queue.
--   2. usp_apply_match_suggestions turns the matching notebook's output into decisions and rebuilds
--      the review queue (see its own header).
--
-- The warehouse LINKS profiles that belong to the same person; it never merges them.
--   dim_client           one row set per source profile (client_key), exactly as the source sent it
--   match_decision       "same person" / "not the same person", keyed on source IDs
--   person_client_link   which person each profile belongs to, with dated history
--   rpt.person           the merged profile per person, rebuilt from the links each run
--
-- Source IDs used in decisions: the Salesforce Contact Id for sf_cis, the Clarity Personal ID for
-- hmis_* sources (the same IDs held in bridge_client_source).
--
-- Rules usp_refresh_person_links applies, every run, inside one transaction:
--   1. Every current profile gets exactly one current person. A profile with no "match" decision
--      is a person of its own (link_method 'self').
--   2. Profiles joined by active "match" decisions, directly or through each other, are one person.
--   3. A reviewer's "not the same person" wins over automatic matches: if it falls inside a group,
--      the automatic ('auto_score') links in that group are dropped and the group is worked out again.
--      If reviewer or exact-rule links still join the two, the decisions contradict each other:
--      those profiles keep their current links and the conflict is written to stage_load_reject
--      (entity_name 'person_link') for a reviewer to resolve.
--   4. Person keys are stable. A key stays with the group holding most of its profiles; when groups
--      join, the oldest (smallest) key survives; new groups get new keys. Keys are never reused,
--      so a key Apricot or a report stored keeps meaning the same person or nobody.
--   5. A link row is only written when a profile's person or link method changes. The old row gets
--      valid_to = this run's time, so any past date can be reproduced:
--        WHERE valid_from <= @as_of AND (valid_to IS NULL OR valid_to > @as_of)
--   6. rpt.person and rpt.person_client are replaced in the same transaction, so reports never see
--      links and merged profiles from different runs.
--   7. Integrity checks run before COMMIT (THROW 50012); a failure saves nothing.
-- Running it twice in a row changes no link rows.
--
-- Pipeline: after the ForEach "Load warehouse", before the Apricot import:
--   EXEC dbo.usp_refresh_person_links @pipeline_run_id = '<pipeline().RunId>';

DROP PROCEDURE IF EXISTS dbo.usp_record_match_decision;
GO

CREATE PROCEDURE dbo.usp_record_match_decision
    @source_system_a VARCHAR(50) = NULL,
    @source_id_a VARCHAR(100) = NULL,
    @source_system_b VARCHAR(50) = NULL,
    @source_id_b VARCHAR(100) = NULL,
    @decision VARCHAR(10) = NULL,
    @decision_method VARCHAR(20) = 'reviewer',
    @decided_by VARCHAR(255) = NULL,
    @match_probability DECIMAL(9,8) = NULL,
    @notes VARCHAR(500) = NULL
AS
BEGIN

DECLARE @now DATETIME2(6) = SYSDATETIME();
DECLARE @sa VARCHAR(50) = LOWER(LTRIM(RTRIM(@source_system_a)));
DECLARE @ia VARCHAR(100) = LTRIM(RTRIM(@source_id_a));
DECLARE @sb VARCHAR(50) = LOWER(LTRIM(RTRIM(@source_system_b)));
DECLARE @ib VARCHAR(100) = LTRIM(RTRIM(@source_id_b));
DECLARE @dec VARCHAR(10) = LOWER(LTRIM(RTRIM(@decision)));
DECLARE @meth VARCHAR(20) = LOWER(LTRIM(RTRIM(@decision_method)));
DECLARE @who VARCHAR(255) = COALESCE(@decided_by, USER_NAME());
DECLARE @tmp_s VARCHAR(50) = NULL;
DECLARE @tmp_i VARCHAR(100) = NULL;
DECLARE @outcome VARCHAR(20) = NULL;

IF @dec IS NULL OR @dec NOT IN ('match', 'no_match', 'unsure')
    THROW 50030, '@decision must be match, no_match or unsure.', 1;
IF @meth IS NULL OR @meth NOT IN ('reviewer', 'exact_rule', 'auto_score')
    THROW 50030, '@decision_method must be reviewer, exact_rule or auto_score.', 1;
IF @dec IN ('no_match', 'unsure') AND @meth <> 'reviewer'
    THROW 50030, 'Only a reviewer can record no_match or unsure. Automatic methods only suggest matches.', 1;
IF @sa IS NULL OR @sb IS NULL OR @ia IS NULL OR @ib IS NULL OR @ia = '' OR @ib = ''
    THROW 50030, 'Both records need a source system and a source ID.', 1;
IF @sa = @sb AND dbo.fn_normalize_key(@ia) = dbo.fn_normalize_key(@ib)
    THROW 50030, 'A record cannot be matched to itself.', 1;
IF NOT EXISTS (SELECT 1 FROM dbo.dim_source WHERE source_system = @sa)
   OR NOT EXISTS (SELECT 1 FROM dbo.dim_source WHERE source_system = @sb)
    THROW 50030, 'Unknown source system. Use a value from dbo.dim_source, such as sf_cis or hmis_lac.', 1;

-- store every pair in the same order, so A-B and B-A are one pair
IF CONCAT(@sa, '|', dbo.fn_normalize_key(@ia)) > CONCAT(@sb, '|', dbo.fn_normalize_key(@ib))
BEGIN
    SET @tmp_s = @sa;
    SET @tmp_i = @ia;
    SET @sa = @sb;
    SET @ia = @ib;
    SET @sb = @tmp_s;
    SET @ib = @tmp_i;
END;

-- an automatic method never replaces a reviewer's decision on the same pair
IF @meth <> 'reviewer' AND EXISTS (
    SELECT 1 FROM dbo.match_decision
    WHERE is_active = 1 AND decision_method = 'reviewer'
      AND source_system_a = @sa AND dbo.fn_normalize_key(source_id_a) = dbo.fn_normalize_key(@ia)
      AND source_system_b = @sb AND dbo.fn_normalize_key(source_id_b) = dbo.fn_normalize_key(@ib))
    SET @outcome = 'kept_reviewer';

-- the same decision by the same method is already active: nothing to do
IF @outcome IS NULL AND EXISTS (
    SELECT 1 FROM dbo.match_decision
    WHERE is_active = 1 AND decision = @dec AND decision_method = @meth
      AND source_system_a = @sa AND dbo.fn_normalize_key(source_id_a) = dbo.fn_normalize_key(@ia)
      AND source_system_b = @sb AND dbo.fn_normalize_key(source_id_b) = dbo.fn_normalize_key(@ib))
    SET @outcome = 'unchanged';

IF @outcome IS NULL
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        UPDATE dbo.match_decision
        SET is_active = 0, revoked_by = @who, revoked_datetime = @now
        WHERE is_active = 1
          AND source_system_a = @sa AND dbo.fn_normalize_key(source_id_a) = dbo.fn_normalize_key(@ia)
          AND source_system_b = @sb AND dbo.fn_normalize_key(source_id_b) = dbo.fn_normalize_key(@ib);

        INSERT INTO dbo.match_decision (
            source_system_a, source_id_a, source_system_b, source_id_b,
            decision, decision_method, match_probability, decided_by, decided_datetime, notes, is_active
        )
        VALUES (@sa, @ia, @sb, @ib, @dec, @meth, @match_probability, @who, @now, @notes, 1);

        COMMIT TRANSACTION;
        SET @outcome = 'recorded';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;

SELECT @outcome AS outcome,
       CASE @outcome
            WHEN 'recorded'      THEN CASE WHEN @dec = 'unsure'
                                           THEN 'Withdrawn. The pair is unlinked at the next usp_refresh_person_links run and goes back to the review queue; automatic rules will not link it again.'
                                           ELSE 'Saved. It takes effect at the next usp_refresh_person_links run.' END
            WHEN 'unchanged'     THEN 'The same decision was already active. Nothing changed.'
            WHEN 'kept_reviewer' THEN 'A reviewer already decided this pair. Automatic decisions never replace it.'
       END AS detail;

END;
GO


DROP PROCEDURE IF EXISTS dbo.usp_apply_match_suggestions;
GO

-- usp_apply_match_suggestions: the matching notebook's latest run -> decisions and review queue.
-- Reads lh_silver.dbo.match_run (one row per notebook run) and lh_silver.dbo.match_suggestion
-- (the pairs from that run: IDs, scores and evidence only, no names or dates).
--   1. Pairs the notebook says to link ('link' = an exact rule held, 'auto_link' = a score above the
--      automatic-link threshold, which is switched off unless the team turns it on) become active
--      'match' decisions with method exact_rule or auto_score.
--   2. A reviewer's decision on a pair is never replaced.
--   3. An automatic link is never broken automatically. When the evidence behind one of the
--      notebook's earlier links is gone (both profiles still live), the link stays and the pair goes to
--      the top of the review queue as 'evidence_changed'; the reviewer's answer replaces it. Breaking it
--      here would unlink and relink people in reports and in Apricot every time a phone number changed.
--      Clarity merges (rule_name 'clarity_merge', written by 03b) are not the notebook's and are left alone.
--   4. dbo.match_review_queue is rebuilt from the 'review' pairs, leaving out pairs a reviewer has
--      decided (match or no_match), pairs linked by step 1 and pairs already in the same person.
--      Pairs a reviewer withdrew ('unsure') stay in the queue, marked 'unsure'.
--   5. Safety: stops (THROW 50031) when this run would add links for more than 5% of live profiles
--      or send more than 5% of the automatic links back to review (each with a floor, so a small change on a
--      small warehouse still goes through). Check the match_run detail; if the change is real, rerun
--      with @allow_mass_change = 1. Stops (THROW 50004) when the newest successful matching run is
--      older than @min_match_run_datetime (the notebook did not run or the endpoint has not synced).
--   6. One transaction; integrity checks before COMMIT (THROW 50012).
-- Pipeline: after "Load warehouse", before usp_refresh_person_links:
--   EXEC dbo.usp_apply_match_suggestions @min_match_run_datetime = '<TriggerTime>',
--        @pipeline_run_id = '<pipeline().RunId>';
CREATE PROCEDURE dbo.usp_apply_match_suggestions
    @min_match_run_datetime DATETIME2(6) = NULL,
    @allow_mass_change BIT = 0,
    @pipeline_run_id VARCHAR(100) = NULL
AS
BEGIN

DECLARE @etl_time DATETIME2(6) = SYSDATETIME();
DECLARE @run_id VARCHAR(60) = CONCAT('match_apply_', CONVERT(VARCHAR(30), SYSDATETIME(), 126));
DECLARE @who VARCHAR(255) = 'nb_match_persons';

IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.16')
    THROW 50011, 'The warehouse schema is older than v8.16. Run migrations/0003_matching.sql first.', 1;

BEGIN TRY

-- 0. LOG RUN START (outside the transaction)
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'ABANDONED'
WHERE script_name = '06_apply_match_suggestions' AND run_status = 'RUNNING';

INSERT INTO dbo.warehouse_run_log (
    script_name, script_version, run_start, run_status, run_by, change_summary, pipeline_run_id
)
VALUES (
    '06_apply_match_suggestions', 'v1.3', @etl_time, 'RUNNING', USER_NAME(),
    'Matching notebook suggestions to match_decision; match_review_queue rebuilt', @pipeline_run_id
);

DECLARE @match_run_id VARCHAR(60) = NULL;
DECLARE @match_run_time DATETIME2(6) = NULL;
DECLARE @live INT = 0;
DECLARE @auto_active INT = 0;
DECLARE @n_add INT = 0;
DECLARE @n_stale INT = 0;
DECLARE @n_kept_reviewer INT = 0;
DECLARE @n_unresolved INT = 0;
DECLARE @n_queue INT = 0;

-- 1. The newest successful matching run
SET @match_run_id = (SELECT TOP 1 match_run_id FROM lh_silver.dbo.match_run
                     WHERE status = 'success' ORDER BY run_datetime DESC, match_run_id DESC);
SET @match_run_time = (SELECT MAX(run_datetime) FROM lh_silver.dbo.match_run
                       WHERE status = 'success' AND match_run_id = @match_run_id);

IF @match_run_id IS NULL
    THROW 50004, 'No successful matching run in lh_silver.dbo.match_run. Run nb_match_persons first.', 1;

IF @min_match_run_datetime IS NOT NULL AND @match_run_time < @min_match_run_datetime
BEGIN
    DECLARE @stale_msg NVARCHAR(2048) = CONCAT('The newest successful matching run (', @match_run_id, ' at ',
        CONVERT(VARCHAR(30), @match_run_time, 126), ') is older than this pipeline run (',
        CONVERT(VARCHAR(30), @min_match_run_datetime, 126),
        '). nb_match_persons did not finish or the SQL endpoint has not synced yet. Wait and rerun.');
    THROW 50004, @stale_msg, 1;
END;

-- 2. Source ID -> client_key, as in usp_refresh_person_links
DROP TABLE IF EXISTS #srcid;
SELECT b.source_system,
       dbo.fn_normalize_key(COALESCE(b.source_personal_id, b.source_client_id)) AS source_key,
       MIN(b.client_key) AS client_key
INTO #srcid
FROM bridge_client_source b
WHERE b.is_active = 1
  AND COALESCE(b.source_personal_id, b.source_client_id) IS NOT NULL
GROUP BY b.source_system, dbo.fn_normalize_key(COALESCE(b.source_personal_id, b.source_client_id));

-- 3. This run's suggestions, each pair in the stored order (a before b), resolved to profiles
DROP TABLE IF EXISTS #raw;
SELECT LOWER(LTRIM(RTRIM(s.source_system_a))) AS sa, LTRIM(RTRIM(s.source_id_a)) AS ia,
       LOWER(LTRIM(RTRIM(s.source_system_b))) AS sb, LTRIM(RTRIM(s.source_id_b)) AS ib,
       s.match_probability, s.match_weight, LOWER(s.suggested_action) AS suggested_action, s.rule_name,
       s.evidence, s.comparison_levels
INTO #raw
FROM lh_silver.dbo.match_suggestion s
WHERE s.match_run_id = @match_run_id
  AND s.source_id_a IS NOT NULL AND s.source_id_b IS NOT NULL;

DROP TABLE IF EXISTS #sug;
SELECT x.sa, x.ia, x.sb, x.ib, x.match_probability, x.match_weight, x.suggested_action, x.rule_name,
       x.evidence, x.comparison_levels, ca.client_key AS ck_a, cb.client_key AS ck_b
INTO #sug
FROM (
    SELECT CASE WHEN CONCAT(sa, '|', dbo.fn_normalize_key(ia)) <= CONCAT(sb, '|', dbo.fn_normalize_key(ib)) THEN sa ELSE sb END AS sa,
           CASE WHEN CONCAT(sa, '|', dbo.fn_normalize_key(ia)) <= CONCAT(sb, '|', dbo.fn_normalize_key(ib)) THEN ia ELSE ib END AS ia,
           CASE WHEN CONCAT(sa, '|', dbo.fn_normalize_key(ia)) <= CONCAT(sb, '|', dbo.fn_normalize_key(ib)) THEN sb ELSE sa END AS sb,
           CASE WHEN CONCAT(sa, '|', dbo.fn_normalize_key(ia)) <= CONCAT(sb, '|', dbo.fn_normalize_key(ib)) THEN ib ELSE ia END AS ib,
           match_probability, match_weight, suggested_action, rule_name, evidence, comparison_levels,
           ROW_NUMBER() OVER (PARTITION BY
                CASE WHEN CONCAT(sa, '|', dbo.fn_normalize_key(ia)) <= CONCAT(sb, '|', dbo.fn_normalize_key(ib))
                     THEN CONCAT(sa, '|', dbo.fn_normalize_key(ia), '|', sb, '|', dbo.fn_normalize_key(ib))
                     ELSE CONCAT(sb, '|', dbo.fn_normalize_key(ib), '|', sa, '|', dbo.fn_normalize_key(ia)) END
                ORDER BY CASE suggested_action WHEN 'link' THEN 1 WHEN 'auto_link' THEN 2 ELSE 3 END,
                         match_probability DESC) AS rn
    FROM #raw
    WHERE suggested_action IN ('link', 'auto_link', 'review')
) x
LEFT JOIN #srcid ca ON ca.source_system = x.sa AND ca.source_key = dbo.fn_normalize_key(x.ia)
LEFT JOIN #srcid cb ON cb.source_system = x.sb AND cb.source_key = dbo.fn_normalize_key(x.ib)
WHERE x.rn = 1
  AND NOT (x.sa = x.sb AND dbo.fn_normalize_key(x.ia) = dbo.fn_normalize_key(x.ib));

SELECT @n_unresolved = COUNT(*) FROM #sug WHERE ck_a IS NULL OR ck_b IS NULL;

-- 4. Active decisions, keyed the same way
DROP TABLE IF EXISTS #act;
SELECT d.decision_key, d.source_system_a AS sa, dbo.fn_normalize_key(d.source_id_a) AS ka,
       d.source_system_b AS sb, dbo.fn_normalize_key(d.source_id_b) AS kb,
       d.source_id_a AS ia, d.source_id_b AS ib,
       d.decision, d.decision_method, d.decided_by, d.rule_name
INTO #act
FROM match_decision d
WHERE d.is_active = 1;

-- 5. What the notebook wants linked now
DROP TABLE IF EXISTS #want;
SELECT s.sa, s.ia, s.sb, s.ib, s.match_probability, s.rule_name, s.evidence,
       CASE WHEN s.suggested_action = 'link' THEN 'exact_rule' ELSE 'auto_score' END AS want_method,
       a.decision_key AS cur_key, a.decision AS cur_decision, a.decision_method AS cur_method
INTO #want
FROM #sug s
LEFT JOIN #act a ON a.sa = s.sa AND a.ka = dbo.fn_normalize_key(s.ia) AND a.sb = s.sb AND a.kb = dbo.fn_normalize_key(s.ib)
WHERE s.suggested_action IN ('link', 'auto_link')
  AND s.ck_a IS NOT NULL AND s.ck_b IS NOT NULL;

SELECT @n_kept_reviewer = COUNT(*) FROM #want WHERE cur_method = 'reviewer';
SELECT @n_add = COUNT(*) FROM #want
WHERE cur_key IS NULL OR (cur_method <> 'reviewer' AND (cur_decision <> 'match' OR cur_method <> want_method));

-- 6. The notebook's earlier automatic links whose evidence is gone, both profiles still live
DROP TABLE IF EXISTS #stale;
SELECT a.decision_key, a.sa, a.ia, a.sb, a.ib, a.rule_name, ca.client_key AS ck_a, cb.client_key AS ck_b
INTO #stale
FROM #act a
JOIN #srcid ca ON ca.source_system = a.sa AND ca.source_key = a.ka
JOIN #srcid cb ON cb.source_system = a.sb AND cb.source_key = a.kb
WHERE a.decision_method IN ('exact_rule', 'auto_score')
  AND a.decision = 'match'
  AND a.decided_by = @who
  AND ISNULL(a.rule_name, '') <> 'clarity_merge'
  AND NOT EXISTS (SELECT 1 FROM #want w
                  WHERE w.sa = a.sa AND dbo.fn_normalize_key(w.ia) = a.ka AND w.sb = a.sb AND dbo.fn_normalize_key(w.ib) = a.kb)
  AND EXISTS (SELECT 1 FROM dim_client c WHERE c.client_key = ca.client_key AND c.is_current = 1 AND ISNULL(c.is_deleted, 0) = 0)
  AND EXISTS (SELECT 1 FROM dim_client c WHERE c.client_key = cb.client_key AND c.is_current = 1 AND ISNULL(c.is_deleted, 0) = 0);

SELECT @n_stale = COUNT(*) FROM #stale;
SELECT @live = COUNT(DISTINCT client_key) FROM dim_client WHERE is_current = 1 AND ISNULL(is_deleted, 0) = 0;
SELECT @auto_active = COUNT(*) FROM #act WHERE decided_by = @who AND decision_method IN ('exact_rule', 'auto_score');

IF @allow_mass_change = 0 AND (
       (@n_add > 50 AND @n_add * 100.0 / NULLIF(@live, 0) > 5.0)
    OR (@n_stale > 10 AND @n_stale * 100.0 / NULLIF(@auto_active, 0) > 5.0))
BEGIN
    DECLARE @mass_msg NVARCHAR(2048) = CONCAT('Matching run ', @match_run_id, ' would add ', @n_add,
        ' automatic links (', @live, ' live profiles) and send ', @n_stale, ' of ', @auto_active,
        ' automatic links back to review. That is more than 5%. Check lh_silver.dbo.match_run and a sample of pairs.',
        ' If it is right, rerun with @allow_mass_change = 1.');
    THROW 50031, @mass_msg, 1;
END;

-- =====================================================================
BEGIN TRANSACTION;
-- =====================================================================

-- 7. Replace or add (one active decision per pair at all times)
UPDATE match_decision
SET is_active = 0, revoked_by = @who, revoked_datetime = @etl_time
WHERE decision_key IN (SELECT cur_key FROM #want
                       WHERE cur_key IS NOT NULL AND cur_method <> 'reviewer'
                         AND (cur_decision <> 'match' OR cur_method <> want_method));

INSERT INTO match_decision (
    source_system_a, source_id_a, source_system_b, source_id_b,
    decision, decision_method, match_probability, decided_by, decided_datetime, notes, is_active, rule_name
)
SELECT w.sa, w.ia, w.sb, w.ib, 'match', w.want_method, w.match_probability, @who, @etl_time,
       LEFT(CONCAT('Matching run ', @match_run_id, ': ', w.evidence), 500), 1,
       COALESCE(w.rule_name, CASE WHEN w.want_method = 'auto_score' THEN 'score' END)
FROM #want w
WHERE w.cur_key IS NULL
   OR (w.cur_method <> 'reviewer' AND (w.cur_decision <> 'match' OR w.cur_method <> w.want_method));

-- 8. Review queue
DROP TABLE IF EXISTS #q;
SELECT s.sa, s.ia, s.sb, s.ib, s.ck_a, s.ck_b, s.match_probability, s.match_weight,
       CASE WHEN EXISTS (SELECT 1 FROM match_decision a
                         WHERE a.is_active = 1 AND a.decision = 'unsure'
                           AND a.source_system_a = s.sa AND dbo.fn_normalize_key(a.source_id_a) = dbo.fn_normalize_key(s.ia)
                           AND a.source_system_b = s.sb AND dbo.fn_normalize_key(a.source_id_b) = dbo.fn_normalize_key(s.ib))
            THEN CAST('unsure' AS VARCHAR(20)) ELSE CAST('new' AS VARCHAR(20)) END AS queue_status,
       CAST(s.evidence AS VARCHAR(2000)) AS evidence, CAST(s.comparison_levels AS VARCHAR(4000)) AS comparison_levels
INTO #q
FROM #sug s
WHERE (s.suggested_action = 'review'
       OR EXISTS (SELECT 1 FROM match_decision u
                  WHERE u.is_active = 1 AND u.decision = 'unsure'
                    AND u.source_system_a = s.sa AND dbo.fn_normalize_key(u.source_id_a) = dbo.fn_normalize_key(s.ia)
                    AND u.source_system_b = s.sb AND dbo.fn_normalize_key(u.source_id_b) = dbo.fn_normalize_key(s.ib)))
  AND s.ck_a IS NOT NULL AND s.ck_b IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM match_decision a
                  WHERE a.is_active = 1 AND a.decision IN ('match', 'no_match')
                    AND a.source_system_a = s.sa AND dbo.fn_normalize_key(a.source_id_a) = dbo.fn_normalize_key(s.ia)
                    AND a.source_system_b = s.sb AND dbo.fn_normalize_key(a.source_id_b) = dbo.fn_normalize_key(s.ib))
  AND NOT EXISTS (SELECT 1 FROM person_client_link la
                  JOIN person_client_link lb ON lb.person_key = la.person_key AND lb.is_current = 1
                  WHERE la.is_current = 1 AND la.client_key = s.ck_a AND lb.client_key = s.ck_b
                    AND NOT EXISTS (SELECT 1 FROM match_decision u
                                    WHERE u.is_active = 1 AND u.decision = 'unsure'
                                      AND u.source_system_a = s.sa AND dbo.fn_normalize_key(u.source_id_a) = dbo.fn_normalize_key(s.ia)
                                      AND u.source_system_b = s.sb AND dbo.fn_normalize_key(u.source_id_b) = dbo.fn_normalize_key(s.ib)));

-- links whose evidence is gone: kept, and first in the queue
INSERT INTO #q (sa, ia, sb, ib, ck_a, ck_b, match_probability, match_weight, queue_status, evidence, comparison_levels)
SELECT t.sa, t.ia, t.sb, t.ib, t.ck_a, t.ck_b, s.match_probability, s.match_weight, 'evidence_changed',
       CONCAT('Linked automatically (', COALESCE(t.rule_name, 'rule'), '); that evidence no longer holds. ',
              COALESCE(CONCAT('Now: ', s.evidence), 'The model no longer scores this pair as a likely match.')),
       s.comparison_levels
FROM #stale t
LEFT JOIN #sug s
  ON s.sa = t.sa AND dbo.fn_normalize_key(s.ia) = dbo.fn_normalize_key(t.ia)
 AND s.sb = t.sb AND dbo.fn_normalize_key(s.ib) = dbo.fn_normalize_key(t.ib);

DROP TABLE IF EXISTS #first;
SELECT source_system_a, dbo.fn_normalize_key(source_id_a) AS ka, source_system_b, dbo.fn_normalize_key(source_id_b) AS kb,
       MIN(first_suggested_datetime) AS first_suggested_datetime
INTO #first
FROM match_review_queue
GROUP BY source_system_a, dbo.fn_normalize_key(source_id_a), source_system_b, dbo.fn_normalize_key(source_id_b);

DELETE FROM match_review_queue;

INSERT INTO match_review_queue (
    source_system_a, source_id_a, source_system_b, source_id_b, client_key_a, client_key_b,
    match_probability, match_weight, queue_status, evidence, comparison_levels,
    first_suggested_datetime, match_run_id, refreshed_datetime
)
SELECT q.sa, q.ia, q.sb, q.ib, q.ck_a, q.ck_b, q.match_probability, q.match_weight, q.queue_status,
       LEFT(q.evidence, 1000), LEFT(q.comparison_levels, 4000),
       COALESCE(f.first_suggested_datetime, @etl_time), @match_run_id, @etl_time
FROM #q q
LEFT JOIN #first f
  ON f.source_system_a = q.sa AND f.ka = dbo.fn_normalize_key(q.ia)
 AND f.source_system_b = q.sb AND f.kb = dbo.fn_normalize_key(q.ib);

SELECT @n_queue = COUNT(*) FROM match_review_queue;

-- 9. Suggestions naming a profile the warehouse does not have (the loaders run first, so this
--    should be empty; a non-zero count means the notebook read newer Silver than the loaders did)
INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, raw_payload, pipeline_run_id
)
SELECT @run_id, CASE WHEN s.ck_a IS NULL THEN s.sa ELSE s.sb END, 'match_suggestion', @match_run_id,
       'suggestion_profile_not_loaded',
       CASE WHEN s.ck_a IS NULL THEN s.ia ELSE s.ib END, NULL, @etl_time, NULL, @pipeline_run_id
FROM #sug s
WHERE s.ck_a IS NULL OR s.ck_b IS NULL;

-- 10. Audit rows
INSERT INTO stage_load_log (
    source_system, entity_name, source_file_name, source_file_date,
    bronze_load_datetime, silver_load_datetime, warehouse_load_datetime,
    row_count_source, row_count_loaded, load_status, error_message,
    run_id, pipeline_run_id, rows_changed, rows_deleted
)
SELECT 'all', 'match_decision_auto', NULL, NULL, NULL, @match_run_time, @etl_time,
       (SELECT COUNT(*) FROM #sug WHERE suggested_action IN ('link', 'auto_link')),
       (SELECT COUNT(*) FROM match_decision WHERE is_active = 1 AND decided_by = @who),
       'SUCCESS',
       CASE WHEN @n_kept_reviewer > 0 OR @n_unresolved > 0 OR @n_stale > 0
            THEN CONCAT(@n_kept_reviewer, ' pair(s) left to the reviewer decision; ',
                        @n_unresolved, ' pair(s) name a profile not loaded; ',
                        @n_stale, ' automatic link(s) lost their evidence and wait in the review queue') END,
       @run_id, @pipeline_run_id, @n_add, 0
UNION ALL
SELECT 'all', 'match_review_queue', NULL, NULL, NULL, @match_run_time, @etl_time,
       (SELECT COUNT(*) FROM #sug WHERE suggested_action = 'review'), @n_queue,
       'SUCCESS', NULL, @run_id, @pipeline_run_id,
       (SELECT COUNT(*) FROM match_review_queue WHERE first_suggested_datetime = @etl_time), NULL;

-- 11. Integrity checks
DECLARE @problems VARCHAR(4000) = NULL;
SELECT @problems = STRING_AGG(CAST(CONCAT(check_name, ': ', problem_rows) AS VARCHAR(MAX)), '; ')
FROM (
    SELECT 'pair with more than one active decision' AS check_name, COUNT(*) AS problem_rows
    FROM (SELECT source_system_a, dbo.fn_normalize_key(source_id_a) AS ka, source_system_b, dbo.fn_normalize_key(source_id_b) AS kb
          FROM match_decision WHERE is_active = 1
          GROUP BY source_system_a, dbo.fn_normalize_key(source_id_a), source_system_b, dbo.fn_normalize_key(source_id_b)
          HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'pair stored out of order', COUNT(*)
    FROM match_decision
    WHERE is_active = 1
      AND CONCAT(source_system_a, '|', dbo.fn_normalize_key(source_id_a)) > CONCAT(source_system_b, '|', dbo.fn_normalize_key(source_id_b))
    UNION ALL SELECT 'automatic no_match or unsure', COUNT(*)
    FROM match_decision WHERE is_active = 1 AND decision_method <> 'reviewer' AND decision <> 'match'
    UNION ALL SELECT 'queue pair repeated', COUNT(*)
    FROM (SELECT source_system_a, dbo.fn_normalize_key(source_id_a) AS ka, source_system_b, dbo.fn_normalize_key(source_id_b) AS kb
          FROM match_review_queue
          GROUP BY source_system_a, dbo.fn_normalize_key(source_id_a), source_system_b, dbo.fn_normalize_key(source_id_b)
          HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'queue pair already decided by a reviewer', COUNT(*)
    FROM match_review_queue q
    JOIN match_decision d
      ON d.is_active = 1 AND d.decision_method = 'reviewer' AND d.decision <> 'unsure'
     AND d.source_system_a = q.source_system_a AND dbo.fn_normalize_key(d.source_id_a) = dbo.fn_normalize_key(q.source_id_a)
     AND d.source_system_b = q.source_system_b AND dbo.fn_normalize_key(d.source_id_b) = dbo.fn_normalize_key(q.source_id_b)
    -- BEGIN declared-key checks (generated from tools/key_checks.py; do not edit by hand)
    UNION ALL SELECT 'FK_match_decision_source_a: source_system_a not in dim_source', (SELECT COUNT(*) FROM dbo.match_decision c WHERE c.source_system_a IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system_a))
    UNION ALL SELECT 'FK_match_decision_source_b: source_system_b not in dim_source', (SELECT COUNT(*) FROM dbo.match_decision c WHERE c.source_system_b IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system_b))
    UNION ALL SELECT 'PK_match_decision: duplicate decision_key', (SELECT COUNT(*) FROM (SELECT decision_key FROM dbo.match_decision WHERE decision_key IS NOT NULL GROUP BY decision_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_match_decision: empty decision_key', (SELECT COUNT(*) FROM dbo.match_decision WHERE decision_key IS NULL)
    UNION ALL SELECT 'FK_match_review_queue_source_a: source_system_a not in dim_source', (SELECT COUNT(*) FROM dbo.match_review_queue c WHERE c.source_system_a IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system_a))
    UNION ALL SELECT 'FK_match_review_queue_source_b: source_system_b not in dim_source', (SELECT COUNT(*) FROM dbo.match_review_queue c WHERE c.source_system_b IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system_b))
    UNION ALL SELECT 'PK_match_review_queue: duplicate source_system_a, source_id_a, source_system_b, source_id_b', (SELECT COUNT(*) FROM (SELECT source_system_a, source_id_a, source_system_b, source_id_b FROM dbo.match_review_queue WHERE source_system_a IS NOT NULL AND source_id_a IS NOT NULL AND source_system_b IS NOT NULL AND source_id_b IS NOT NULL GROUP BY source_system_a, source_id_a, source_system_b, source_id_b HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_match_review_queue: empty source_system_a, source_id_a, source_system_b, source_id_b', (SELECT COUNT(*) FROM dbo.match_review_queue WHERE source_system_a IS NULL OR source_id_a IS NULL OR source_system_b IS NULL OR source_id_b IS NULL)
    -- END declared-key checks
) c
WHERE problem_rows > 0;

IF @problems IS NOT NULL
BEGIN
    DECLARE @int_msg NVARCHAR(4000) = CONCAT('Match suggestion check failed, nothing was saved: ', @problems);
    THROW 50012, @int_msg, 1;
END;

-- 12. LOG RUN END (inside the transaction)
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'SUCCESS'
WHERE script_name = '06_apply_match_suggestions' AND run_start = @etl_time;

COMMIT TRANSACTION;

SELECT entity_name, row_count_source, row_count_loaded, rows_changed, rows_deleted, error_message
FROM stage_load_log
WHERE run_id = @run_id
ORDER BY entity_name;

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
    VALUES ('all', 'match_decision_auto', NULL, NULL, NULL, NULL, SYSDATETIME(),
            NULL, NULL, 'FAILED', ERROR_MESSAGE(), @run_id, @pipeline_run_id, 0, 0);

    UPDATE dbo.warehouse_run_log
    SET run_end = SYSDATETIME(), run_status = 'FAILED', error_message = ERROR_MESSAGE()
    WHERE script_name = '06_apply_match_suggestions' AND run_start = @etl_time;

    THROW;

END CATCH;

END;
GO


DROP PROCEDURE IF EXISTS dbo.usp_refresh_person_links;
GO

CREATE PROCEDURE dbo.usp_refresh_person_links
    @pipeline_run_id VARCHAR(100) = NULL
AS
BEGIN

DECLARE @etl_time DATETIME2(6) = SYSDATETIME();
DECLARE @run_id VARCHAR(60) = CONCAT('person_links_', CONVERT(VARCHAR(30), SYSDATETIME(), 126));

IF NOT EXISTS (SELECT 1 FROM dbo.warehouse_schema_version WHERE version = 'v8.15')
    THROW 50011, 'The warehouse schema is older than v8.15. Run migrations/0002_person_links.sql first.', 1;

BEGIN TRY

-- 0. LOG RUN START (outside the transaction)
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'ABANDONED'
WHERE script_name = '06_refresh_person_links' AND run_status = 'RUNNING';

INSERT INTO dbo.warehouse_run_log (
    script_name, script_version, run_start, run_status, run_by, change_summary, pipeline_run_id
)
VALUES (
    '06_refresh_person_links', 'v1.3', @etl_time, 'RUNNING', USER_NAME(),
    'Person links from match decisions; rpt.person and rpt.person_client rebuilt', @pipeline_run_id
);

DECLARE @pass INT = 1;
DECLARE @changed INT = 1;
DECLARE @iter INT = 0;
DECLARE @max_person BIGINT = 0;
DECLARE @closed_only INT = 0;
DECLARE @dec_active INT = 0;
DECLARE @dec_used INT = 0;
DECLARE @conflicts INT = 0;

-- =====================================================================
BEGIN TRANSACTION;
-- =====================================================================

-- 1. Current source profiles
DROP TABLE IF EXISTS #prof;
SELECT DISTINCT client_key
INTO #prof
FROM dim_client
WHERE is_current = 1;

-- 2. Source ID -> client_key (Salesforce Contact Id or Clarity Personal ID)
DROP TABLE IF EXISTS #srcid;
SELECT b.source_system,
       dbo.fn_normalize_key(COALESCE(b.source_personal_id, b.source_client_id)) AS source_key,
       MIN(b.client_key) AS client_key
INTO #srcid
FROM bridge_client_source b
WHERE b.is_active = 1
  AND COALESCE(b.source_personal_id, b.source_client_id) IS NOT NULL
GROUP BY b.source_system, dbo.fn_normalize_key(COALESCE(b.source_personal_id, b.source_client_id));

-- 3. Active decisions naming two different loaded profiles
DROP TABLE IF EXISTS #dec;
SELECT d.decision_key, d.decision, d.decision_method,
       CASE d.decision_method WHEN 'reviewer' THEN 1 WHEN 'exact_rule' THEN 2 ELSE 3 END AS method_rank,
       a.client_key AS ck_a, b.client_key AS ck_b
INTO #dec
FROM match_decision d
JOIN #srcid a ON a.source_system = d.source_system_a AND a.source_key = dbo.fn_normalize_key(d.source_id_a)
JOIN #srcid b ON b.source_system = d.source_system_b AND b.source_key = dbo.fn_normalize_key(d.source_id_b)
WHERE d.is_active = 1
  AND a.client_key <> b.client_key
  AND a.client_key IN (SELECT client_key FROM #prof)
  AND b.client_key IN (SELECT client_key FROM #prof);

SELECT @dec_active = COUNT(*) FROM match_decision WHERE is_active = 1;
SELECT @dec_used = COUNT(*) FROM #dec;

-- 4. "Match" edges, both directions
DROP TABLE IF EXISTS #edge;
SELECT x.a, x.b, x.method_rank, x.decision_key
INTO #edge
FROM (
    SELECT ck_a AS a, ck_b AS b, method_rank, decision_key FROM #dec WHERE decision = 'match'
    UNION ALL
    SELECT ck_b AS a, ck_a AS b, method_rank, decision_key FROM #dec WHERE decision = 'match'
) x;

-- 5. Group the profiles (connected groups through match edges). Pass 1 uses every edge. Where a
--    reviewer's no_match falls inside a group, pass 2 drops that group's automatic edges and regroups.
DROP TABLE IF EXISTS #lab;
SELECT client_key, client_key AS grp
INTO #lab
FROM #prof;

DROP TABLE IF EXISTS #conflict;
SELECT client_key AS grp
INTO #conflict
FROM #prof
WHERE 1 = 0;

WHILE @pass <= 2
BEGIN
    UPDATE #lab SET grp = client_key;
    SET @changed = 1;
    SET @iter = 0;

    WHILE @changed > 0
    BEGIN
        SET @iter = @iter + 1;
        IF @iter > 500
            THROW 50012, 'Person links: grouping did not settle after 500 rounds, nothing was saved.', 1;

        DROP TABLE IF EXISTS #nb;
        SELECT l.client_key, MIN(n.grp) AS new_grp
        INTO #nb
        FROM #lab l
        JOIN #edge e ON e.a = l.client_key
        JOIN #lab n ON n.client_key = e.b
        GROUP BY l.client_key;

        SELECT @changed = COUNT(*)
        FROM #nb x
        JOIN #lab l ON l.client_key = x.client_key
        WHERE x.new_grp < l.grp;

        UPDATE l
        SET l.grp = x.new_grp
        FROM #lab l
        JOIN #nb x ON x.client_key = l.client_key
        WHERE x.new_grp < l.grp;
    END;

    DELETE FROM #conflict;
    INSERT INTO #conflict (grp)
    SELECT DISTINCT la.grp
    FROM #dec d
    JOIN #lab la ON la.client_key = d.ck_a
    JOIN #lab lb ON lb.client_key = d.ck_b
    WHERE d.decision = 'no_match' AND la.grp = lb.grp;

    IF @pass = 1
        DELETE FROM #edge
        WHERE method_rank = 3
          AND EXISTS (SELECT 1 FROM #lab l JOIN #conflict c ON c.grp = l.grp WHERE l.client_key = #edge.a);

    SET @pass = @pass + 1;
END;

SELECT @conflicts = COUNT(*) FROM #conflict;

-- 6. Current links
DROP TABLE IF EXISTS #cur;
SELECT client_key, person_key, link_method, decision_key
INTO #cur
FROM person_client_link
WHERE is_current = 1;

-- 7. Groups that will get a person key. Profiles in a group whose decisions still contradict each
--    other keep their current person (or get their own if new) until a reviewer resolves it.
DROP TABLE IF EXISTS #member;
SELECT l.client_key,
       CASE WHEN c.grp IS NULL THEN 'G' WHEN cur.person_key IS NOT NULL THEN 'P' ELSE 'S' END AS grp_type,
       CASE WHEN c.grp IS NULL THEN l.grp WHEN cur.person_key IS NOT NULL THEN cur.person_key ELSE l.client_key END AS grp_id,
       CASE WHEN c.grp IS NULL THEN 0 ELSE 1 END AS is_frozen
INTO #member
FROM #lab l
LEFT JOIN #conflict c ON c.grp = l.grp
LEFT JOIN #cur cur ON cur.client_key = l.client_key;

DROP TABLE IF EXISTS #grp;
SELECT grp_type, grp_id, COUNT(*) AS group_size, MIN(client_key) AS min_ck
INTO #grp
FROM #member
GROUP BY grp_type, grp_id;

-- 8. Strongest decision behind each profile's link (reviewer > exact rule > automatic score)
DROP TABLE IF EXISTS #best;
SELECT x.client_key, x.method_rank, x.decision_key
INTO #best
FROM (
    SELECT m.client_key, e.method_rank, e.decision_key,
           ROW_NUMBER() OVER (PARTITION BY m.client_key ORDER BY e.method_rank, e.decision_key) AS rn
    FROM #member m
    JOIN #edge e ON e.a = m.client_key
    JOIN #member m2 ON m2.client_key = e.b AND m2.grp_type = m.grp_type AND m2.grp_id = m.grp_id
    WHERE m.is_frozen = 0
) x
WHERE x.rn = 1;

-- 9. Person keys: a key stays with the group holding most of its profiles (frozen groups first);
--    when groups join, the smallest key survives; groups left without a key get a new one.
DROP TABLE IF EXISTS #claim;
SELECT m.grp_type, m.grp_id, cur.person_key,
       MAX(m.is_frozen) AS frozen, COUNT(*) AS n, MIN(m.client_key) AS min_ck
INTO #claim
FROM #member m
JOIN #cur cur ON cur.client_key = m.client_key
GROUP BY m.grp_type, m.grp_id, cur.person_key;

DROP TABLE IF EXISTS #won;
SELECT x.grp_type, x.grp_id, x.person_key
INTO #won
FROM (
    SELECT grp_type, grp_id, person_key,
           ROW_NUMBER() OVER (PARTITION BY person_key ORDER BY frozen DESC, n DESC, min_ck) AS rn
    FROM #claim
) x
WHERE x.rn = 1;

DROP TABLE IF EXISTS #gkey;
SELECT x.grp_type, x.grp_id, x.person_key
INTO #gkey
FROM (
    SELECT grp_type, grp_id, person_key,
           ROW_NUMBER() OVER (PARTITION BY grp_type, grp_id ORDER BY person_key) AS rn
    FROM #won
) x
WHERE x.rn = 1;

SELECT @max_person = ISNULL(MAX(person_key), 0) FROM person_client_link;

INSERT INTO #gkey (grp_type, grp_id, person_key)
SELECT g.grp_type, g.grp_id, @max_person + ROW_NUMBER() OVER (ORDER BY g.min_ck)
FROM #grp g
WHERE NOT EXISTS (SELECT 1 FROM #gkey k WHERE k.grp_type = g.grp_type AND k.grp_id = g.grp_id);

-- 10. The link each profile should have
DROP TABLE IF EXISTS #want;
SELECT m.client_key, k.person_key,
       CASE WHEN m.is_frozen = 1 THEN COALESCE(cur.link_method, 'self')
            WHEN g.group_size = 1 THEN 'self'
            WHEN b.method_rank = 1 THEN 'reviewer'
            WHEN b.method_rank = 2 THEN 'exact_rule'
            ELSE 'auto_score' END AS link_method,
       CASE WHEN m.is_frozen = 1 THEN cur.decision_key
            WHEN g.group_size = 1 THEN NULL
            ELSE b.decision_key END AS decision_key
INTO #want
FROM #member m
JOIN #grp g ON g.grp_type = m.grp_type AND g.grp_id = m.grp_id
JOIN #gkey k ON k.grp_type = m.grp_type AND k.grp_id = m.grp_id
LEFT JOIN #best b ON b.client_key = m.client_key
LEFT JOIN #cur cur ON cur.client_key = m.client_key;

-- 11. Write only what changed: close the old link, open the new one at the same instant
DROP TABLE IF EXISTS #chg;
SELECT w.client_key
INTO #chg
FROM #want w
LEFT JOIN #cur c ON c.client_key = w.client_key
WHERE c.client_key IS NULL OR c.person_key <> w.person_key OR c.link_method <> w.link_method;

SELECT @closed_only = COUNT(*)
FROM #cur c
WHERE NOT EXISTS (SELECT 1 FROM #prof p WHERE p.client_key = c.client_key);

UPDATE person_client_link
SET valid_to = @etl_time, is_current = 0
WHERE is_current = 1
  AND (client_key IN (SELECT client_key FROM #chg)
       OR client_key NOT IN (SELECT client_key FROM #prof));

INSERT INTO person_client_link (
    client_key, person_key, link_method, decision_key, valid_from, valid_to, is_current, run_id, pipeline_run_id
)
SELECT w.client_key, w.person_key, w.link_method, w.decision_key, @etl_time, NULL, 1, @run_id, @pipeline_run_id
FROM #want w
WHERE w.client_key IN (SELECT client_key FROM #chg);

-- 12. Reporting tables, replaced in this same transaction
DELETE FROM rpt.person_client;
INSERT INTO rpt.person_client (
    client_key, person_key, source_system, salesforce_id, hmis_personal_id,
    link_method, linked_since, is_deleted, refreshed_datetime
)
SELECT l.client_key, l.person_key, c.source_system, c.salesforce_id, c.hmis_personal_id,
       l.link_method, l.valid_from, CASE WHEN ISNULL(c.is_deleted, 0) = 1 THEN 1 ELSE 0 END, @etl_time
FROM person_client_link l
JOIN dim_client c ON c.client_key = l.client_key AND c.is_current = 1
WHERE l.is_current = 1;

DELETE FROM rpt.person;
INSERT INTO rpt.person (
    person_key, profile_count, active_profile_count, salesforce_profiles, hmis_profiles,
    is_active, has_unconfirmed_link, salesforce_id, hmis_personal_id,
    first_name, middle_name, last_name, date_of_birth, dob_data_quality,
    sex, gender, race, ethnicity, veteran_status, military_branch, discharge_status,
    email, home_phone, mobile_phone, address_line1, city, state, postal_code, county,
    first_seen_date, last_updated, refreshed_datetime
)
SELECT person_key, profile_count, active_profile_count, salesforce_profiles, hmis_profiles,
       is_active, has_unconfirmed_link, salesforce_id, hmis_personal_id,
       first_name, middle_name, last_name, date_of_birth, dob_data_quality,
       sex, gender, race, ethnicity, veteran_status, military_branch, discharge_status,
       email, home_phone, mobile_phone, address_line1, city, state, postal_code, county,
       first_seen_date, last_updated, @etl_time
FROM v_person_golden;

-- 13. Contradicting decisions, for a reviewer
INSERT INTO stage_load_reject (
    run_id, source_system, entity_name, source_record_id, reject_reason,
    referenced_client_id, referenced_program, detected_datetime, raw_payload, pipeline_run_id
)
SELECT @run_id, 'all', 'person_link', CAST(d.decision_key AS VARCHAR(50)), 'conflicting_match_decisions',
       CONCAT(md.source_system_a, ':', md.source_id_a, ' / ', md.source_system_b, ':', md.source_id_b),
       NULL, @etl_time,
       'A reviewer said these are not the same person, but reviewer or exact-rule matches join them through other profiles. Their links were left as they were. Revoke or change one of the decisions.',
       @pipeline_run_id
FROM #dec d
JOIN match_decision md ON md.decision_key = d.decision_key
JOIN #lab la ON la.client_key = d.ck_a
JOIN #lab lb ON lb.client_key = d.ck_b
WHERE d.decision = 'no_match' AND la.grp = lb.grp;

-- 14. Audit rows
INSERT INTO stage_load_log (
    source_system, entity_name, source_file_name, source_file_date,
    bronze_load_datetime, silver_load_datetime, warehouse_load_datetime,
    row_count_source, row_count_loaded, load_status, error_message,
    run_id, pipeline_run_id, rows_changed, rows_deleted
)
SELECT 'all', 'person_client_link', NULL, NULL, NULL, NULL, @etl_time,
       (SELECT COUNT(*) FROM #prof),
       (SELECT COUNT(*) FROM person_client_link WHERE is_current = 1),
       'SUCCESS', NULL, @run_id, @pipeline_run_id,
       (SELECT COUNT(*) FROM #chg), @closed_only
UNION ALL
SELECT 'all', 'match_decision', NULL, NULL, NULL, NULL, @etl_time,
       @dec_active, @dec_used, 'SUCCESS',
       CASE WHEN @conflicts > 0 THEN CONCAT(@conflicts, ' group(s) with contradicting decisions, see stage_load_reject') END,
       @run_id, @pipeline_run_id, @conflicts, NULL
UNION ALL
SELECT 'all', 'rpt.person', NULL, NULL, NULL, NULL, @etl_time,
       (SELECT COUNT(DISTINCT person_key) FROM person_client_link WHERE is_current = 1),
       (SELECT COUNT(*) FROM rpt.person),
       'SUCCESS', NULL, @run_id, @pipeline_run_id, NULL, NULL;

-- 15. Integrity checks: any problem rolls everything back
DECLARE @problems VARCHAR(4000) = NULL;
SELECT @problems = STRING_AGG(CAST(CONCAT(check_name, ': ', problem_rows) AS VARCHAR(MAX)), '; ')
FROM (
    SELECT 'profile with no current person' AS check_name, COUNT(*) AS problem_rows
    FROM #prof p
    WHERE NOT EXISTS (SELECT 1 FROM person_client_link l WHERE l.client_key = p.client_key AND l.is_current = 1)
    UNION ALL SELECT 'profile with more than one current person', COUNT(*)
    FROM (SELECT client_key FROM person_client_link WHERE is_current = 1 GROUP BY client_key HAVING COUNT(*) > 1) x
    UNION ALL SELECT 'current link to a profile not in dim_client', COUNT(*)
    FROM person_client_link l
    WHERE l.is_current = 1 AND NOT EXISTS (SELECT 1 FROM #prof p WHERE p.client_key = l.client_key)
    UNION ALL SELECT 'overlapping link history', COUNT(*)
    FROM person_client_link a
    JOIN person_client_link b ON a.client_key = b.client_key AND a.link_key < b.link_key
    WHERE a.valid_from < ISNULL(b.valid_to, CAST('9999-12-31' AS DATETIME2(6)))
      AND b.valid_from < ISNULL(a.valid_to, CAST('9999-12-31' AS DATETIME2(6)))
    UNION ALL SELECT 'reviewer no_match pair joined by this run', COUNT(*)
    FROM #dec d
    JOIN #want wa ON wa.client_key = d.ck_a
    JOIN #want wb ON wb.client_key = d.ck_b
    JOIN #member ma ON ma.client_key = d.ck_a
    WHERE d.decision = 'no_match' AND wa.person_key = wb.person_key AND ma.is_frozen = 0
    UNION ALL SELECT 'rpt.person differs from current persons', ABS(
        (SELECT COUNT(*) FROM rpt.person) - (SELECT COUNT(DISTINCT person_key) FROM person_client_link WHERE is_current = 1))
    UNION ALL SELECT 'rpt.person_client differs from current links', ABS(
        (SELECT COUNT(*) FROM rpt.person_client) - (SELECT COUNT(*) FROM person_client_link WHERE is_current = 1))
    -- BEGIN declared-key checks (generated from tools/key_checks.py; do not edit by hand)
    UNION ALL SELECT 'FK_person_client_link_decision: decision_key not in match_decision', (SELECT COUNT(*) FROM dbo.person_client_link c WHERE c.decision_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.match_decision p WHERE p.decision_key = c.decision_key))
    UNION ALL SELECT 'PK_person_client_link: duplicate link_key', (SELECT COUNT(*) FROM (SELECT link_key FROM dbo.person_client_link WHERE link_key IS NOT NULL GROUP BY link_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_person_client_link: empty link_key', (SELECT COUNT(*) FROM dbo.person_client_link WHERE link_key IS NULL)
    UNION ALL SELECT 'UQ_person_client_link_start: duplicate client_key, valid_from', (SELECT COUNT(*) FROM (SELECT client_key, valid_from FROM dbo.person_client_link WHERE client_key IS NOT NULL AND valid_from IS NOT NULL GROUP BY client_key, valid_from HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_rpt_person: duplicate person_key', (SELECT COUNT(*) FROM (SELECT person_key FROM rpt.person WHERE person_key IS NOT NULL GROUP BY person_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_rpt_person: empty person_key', (SELECT COUNT(*) FROM rpt.person WHERE person_key IS NULL)
    UNION ALL SELECT 'FK_rpt_person_client_person: person_key not in rpt.person', (SELECT COUNT(*) FROM rpt.person_client c WHERE c.person_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM rpt.person p WHERE p.person_key = c.person_key))
    UNION ALL SELECT 'FK_rpt_person_client_source: source_system not in dim_source', (SELECT COUNT(*) FROM rpt.person_client c WHERE c.source_system IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.dim_source p WHERE p.source_system = c.source_system))
    UNION ALL SELECT 'PK_rpt_person_client: duplicate client_key', (SELECT COUNT(*) FROM (SELECT client_key FROM rpt.person_client WHERE client_key IS NOT NULL GROUP BY client_key HAVING COUNT(*) > 1) d)
    UNION ALL SELECT 'PK_rpt_person_client: empty client_key', (SELECT COUNT(*) FROM rpt.person_client WHERE client_key IS NULL)
    -- END declared-key checks
) c
WHERE problem_rows > 0;

IF @problems IS NOT NULL
BEGIN
    DECLARE @int_msg NVARCHAR(4000) = CONCAT('Person link check failed, nothing was saved: ', @problems);
    THROW 50012, @int_msg, 1;
END;

-- 16. LOG RUN END (inside the transaction)
UPDATE dbo.warehouse_run_log
SET run_end = SYSDATETIME(), run_status = 'SUCCESS'
WHERE script_name = '06_refresh_person_links' AND run_start = @etl_time;

COMMIT TRANSACTION;

-- 17. What changed this run
SELECT entity_name, row_count_source, row_count_loaded, rows_changed, rows_deleted, error_message
FROM stage_load_log
WHERE run_id = @run_id
ORDER BY entity_name;

SELECT reject_reason, COUNT(*) AS cnt
FROM stage_load_reject
WHERE run_id = @run_id
GROUP BY reject_reason;

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
    VALUES ('all', 'person_client_link', NULL, NULL, NULL, NULL, SYSDATETIME(),
            NULL, NULL, 'FAILED', ERROR_MESSAGE(), @run_id, @pipeline_run_id, 0, 0);

    UPDATE dbo.warehouse_run_log
    SET run_end = SYSDATETIME(), run_status = 'FAILED', error_message = ERROR_MESSAGE()
    WHERE script_name = '06_refresh_person_links' AND run_start = @etl_time;

    THROW;

END CATCH;

END;
GO

-- Record a reviewer's decision (IDs are the Salesforce Contact Id and the Clarity Personal ID):
-- EXEC dbo.usp_record_match_decision @source_system_a = 'sf_cis', @source_id_a = '003XXXXXXXXXXXXXXX',
--      @source_system_b = 'hmis_lac', @source_id_b = '<Personal ID>', @decision = 'match',
--      @decided_by = 'reviewer@example.org', @notes = 'Same DOB, phone and DD-214 dates';
-- Change it: record the new decision for the same pair (the old one is kept, marked inactive).
-- Withdraw a match without saying they are different people: @decision = 'unsure'.
-- Apply decisions now instead of waiting for the weekly run:
-- EXEC dbo.usp_refresh_person_links;
