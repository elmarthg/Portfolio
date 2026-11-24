-- checks/check_view_security.sql (optional, informational)
--
-- Confirms in YOUR warehouse what Microsoft's community answers say: Fabric has no ownership
-- chaining, so a view does not let a user read a table they are denied. The design does not depend
-- on the answer (rpt holds stored tables, not views), but it tells us whether reporting views could
-- ever replace those tables.
--
-- Needs migrations/0002 and 04_security v1.8, plus a colleague (or a test account) who is in the
-- warehouse_reader role and is NOT a workspace Admin, Member or Contributor (those roles bypass
-- warehouse permissions, so the test would prove nothing).

-- PART A: you, as the warehouse owner
DROP VIEW IF EXISTS rpt.zz_chain_check;
DROP TABLE IF EXISTS dbo.zz_chain_secret;
CREATE TABLE dbo.zz_chain_secret (id INT, secret VARCHAR(30));
INSERT INTO dbo.zz_chain_secret VALUES (1, 'report users must not see this');
DENY SELECT ON dbo.zz_chain_secret TO warehouse_reader;
GO

CREATE VIEW rpt.zz_chain_check AS
SELECT id, secret FROM dbo.zz_chain_secret;
GO

-- PART B: the test user runs these two queries (SQL connection string, SSMS or the query editor)
-- SELECT * FROM rpt.zz_chain_check;    -- an error here means views cannot hide tables (expected)
--                                      -- one row here means views CAN hide tables in your tenant
-- SELECT * FROM dbo.zz_chain_secret;   -- must always fail; if it returns a row, stop and tell me:
--                                      -- the DENY is not being applied to that user

-- PART C: you, afterwards
-- DROP VIEW IF EXISTS rpt.zz_chain_check;
-- DROP TABLE IF EXISTS dbo.zz_chain_secret;
