# Deploying to Microsoft Fabric

What you need: a Fabric workspace with two lakehouses (`lh_bronze`, `lh_silver`) and a warehouse. Run each SQL script in the warehouse's query editor. Import each notebook from `notebooks/` (a Fabric notebook export is a `.py` file with `CELL` markers; mark cell 0 as the parameter cell).

## Fresh build

Add a restore point first (Warehouse > Settings > Restore points).

| # | Run | You should see |
|---|---|---|
| 1 | `00_clean_warehouse.sql` with `@dry_run = 1`, then `0` (only when rebuilding) | what it would drop and keep, then "Dropped N objects" |
| 2 | `01_ddl_v8.13.sql` | run log `01_ddl` SUCCESS |
| 3 | `migrations/0000_verify_fabric_capabilities.sql` | eight rows, all PASS |
| 4 | `migrations/0001`, `0002` | last result of each: every `ok` = 1 |
| 5 | `02_seed_reference_data_v2.3.sql`, `02b_seed_etl_source_config.sql` | calendar 2000-2040; the source list |
| 6 | run each Silver notebook once (0003 reads `lh_silver.dbo.etl_quality_log`) | |
| 7 | `migrations/0003` to `0006` | every `ok` = 1 |
| 8 | `04_security_v1.10.sql` | roles created; policy present but OFF |
| 9 | install `03a`, `03b`, `05`, `06`, `07` | "Commands completed" |
| 10 | `SELECT version FROM dbo.warehouse_schema_version` | v8.13 through v8.19 |

Then run one load by hand (step 3 to 7 of the pipeline below), run it again, and check that every `rows_changed` and `rows_deleted` in `stage_load_log` is 0 the second time. Finish with `checks/check_declared_keys.sql`: every `problem_rows` should be 0.

After this, never drop the warehouse for a normal change: schema changes are new files in `migrations/`, and new procedure versions are installed over the old ones.

## Weekly pipeline

See the orchestration table in [architecture.md](architecture.md#weekly-orchestration-fabric-data-pipeline). Settings that matter:

- **Concurrency 1.** Two runs at once would write the same tables and Fabric would cancel one.
- **Timeouts:** 1 hour per notebook activity, 30 minutes per stored-procedure activity.
- **Parameters passed down:** `@pipeline_run_id = @pipeline().RunId` to every procedure, and `@min_silver_load_datetime = @pipeline().TriggerTime` to the loaders, so a loader refuses Silver the SQL endpoint hasn't synced yet.

## Procedure parameters

| Procedure | Parameter | Meaning |
|---|---|---|
| loaders | `@src` | Clarity source, e.g. `'hmis_lac'` (Clarity loader only) |
| loaders | `@min_silver_load_datetime` | stops (50004) if Silver is older than the pipeline run |
| loaders | `@allow_mass_delete` | 1 only after confirming that over 5% of records really left the source |
| `usp_apply_match_suggestions` | `@min_match_run_datetime`, `@allow_mass_change` | the same two safeguards for the matching output |
| `usp_record_match_decision` | `@decision` | `match`, `no_match`, or `unsure` (withdraws a match without saying the profiles are different people) |
| `usp_admin_clear_warehouse_data` | `@confirm` | must be exactly `'DELETE ALL LOADED DATA'` |
| `usp_purge_audit_history` | `@keep_months`, `@dry_run` | keeps at least 12 months; dry run by default |

## Roles

```sql
ALTER ROLE warehouse_reader    ADD MEMBER [analyst@example.org];
ALTER ROLE match_reviewer      ADD MEMBER [reviewer@example.org];
ALTER ROLE pipeline_monitor    ADD MEMBER [data-team@example.org];
ALTER ROLE dq_corrector        ADD MEMBER [qa-staff@example.org];
ALTER ROLE apricot_integration ADD MEMBER [integration-service-principal];
```

Workspace Admin, Member and Contributor roles bypass warehouse permissions, so give these users the warehouse through item sharing. Turn row-level security on only after an active `program_id = -1` admin grant exists (`04_security` adds one for whoever runs it):

```sql
ALTER SECURITY POLICY dbo.ClientProgramAccessPolicy WITH (STATE = ON);
```

## Error codes

| Code | Meaning |
|---|---|
| 50001 | no Silver rows for the source |
| 50002 | `@src` isn't a Clarity source in `dim_source` |
| 50003 | the source is blocked in `etl_source_config` |
| 50004 | Silver or the matching run is older than this run; wait for the endpoint sync |
| 50005 | the source has no `etl_source_config` row |
| 50006 | more than 5% of a source would be flagged deleted |
| 50011 | schema too old for this procedure; run the missing migration |
| 50012 | an integrity or declared-key check failed; nothing was saved |
| 50013 | admin clear refused: confirmation phrase missing |
| 50020 | `01_ddl` refused: the warehouse holds data |
| 50030 | invalid match decision |
| 50031 | the matching run would change more than 5% of links |
| 50051, 50052 | migration 0005 stopped before changing anything: a key it would declare doesn't hold |
| 50060 | purge refused: `@keep_months` under 12 |
| 50061, 50062 | migration 0006 stopped before changing anything; run `checks/check_declared_keys.sql` |
