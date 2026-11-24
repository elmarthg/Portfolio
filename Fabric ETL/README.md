# Fabric medallion ETL: one view of each client across Salesforce and HMIS

A Microsoft Fabric data platform for a multi-program social-services agency. It brings client, enrollment and service records from **Salesforce** (case management) and **Clarity HMIS** (one instance per county) into one warehouse. It finds the people who appear in more than one system, and keeps full history, so reports and funder outcome measures count each person once.

This repository is a **demo copy with synthetic data**. Every person, organization, program and ID in it is invented, and the data contains no Social Security numbers. The code is the real pipeline: `run_demo.py` runs the actual Fabric notebooks on local Spark and the actual T-SQL warehouse scripts on DuckDB, over two weeks of generated exports, in about two minutes.

```
pip install -r requirements.txt      # Python 3.11 to 3.13 and Java 17 or 21 (for PySpark)
python run_demo.py
```

## What a run shows

```
[1/7] Generating two weeks of synthetic exports
   970 fictional people: 637 Clarity client rows and 389 Salesforce contacts in week 1, a second export a week later.
   Planted: 12 Clarity duplicates, 40 people in both systems, 5 Salesforce duplicates, 4 look-alike pairs.
[2/7] Week 1: Bronze and Silver (local Spark)
   Bronze -> Silver: 613 Clarity clients, 389 Salesforce contacts, 1158 enrollments; quality gates 58 pass, 6 warn, 0 stop
[3/7] Week 1: matching (nb_match_persons)
   Matching: 1002 profiles, 964 pairs scored, 5 exact-rule links, 51 pairs for review
[4/7] Building the warehouse from the T-SQL scripts
   Warehouse built from 01_ddl + migrations 0000-0006: schema v8.13 -> v8.19 (7 versions, 211 declared keys)
[5/7] Week 1: warehouse load, review, person links
   Loaded: 1002 client versions, 1145 enrollments, 2855 services, 1900 case notes, 63443 bed nights
   Decisions: 5 exact-rule links recorded; the reviewer worked 51 queued pairs (50 same person, 1 not the same)
   Person links: 1002 profiles -> 947 people; 55 of 55 linked pairs correct, 55 of 58 true pairs found
   Rerun: 0 rows changed or flagged across every table (expected 0)
[6/7] Week 2: a new export with edits, exits, new people, deletions and a Clarity merge
   Client history: 12 new client versions (old versions kept, valid_to closed)
   Removed at the source and flagged (never physically deleted): 4 services, 4 clients
   Source-system merge detected and kept as a link: enrollments of Personal ID 499013 moved to 400059 ...
   Held back and listed for staff to correct in the source (v_dq_corrections): 14 records
     1 x The Project Start Date is outside 2000 to 2040, the years the warehouse covers. It is probably a typing error
   Person links: 1028 profiles -> 972 people; 56 of 56 linked pairs correct, 56 of 58 true pairs found
[7/7] Integrity: every declared key checked across the whole warehouse
   check_declared_keys.sql: 255 key checks, 0 with problems
```

The data is the same on every run (the same people, records and planted cases), dated relative to the day you run it. The quality-gate warnings are planted too: data-entry problems such as contacts marked enrolled with no enrollment date, which the pipeline is supposed to catch and report.

The generator plants known cases with an answer key, so the run can score itself: duplicates, people in both systems, look-alikes who must not be linked, and edits and deletions in week 2. The reviewer step reads that answer key in place of a person. The two missed pairs are a one-letter typo in a short surname with no ZIP code to back it up. That's the trade-off the matcher makes on purpose: a missed link waits for a reviewer, but a wrong link merges two people's records ([decision 8](docs/design_decisions.md)).

Afterwards, `output/warehouse.duckdb` holds the finished warehouse:

```python
import duckdb
con = duckdb.connect("output/warehouse.duckdb", read_only=True)
con.sql("SELECT person_key, profile_count, first_name, last_name, salesforce_id, hmis_personal_id FROM rpt_person WHERE profile_count > 1 LIMIT 10").show()
con.sql("SELECT client_key, last_name, postal_code, valid_from, valid_to, is_current FROM dim_client WHERE client_key IN (SELECT client_key FROM dim_client GROUP BY 1 HAVING COUNT(*) > 1) ORDER BY client_key, valid_from").show()
con.sql("SELECT entity_name, rows_changed, rows_deleted FROM stage_load_log WHERE pipeline_run_id = 'week2'").show()
con.sql("SELECT system_name, record_type, record_id, problem, how_to_fix FROM v_dq_corrections").show()
```

## What's worth a look

- **Persistent, idempotent loads.** Each loader is one T-SQL transaction. It inserts new rows, updates only rows that changed (`NOT EXISTS (... INTERSECT ...)` comparisons, so NULLs compare correctly), and flags rows that left the source instead of deleting them. Client history is kept as SCD type 2 with half-open validity ranges. The demo reruns week 1 and proves nothing changes. ([03b_load_hmis_clarity.sql](warehouse/03b_load_hmis_clarity.sql))
- **Record linkage with a human in the loop.** Deterministic rules plus a Splink (Fellegi-Sunter) model. Its m-probabilities come from written priors rather than unsupervised training, because EM learned "same household" instead of "same person". Scored pairs go to a review queue, and every decision is a row with who made it and when. ([nb_match_persons.py](notebooks/nb_match_persons.py), [06_proc_person_links.sql](warehouse/06_proc_person_links.sql))
- **Link, never merge.** Source profiles stay as each system sent them. A `person_key` groups them through a link table with dated history, so any past report can be reproduced and a wrong link is undone with one decision. A merge done inside an HMIS is detected and kept as a link.
- **Keys Fabric doesn't enforce, enforced anyway.** Fabric Warehouse constraints are `NOT ENFORCED`, but the optimizer may trust them. Every procedure checks each declared key on the tables it writes before `COMMIT` (211 keys in all), using check lists generated from the schema itself. ([tools/key_checks.py](tools/key_checks.py))
- **Data quality that ends with a person fixing the source.** Gates stop a Silver write on schema drift, row-count drops, duplicates, orphans or stale exports. Records the warehouse can't load land in a plain-language correction list with how to fix each one. ([v_dq_corrections in 0006](warehouse/migrations/0006_key_checks_and_dates.sql))
- **Privacy by design.** The full SSN lives in one restricted table. Matching compares SSNs as per-run tokens whose key is thrown away, and the suggestions it writes hold only IDs and evidence. Sensitive columns are masked and denied to report roles, and row-level security by program is ready to enable. ([04_security_v1.10.sql](warehouse/04_security_v1.10.sql))
- **Schema changes as migrations.** Numbered, rerunnable, check-first migrations, each recording its version; loaders refuse a schema older than the one they were written for.
- **Testing T-SQL without Fabric.** [local/tsql_sim.py](local/tsql_sim.py) is a small T-SQL interpreter over DuckDB: batches, variables, control flow, `TRY`/`CATCH`, transactions and the constraint catalog. Static lints cover the Fabric behaviors it can't simulate, each one learned on a real Fabric run. ([tests/](tests))

The reasoning behind each choice is in [docs/design_decisions.md](docs/design_decisions.md).

## Layout

| Folder | What's in it |
|---|---|
| [`notebooks/`](notebooks) | Fabric PySpark notebooks: Bronze landing, Silver conform (Salesforce, Clarity), matching, the reviewer screens, SQL endpoint sync |
| [`warehouse/`](warehouse) | T-SQL: baseline DDL, seeds, loaders, person links, security, admin and purge procedures |
| [`warehouse/migrations/`](warehouse/migrations) | schema v8.13 to v8.19, in order |
| [`warehouse/checks/`](warehouse/checks) | read-only checks: every declared key, view security, source field lengths |
| [`data/`](data) | the synthetic data generator (Clarity Looker exports and Salesforce contacts) |
| [`local/`](local) | the local runners: notebooks on local Spark, T-SQL on DuckDB |
| [`tests/`](tests), [`tools/`](tools) | Fabric lints and the key-check generator; `python tests/run_checks.py` runs them all |
| [`docs/`](docs) | [architecture](docs/architecture.md), [design decisions](docs/design_decisions.md), [deploying to Fabric](docs/deploy_to_fabric.md) |

## Stack

Microsoft Fabric (lakehouses, Data Pipeline, Warehouse), PySpark, Delta Lake, T-SQL, Splink, Power BI. Locally: PySpark, DuckDB, sqlglot.

## Limits of the local run

- Tested on Linux with Java 21 and Python 3.11 and 3.13. On Windows, run it in WSL: local Spark on Windows needs extra Hadoop setup.

- Delta becomes parquet, and Fabric's notebook utilities aren't used; [local/notebooks.py](local/notebooks.py) lists every substitution. The notebooks target Fabric Runtime 2.0 (Spark 4); the demo is tested on PySpark 3.5.
- `04_security` (roles, denies, masking, row-level security) and `00_clean_warehouse` (system catalog) only run in Fabric. The simulator doesn't run them.
- The simulator covers the T-SQL these scripts use, not all of T-SQL.
- The Salesforce field names and the federal program names (SSVF, HVRP, GPD) are kept because the code parses them; the organization, its programs, staff, grants and addresses are fictional.
