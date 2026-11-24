# Design decisions

Why the pipeline is built the way it is. Each entry gives the decision, the reason, and where it lives in the code. The numbers are referenced from code comments.

## Loading and history

**1. Medallion layers with one tool per job.** Bronze keeps each source export as it arrived, with an append-only history table. Silver conforms every source to one shape with PySpark notebooks. The warehouse is T-SQL in Fabric Warehouse, because its consumers are SQL reports and Power BI. Each layer can be rerun on its own from the layer below it.

**2. A persistent warehouse, not a weekly rebuild.** The loaders insert new rows, update only rows whose values changed, keep client history as SCD type 2, and never reissue a key. A full rebuild would hand out new `client_key` and `enrollment_key` values every week, breaking anything downstream that stored them, and it would throw away history that Silver (which holds only the latest export) cannot recreate. Each loader is one transaction with integrity checks before `COMMIT`. Running a loader twice on the same Silver changes nothing, and the demo proves it every run. *(03a, 03b, migration 0001)*

**3. Soft deletes.** A record missing from the latest export gets `is_deleted = 1` and the date it disappeared; it is restored if it comes back. A run that would flag more than 5% of a source stops (error 50006) unless someone confirms the removal is real, because a truncated export looks exactly like a mass deletion. *(03a, 03b)*

**4. Schema changes are migrations.** `01_ddl` is the baseline and refuses to run on a loaded warehouse (50020). Every later change is a numbered file in `warehouse/migrations/`. Each one checks before it changes anything, is safe to rerun, and records its version in `warehouse_schema_version`. Loaders refuse to run on a schema older than the one they were written for (50011).

**5. Declared keys are checked, because Fabric doesn't enforce them.** Fabric Warehouse accepts primary, unique and foreign keys only as `NOT ENFORCED`, and its optimizer may rely on them, so a key that stops holding can return wrong query results. Every procedure checks every key on the tables it writes before `COMMIT` (error 50012 names the key), and a migration declares a key only after checking it holds. All of those check lists are generated from the same list of declared keys, and `tools/key_checks.py` confirms they haven't drifted. *(migrations 0005 and 0006, `checks/check_declared_keys.sql`)*

## People across systems

**6. Link profiles, never merge them.** Each source profile stays exactly as its system sent it (`dim_client`). A `person_key` groups the profiles of one person through `person_client_link`, which has dated history, so any past report can be reproduced and a wrong link is undone by recording one decision. *(migration 0002, `06_proc_person_links.sql`)*

**7. Survivorship: the newest complete value wins.** For each field of the merged profile (`rpt.person`), the most recently updated non-blank value wins. Address fields come together from one profile, so a street never pairs with another profile's ZIP code. A date of birth marked approximate or unknown loses to a full one even if it's newer, and profiles flagged deleted come last. *(`v_person_golden`, migration 0002)*

**8. Matching puts precision first.** Exact rules link without review (same SSN token plus date of birth or name; or same name, full date of birth and a shared phone or email). A probabilistic Splink model scores everything else, and pairs above the threshold go to a reviewer. Linking on score alone is off by default. A wrong link merges two people's records in downstream systems, while a missed link only means a reviewer sees the pair later. An identifier shared by more than three profiles, such as a shelter's front-desk phone, never counts as evidence. *(`nb_match_persons`)*

**9. The model's m-probabilities come from written priors, then from reviewers.** Unsupervised training (EM) on this kind of data learned "same household" instead of "same person": family members share phones, addresses and last names, so it scored parents and children, and twins, as matches. Phone, email and address are also one combined comparison, so a shared household isn't counted three times. Retraining is never automatic: someone runs it on purpose (`TRAIN_MODEL = True`), checks the new match weights, and switches `MODEL_FILE` to it. Until then every run scores with the pinned model. Once reviewers have confirmed enough pairs, m is learned from their decisions instead of the priors. *(`nb_match_persons`)*

**10. An automatic link is never broken automatically.** When the evidence behind an exact-rule link disappears (someone's phone number changed), the link stays and the pair goes to the top of the review queue as `evidence_changed`. Breaking it automatically would unlink and relink people in reports every time contact details changed. A run that would send more than 5% of automatic links back to review stops (50031). *(`usp_apply_match_suggestions`)*

**11. Source-system merges become links.** When an HMIS administrator merges two duplicate clients, one Personal ID disappears and its enrollments reappear under the other. The loader recognizes that pattern and records an exact-rule "same person" decision, so the old profile's history stays attached to the person instead of looking deleted. A reviewer's decision on the same pair is never overridden. The demo plants a duplicate no matcher could find and merges it in week 2. *(03b, section 3b)*

**12. SSNs: restricted storage, no stored matching key.** The full SSN lives in one restricted table (`dim_client_identifier`) that only the integration role can read; `dim_client` holds a keyed hash and the last four digits, both masked. For matching, the notebook turns each valid SSN into a token with a random key generated for that run only, and writes out only IDs and the evidence "same SSN". The SSNs, the tokens and the key are discarded when the run ends. Placeholders and invalid numbers (`123-45-6789`, `000-00-0000`, area 000, 666 or 9xx) are never hashed or used as evidence. The demo data contains no full SSNs at all. *(01_ddl, 04_security, `nb_silver_hmis_clarity_conform`, `nb_match_persons`)*

## Data quality

**13. Assessments are not enrollments.** In the case-management system, people are assessed first and enrolled only once they qualify. A contact with no enrollment date is a client with an assessment, not an enrollment with a missing date, so it loads into `fact_assessment` instead of being rejected. A contact whose status says "Enrolled" but has no enrollment date is a real data-entry error and is listed for correction. *(`nb_silver_sf_cis_conform`, 03a, migration 0004)*

**14. A plain-language correction list.** Records a load has to hold back, such as an enrollment with no client or a date typed with the wrong year, are listed in `v_dq_corrections` for the staff who can fix them at the source. Each item says what's wrong, how to fix it, the record ID, the program, and how long it has been open. An item disappears by itself once it is fixed and the next run loads it. The client profile itself is never held back. *(migrations 0004 and 0006)*

**15. Gates before every Silver write.** Schema drift, row-count drops (over 10% stops), duplicate keys, orphan records, mixed export batches and stale exports (over 8 days old) all stop the run before Silver is written. Each gate has a one-run override for when a change is real, and every check is logged to `etl_quality_log`. *(all Silver notebooks, `nb_bronze_land_files`)*

**16. Dates outside the calendar are held back.** A date outside `dim_date` (2000 to 2040) would create a date key with no calendar row, breaking a declared key and silently hiding the row from date filters. The record is held back and listed for correction, and the warehouse keeps its last good version. *(03a, 03b, migration 0006)*

**17. Race and ethnicity conform to HUD's FY2024 categories.** Free-text and older picklist values are mapped onto HUD's combined race-and-ethnicity list; the source's raw value stays in `race_detail`. *(`nb_silver_sf_cis_conform`)*

## Reporting and security

**18. Reporting tables, not views.** Fabric Warehouse has no ownership chaining, so a view can't hide the columns or tables behind it from a user who is denied them, and views also push Direct Lake semantic models into DirectQuery. The `rpt` schema holds stored tables rebuilt inside the person-link transaction, so reports never see links and merged profiles from different runs. *(migration 0002; `checks/check_view_security.sql` tests the permission behavior)*

**19. Least-privilege roles.** Report readers are denied the SSN table, sensitive columns (gender identity, sexual orientation, HIV and domestic-violence fields) and the reject log. Separate roles cover the match reviewer, the pipeline monitor, the data-entry corrector and the integration account. Row-level security by program is defined but created OFF, so it is enabled deliberately after an admin grant exists. Recreating a view drops its direct grants, so every migration that recreates one gives them back, and a lint checks that it does. *(04_security, `tests/lint_view_grants.py`)*

**20. Multi-grant credit is full credit.** An enrollment in a program funded by several grants counts in full for every grant (`allocation_share` stays NULL). Totals across grants therefore add up to more than the number of people served; reports that need an unduplicated count use distinct people. *(`bridge_enrollment_grant`)*

**21. Recovery uses warehouse restore points.** A restore point before every migration or manual fix, plus Fabric's automatic ones. The admin clear procedure (`05`) is for test warehouses only and refuses to run without a typed confirmation phrase.

## Open questions

**A. Re-enrollments in Salesforce.** A Salesforce contact holds only its current enrollment, so the enrollment ID includes its start date. If staff re-enroll a returning client by overwriting the date instead of correcting a typo, the earlier period is flagged deleted and the new one inserted, so the history of the first stay is lost from current reporting. The fix belongs in the source system (an enrollment object, not fields on the contact). Until then, the warehouse's deleted-flag history keeps the earlier period recoverable.
