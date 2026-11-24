"""Run the whole pipeline end to end on synthetic data, on one machine.

    python run_demo.py

Two weeks of fake source exports go through the same code that runs in Microsoft Fabric:
  Bronze   nb_bronze_land_files             Clarity CSV exports -> Bronze tables (+ history)
  Silver   nb_silver_hmis_clarity_conform   Clarity Bronze -> conformed Silver
           nb_silver_sf_cis_conform         Salesforce Contact pull -> conformed Silver
  Match    nb_match_persons                 exact rules + Splink model -> suggested pairs
  Gold     warehouse/*.sql                  T-SQL warehouse: migrations, persistent loaders,
                                            match decisions, person links, integrity checks
The notebooks run on local Spark (parquet in place of Delta). The T-SQL runs on DuckDB through a small
T-SQL interpreter (local/tsql_sim.py), statement by statement, exactly as written for Fabric.

Outputs (in ./output): demo_report.md, warehouse.duckdb (query it with DuckDB), demo.log (everything
the notebooks and procedures printed).
"""
import contextlib, io, json, os, shutil, sys, time
from collections import defaultdict
from itertools import combinations

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "local"))
sys.path.insert(0, os.path.join(ROOT, "data"))

OUT = os.path.join(ROOT, "output")
WORK = os.path.join(OUT, "work")              # lakehouse stand-in: Files/ folders and table folders
WH = os.path.join(WORK, "tables")
BRONZE_FILES = os.path.join(OUT, "bronze_exports")
MODEL_DIR = os.path.join(WORK, "Files", "matching")
SQL = os.path.join(ROOT, "warehouse")
P6 = os.path.join(SQL, "06_proc_person_links.sql")
REVIEWER = "demo.reviewer@example.org"

LOG = None
REPORT = []


def say(text=""):
    print(text, flush=True)
    LOG.write(text + "\n")
    LOG.flush()


def note(text=""):
    say(text)
    REPORT.append(text)


@contextlib.contextmanager
def quiet(label):
    """Send everything a step prints to demo.log, and time it."""
    t0 = time.time()
    LOG.write(f"\n===== {label}\n")
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            yield
    except Exception:
        LOG.write(buf.getvalue())
        say(f"   FAILED in {label}; the last lines it printed:")
        for line in buf.getvalue().splitlines()[-15:]:
            say("   | " + line)
        raise
    LOG.write(buf.getvalue())
    LOG.write(f"===== {label}: {time.time() - t0:.0f}s\n")


def rows(con, sql):
    return con.execute(sql).fetchall()


def one(con, sql):
    return con.execute(sql).fetchone()[0]


# ---------------------------------------------------------------------------- the steps
def land_week(spark, label):
    from notebooks import land_clarity, conform_clarity, conform_salesforce
    src = os.path.join(BRONZE_FILES, label, "hmis_lac")
    dst = os.path.join(WORK, "Files", "landing", "hmis_lac")
    shutil.copytree(src, dst, dirs_exist_ok=True)
    with quiet(f"{label}: nb_bronze_land_files"):
        land_clarity(spark, WORK)
    with quiet(f"{label}: nb_silver_hmis_clarity_conform"):
        conform_clarity(spark)
    with quiet(f"{label}: nb_silver_sf_cis_conform"):
        conform_salesforce(spark, os.path.join(BRONZE_FILES, label, "sf_cis", "contact.csv"),
                           os.path.join(BRONZE_FILES, "zips.csv"))
    q = spark.read.table("silver_etl_quality_log")
    latest = q.groupBy("source_system").agg({"run_id": "max"}).withColumnRenamed("max(run_id)", "run_id")
    stats = {r["status"]: r["count"] for r in q.join(latest, ["source_system", "run_id"]).groupBy("status").count().collect()}
    n_cli = spark.read.table("silver_conf_hmis_clarity_client").count()
    n_sf = spark.read.table("silver_conf_sf_cis_client").count()
    n_enr = (spark.read.table("silver_conf_hmis_clarity_enrollment").count()
             + spark.read.table("silver_conf_sf_cis_enrollment").count())
    note(f"   Bronze -> Silver: {n_cli} Clarity clients, {n_sf} Salesforce contacts, {n_enr} enrollments; "
         f"quality gates {stats.get('pass', 0)} pass, {stats.get('warn', 0)} warn, {stats.get('stop', 0)} stop")


def match(spark, label):
    from notebooks import match_persons
    with quiet(f"{label}: nb_match_persons"):
        g = match_persons(spark, WH, MODEL_DIR)
    info = g["RUN_INFO"]
    note(f"   Matching: {info.get('profiles', 0)} profiles, {info.get('pairs_scored', 0)} pairs scored, "
         f"{info.get('exact_links', 0)} exact-rule links, {info.get('review_pairs', 0)} pairs for review")


def build_warehouse():
    from sim_build import new_warehouse, run_script, attach_silver
    with quiet("build warehouse"):
        con, _ = new_warehouse(os.path.join(SQL, "01_ddl_v8.13.sql"))
        con.execute("""INSERT INTO warehouse_schema_version (version, applied_date, applied_by, summary)
                       VALUES ('v8.13', TIMESTAMP '2000-01-01', 'demo', 'baseline schema (01_ddl)')""")
        attach_silver(con, WH)
        for script in ("migrations/0000_verify_fabric_capabilities", "migrations/0001_persistent_loads",
                       "migrations/0002_person_links", "02b_seed_etl_source_config", "migrations/0003_matching",
                       "migrations/0004_assessments", "migrations/0005_keys_and_cleanup",
                       "migrations/0006_key_checks_and_dates"):
            run_script(con, os.path.join(SQL, f"{script}.sql"), log=print)
    versions = [r[0] for r in rows(con, "SELECT version FROM warehouse_schema_version ORDER BY applied_date, version")]
    note(f"   Warehouse built from 01_ddl + migrations 0000-0006: schema {versions[0]} -> {versions[-1]} "
         f"({len(versions)} versions, {one(con, 'SELECT COUNT(*) FROM sim_constraints')} declared keys)")
    return con


def load(con, label, run_id):
    from sim_build import attach_silver
    from tsql_sim import call_proc
    attach_silver(con, WH)
    with quiet(f"{label}: warehouse load ({run_id})"):
        call_proc(con, os.path.join(SQL, "03a_proc_load_sf_cis.sql"), "usp_load_sf_cis", log=print, pipeline_run_id=run_id)
        call_proc(con, os.path.join(SQL, "03b_load_hmis_clarity.sql"), "usp_load_hmis_clarity", log=print,
                  src="hmis_lac", pipeline_run_id=run_id)
        call_proc(con, P6, "usp_apply_match_suggestions", log=print, pipeline_run_id=run_id)
    steps = rows(con, f"SELECT script_name, run_status FROM warehouse_run_log WHERE pipeline_run_id = '{run_id}'")
    assert steps and all(s == "SUCCESS" for _, s in steps), steps
    return {e: (c, d) for e, c, d in rows(con, f"""
        SELECT entity_name, SUM(COALESCE(rows_changed, 0)), SUM(COALESCE(rows_deleted, 0))
        FROM stage_load_log WHERE pipeline_run_id = '{run_id}' GROUP BY entity_name""")}


def refresh_links(con, run_id):
    from tsql_sim import call_proc
    with quiet(f"usp_refresh_person_links ({run_id})"):
        call_proc(con, P6, "usp_refresh_person_links", log=print, pipeline_run_id=run_id)
    return {e: (c, d) for e, c, d in rows(con, f"""
        SELECT entity_name, SUM(COALESCE(rows_changed, 0)), SUM(COALESCE(rows_deleted, 0))
        FROM stage_load_log WHERE pipeline_run_id = '{run_id}' AND entity_name = 'person_client_link'
        GROUP BY entity_name""")}


def review_queue(con, truth):
    """A reviewer works through the queue. Here the answer key stands in for the reviewer's judgment;
    every decision goes through dbo.usp_record_match_decision exactly as the review notebook's buttons do."""
    from tsql_sim import call_proc
    queue = rows(con, "SELECT source_system_a, source_id_a, source_system_b, source_id_b, queue_status FROM match_review_queue")
    same = diff = 0
    with quiet("reviewer decisions"):
        for sa, ia, sb, ib, _ in queue:
            is_same = truth.get(f"{sa}:{ia}") is not None and truth.get(f"{sa}:{ia}") == truth.get(f"{sb}:{ib}")
            call_proc(con, P6, "usp_record_match_decision", log=print, source_system_a=sa, source_id_a=ia,
                      source_system_b=sb, source_id_b=ib, decision="match" if is_same else "no_match",
                      decided_by=REVIEWER, notes="demo reviewer")
            same += is_same
            diff += not is_same
    return len(queue), same, diff


def changed(stats, *entities):
    return sum(stats.get(e, (0, 0))[0] for e in entities), sum(stats.get(e, (0, 0))[1] for e in entities)


def score_links(con, truth):
    """Pairwise precision and recall of the person links against the answer key."""
    linked = defaultdict(set)
    for s, i, p in rows(con, "SELECT source_system, COALESCE(salesforce_id, hmis_personal_id), person_key FROM rpt.person_client"):
        linked[p].add(f"{s}:{i}")
    real = defaultdict(set)
    for prof, person in truth.items():
        real[person].add(prof)
    loaded = {p for g in linked.values() for p in g}
    pairs = lambda groups: {frozenset(x) for g in groups for x in combinations(sorted(g & loaded), 2)}
    got, want = pairs(linked.values()), pairs(real.values())
    tp = len(got & want)
    return tp, len(got), len(want)


EXPORTED_VIEWS = ["v_dq_corrections", "v_person_golden", "v_dim_client_current", "v_fact_enrollment_primary",
                  "v_health_runs", "v_health_table_changes", "v_health_people"]


def write_duckdb(con, path):
    """Copy the warehouse to a DuckDB file. Tables keep their names (rpt.person becomes rpt_person); the main
    views are saved as tables of the same name, since their definitions rely on the simulator's T-SQL macros."""
    if os.path.exists(path):
        os.remove(path)
    con.execute(f"ATTACH '{path}' AS export_db")
    for schema, table in rows(con, """SELECT table_schema, table_name FROM information_schema.tables
                                      WHERE table_type = 'BASE TABLE' AND table_catalog = current_database()
                                        AND table_schema IN ('main', 'rpt') AND table_name NOT LIKE 'sim_%'"""):
        target = table if schema == "main" else f"{schema}_{table}"
        con.execute(f'CREATE TABLE export_db."{target}" AS SELECT * FROM {schema}."{table}"')
    for view in EXPORTED_VIEWS:
        con.execute(f'CREATE TABLE export_db."{view}" AS SELECT * FROM {view}')
    con.execute("DETACH export_db")


def preflight():
    """Fail early, in plain words, on the two things a fresh machine is most likely to be missing."""
    if not shutil.which("java") and not os.environ.get("JAVA_HOME"):
        sys.exit("PySpark needs Java 17 or 21, and no 'java' was found. Install a JDK (for example Temurin 21) "
                 "and make sure 'java -version' works, then rerun.")
    try:
        import pyspark, splink, duckdb, sqlglot  # noqa: F401
    except ImportError as e:
        sys.exit(f"Missing package ({e.name}). Run: pip install -r requirements.txt")


# ---------------------------------------------------------------------------- main
def main():
    global LOG
    preflight()
    shutil.rmtree(OUT, ignore_errors=True)
    os.makedirs(WH)
    LOG = open(os.path.join(OUT, "demo.log"), "w")
    t0 = time.time()

    note("# Pipeline demo run\n")
    say("[1/7] Generating two weeks of synthetic exports")
    from generate import generate
    g = generate(BRONZE_FILES)
    key = json.load(open(os.path.join(BRONZE_FILES, "truth.json")))
    truth, planted = key["truth"], key["planted"]
    note(f"   {g['people']} fictional people: {g['clarity_clients']} Clarity client rows and "
         f"{g['salesforce_contacts']} Salesforce contacts in week 1 ({g['week1']}), a second export on {g['week2']}.")
    note(f"   Planted: {len(planted['clarity_duplicates'])} Clarity duplicates, {len(planted['cross_system'])} people in both "
         f"systems, {len(planted['salesforce_duplicates'])} Salesforce duplicates, {len(planted['look_alikes'])} look-alike pairs.")

    from notebooks import spark_session
    say("\n[2/7] Week 1: Bronze and Silver (local Spark)")
    with quiet("start Spark"):
        spark = spark_session(WH)
    note("\n## Week 1")
    land_week(spark, "week1")

    say("\n[3/7] Week 1: matching (nb_match_persons)")
    match(spark, "week1")

    say("\n[4/7] Building the warehouse from the T-SQL scripts")
    con = build_warehouse()

    say("\n[5/7] Week 1: warehouse load, review, person links")
    load(con, "week1", "week1")
    note(f"   Loaded: {one(con, 'SELECT COUNT(*) FROM dim_client')} client versions, "
         f"{one(con, 'SELECT COUNT(*) FROM fact_enrollment')} enrollments, {one(con, 'SELECT COUNT(*) FROM fact_service')} services, "
         f"{one(con, 'SELECT COUNT(*) FROM fact_case_note')} case notes, {one(con, 'SELECT COUNT(*) FROM fact_bed_night')} bed nights")
    exact = one(con, "SELECT COUNT(*) FROM match_decision WHERE is_active = 1 AND decision_method = 'exact_rule'")
    n_q, n_same, n_diff = review_queue(con, truth)
    note(f"   Decisions: {exact} exact-rule links recorded; the reviewer worked {n_q} queued pairs "
         f"({n_same} same person, {n_diff} not the same)")
    refresh_links(con, "week1")
    tp, got, want = score_links(con, truth)
    note(f"   Person links: {one(con, 'SELECT COUNT(*) FROM rpt.person_client')} profiles -> "
         f"{one(con, 'SELECT COUNT(*) FROM rpt.person')} people; {tp} of {got} linked pairs correct, "
         f"{tp} of {want} true pairs found")

    say("\n   Rerunning week 1 on the same Silver (the loads must be idempotent)")
    s1b = load(con, "week1", "week1-rerun")
    l1b = refresh_links(con, "week1-rerun")
    total = sum(c + d for c, d in s1b.values()) - sum(s1b.get(e, (0, 0))[0] for e in ("match_review_queue",))
    total += sum(c + d for c, d in l1b.values())
    note(f"   Rerun: {total} rows changed or flagged across every table (expected 0)")

    say("\n[6/7] Week 2: a new export with edits, exits, new people, deletions and a Clarity merge")
    note("\n## Week 2")
    land_week(spark, "week2")
    match(spark, "week2")
    s2 = load(con, "week2", "week2")
    n_q2, n_same2, n_diff2 = review_queue(con, truth)
    refresh_links(con, "week2")
    versions = one(con, "SELECT COUNT(*) FROM dim_client WHERE valid_from > CAST('1900-01-01' AS TIMESTAMP)")
    note(f"   Client history: {versions} new client versions (old versions kept, valid_to closed)")
    enr_c, enr_d = changed(s2, "fact_enrollment")
    note(f"   Enrollments: {enr_c} rows inserted or updated, {enr_d} flagged deleted")
    _, svc_d = changed(s2, "fact_service")
    _, con_d = changed(s2, "dim_client")
    note(f"   Removed at the source and flagged (never physically deleted): {svc_d} services, {con_d} clients")
    merges = rows(con, "SELECT notes FROM match_decision WHERE rule_name = 'clarity_merge' AND is_active = 1")
    for (m,) in merges:
        note(f"   Source-system merge detected and kept as a link: {m}")
    held = rows(con, """SELECT problem, how_to_fix, COUNT(*) FROM v_dq_corrections
                        GROUP BY problem, how_to_fix ORDER BY COUNT(*) DESC""")
    note(f"   Held back and listed for staff to correct in the source (v_dq_corrections): "
         f"{sum(r[2] for r in held)} records")
    for issue, fix, n in held:
        note(f"     {n} x {issue}")
        note(f"         fix: {fix}")
    note(f"   The reviewer worked {n_q2} new queued pairs ({n_same2} same person, {n_diff2} not the same)")
    tp, got, want = score_links(con, truth)
    note(f"   Person links: {one(con, 'SELECT COUNT(*) FROM rpt.person_client')} profiles -> "
         f"{one(con, 'SELECT COUNT(*) FROM rpt.person')} people; {tp} of {got} linked pairs correct, "
         f"{tp} of {want} true pairs found")

    say("\n[7/7] Integrity: every declared key checked across the whole warehouse")
    from sim_build import run_script
    with quiet("checks/check_declared_keys.sql"):
        ses = run_script(con, os.path.join(SQL, "checks", "check_declared_keys.sql"), log=print)
    result = [r for _, rs in ses.results for r in rs]
    bad = [r for r in result if r[1] and r[1] > 0]
    note(f"\n## Integrity\n   check_declared_keys.sql: {len(result)} key checks, {len(bad)} with problems")

    write_duckdb(con, os.path.join(OUT, "warehouse.duckdb"))
    open(os.path.join(OUT, "demo_report.md"), "w").write("\n".join(REPORT) + "\n")
    say(f"\nDone in {time.time() - t0:.0f}s. See output/demo_report.md, output/warehouse.duckdb and output/demo.log")
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())
