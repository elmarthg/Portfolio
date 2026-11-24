"""Run the Fabric notebooks unchanged on a local Spark session.

Only the storage calls are swapped, so the notebooks' logic, data-quality gates and outputs are the ones
that run in Fabric:
  * lh_bronze.dbo.<t> -> bronze_<t> and lh_silver.dbo.<t> -> silver_<t> (tables in a local warehouse folder)
  * Delta -> parquet (local Spark has no Delta Lake), so Delta-only write options are replaced by an
    equivalent read-filter-rewrite
  * Fabric's notebookutils is not needed: no Key Vault key is set (the demo has no SSNs to hash)
Every substitution asserts that the text it replaces is still in the notebook, so a notebook change
that breaks the local run fails loudly instead of running something different.
"""
import csv, glob, json, os, re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NB_DIR = os.path.join(ROOT, "notebooks")
_spark = {}


def spark_session(wh):
    import sys
    from pyspark.sql import SparkSession
    if wh in _spark:
        return _spark[wh]
    # Spark starts its Python workers with whatever 'python3' is first on the PATH unless told otherwise;
    # a different version from this interpreter (e.g. a venv run without activating it) fails the first UDF.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    s = (SparkSession.builder.master("local[2]").appName("fabric-demo")
         .config("spark.ui.enabled", "false").config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.shuffle.partitions", "2").config("spark.default.parallelism", "2")
         .config("spark.sql.warehouse.dir", wh).config("spark.sql.session.timeZone", "UTC")
         .config("spark.driver.memory", "2g").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    _spark[wh] = s
    return s


def _source(name):
    return open(os.path.join(NB_DIR, f"{name}.py"), encoding="utf-8").read()


def _replace(src, old, new):
    assert old in src, f"notebook text not found (the local runner needs updating): {old[:70]!r}"
    return src.replace(old, new)


def _run(spark, code, name):
    exec(compile(code, name, "exec"), {"spark": spark, "__name__": "__main__"})


def _register(spark, wh):
    """Make every table folder in the warehouse visible to this Spark session's catalog."""
    have = {t.name for t in spark.catalog.listTables()}
    for d in sorted(glob.glob(os.path.join(wh, "*"))):
        name = os.path.basename(d)
        if not re.match(r"^(bronze|silver)_", name) or not glob.glob(os.path.join(d, "*.parquet")):
            continue
        if name in have:
            spark.catalog.refreshTable(name)
        else:
            spark.catalog.createTable(name, path=d, source="parquet")


_KEEP_OTHER_SOURCES = """    if table_exists(TGT[key]):
        keep = spark.read.table(TGT[key]).filter(F.col("source_system") != SOURCE_SYSTEM)
        keep.unionByName(out, allowMissingColumns=True).localCheckpoint().write.mode("overwrite").saveAsTable(TGT[key])
    else:
        out.write.mode("overwrite").saveAsTable(TGT[key])
"""


def land_clarity(spark, workspace, source="hmis_lac"):
    """nb_bronze_land_files: Files/landing/<source>/<entity>/*.csv -> bronze_<source>_<entity> (+ _history)."""
    nb = _source("nb_bronze_land_files")
    nb = nb.replace("lh_bronze.dbo.", "bronze_")
    a = nb.index("def _append_history(df, history, names):")
    b = nb.index("for entity, files, out in staged:", a)
    nb = nb[:a] + '''def _append_history(df, history, names):
    if table_exists(history):
        keep = spark.read.table(history).filter(~((F.col("bronze_batch_date") == F.lit(batch_date).cast("date"))
                                                  & F.col("source_file_name").isin(names)))
        keep.unionByName(df, allowMissingColumns=True).localCheckpoint().write.mode("overwrite").saveAsTable(history)
    else:
        df.write.mode("overwrite").saveAsTable(history)

''' + nb[b:]
    nb = _replace(nb, '.write.format("delta").mode("overwrite").option("overwriteSchema", "true")',
                  '.write.mode("overwrite")')
    nb = _replace(nb, 'SOURCE_SYSTEM = "hmis_lac"', f'SOURCE_SYSTEM = "{source}"')
    nb = _replace(nb, 'LAKEHOUSE_ROOT = ""', f'LAKEHOUSE_ROOT = {os.path.abspath(workspace) + "/"!r}')
    _run(spark, nb, "nb_bronze_land_files")


def conform_clarity(spark, source="hmis_lac"):
    """nb_silver_hmis_clarity_conform: Bronze Clarity tables -> silver_conf_hmis_clarity_*."""
    nb = _source("nb_silver_hmis_clarity_conform")
    nb = nb.replace("lh_bronze.dbo.", "bronze_").replace("lh_silver.dbo.", "silver_")
    a = nb.index('    w = out.write.format("delta")')
    b = nb.index("    n = spark.read.table(TGT[key])")
    nb = nb[:a] + _KEEP_OTHER_SOURCES + nb[b:]
    nb = _replace(nb, '.write.format("delta").mode("append")', '.write.mode("append")')
    cfg = json.dumps({"source_system": source, "is_enabled": 1, "block_reason": None})
    nb = _replace(nb, 'SOURCE_CONFIG_JSON = ""', f"SOURCE_CONFIG_JSON = {cfg!r}")
    nb = _replace(nb, 'SOURCE_SYSTEM = "hmis_lac"', f'SOURCE_SYSTEM = "{source}"')
    _run(spark, nb, "nb_silver_hmis_clarity_conform")


def conform_salesforce(spark, contact_csv, zips_csv):
    """nb_silver_sf_cis_conform: the Salesforce Contact pull -> silver_conf_sf_cis_*.
    In Fabric a Dataflow writes the pull to lh_bronze.dbo.sf_cis_contact; here the CSV stands in for it."""
    with open(contact_csv, newline="", encoding="utf-8") as f:
        cols = next(csv.reader(f))
    schema = ", ".join(f"`{c}` string" for c in cols)
    (spark.read.option("header", True).option("multiLine", True).schema(schema).csv(contact_csv)
          .write.mode("overwrite").saveAsTable("bronze_sf_cis_contact"))
    (spark.read.option("header", True).schema("zip string, city string, state_id string, county_name string")
          .csv(zips_csv).write.mode("overwrite").saveAsTable("bronze_ref_uszips"))
    nb = _source("nb_silver_sf_cis_conform")
    nb = nb.replace("lh_bronze.dbo.", "bronze_").replace("lh_silver.dbo.", "silver_")
    a = nb.index("def _append_history(df, batch_date):")
    b = nb.index("# ---------------- CELL 2b", a)
    nb = nb[:a] + '''def _append_history(df, batch_date):
    if table_exists(BRONZE_HISTORY):
        keep = spark.read.table(BRONZE_HISTORY).filter(F.col("bronze_batch_date") != F.lit(batch_date).cast("date"))
        keep.unionByName(df, allowMissingColumns=True).localCheckpoint().write.mode("overwrite").saveAsTable(BRONZE_HISTORY)
    else:
        df.write.mode("overwrite").saveAsTable(BRONZE_HISTORY)


''' + nb[b:]
    a = nb.index('    w = out.write.format("delta")')
    b = nb.index("    n = spark.read.table(TGT[key])")
    nb = nb[:a] + _KEEP_OTHER_SOURCES + nb[b:]
    nb = nb.replace('.write.format("delta")', ".write")
    cfg = json.dumps({"source_system": "sf_cis", "is_enabled": 1})
    nb = _replace(nb, 'SOURCE_CONFIG_JSON = ""', f"SOURCE_CONFIG_JSON = {cfg!r}")
    _run(spark, nb, "nb_silver_sf_cis_conform")


def match_persons(spark, wh, model_dir, **params):
    """nb_match_persons: Silver client profiles -> silver_match_suggestion and silver_match_run.
    Returns the notebook's globals (RUN_INFO holds the run's counts)."""
    _register(spark, wh)
    nb = _source("nb_match_persons")
    nb = nb.replace("lh_silver.dbo.", "silver_").replace('.format("delta")', '.format("parquet")')
    nb = _replace(nb, 'spark.sql(f"DELETE FROM {SUGGESTION_TABLE} WHERE match_run_id NOT IN ({ids})")',
                  '(spark.read.table(SUGGESTION_TABLE).filter(F.col("match_run_id").isin(keep))'
                  '.localCheckpoint().write.mode("overwrite").saveAsTable(SUGGESTION_TABLE))')
    params = {"MODEL_DIR": model_dir, **params}
    for k, v in params.items():
        pat = re.compile(rf"^{k} = .*$", re.M)
        assert pat.search(nb), k
        nb = pat.sub(f"{k} = {v!r}", nb, count=1)
    g = {"spark": spark, "__name__": "__main__"}
    exec(compile(nb, "nb_match_persons", "exec"), g)
    _register(spark, wh)
    return g
