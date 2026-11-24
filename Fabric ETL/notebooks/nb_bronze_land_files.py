# nb_bronze_land_files v1.1
# Lands Clarity export files from Files/landing/<source>/<entity>/ into Bronze current and history tables.

# ---------------- CELL 0: PARAMETERS (mark as the parameter cell) ----------------
SOURCE_SYSTEM = "hmis_lac"
LAKEHOUSE_ROOT = ""   # leave blank when lh_bronze is this notebook's default lakehouse
ALLOW_PARTIAL = ""    # "yes" lands a batch that is missing some of the four core files


# ---------------- CELL 1: CONFIG ----------------
LANDING_DIR = f"{LAKEHOUSE_ROOT}Files/landing/{SOURCE_SYSTEM}"
PROCESSED_DIR = f"{LAKEHOUSE_ROOT}Files/processed/{SOURCE_SYSTEM}"
BRONZE_TABLE = "lh_bronze.dbo.{src}_{entity}"
ENTITIES = ["organization", "program", "client", "enrollment", "service", "case_note",
            "household_member", "bed_unit", "funding", "roi"]
CORE_ENTITIES = ["organization", "program", "client", "enrollment"]


# ---------------- CELL 2: HELPERS ----------------
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
PACIFIC = ZoneInfo("America/Los_Angeles")

try:
    from notebookutils import mssparkutils
    fs = mssparkutils.fs
except ImportError:
    fs = None

def norm(name: str) -> str:
    n = re.sub(r"[^0-9a-z]+", "_", name.strip().lower())
    return re.sub(r"_+", "_", n).strip("_")

def list_files(path):
    """[(name, full_path, modified_epoch_ms)] for files directly in path."""
    if fs is None:
        import os
        if not os.path.isdir(path):
            return []
        return [(f, os.path.join(path, f), int(os.path.getmtime(os.path.join(path, f)) * 1000))
                for f in sorted(os.listdir(path)) if os.path.isfile(os.path.join(path, f))]
    try:
        items = fs.ls(path)
    except Exception:
        return []
    return [(i.name, i.path, getattr(i, "modifyTime", None)) for i in items if not i.isDir]

def move(src, dst_dir):
    if fs is None:
        import os, shutil
        os.makedirs(dst_dir, exist_ok=True)
        shutil.move(src, os.path.join(dst_dir, os.path.basename(src)))
    else:
        fs.mkdirs(dst_dir)
        fs.mv(src, f"{dst_dir}/{src.rstrip('/').split('/')[-1]}")

def export_date(file_name, modified_ms):
    """Export date from the file name if it has one, else the upload time (Pacific)."""
    m = re.search(r"(20\d\d)[-_]?([01]\d)[-_]?([0-3]\d)", file_name)
    if m:
        try:
            return datetime(int(m[1]), int(m[2]), int(m[3])).date(), "file name"
        except ValueError:
            pass
    if modified_ms:
        return datetime.fromtimestamp(modified_ms / 1000, tz=timezone.utc).astimezone(PACIFIC).date(), "upload time"
    return None, "unknown"

def read_file(path, file_name):
    """Read one export as all-text columns with normalized, de-duplicated names."""
    if file_name.lower().endswith((".xlsx", ".xls")):
        import pandas as pd
        local = path if fs is None else "/lakehouse/default/Files/" + path.split("/Files/", 1)[-1]
        pdf = pd.read_excel(local, dtype=str).fillna("")
        df = spark.createDataFrame(pdf.astype(str))
    else:
        import csv, io
        from pyspark.sql.types import StructType, StructField, StringType
        text = (fs.head(path, 1 << 16) if fs is not None
                else open(path, encoding="utf-8", errors="replace").read(1 << 16))
        raw_headers = next(csv.reader(io.StringIO(text.lstrip("﻿"))))
        schema = StructType([StructField(f"_c{i}", StringType()) for i in range(len(raw_headers))])
        df = (spark.read.option("header", True).option("multiLine", True).option("escape", '"')
                   .schema(schema).csv(path))
        df = df.toDF(*raw_headers)
    seen, cols = {}, []
    for c in df.columns:
        n = norm(c) or "col"
        if n in seen:
            seen[n] += 1
            n = f"{n}_{seen[n]}"
        else:
            seen[n] = 0
        cols.append(n)
    df = df.toDF(*cols)
    return df.select([F.col(c).cast("string").alias(c) for c in df.columns])

def table_exists(name):
    try:
        spark.read.table(name).limit(0).collect()
        return True
    except Exception:
        return False


# ---------------- CELL 3: LAND ----------------
batch_ts = datetime.now(timezone.utc)
batch_id = batch_ts.astimezone(PACIFIC).strftime("%Y%m%d_%H%M%S")
batch_date = batch_ts.astimezone(PACIFIC).date()
summary = {}

staged = []
for entity in ENTITIES:
    files = [f for f in list_files(f"{LANDING_DIR}/{entity}")
             if f[0].lower().endswith((".csv", ".xlsx", ".xls"))]
    if not files:
        continue
    parts = []
    for name, path, mtime in files:
        fdate, how = export_date(name, mtime)
        df = read_file(path, name)
        n = df.count()
        if n == 0:
            raise ValueError(f"{entity}/{name} has no data rows. Remove it or replace it, then rerun. "
                             f"Nothing was landed.")
        parts.append(df.withColumn("source_file_name", F.lit(name))
                       .withColumn("source_file_date", F.lit(fdate).cast("date"))
                       .withColumn("file_date_from", F.lit(how)))
        print(f"{entity}: {name}  {n:,} rows, export date {fdate} (from {how})")
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p, allowMissingColumns=True)
    staged.append((entity, files, out))

landing = {e for e, _, _ in staged}
if landing & set(CORE_ENTITIES):
    missing = [e for e in CORE_ENTITIES if e not in landing]
    never = [e for e in CORE_ENTITIES
             if e not in landing and not table_exists(BRONZE_TABLE.format(src=SOURCE_SYSTEM, entity=e))]
    if never:
        raise ValueError(f"First export for {SOURCE_SYSTEM} must include all four core files. Missing: "
                         f"{', '.join(never)}. Nothing was landed.")
    if missing and ALLOW_PARTIAL.strip().lower() != "yes":
        raise ValueError(f"This batch has {', '.join(sorted(landing & set(CORE_ENTITIES)))} but not "
                         f"{', '.join(missing)}. Core files must land together so clients and enrollments "
                         f"come from the same export. Add the missing files, or rerun with ALLOW_PARTIAL = 'yes'. "
                         f"Nothing was landed.")

def _append_history(df, history, names):
    w = df.write.format("delta").option("mergeSchema", "true")
    if table_exists(history):
        listed = ", ".join("'" + n.replace("'", "''") + "'" for n in names)
        (w.mode("overwrite")
          .option("replaceWhere", f"bronze_batch_date = '{batch_date}' AND source_file_name IN ({listed})")
          .saveAsTable(history))
    else:
        w.mode("overwrite").saveAsTable(history)

for entity, files, out in staged:
    out = (out.withColumn("bronze_load_datetime", F.current_timestamp())
              .withColumn("bronze_batch_id", F.lit(batch_id))
              .withColumn("bronze_batch_date", F.lit(batch_date).cast("date")))
    current = BRONZE_TABLE.format(src=SOURCE_SYSTEM, entity=entity)
    history = current + "_history"
    _append_history(out, history, [name for name, _, _ in files])
    (out.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(current))
    rows = spark.read.table(current).count()
    summary[entity] = rows
    print(f"  -> {current}: {rows:,} rows (history appended to {history})")
    for name, path, _ in files:
        move(path, f"{PROCESSED_DIR}/{entity}/{batch_date}")

if not summary:
    print(f"No files found under {LANDING_DIR}. Nothing landed; Bronze is unchanged.")



# ---------------- CELL 4: EXIT ----------------
import json
result = json.dumps({"source_system": SOURCE_SYSTEM, "batch_id": batch_id, "landed": summary})
try:
    import notebookutils
except ImportError:
    notebookutils = None
if notebookutils is not None:
    notebookutils.notebook.exit(result)
else:
    print(result)
