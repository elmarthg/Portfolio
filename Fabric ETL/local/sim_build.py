"""Build the baseline warehouse (01_ddl) in DuckDB, seed the reference rows 02_seed adds, and attach Silver.

01_ddl and 02_seed are emulated here: the DDL is parsed into DuckDB tables (identity columns become
sequences, NOT ENFORCED keys go into a constraint catalog the simulator checks), and the calendar and
source rows are inserted directly. Every later script (migrations, procedures) runs through tsql_sim."""
import re, glob, os, datetime as dt, duckdb, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tsql_sim import Session, parse, strip_comments, to_duck, ensure_constraint_catalog, add_constraint

def build_schema(con, ddl_path):
    s = strip_comments(open(ddl_path).read())
    s2 = re.sub(r"\s+MASKED WITH \(FUNCTION = '[^']*'\)", "", s)
    tables = {}
    for m in re.finditer(r"CREATE TABLE (?:dbo\.)?(\w+)\s*\((.*?)\n\);", s2, re.S):
        name, cols = m.group(1), []
        for line in m.group(2).split("\n"):
            t = line.strip().rstrip(",")
            if not t:
                continue
            col, typ = t.split()[0], " ".join(t.split()[1:])
            ident = "IDENTITY" in typ
            typ = typ.replace("IDENTITY", "")
            typ = re.sub(r"DATETIME2\(\d\)", "TIMESTAMP", typ)
            typ = re.sub(r"\bN?VARCHAR\(MAX\)", "VARCHAR", typ)
            typ = re.sub(r"\bBIT\b", "INTEGER", typ)
            if ident:
                con.execute(f"CREATE SEQUENCE IF NOT EXISTS seq_{name}_{col}")
                typ = typ + f" DEFAULT nextval('seq_{name}_{col}')"
            cols.append(f'"{col}" {typ.strip()}')
        con.execute(f"CREATE TABLE {name} ({', '.join(cols)})")
        tables[name] = [c.split()[0].strip('"') for c in cols]
    con.execute("""CREATE MACRO object_id(n, t := NULL) AS (
        SELECT CASE WHEN COUNT(*) > 0 THEN n END FROM information_schema.tables
        WHERE table_name = split_part(n, '.', -1)
          AND (strpos(n, '.') = 0 OR table_schema = split_part(n, '.', 1)))""")
    con.execute("""CREATE MACRO schema_id(n) AS (
        SELECT CASE WHEN COUNT(*) > 0 THEN 1 END FROM information_schema.schemata WHERE schema_name = n)""")
    con.execute("CREATE SCHEMA IF NOT EXISTS sys")
    con.execute("CREATE VIEW sys.columns AS SELECT table_name AS object_id, column_name AS name FROM information_schema.columns")
    ensure_constraint_catalog(con)
    for m in re.finditer(r"^\s*ALTER TABLE [\w.]+ ADD CONSTRAINT [^;]+;", s, re.M):
        add_constraint(con, m.group(0))
    con.execute("""CREATE VIEW sys.key_constraints AS SELECT name, table_name AS parent_object_id,
                   CASE kind WHEN 'PRIMARY KEY' THEN 'PK' ELSE 'UQ' END AS type FROM sim_constraints
                   WHERE kind IN ('PRIMARY KEY', 'UNIQUE')""")
    con.execute("""CREATE VIEW sys.foreign_keys AS SELECT name, table_name AS parent_object_id, ref_table AS referenced_object_id
                   FROM sim_constraints WHERE kind = 'FOREIGN KEY'""")
    con.execute("CREATE VIEW sys.database_principals AS SELECT CAST(NULL AS VARCHAR) AS name, CAST(NULL AS VARCHAR) AS type WHERE false")   # no roles in the simulator
    con.execute("CREATE VIEW sys.tables AS SELECT table_name AS name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
    con.execute("""CREATE MACRO fn_normalize_key(v) AS
        lower(replace(replace(replace(trim(CAST(v AS VARCHAR)), chr(9), ' '), chr(10), ' '), chr(13), ' '))""")
    for m in re.finditer(r"CREATE VIEW (?:dbo\.)?(\w+) AS\s*(.*?);\s*\nGO", s, re.S):
        con.execute(to_duck(f"CREATE VIEW {m.group(1)} AS {m.group(2)}"))
    return tables

def seed(con):
    rows, x = [], dt.date(2000, 1, 1)          # the same range as 02_seed_reference_data
    while x <= dt.date(2040, 12, 31):
        fy = x.year + 1 if x.month >= 10 else x.year
        rows.append((int(x.strftime("%Y%m%d")), x, x.year, (x.month - 1) // 3 + 1, x.month, x.strftime("%B"), x.day,
                     x.isoweekday(), x.strftime("%A"), int(x.isoweekday() > 5), fy, ((x.month - 10) % 12) // 3 + 1))
        x += dt.timedelta(days=1)
    con.executemany("INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend, fiscal_year, fiscal_quarter) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.execute("""INSERT INTO dim_source (source_system, platform, county, is_active) VALUES
        ('sf_cis','Salesforce',NULL,1),('hmis_lac','Clarity','Los Angeles',1),('hmis_sandiego','Clarity','San Diego',1),
        ('hmis_riverside','Clarity','Riverside',0),('hmis_ventura','WellSky','Ventura',0)""")

def run_script(con, path, log=print):
    ses = Session(con, log)
    for batch in re.split(r"^\s*GO\s*$", strip_comments(open(path).read()), flags=re.M):
        if batch.strip():
            ses.run(parse(batch))
    return ses

def attach_silver(con, *warehouses):
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    for wh in warehouses:
        for d in sorted(glob.glob(f"{wh}/silver_*")):
            name = os.path.basename(d)[len("silver_"):]
            files = glob.glob(f"{d}/*.parquet")
            if not files:
                continue
            con.execute(f"CREATE OR REPLACE TABLE silver.{name} AS SELECT * FROM read_parquet('{d}/*.parquet', union_by_name=true)")

def new_warehouse(ddl):
    con = duckdb.connect(":memory:")
    tables = build_schema(con, ddl)
    seed(con)
    return con, tables
