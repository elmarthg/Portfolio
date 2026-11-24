"""Every declared key in the warehouse, and the SQL that checks each one holds.

Fabric Warehouse accepts PRIMARY KEY, UNIQUE and FOREIGN KEY constraints only as NOT ENFORCED, and its query
optimizer may rely on them: a key that silently stops holding can return wrong results. So the warehouse checks
them itself. Every procedure that writes a table checks the keys on that table before COMMIT, and
checks/check_declared_keys.sql checks them all at once. Those check lists are generated from one source:
the keys the DDL and migrations actually declare (read here from a simulated build).

    python tools/key_checks.py        # after run_demo.py: confirm the committed checks match the declared keys
"""
import os, re, sys, functools

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "local"))
SQL = os.path.join(ROOT, "warehouse")
SILVER = os.path.join(ROOT, "output", "work", "tables")


@functools.lru_cache()
def catalog():
    """(name, kind, table, cols, ref_table, ref_cols) for every key declared by 01_ddl and migrations 0001-0006."""
    from sim_build import new_warehouse, run_script, attach_silver
    if not os.path.isdir(SILVER):
        sys.exit("Run python run_demo.py first: the migrations read Silver tables it builds.")
    con, _ = new_warehouse(os.path.join(SQL, "01_ddl_v8.13.sql"))
    attach_silver(con, SILVER)
    for s in ("migrations/0001_persistent_loads", "migrations/0002_person_links", "02b_seed_etl_source_config",
              "migrations/0003_matching", "migrations/0004_assessments", "migrations/0005_keys_and_cleanup",
              "migrations/0006_key_checks_and_dates"):
        run_script(con, os.path.join(SQL, f"{s}.sql"), log=lambda *a: None)
    return tuple(tuple(r) for r in con.execute(
        "SELECT name, kind, table_name, cols, ref_table, ref_cols FROM sim_constraints ORDER BY table_name, kind, name").fetchall())


def sqlname(t):
    return t if "." in t else f"dbo.{t}"


def check_rows(tables=None):
    """(label, count query) per check, for keys on `tables` (all if None)."""
    out = []
    for name, kind, t, cols, rt, rcols in catalog():
        if tables is not None and t not in tables:
            continue
        cl = cols.split(",")
        tq = sqlname(t)
        if kind in ("PRIMARY KEY", "UNIQUE"):
            nn = " AND ".join(f"{c} IS NOT NULL" for c in cl)
            c = ", ".join(cl)
            out.append((f"{name}: duplicate {c}",
                        f"SELECT COUNT(*) FROM (SELECT {c} FROM {tq} WHERE {nn} GROUP BY {c} HAVING COUNT(*) > 1) d"))
            if kind == "PRIMARY KEY":
                out.append((f"{name}: empty {c}", f"SELECT COUNT(*) FROM {tq} WHERE " + " OR ".join(f"{x} IS NULL" for x in cl)))
        else:
            rl = rcols.split(",")
            on = " AND ".join(f"p.{r} = c.{x}" for x, r in zip(cl, rl))
            nn = " AND ".join(f"c.{x} IS NOT NULL" for x in cl)
            out.append((f"{name}: {', '.join(cl)} not in {rt}",
                        f"SELECT COUNT(*) FROM {tq} c WHERE {nn} AND NOT EXISTS (SELECT 1 FROM {sqlname(rt)} p WHERE {on})"))
    return out


def labels_in(path, generated_block_only=False):
    text = open(path).read()
    if generated_block_only:
        text = "\n".join(re.findall(r"-- BEGIN declared-key checks.*?\n(.*?)-- END declared-key checks", text, re.S))
    return set(re.findall(r"SELECT '((?:PK|UQ|FK)_[^']+)'", text))


if __name__ == "__main__":
    want = {lab for lab, _ in check_rows()}
    problems = []
    have = labels_in(os.path.join(SQL, "checks", "check_declared_keys.sql"))
    if have != want:
        problems.append(f"check_declared_keys.sql: missing {sorted(want - have)[:5]}, stale {sorted(have - want)[:5]}")
    for f in ("03a_proc_load_sf_cis.sql", "03b_load_hmis_clarity.sql", "06_proc_person_links.sql"):
        stale = labels_in(os.path.join(SQL, f), generated_block_only=True) - want
        if stale:
            problems.append(f"{f}: checks for keys that are not declared: {sorted(stale)[:5]}")
    keys = catalog()
    print(f"{len(keys)} declared keys ({sum(k[1] == 'PRIMARY KEY' for k in keys)} primary, "
          f"{sum(k[1] == 'UNIQUE' for k in keys)} unique, {sum(k[1] == 'FOREIGN KEY' for k in keys)} foreign), "
          f"{len(want)} checks")
    print("\n".join(problems) if problems else "OK: every generated check list matches the declared keys")
    sys.exit(1 if problems else 0)
