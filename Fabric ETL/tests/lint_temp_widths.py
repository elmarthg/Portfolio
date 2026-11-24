"""Static check for a Fabric rule the simulator cannot enforce (it ignores text lengths):
a temp table made by SELECT ... INTO sizes each text column from its first values, so a column built from a
literal or a CASE of literals must not later receive a longer value through INSERT INTO #t or UPDATE #t.
Fabric fails with Msg 2628 (found on the first Fabric run, 2026-11-22). Declare such columns with CAST."""
import re, sys
import os; sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "local"))
from tsql_sim import strip_comments
V = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "warehouse")
FILES = ["03a_proc_load_sf_cis.sql", "03b_load_hmis_clarity.sql", "05_proc_admin_clear_warehouse.sql", "06_proc_person_links.sql",
         "migrations/0001_persistent_loads.sql", "migrations/0002_person_links.sql", "migrations/0003_matching.sql", "migrations/0004_assessments.sql", "migrations/0005_keys_and_cleanup.sql", "migrations/0006_key_checks_and_dates.sql", "07_proc_purge_audit_history.sql"]
problems = []
for f in FILES:
    created = {}
    for st in re.split(r";\s*\n", strip_comments(open(f"{V}/{f}").read())):
        m = re.search(r"\bINTO\s+#(\w+)\s+FROM", st, re.I)
        if m and re.search(r"\bSELECT\b", st, re.I):
            cols = {c.lower() for lit, c in re.findall(r"(?:(?<!AS VARCHAR\(\d\d\)\))'([^']*)'|CASE\b(?:(?!CAST\().)*?\bEND)\s+AS\s+(\w+)", st, re.S | re.I)}
            created[m.group(1)] = cols
        m2 = re.match(r"\s*INSERT\s+INTO\s+#(\w+)\s*\(([^)]*)\)", st, re.I)
        if m2 and created.get(m2.group(1)):
            hit = set(c.strip().lower() for c in m2.group(2).split(",")) & created[m2.group(1)]
            if hit:
                problems.append(f"{f}: INSERT INTO #{m2.group(1)} writes {sorted(hit)}, sized by SELECT INTO from literals")
for p in problems:
    print("FAIL", p)
print("PASS no literal-sized temp column receives later rows" if not problems else f"{len(problems)} problem(s)")
