"""Static check: dropping a view drops the permissions granted on it. Any migration that recreates a view (or table) that
04_security grants on directly must give those grants back after recreating it, so a rerun never silently locks a role out."""
import re, sys, glob
import os; sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "local"))
from tsql_sim import strip_comments
V = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "warehouse")
sec = strip_comments(open(glob.glob(f"{V}/04_security_v*.sql")[-1] if False else f"{V}/04_security_v1.10.sql").read())
grants = {}
for m in re.finditer(r"GRANT\s+SELECT\s+ON\s+(?:OBJECT::)?((?:dbo|rpt)\.\w+)\s+TO\s+(\w+)", sec, re.I):
    grants.setdefault(m.group(1).lower(), set()).add(m.group(2).lower())
problems = []
for f in sorted(glob.glob(f"{V}/migrations/*.sql")) + [f"{V}/06_proc_person_links.sql"]:
    s = strip_comments(open(f).read())
    for obj, roles in grants.items():
        for m in re.finditer(r"DROP\s+(?:VIEW|TABLE)\s+(?:IF\s+EXISTS\s+)?" + re.escape(obj) + r"\b", s, re.I):
            rest = s[m.end():]
            missing = [r for r in roles if not re.search(r"GRANT\s+SELECT\s+ON\s+(?:OBJECT::)?" + re.escape(obj) + r"\s+TO\s+" + r, rest, re.I)]
            if missing:
                problems.append(f"{f.split('/')[-1]}: drops {obj} but does not give back SELECT to {', '.join(sorted(missing))}")
for p in sorted(set(problems)):
    print("FAIL", p)
print("PASS every recreated object keeps its direct grants" if not problems else f"{len(set(problems))} problem(s)")
