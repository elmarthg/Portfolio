"""Static check for a SQL Server / Fabric rule the simulator cannot enforce: a view that selects * (or alias.*) keeps the
column list it was created with. When a migration adds, drops, renames or swaps a column of a table under such a view, the
same migration must recreate the view afterwards, or the view shows stale columns or fails."""
import re, sys, glob
import os; sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "local"))
from tsql_sim import strip_comments
V = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "warehouse")
files = [f"{V}/01_ddl_v8.13.sql"] + sorted(glob.glob(f"{V}/migrations/*.sql"))
star = {}            # view -> base table or view
problems = []
def bases(v, seen=()):
    b = star.get(v)
    return set() if b is None or b in seen else {b} | bases(b, seen + (b,))
for f in files:
    s = strip_comments(open(f).read())
    s = re.sub(r"EXEC\('((?:[^']|'')*)'\)", lambda m: m.group(1).replace("''", "'"), s)
    events = []
    for m in re.finditer(r"CREATE\s+VIEW\s+(?:dbo\.)?(\w+)\s+AS\s+SELECT\s+(.*?)\bFROM\s+(?:dbo\.)?(\w+)", s, re.S | re.I):
        if re.search(r"(^|,|\s)(\w+\.)?\*", m.group(2)):
            events.append((m.start(), "view", m.group(1).lower(), m.group(3).lower()))
    for m in re.finditer(r"ALTER\s+TABLE\s+(?:dbo\.)?(\w+)\s+(?:ADD\s+(?!CONSTRAINT)\w+|DROP\s+COLUMN)", s, re.I):
        events.append((m.start(), "change", m.group(1).lower(), None))
    for m in re.finditer(r"sp_rename\s+'(?:dbo\.)?(\w+)\.\w+'", s, re.I):
        events.append((m.start(), "change", m.group(1).lower(), None))
    events.sort()
    for i, (pos, kind, a, b) in enumerate(events):
        if kind == "view":
            star[a] = b
    for i, (pos, kind, t, _) in enumerate(events):
        if kind != "change" or f.endswith("01_ddl_v8.13.sql"):
            continue
        for v in [v for v in star if t in bases(v)]:
            if not any(k == "view" and n == v and p > pos for p, k, n, _ in events):
                problems.append(f"{f.split('/')[-1]}: changes {t} but does not recreate star view {v} afterwards")
for p in sorted(set(problems)):
    print("FAIL", p)
print("PASS every star view over a changed table is recreated by the same migration" if not problems else f"{len(set(problems))} problem(s)")
