"""Static check for a Fabric rule the simulator cannot enforce: a batch is compiled before it runs, so a statement
that names a column the same batch adds (ALTER TABLE ... ADD col) or renames into place (sp_rename ... 'col') fails
with Msg 207 'Invalid column name' and nothing in the batch runs. Use such a column only after GO or inside EXEC('...')."""
import re, sys, glob
import os; sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "local"))
from tsql_sim import strip_comments
V = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "warehouse")
files = sys.argv[1:] or sorted(glob.glob(f"{V}/migrations/*.sql"))
problems = []
for f in files:
    for bi, batch in enumerate(re.split(r"^\s*GO\s*$", strip_comments(open(f).read()), flags=re.M)):
        nodyn = re.sub(r"EXEC(?:UTE)?\s*\(\s*'(?:[^']|'')*'\s*\)", "EXEC(...)", batch, flags=re.I)
        new = {}
        for m in re.finditer(r"ALTER\s+TABLE\s+([\w.]+)\s+ADD\s+(?!CONSTRAINT\b)(\w+)\s", nodyn, re.I):
            new.setdefault((m.group(1).split(".")[-1].lower(), m.group(2).lower()), m.end())
        for m in re.finditer(r"sp_rename\s+'(?:\w+\.)?(\w+)\.\w+',\s*'(\w+)',\s*'COLUMN'", nodyn, re.I):
            new.setdefault((m.group(1).lower(), m.group(2).lower()), m.end())
        for (tab, col), pos in new.items():
            rest = nodyn[pos:]
            for st in re.split(r";", rest):
                if re.search(r"\b" + col + r"\b", st, re.I) and re.search(r"\b" + tab + r"\b", st, re.I) and not re.match(r"\s*(ALTER\s+TABLE\s+[\w.]+\s+ADD|EXEC\s+sp_rename|IF\s+(@\w+\s*=\s*\d\s+AND\s+)?NOT\s+EXISTS\s*\(SELECT\s+1\s+FROM\s+sys\.columns)", st, re.I) \
                   and "sys.columns" not in st:
                    problems.append(f"{f.split('/')[-1]} batch {bi}: '{col}' is added or renamed in this batch and used by: {' '.join(st.split())[:110]}")
                    break
for p in problems:
    print("FAIL", p)
print("PASS no batch uses a column it adds or renames" if not problems else f"{len(problems)} problem(s)")
