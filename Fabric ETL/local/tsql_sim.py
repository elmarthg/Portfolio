"""Run the warehouse T-SQL (DDL, seeds, stored procedures) in DuckDB.

A small interpreter: DECLARE / SELECT @v = / IF ... BEGIN ... END / THROW /
TRY-CATCH / PRINT run in Python, every other statement is rewritten from
T-SQL to DuckDB with sqlglot plus a few targeted rewrites.
"""
import re, sqlglot, duckdb, datetime as dt

KW_STMT = r"(BEGIN|THROW|SET|SELECT|INSERT|UPDATE|DELETE|DECLARE|PRINT|EXEC|RETURN|DROP|TRUNCATE|WITH|IF|COMMIT|ROLLBACK|ALTER|CREATE|GRANT|DENY|REVOKE)"


class SqlThrow(Exception):
    def __init__(self, number, message):
        super().__init__(f"{number}: {message}")
        self.number, self.message = number, message


def strip_comments(sql):
    out, i, n, q = [], 0, len(sql), False
    while i < n:
        c = sql[i]
        if c == "'":
            q = not q; out.append(c); i += 1
        elif not q and sql.startswith("--", i):
            j = sql.find("\n", i); i = n if j == -1 else j
        else:
            out.append(c); i += 1
    return "".join(out)


def scan(sql, start=0):
    """Yield (index, char, depth) outside string literals."""
    depth, q = 0, False
    for i in range(start, len(sql)):
        c = sql[i]
        if c == "'":
            q = not q
        if q or c == "'":
            yield i, c, depth, True
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        yield i, c, depth, False


def find_top(sql, pattern, start=0):
    rx = re.compile(pattern, re.I)
    wordy = pattern[:1].isalpha()
    for i, c, d, inq in scan(sql, start):
        if not inq and d == 0 and (not wordy or i == 0 or not (sql[i - 1].isalnum() or sql[i - 1] in "_@#")):
            m = rx.match(sql, i)
            if m:
                return m
    return None


def match_end(sql, start):
    """Index just after the END that closes the BEGIN ending at `start`."""
    depth, case = 1, 0
    for m in re.finditer(r"'(?:[^']|'')*'|\b(BEGIN(?!\s+TRAN)|END|CASE)\b", sql[start:], re.I):
        w = (m.group(1) or "").upper()
        if not w:
            continue
        if w == "CASE":
            case += 1
        elif w == "BEGIN":
            depth += 1
        elif w == "END":
            if case:
                case -= 1
            else:
                depth -= 1
                if depth == 0:
                    return start + m.end()
    raise ValueError("unmatched BEGIN")


def parse(sql):
    """List of nodes: ('stmt', text) | ('if', cond, body, else) | ('try', body, catch)."""
    nodes, i, n = [], 0, len(sql)
    while True:
        while i < n and (sql[i].isspace() or sql[i] == ";"):
            i += 1
        if i >= n:
            return nodes
        rest = sql[i:]
        if re.match(r"BEGIN\s+TRY\b", rest, re.I):
            j = re.search(r"\bEND\s+TRY\b", rest, re.I)
            body = rest[re.match(r"BEGIN\s+TRY\b", rest, re.I).end():j.start()]
            k = re.search(r"BEGIN\s+CATCH\b", rest[j.end():], re.I)
            cs = j.end() + k.end()
            ce = re.search(r"\bEND\s+CATCH\b", rest[cs:], re.I)
            nodes.append(("try", parse(body), parse(rest[cs:cs + ce.start()])))
            i += cs + ce.end()
        elif re.match(r"WHILE\b", rest, re.I):
            bm = find_top(rest, r"BEGIN\b", 5)
            cond = rest[5:bm.start()].strip()
            e = match_end(rest, bm.start() + 5)
            nodes.append(("while", cond, parse(rest[bm.start() + 5:e - 3])))
            i += e
        elif re.match(r"IF\b", rest, re.I):
            m = re.compile(r"\s*\n\s*" + KW_STMT + r"\b|\s+(BEGIN|THROW)\b", re.I)
            body_m = None
            for k, c, d, inq in scan(rest, 2):
                if not inq and d == 0:
                    mm = m.match(rest, k)
                    if mm:
                        body_m = mm; break
            cond = rest[2:body_m.start()].strip()
            j = rest.index((body_m.group(1) or body_m.group(2)), body_m.start())
            if re.match(r"BEGIN\b", rest[j:], re.I):
                e = match_end(rest, j + 5)
                body = parse(rest[j + 5:e - 3])
                k = e
            else:
                e = find_top(rest, r";", j)
                body = parse(rest[j:e.start() + 1])
                k = e.end()
            els = []
            mm = re.match(r"\s*;?\s*ELSE\b\s*", rest[k:], re.I)
            if mm:
                k2 = k + mm.end()
                if re.match(r"BEGIN\b", rest[k2:], re.I):
                    e = match_end(rest, k2 + 5); els = parse(rest[k2 + 5:e - 3]); k = e
                else:
                    e = find_top(rest, r";", k2); els = parse(rest[k2:e.start() + 1]); k = e.end()
            nodes.append(("if", cond, body, els))
            i += k
        else:
            e = find_top(rest, r";")
            if e is None:
                nodes.append(("stmt", rest.strip())); return nodes
            nodes.append(("stmt", rest[:e.start()].strip()))
            i += e.end()


# ---- Fabric table constraints (catalog kept in sim_constraints) ----------------------------------------
# Fabric: PRIMARY KEY and UNIQUE only as NONCLUSTERED ... NOT ENFORCED, FOREIGN KEY only NOT ENFORCED, added with
# ALTER TABLE. A foreign key must reference the columns of a primary key or unique constraint on the other table.
# A column that belongs to a constraint cannot be dropped.
CON_RE = re.compile(
    r"ALTER\s+TABLE\s+([\w.]+)\s+ADD\s+CONSTRAINT\s+(\w+)\s+(PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY)\s*(NONCLUSTERED)?\s*"
    r"\(([^)]*)\)\s*(?:REFERENCES\s+([\w.]+)\s*\(([^)]*)\))?\s*(NOT\s+ENFORCED)?\s*;?\s*$", re.I | re.S)

def _tname(n):
    return re.sub(r"^dbo\.", "", n.strip())

def ensure_constraint_catalog(con):
    con.execute("""CREATE TABLE IF NOT EXISTS sim_constraints (name VARCHAR, table_name VARCHAR, kind VARCHAR,
                   cols VARCHAR, ref_table VARCHAR, ref_cols VARCHAR)""")

def _cols_of(con, t):
    schema, name = (t.split(".", 1) if "." in t else ("main", t))
    return {r[0].lower() for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ? AND table_schema = ?",
        [name, schema]).fetchall()}

def add_constraint(con, stmt):
    ensure_constraint_catalog(con)
    m = CON_RE.match(stmt.strip())
    if not m:
        raise SqlThrow(102, f"Unsupported constraint syntax: {stmt.strip()[:120]}")
    t, name, kind, noncl, cols, rt, rcols, ne = m.groups()
    t, kind = _tname(t), re.sub(r"\s+", " ", kind.upper())
    cols = [c.strip().lower() for c in cols.split(",")]
    if not ne:
        raise SqlThrow(24584, f"{name}: Fabric supports {kind} only with NOT ENFORCED.")
    if kind in ("PRIMARY KEY", "UNIQUE") and not noncl:
        raise SqlThrow(24584, f"{name}: {kind} needs NONCLUSTERED in Fabric.")
    have = _cols_of(con, t)
    if not have:
        raise SqlThrow(4902, f"Cannot find the object '{t}'.")
    miss = [c for c in cols if c not in have]
    if miss:
        raise SqlThrow(1911, f"{name}: column(s) {miss} do not exist in {t}.")
    if con.execute("SELECT COUNT(*) FROM sim_constraints WHERE lower(name) = lower(?)", [name]).fetchone()[0]:
        raise SqlThrow(2714, f"There is already an object named '{name}' in the database.")
    if kind == "PRIMARY KEY" and con.execute(
            "SELECT COUNT(*) FROM sim_constraints WHERE table_name = ? AND kind = 'PRIMARY KEY'", [t]).fetchone()[0]:
        raise SqlThrow(1779, f"Table '{t}' already has a primary key defined on it.")
    ref = None
    if kind == "FOREIGN KEY":
        rt = _tname(rt); rcols = [c.strip().lower() for c in rcols.split(",")]
        if len(rcols) != len(cols):
            raise SqlThrow(8139, f"{name}: number of referencing columns differs from referenced columns.")
        keys = [r[0] for r in con.execute(
            "SELECT cols FROM sim_constraints WHERE table_name = ? AND kind IN ('PRIMARY KEY', 'UNIQUE')", [rt]).fetchall()]
        if ",".join(rcols) not in keys:
            raise SqlThrow(1776, f"{name}: there are no primary or candidate keys in '{rt}' that match the referencing column list.")
        ref = (rt, ",".join(rcols))
    con.execute("INSERT INTO sim_constraints VALUES (?, ?, ?, ?, ?, ?)",
                [name, t, kind, ",".join(cols), ref[0] if ref else None, ref[1] if ref else None])

def drop_constraint(con, t, name):
    ensure_constraint_catalog(con)
    if not con.execute("SELECT COUNT(*) FROM sim_constraints WHERE lower(name) = lower(?) AND table_name = ?",
                       [name, _tname(t)]).fetchone()[0]:
        raise SqlThrow(3728, f"'{name}' is not a constraint.")
    if con.execute("SELECT COUNT(*) FROM sim_constraints WHERE kind = 'FOREIGN KEY' AND ref_table = ? AND "
                   "(SELECT cols FROM sim_constraints WHERE lower(name) = lower(?)) = ref_cols", [_tname(t), name]).fetchone()[0]:
        raise SqlThrow(3725, f"The constraint '{name}' is being referenced by a foreign key.")
    con.execute("DELETE FROM sim_constraints WHERE lower(name) = lower(?)", [name])

def drop_column_check(con, t, col):
    ensure_constraint_catalog(con)
    for (name, cols) in con.execute("SELECT name, cols FROM sim_constraints WHERE table_name = ?", [_tname(t)]).fetchall():
        if col.lower() in cols.split(","):
            raise SqlThrow(5074, f"The object '{name}' is dependent on column '{col}'.")
    for (name, rcols) in con.execute("SELECT name, ref_cols FROM sim_constraints WHERE ref_table = ?", [_tname(t)]).fetchall():
        if col.lower() in rcols.split(","):
            raise SqlThrow(5074, f"The object '{name}' is dependent on column '{col}'.")


def lit(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, dt.datetime):
        return f"CAST('{v.isoformat(sep=' ')}' AS DATETIME2(6))"
    if isinstance(v, dt.date):
        return f"CAST('{v.isoformat()}' AS DATE)"
    return "'" + str(v).replace("'", "''") + "'"


def split_top(text, sep_pattern):
    parts, last, skip = [], 0, -1
    rx = re.compile(sep_pattern, re.I)
    for i, c, d, inq in scan(text):
        if i < skip:
            continue
        if not inq and d == 0 and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")):
            m = rx.match(text, i)
            if m and i >= last:
                parts.append(text[last:i]); last = i; skip = m.end()
    parts.append(text[last:])
    return [p for p in parts if p.strip()]


def rewrite_update(sql):
    """T-SQL UPDATE alias SET ... FROM t alias JOIN ... -> DuckDB UPDATE t AS alias SET ... FROM ... WHERE ..."""
    m = re.match(r"UPDATE\s+(\w+)\s+SET\s", sql, re.I)
    if not m:
        return sql
    alias = m.group(1)
    fm = find_top(sql, r"FROM\b")
    if fm is None:
        return sql
    wm = find_top(sql, r"WHERE\b", fm.end())
    set_part = sql[m.end():fm.start()]
    from_part = sql[fm.end():wm.start() if wm else len(sql)]
    where = sql[wm.end():] if wm else ""
    segs = split_top(from_part, r"(INNER\s+JOIN|LEFT\s+JOIN|JOIN|CROSS\s+APPLY)\b")
    target = segs[0].strip().split()
    if len(target) < 2 or target[-1] != alias:
        return sql
    table = target[0]
    sets = re.sub(r"(^|,)\s*" + alias + r"\.(\w+)\s*=", r"\1 \2 =", set_part.strip())
    if len(segs) == 1:
        return f"UPDATE {table} AS {alias} SET {sets}" + (f" WHERE {where}" if where.strip() else "")
    first = segs[1].strip()
    fj = re.match(r"(INNER\s+JOIN|JOIN)\s+", first, re.I)
    on = find_top(first, r"ON\b", fj.end())
    item = first[fj.end():on.start()].strip()
    cond = first[on.end():].strip()
    others = " ".join(s.strip() for s in segs[2:])
    others = re.sub(r"CROSS\s+APPLY", "CROSS JOIN LATERAL", others, flags=re.I)
    sets = re.sub(r"(^|,)\s*" + alias + r"\.(\w+)\s*=", r"\1 \2 =", set_part.strip())
    w = f"({cond})" + (f" AND ({where.strip()})" if where.strip() else "")
    return f"UPDATE {table} AS {alias} SET {sets} FROM {item} {others} WHERE {w}"


def to_duck(sql):
    s = re.sub(r"\blh_silver\.INFORMATION_SCHEMA\.TABLES\b",
               "(SELECT 'dbo' AS TABLE_SCHEMA, table_name AS TABLE_NAME FROM information_schema.tables WHERE table_schema = 'silver')", sql)
    s = re.sub(r"\blh_silver\.dbo\.", "silver.", s)
    s = re.sub(r"\blh_bronze\.dbo\.", "bronze.", s)
    s = re.sub(r"\bdbo\.", "", s)
    s = re.sub(r"#(\w+)", r"tmp_\1", s)
    s = re.sub(r"USER_NAME\(\)", "'sim_user'", s, flags=re.I)
    s = re.sub(r"CONVERT\(VARCHAR\(64\),\s*HASHBYTES\('SHA2_256',\s*", "sha256(", s)
    s = re.sub(r"\)\),\s*2\)(\s+AS\s+row_hash)", r"))\1", s)
    s = re.sub(r"CONVERT\(VARCHAR\(\d+\),\s*([\w.]+),\s*112\)", r"strftime(\1, '%Y%m%d')", s)
    s = re.sub(r"CONVERT\(VARCHAR\(\d+\),\s*([\w.]+(?:\(\))?),\s*\d+\)", r"CAST(\1 AS VARCHAR)", s)
    s = re.sub(r"SYSDATETIMEOFFSET\(\)\s+AT\s+TIME\s+ZONE\s+'[^']*'", "CURRENT_TIMESTAMP", s, flags=re.I)
    s = re.sub(r"\bTRUNCATE\s+TABLE\b", "DELETE FROM", s, flags=re.I)
    s = re.sub(r"\bN?VARCHAR\(MAX\)", "VARCHAR", s, flags=re.I)
    if re.match(r"\s*ALTER\s+TABLE", s, re.I):
        s = re.sub(r"\bBIT\b", "INTEGER", s)
    s = rewrite_update(s)
    out = sqlglot.transpile(s, read="tsql", write="duckdb")[0]
    out = out.replace("TIMESTAMP(6)", "TIMESTAMP")
    out = re.sub(r"INNER JOIN LATERAL (\((?:[^()]|\([^()]*\))*\)) AS (\w+)(?!\s+ON)", r"CROSS JOIN LATERAL \1 AS \2", out)
    return out


def duck_create_table(con, name, body):
    """CREATE TABLE from T-SQL column definitions (IDENTITY -> sequence, BIT -> INTEGER)."""
    name = re.sub(r"#(\w+)", r"tmp_\1", re.sub(r"^dbo\.", "", name))
    body = re.sub(r"\s+MASKED WITH \(FUNCTION = '[^']*'\)", "", body)
    cols = []
    for line in body.split("\n"):
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
            seq = "seq_" + name.replace(".", "_") + "_" + col
            con.execute(f"CREATE SEQUENCE IF NOT EXISTS {seq}")
            typ = typ + f" DEFAULT nextval('{seq}')"
        cols.append(f'"{col}" {typ.strip()}')
    con.execute(f"CREATE TABLE {name} ({', '.join(cols)})")
    return [c.split()[0].strip('"') for c in cols]


class Session:
    def __init__(self, con, log=None):
        self.con, self.vars, self.log, self.results = con, {}, log or (lambda *a: None), []
        self.error_message = None
        self.trancount = 0
        self.mdf_temps = set()   # temp tables made by CREATE TABLE #x (...) without DISTRIBUTION: Fabric's MDF-backed kind

    def subst(self, text):
        text = re.sub(r"@@TRANCOUNT", str(self.trancount), text, flags=re.I)
        def rep(m):
            name = m.group(0)[1:].lower()
            if name not in self.vars:
                raise KeyError(f"undeclared @{name}")
            return lit(self.vars[name])
        out, last = [], 0
        for m in re.finditer(r"'(?:[^']|'')*'|@\w+", text):
            out.append(text[last:m.start()])
            out.append(m.group(0) if m.group(0).startswith("'") else rep(m))
            last = m.end()
        out.append(text[last:])
        return "".join(out)

    def eval(self, expr):
        q = to_duck("SELECT " + self.subst(expr))
        return self.con.execute(q).fetchone()[0]

    def run(self, nodes):
        for node in nodes:
            if node[0] == "if":
                _, cond, body, els = node
                ok = self.eval(f"CASE WHEN {cond} THEN 1 ELSE 0 END")
                self.run(body if ok else els)
            elif node[0] == "while":
                _, cond, body = node
                guard = 0
                while self.eval(f"CASE WHEN {cond} THEN 1 ELSE 0 END"):
                    guard += 1
                    if guard > 10000:
                        raise RuntimeError("WHILE loop did not end")
                    self.run(body)
            elif node[0] == "try":
                try:
                    self.run(node[1])
                except Exception as e:
                    if self.trancount:
                        self.con.execute("ROLLBACK"); self.trancount = 0
                    self.error_message = getattr(e, "message", str(e))
                    self.vars["__err"] = e
                    self.run(node[2])
            else:
                self.stmt(node[1])

    def stmt(self, s):
        if not s:
            return
        u = s.lstrip().upper()
        if u.startswith("DECLARE"):
            m = re.match(r"DECLARE\s+@(\w+)\s+[\w]+(?:\([\d,\s]+\))?\s*(?:=\s*(.*))?$", s.strip(), re.I | re.S)
            name, expr = m.group(1).lower(), m.group(2)
            self.vars[name] = None
            if expr:
                self.vars[name] = self.eval(expr)
            return
        if u.startswith("THROW"):
            rest = s.strip()[5:].strip()
            if not rest:
                raise self.vars.get("__err") or SqlThrow(0, self.error_message)
            parts, last = [], 0
            for i, c, d, inq in scan(rest):
                if c == "," and not inq and d == 0:
                    parts.append(rest[last:i]); last = i + 1
            parts.append(rest[last:])
            raise SqlThrow(int(parts[0].strip()), self.eval(parts[1].strip()))
        if re.match(r"BEGIN\s+TRAN", u):
            self.con.execute("BEGIN TRANSACTION"); self.trancount = 1; return
        if re.match(r"COMMIT\b", u):
            if self.trancount:
                self.con.execute("COMMIT")
            self.trancount = 0; return
        if re.match(r"ROLLBACK\b", u):
            if self.trancount:
                self.con.execute("ROLLBACK")
            self.trancount = 0; return
        if re.match(r"(GRANT|DENY|REVOKE)\b", u):
            self.log("SECURITY (not simulated)", s[:120]); return
        if u.startswith("PRINT"):
            self.log("PRINT", self.eval(s.strip()[5:])); return
        if u.startswith("RAISERROR"):
            self.log("RAISERROR", s[:120]); return
        m = re.match(r"SET\s+@(\w+)\s*=\s*(.*)$", s.strip(), re.I | re.S)
        if m:
            self.vars[m.group(1).lower()] = self.eval(m.group(2))
            return
        m = re.match(r"SELECT\s+@(\w+)\s*=\s*(.*)$", s.strip(), re.I | re.S)
        if m:
            q = to_duck("SELECT " + self.subst(m.group(2)))
            row = self.con.execute(q).fetchone()
            self.vars[m.group(1).lower()] = row[0] if row else None
            return
        if re.match(r"ALTER\s+TABLE\s+[\w.]+\s+ADD\s+CONSTRAINT\b", s.strip(), re.I):
            add_constraint(self.con, s); return
        if re.match(r"ALTER\s+TABLE\s+#", s.strip(), re.I):
            return
        m = re.match(r"ALTER\s+TABLE\s+([\w.]+)\s+DROP\s+CONSTRAINT\s+(\w+)\s*;?$", s.strip(), re.I)
        if m:
            drop_constraint(self.con, m.group(1), m.group(2)); return
        m = re.match(r"ALTER\s+TABLE\s+([\w.]+)\s+DROP\s+COLUMN\s+(\w+)\s*;?$", s.strip(), re.I)
        if m:
            drop_column_check(self.con, m.group(1), m.group(2))
            self.con.execute(f"ALTER TABLE {_tname(m.group(1))} DROP COLUMN {m.group(2)}"); return
        m = re.match(r"EXEC(?:UTE)?\s+(?:dbo\.|sys\.)?sp_rename\s+'([\w.]+)'\s*,\s*'(\w+)'\s*,\s*'COLUMN'\s*;?$", s.strip(), re.I)
        if m:
            parts = m.group(1).split(".")
            col, tbl = parts[-1], ".".join(p for p in parts[:-1] if p.lower() != "dbo")
            if col.lower() not in _cols_of(self.con, tbl):
                raise SqlThrow(15248, f"Either the parameter @objname is ambiguous or the claimed @objtype (COLUMN) is wrong: {m.group(1)}")
            self.con.execute(f'ALTER TABLE {tbl} RENAME COLUMN "{col}" TO "{m.group(2)}"')
            ensure_constraint_catalog(self.con)
            for (name, cols) in self.con.execute("SELECT name, cols FROM sim_constraints WHERE table_name = ?", [tbl]).fetchall():
                self.con.execute("UPDATE sim_constraints SET cols = ? WHERE name = ?",
                                 [",".join(m.group(2).lower() if c == col.lower() else c for c in cols.split(",")), name])
            return
        m = re.match(r"EXEC(?:UTE)?\s*\(\s*'((?:[^']|'')*)'\s*\)\s*$", s.strip(), re.I | re.S)
        if m:
            self.run(parse(m.group(1).replace("''", "'"))); return
        # Fabric temp tables. CREATE TABLE #x (...) without WITH (DISTRIBUTION = ROUND_ROBIN) makes an MDF-backed
        # table that can only be used with literal rows, other MDF temp tables and catalog views; any statement
        # that mixes it with a warehouse table or a distributed temp table fails with Msg 15816. SELECT ... INTO #x
        # always makes a distributed temp table.
        mt = re.match(r"CREATE\s+TABLE\s+#(\w+)\s*\(", s.strip(), re.I)
        if mt:
            dist = re.search(r"\)\s*WITH\s*\(\s*DISTRIBUTION\s*=\s*ROUND_ROBIN\s*\)\s*$", s.strip(), re.I)
            (self.mdf_temps.discard if dist else self.mdf_temps.add)(mt.group(1).lower())
            if dist:
                s = s.strip()[:dist.start() + 1]
        # Catalog views (sys.*) are not distributed either: one statement may read them or warehouse
        # tables, not both (Msg 15816).
        objs_all = re.findall(r"\b(?:FROM|JOIN)\s+([#\w][\w.#]*)", s, re.I)
        if any(o.lower().startswith("sys.") for o in objs_all) and any(
                not o.lower().startswith(("sys.", "information_schema.")) and not o.startswith("#") for o in objs_all):
            raise SqlThrow(15816, "The query references an object that is not supported in distributed processing mode.")
        md = re.match(r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?#(\w+)", s.strip(), re.I)
        if md:
            self.mdf_temps.discard(md.group(1).lower())
        elif self.mdf_temps:
            temps = {t.lower() for t in re.findall(r"#(\w+)", s)}
            if temps & self.mdf_temps and not mt:
                objs = re.findall(r"\b(?:FROM|JOIN|INTO|UPDATE)\s+([#\w][\w.#]*)", s, re.I)
                bad = [o for o in objs if not (o.startswith("#") and o[1:].lower() in self.mdf_temps)
                       and not o.lower().startswith(("sys.", "information_schema."))]
                if bad or re.search(r"\bSELECT\b[^;]*?\bINTO\s+#", s, re.I | re.S):
                    raise SqlThrow(15816, "The query references an object that is not supported in distributed processing mode.")
        m = re.match(r"CREATE\s+TABLE\s+([\w.]+)\s*\((.*)\)\s*$", s.strip(), re.I | re.S)
        if m and not re.search(r"\bAS\s+SELECT\b", s, re.I):
            duck_create_table(self.con, m.group(1), m.group(2)); return
        s2 = s.replace("ERROR_MESSAGE()", lit(self.error_message))
        q = to_duck(self.subst(s2))
        try:
            cur = self.con.execute(q)
        except Exception as e:
            raise RuntimeError(f"{e}\n--- T-SQL ---\n{s[:600]}\n--- DuckDB ---\n{q[:900]}")
        if u.startswith("SELECT") and " INTO " not in u.split("FROM")[0]:
            cols = [d[0] for d in cur.description]
            self.results.append((cols, cur.fetchall()))


def proc_body(text, name):
    text = strip_comments(text)
    m = re.search(r"CREATE\s+PROCEDURE\s+(?:dbo\.)?" + name + r"\s*(.*?)\bAS\s*\nBEGIN\b", text, re.S | re.I)
    params = {}
    for p in re.finditer(r"@(\w+)\s+[\w]+(?:\([\d,\s]+\))?\s*(?:=\s*([^,\n]+))?", m.group(1)):
        params[p.group(1).lower()] = p.group(2).strip() if p.group(2) else None
    start = m.end()
    end = match_end(text, start)
    return params, strip_comments(text[start:end - 3])


def call_proc(con, path, name, log=None, **args):
    text = open(path).read()
    params, body = proc_body(text, name)
    ses = Session(con, log)
    for k, default in params.items():
        if k in args:
            ses.vars[k] = args[k]
        else:
            ses.vars[k] = None if default in (None, "NULL") else ses.eval(default)
    ses.run(parse(body))
    return ses
