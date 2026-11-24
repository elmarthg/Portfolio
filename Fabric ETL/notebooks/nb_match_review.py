# nb_match_review v1.3
# The screens reviewers use to decide possible matches. Run it interactively in Fabric (Run all).
#   Review queue   pairs the matching notebook scored as possible matches, side by side, with
#                  Same person / Not the same person / Not sure. Automatic links whose evidence changed
#                  come first (they stay linked until decided), then new pairs, then withdrawn ones.
#   Person lookup  find a person by name, Salesforce Contact Id, Clarity Personal ID or person key;
#                  see their linked profiles and the decisions behind them; Withdraw a link or record
#                  that two of their profiles are not the same person
#   Results log    what you did this session (every save and every failure), and the reviewer decisions saved
#                  in the warehouse (yours or everyone's), including ones later replaced
#   Conflicts      decisions that contradict each other (from the last person-link run)
#   Apricot        merges and splits staff must do by hand in Apricot
#
# Every action calls dbo.usp_record_match_decision as the person running the notebook, so each decision
# is recorded under their own sign-in. Nothing is deleted: a changed decision keeps the old row, marked
# inactive. Decisions take effect at the next pipeline run (usp_refresh_person_links).
# The screens never show SSNs, gender identity or sexual orientation.
#
# Setup:
#   - Reviewers need the match_reviewer database role (04_security v1.10) and the warehouse shared with them.
#     Keep this notebook in a separate workspace where reviewers are Contributors; do not make them
#     Contributors on the warehouse's workspace (that role bypasses the warehouse's column denies).
#   - WAREHOUSE_SERVER is the warehouse's SQL connection string (Warehouse > Settings > SQL endpoint).
#
# v1.1: saving now skips the procedure's 'rows affected' messages before reading its answer (v1.0 read the
#       first message, failed after the decision was already saved, and the screen did not move on).
#       Every button shows 'Working...' at once, is greyed out until done, and shows any error on screen.
#       The connection reopens by itself when the sign-in token expires during a long review session.
# v1.2: Results log tab.
# v1.3: the sign-in waits up to 30 seconds and tries 3 times (a paused or busy capacity can be slow to answer),
#       and a failed sign-in says what to check. The server name is cleaned of https://, a port or spaces.

# ---------------- CELL 0: PARAMETERS ----------------
WAREHOUSE_SERVER = "REPLACE_WITH_SQL_CONNECTION_STRING"   # xxxx.datawarehouse.fabric.microsoft.com
WAREHOUSE_NAME = "REPLACE_WITH_WAREHOUSE_NAME"
QUEUE_LIMIT = 200


# ---------------- CELL 1: WAREHOUSE CONNECTION ----------------
import struct
import pandas as pd

class WarehouseDB:
    """Parameterised T-SQL over ODBC with the signed-in user's Entra token (no stored passwords)."""
    LOST_CONNECTION = ("08S01", "08001", "08003", "HYT00", "Communication link failure", "token", "Login failed",
                       "Login timeout", "connection is closed", "Connection is busy")

    def __init__(self, server, database):
        server = (server or "").strip().replace("https://", "").replace("tcp:", "").split(",")[0].strip().rstrip("/")
        if not server.endswith(".datawarehouse.fabric.microsoft.com"):
            raise ValueError(f"WAREHOUSE_SERVER looks wrong: {server!r}. Copy the SQL connection string from "
                             "Warehouse > Settings > SQL endpoint; it ends in .datawarehouse.fabric.microsoft.com")
        self.server, self.database = server, (database or "").strip()
        self.conn = None
        self._connect()

    def _connect(self, attempts=3):
        import time
        import pyodbc
        import notebookutils
        for attempt in range(attempts):
            token = notebookutils.credentials.getToken("https://database.windows.net/").encode("UTF-16-LE")
            attrs = {1256: struct.pack(f"<I{len(token)}s", len(token), token)}   # SQL_COPT_SS_ACCESS_TOKEN
            try:
                self.conn = pyodbc.connect(
                    f"Driver={{ODBC Driver 18 for SQL Server}};Server={self.server},1433;Database={self.database};"
                    "Encrypt=yes;TrustServerCertificate=no;", attrs_before=attrs, autocommit=True, timeout=30)
                return
            except Exception as e:
                if attempt == attempts - 1:
                    raise RuntimeError(
                        f"Could not sign in to {self.database} on {self.server} after {attempts} tries ({e}). Check that "
                        "the Fabric capacity is running, that WAREHOUSE_SERVER and WAREHOUSE_NAME are right, and that "
                        "this workspace can reach the warehouse.") from e
                time.sleep(5 * (attempt + 1))

    def _lost(self, e):
        return any(x.lower() in str(e).lower() for x in self.LOST_CONNECTION)

    @staticmethod
    def _to_result(cur):
        """Move past 'rows affected' messages (statements with no result set) to the first real result."""
        while cur.description is None:
            if not cur.nextset():
                raise RuntimeError("The warehouse sent no result back.")
        return cur

    def query(self, sql, params=()):
        for attempt in range(2):
            try:
                cur = self.conn.cursor()
                cur.execute(sql, params)
                self._to_result(cur)
                cols = [c[0] for c in cur.description]
                return pd.DataFrame.from_records(cur.fetchall(), columns=cols)
            except Exception as e:
                if attempt == 1 or not self._lost(e):
                    raise
                self._connect()

    def record_decision(self, sa, ia, sb, ib, decision, notes):
        # Fabric detects write conflicts per table: if the pipeline or another reviewer writes
        # match_decision at the same moment, one of them is rolled back with error 24556 or 24706.
        # The procedure is safe to repeat (a decision already saved comes back 'unchanged'), so retry.
        # A lost connection (expired sign-in token) is reopened and the save repeated, for the same reason.
        # The procedure sends 'rows affected' messages before its answer; _to_result skips them.
        import time
        reconnected = False
        for attempt in range(4):
            try:
                cur = self.conn.cursor()
                cur.execute("EXEC dbo.usp_record_match_decision @source_system_a = ?, @source_id_a = ?, "
                            "@source_system_b = ?, @source_id_b = ?, @decision = ?, @decision_method = 'reviewer', "
                            "@notes = ?", (sa, ia, sb, ib, decision, notes or None))
                row = self._to_result(cur).fetchone()
                if row is None:
                    raise RuntimeError("The save procedure returned no outcome.")
                return row[0], row[1]
            except Exception as e:
                if self._lost(e) and not reconnected:
                    self._connect()
                    reconnected = True
                    continue
                if attempt == 3 or not any(code in str(e) for code in ("24556", "24706", "update conflict")):
                    raise
                time.sleep(2 * (attempt + 1))


# ---------------- CELL 2: DATA ----------------
PAIR_FIELDS = [("Name", ["first_name", "middle_name", "last_name"]), ("Date of birth", ["date_of_birth"]),
               ("DOB quality", ["dob_data_quality"]), ("Sex", ["sex"]), ("Gender", ["gender"]),
               ("Mobile phone", ["mobile_phone"]), ("Home phone", ["home_phone"]), ("Email", ["email"]),
               ("Address", ["address_line1"]), ("City", ["city"]), ("ZIP", ["postal_code"]),
               ("Veteran", ["veteran_status"]), ("Branch", ["military_branch"]), ("Discharge", ["discharge_status"]),
               ("Enrollments", ["enrollments"]), ("Latest program", ["latest_program"]), ("Latest start", ["latest_start"])]
SOURCE_LABEL = {"sf_cis": "Salesforce"}

def source_label(s):
    return SOURCE_LABEL.get(s, "Clarity " + s.split("_", 1)[-1].upper() if s.startswith("hmis_") else s)

def load_queue(db, limit=QUEUE_LIMIT):
    return db.query(
        f"SELECT TOP {int(limit)} * FROM dbo.v_match_review_pairs "
        "ORDER BY CASE queue_status WHEN 'evidence_changed' THEN 0 WHEN 'new' THEN 1 ELSE 2 END, match_probability DESC, "
        "first_suggested_datetime, source_id_a, source_id_b")

def pair_table(row):
    """Side-by-side rows for one queue pair; values that differ are marked."""
    out = []
    for label, cols in PAIR_FIELDS:
        a = " ".join(str(row.get(f"{c}_a")) for c in cols if not _blank(row.get(f"{c}_a")))
        b = " ".join(str(row.get(f"{c}_b")) for c in cols if not _blank(row.get(f"{c}_b")))
        out.append((label, a, b, bool(a and b and a.strip().upper() != b.strip().upper())))
    return out

def _blank(v):
    return v is None or (not isinstance(v, (list, tuple)) and bool(pd.isna(v)))

def search_people(db, text):
    text = (text or "").strip()
    if not text:
        return pd.DataFrame()
    base = ("SELECT TOP 50 pc.person_key, pc.source_system, COALESCE(pc.salesforce_id, pc.hmis_personal_id) AS source_id, "
            "c.first_name, c.last_name, c.date_of_birth, c.city, pc.link_method "
            "FROM rpt.person_client pc JOIN dbo.dim_client c ON c.client_key = pc.client_key AND c.is_current = 1 ")
    if text.isdigit() and len(text) <= 9:
        df = db.query(base + "WHERE pc.person_key = ? OR pc.hmis_personal_id = ? ORDER BY pc.person_key",
                      (int(text), text))
        if len(df):
            return df
    if " " not in text and any(ch.isdigit() for ch in text):
        return db.query(base + "WHERE pc.salesforce_id = ? OR pc.hmis_personal_id = ? ORDER BY pc.person_key",
                        (text, text))
    parts = text.replace(",", " ").split()
    if len(parts) == 1:
        return db.query(base + "WHERE c.last_name LIKE ? ORDER BY c.last_name, c.first_name, pc.person_key",
                        (parts[0] + "%",))
    return db.query(base + "WHERE (c.first_name LIKE ? AND c.last_name LIKE ?) OR (c.first_name LIKE ? AND c.last_name LIKE ?) "
                    "ORDER BY c.last_name, c.first_name, pc.person_key",
                    (parts[0] + "%", parts[-1] + "%", parts[-1] + "%", parts[0] + "%"))

def person_profiles(db, person_key):
    return db.query(
        "SELECT pc.source_system, COALESCE(pc.salesforce_id, pc.hmis_personal_id) AS source_id, pc.link_method, "
        "pc.linked_since, pc.is_deleted, c.first_name, c.middle_name, c.last_name, c.date_of_birth, c.dob_data_quality, "
        "c.mobile_phone, c.email, c.city, c.postal_code "
        "FROM rpt.person_client pc JOIN dbo.dim_client c ON c.client_key = pc.client_key AND c.is_current = 1 "
        "WHERE pc.person_key = ? ORDER BY pc.source_system, source_id", (int(person_key),))

def person_decisions(db, profiles):
    """Active decisions that involve any of these profiles."""
    if profiles is None or not len(profiles):
        return pd.DataFrame()
    conds, params = [], []
    for s, i in zip(profiles["source_system"], profiles["source_id"]):
        conds.append("(source_system_a = ? AND source_id_a = ?) OR (source_system_b = ? AND source_id_b = ?)")
        params += [s, i, s, i]
    return db.query(
        "SELECT decision_key, source_system_a, source_id_a, source_system_b, source_id_b, decision, decision_method, "
        "rule_name, decided_by, decided_datetime, notes FROM dbo.match_decision WHERE is_active = 1 AND ("
        + " OR ".join(conds) + ") ORDER BY decided_datetime DESC", tuple(params))

def load_conflicts(db):
    return db.query(
        "SELECT r.source_record_id AS decision_key, r.referenced_client_id AS pair, r.detected_datetime "
        "FROM dbo.stage_load_reject r WHERE r.entity_name = 'person_link' AND r.run_id = ("
        "SELECT MAX(g.run_id) FROM dbo.stage_load_log g WHERE g.entity_name = 'person_client_link' "
        "AND g.load_status = 'SUCCESS') ORDER BY r.source_record_id")

def load_worklist(db):
    return db.query("SELECT task_id, task_type, person_key, apricot_record_id, action, source_system, source_id, "
                    "live_enrollments, live_services, live_case_notes, since "
                    "FROM dbo.v_apricot_worklist ORDER BY task_type, task_id, action")


# ---------------- CELL 3: SCREENS ----------------
import html
import ipywidgets as W
from IPython.display import display

DISPLAY_TIMEZONE = "America/Los_Angeles"     # the warehouse stores UTC; the results log shows this time zone
DECISION_LABEL = {"match": "Same person", "no_match": "Not the same person", "unsure": "Not sure / withdrawn"}

def _esc(v):
    return html.escape("" if _blank(v) else str(v))

def _local(v):
    if _blank(v):
        return ""
    t = pd.Timestamp(v)
    t = t.tz_localize("UTC") if t.tzinfo is None else t
    return t.tz_convert(DISPLAY_TIMEZONE).strftime("%Y-%m-%d %I:%M %p")

def _now_local():
    return pd.Timestamp.now(tz="UTC").tz_convert(DISPLAY_TIMEZONE).strftime("%I:%M:%S %p")

def load_decision_log(db, mine_only=True, limit=200):
    """Reviewer decisions saved in the warehouse, newest first, including ones later replaced."""
    mine = " AND decided_by = USER_NAME()" if mine_only else ""
    return db.query(
        f"SELECT TOP {int(limit)} decided_datetime, decided_by, source_system_a, source_id_a, source_system_b, source_id_b, "
        "decision, is_active, revoked_datetime, notes FROM dbo.match_decision "
        f"WHERE decision_method = 'reviewer'{mine} ORDER BY decided_datetime DESC, decision_key DESC")

def _pair_text(sa, ia, sb, ib, name_a="", name_b=""):
    """HTML for a pair; every value is escaped here."""
    a = _esc(f"{source_label(sa)} {ia}" + (f" ({name_a})" if name_a else ""))
    b = _esc(f"{source_label(sb)} {ib}" + (f" ({name_b})" if name_b else ""))
    return f"{a} &harr; {b}"

class ReviewApp:
    def __init__(self, db):
        self.db = db
        self.queue = pd.DataFrame()
        self.pos = 0
        self.decided = 0
        self.busy = False
        self.session_log = []         # this session's actions, newest first
        self.status = W.HTML()
        self.errors = W.Output()      # full error details, for whoever supports the notebook
        self.tabs = W.Tab(children=[self._queue_tab(), self._log_tab(), self._lookup_tab(), self._conflicts_tab(),
                                    self._worklist_tab()])
        for i, t in enumerate(["Review queue", "Results log", "Person lookup", "Conflicts", "Apricot"]):
            self.tabs.set_title(i, t)
        self.tabs.observe(self._on_tab, names="selected_index")
        # the status line sits right under the buttons, where the reviewer is looking
        self.widget = W.VBox([W.HTML("<b>Matching review</b> &nbsp; Decisions take effect at the next pipeline run."),
                              self.tabs, self.status, self.errors])
        self.reload_queue()

    def say(self, text, ok=True, working=False):
        color = "#57606a" if working else ("#1a7f37" if ok else "#b42318")
        self.status.value = f"<p style='color:{color};margin:6px 0'><b>{_esc(text)}</b></p>"

    def _all_buttons(self):
        return [self.b_same, self.b_diff, self.b_unsure, self.b_skip, self.b_reload, self.l_go, self.l_open,
                self.l_diff, self.c_refresh, self.w_refresh, self.r_refresh, self.r_mine]

    def _act(self, working_text, fn):
        """Wrap a button: say what is happening at once, grey out every button until it finishes,
        ignore extra clicks meanwhile, and show any error on screen instead of failing silently."""
        def handler(*_):
            if self.busy:
                return
            self.busy = True
            for b in self._all_buttons():
                b.disabled = True
            self.say(working_text, working=True)
            try:
                fn()
            except Exception as e:
                import traceback
                self.say(f"Something went wrong: {type(e).__name__}: {e}. Nothing is lost: clicking again is safe.",
                         ok=False)
                self._log(working_text.rstrip(". "), "", "failed", f"{type(e).__name__}: {e}", ok=False)
                with self.errors:
                    traceback.print_exc()
            finally:
                for b in self._all_buttons():
                    b.disabled = False
                self._queue_buttons()
                self.busy = False
        return handler

    def _queue_buttons(self):
        empty = self.current() is None
        for b in (self.b_same, self.b_diff, self.b_unsure, self.b_skip):
            b.disabled = empty

    # ---- review queue
    def _queue_tab(self):
        self.q_head = W.HTML()
        self.q_body = W.HTML()
        self.q_notes = W.Text(placeholder="Optional note (what you checked)", layout=W.Layout(width="60%"))
        self.b_same = W.Button(description="Same person", button_style="success")
        self.b_diff = W.Button(description="Not the same person", button_style="danger")
        self.b_unsure = W.Button(description="Not sure")
        self.b_skip = W.Button(description="Skip")
        self.b_reload = W.Button(description="Reload queue")
        self.b_same.on_click(self._act("Saving: same person...", lambda: self.decide_current("match")))
        self.b_diff.on_click(self._act("Saving: not the same person...", lambda: self.decide_current("no_match")))
        self.b_unsure.on_click(self._act("Saving: not sure...", lambda: self.decide_current("unsure")))
        self.b_skip.on_click(self._act("Skipping...", lambda: self.move(1)))
        self.b_reload.on_click(self._act("Reloading the queue...", self.reload_queue))
        return W.VBox([self.q_head, self.q_body, self.q_notes,
                       W.HBox([self.b_same, self.b_diff, self.b_unsure, self.b_skip, self.b_reload])])

    def reload_queue(self):
        self.queue = load_queue(self.db)
        self.pos = 0
        self.show_pair()
        self.say(f"{len(self.queue)} pair(s) waiting for review.")

    def current(self):
        if self.pos < len(self.queue):
            return self.queue.iloc[self.pos].to_dict()
        return None

    def show_pair(self):
        row = self.current()
        self._queue_buttons()
        if row is None:
            self.q_head.value = f"<p>The queue is empty ({self.decided} decided this session).</p>"
            self.q_body.value = ""
            return
        p = row.get("match_probability")
        score = "" if _blank(p) else f"{float(p):.1%}"
        tag = {"unsure": " &nbsp;<i>withdrawn earlier, needs a fresh look</i>",
               "evidence_changed": " &nbsp;<i>linked automatically; the evidence changed, so it is still linked until you decide</i>"
               }.get(row.get("queue_status"), "")
        self.q_head.value = (f"<p>Pair {self.pos + 1} of {len(self.queue)} &nbsp; Score {score}{tag}<br>"
                             f"<small>{_esc(row.get('evidence'))}</small></p>")
        hdr = (f"<tr><th></th><th>{_esc(source_label(row['source_system_a']))} {_esc(row['source_id_a'])}</th>"
               f"<th>{_esc(source_label(row['source_system_b']))} {_esc(row['source_id_b'])}</th></tr>")
        body = "".join(
            f"<tr><td><b>{_esc(l)}</b></td><td{' style=background:#fff4e5' if d else ''}>{_esc(a)}</td>"
            f"<td{' style=background:#fff4e5' if d else ''}>{_esc(b)}</td></tr>" for l, a, b, d in pair_table(row))
        self.q_body.value = f"<table style='border-collapse:collapse' cellpadding=4>{hdr}{body}</table>"
        self.q_notes.value = ""

    def move(self, step):
        self.pos = max(0, self.pos + step)
        self.show_pair()
        if self.current() is not None:
            self.say(f"Skipped. It stays in the queue for next time.")

    def decide_current(self, decision):
        row = self.current()
        if row is None:
            return
        outcome, detail = self.db.record_decision(row["source_system_a"], row["source_id_a"],
                                                  row["source_system_b"], row["source_id_b"],
                                                  decision, self.q_notes.value.strip())
        self.decided += 1
        self.say(f"Pair {self.pos + 1} {outcome}: {detail}")
        name = lambda x: " ".join(str(row.get(f"{c}_{x}")) for c in ("first_name", "last_name")
                                  if not _blank(row.get(f"{c}_{x}")))
        self._log(DECISION_LABEL[decision],
                  _pair_text(row["source_system_a"], row["source_id_a"], row["source_system_b"], row["source_id_b"],
                             name("a"), name("b")), outcome, detail)
        self.queue = self.queue.drop(self.queue.index[self.pos]).reset_index(drop=True)
        self.show_pair()

    # ---- results log
    def _log_tab(self):
        self.r_session = W.HTML()
        self.r_mine = W.Checkbox(value=True, description="Only my decisions", indent=False)
        self.r_refresh = W.Button(description="Refresh")
        self.r_saved = W.HTML()
        self.r_refresh.on_click(self._act("Loading saved decisions...", self.show_saved_log))
        self.r_mine.observe(self._act("Loading saved decisions...", self.show_saved_log), names="value")
        self._render_session_log()
        return W.VBox([W.HTML("<b>This session</b>"), self.r_session,
                       W.HTML("<b>Saved in the warehouse</b> &nbsp; <small>reviewer decisions, newest first; times are "
                              f"{_esc(DISPLAY_TIMEZONE)}</small>"),
                       W.HBox([self.r_mine, self.r_refresh]), self.r_saved])

    def _log(self, action, pair, outcome, detail, ok=True):
        self.session_log.insert(0, dict(time=_now_local(), action=action, pair=pair, outcome=outcome,
                                        detail=detail, ok=ok))
        self._render_session_log()

    def _render_session_log(self):
        if not self.session_log:
            self.r_session.value = "<p><i>Nothing yet. Every save and every failure this session shows here.</i></p>"
            return
        ok_n = sum(1 for r in self.session_log if r["ok"])
        bad_n = len(self.session_log) - ok_n
        rows = "".join(
            f"<tr><td>{_esc(r['time'])}</td><td>{_esc(r['action'])}</td><td>{r['pair']}</td>"
            f"<td style='color:{'#1a7f37' if r['ok'] else '#b42318'}'><b>{_esc(r['outcome'])}</b></td>"
            f"<td><small>{_esc(r['detail'])}</small></td></tr>" for r in self.session_log)
        self.r_session.value = (f"<p>{ok_n} saved, {bad_n} failed</p><table cellpadding=4><tr><th>Time</th><th>Action</th>"
                                f"<th>Pair</th><th>Result</th><th>Detail</th></tr>{rows}</table>")

    def show_saved_log(self, *_):
        df = load_decision_log(self.db, mine_only=self.r_mine.value)
        if not len(df):
            self.r_saved.value = "<p><i>No reviewer decisions saved yet.</i></p>"
        else:
            rows = "".join(
                f"<tr><td>{_esc(_local(r.decided_datetime))}</td><td>{_esc(r.decided_by)}</td>"
                f"<td>{_pair_text(r.source_system_a, r.source_id_a, r.source_system_b, r.source_id_b)}</td>"
                f"<td><b>{_esc(DECISION_LABEL.get(r.decision, r.decision))}</b></td>"
                f"<td>{'In effect' if r.is_active else 'Replaced ' + _esc(_local(r.revoked_datetime))}</td>"
                f"<td><small>{_esc(r.notes)}</small></td></tr>" for r in df.itertuples())
            self.r_saved.value = ("<table cellpadding=4><tr><th>Decided</th><th>By</th><th>Pair</th><th>Decision</th>"
                                  f"<th>Status</th><th>Note</th></tr>{rows}</table>")
        if self.busy:
            self.say(f"{len(df)} saved decision(s) shown.")

    def _on_tab(self, *_):
        if self.tabs.selected_index == 1:                  # Results log: show the latest saved decisions
            self._act("Loading saved decisions...", self.show_saved_log)()

    # ---- person lookup
    def _lookup_tab(self):
        self.l_text = W.Text(placeholder="Name, Salesforce Id, Personal ID or person key", layout=W.Layout(width="50%"))
        self.l_go = W.Button(description="Search")
        self.l_results = W.Select(options=[], rows=8, layout=W.Layout(width="90%"))
        self.l_open = W.Button(description="Open person")
        self.l_detail = W.HTML()
        self.l_decisions = W.VBox()
        self.l_pick_a = W.Dropdown(description="Profile")
        self.l_pick_b = W.Dropdown(description="and")
        self.l_diff_note = W.Text(placeholder="Why they are different people", layout=W.Layout(width="50%"))
        self.l_diff = W.Button(description="Not the same person", button_style="danger")
        self.l_go.on_click(self._act("Searching...", self.search))
        self.l_open.on_click(self._act("Opening...", lambda: self.open_person(self.l_results.value)))
        self.l_diff.on_click(self._act("Saving: not the same person...", self.split_selected))
        return W.VBox([W.HBox([self.l_text, self.l_go]), self.l_results, self.l_open, self.l_detail,
                       self.l_decisions, W.HTML("<b>Two of these profiles are different people</b>"),
                       W.HBox([self.l_pick_a, self.l_pick_b]), W.HBox([self.l_diff_note, self.l_diff])])

    def search(self):
        df = search_people(self.db, self.l_text.value)
        self.l_results.options = [(f"Person {r.person_key}: {r.first_name} {r.last_name}, born {r.date_of_birth}, "
                                   f"{source_label(r.source_system)} {r.source_id}", int(r.person_key))
                                  for r in df.itertuples()] if len(df) else []
        self.say(f"{len(df)} profile(s) found" if len(df) else "No profiles found", ok=bool(len(df)))

    def open_person(self, person_key):
        if person_key is None:
            self.say("Search, then pick a profile first.", ok=False)
            return
        self.person_key = int(person_key)
        prof = person_profiles(self.db, self.person_key)
        self.l_profiles = prof
        rows = "".join(
            f"<tr><td>{_esc(source_label(r.source_system))} {_esc(r.source_id)}</td><td>{_esc(r.first_name)} "
            f"{_esc(r.middle_name)} {_esc(r.last_name)}</td><td>{_esc(r.date_of_birth)} {_esc(r.dob_data_quality)}</td>"
            f"<td>{_esc(r.mobile_phone)}</td><td>{_esc(r.email)}</td><td>{_esc(r.city)} {_esc(r.postal_code)}</td>"
            f"<td>{_esc(r.link_method)}{' (deleted at source)' if r.is_deleted else ''}</td></tr>" for r in prof.itertuples())
        self.l_detail.value = (f"<p><b>Person {self.person_key}</b> has {len(prof)} profile(s)</p>"
                               f"<table cellpadding=4><tr><th>Profile</th><th>Name</th><th>DOB</th><th>Mobile</th>"
                               f"<th>Email</th><th>Place</th><th>Linked by</th></tr>{rows}</table>")
        opts = [(f"{source_label(r.source_system)} {r.source_id}", (r.source_system, r.source_id)) for r in prof.itertuples()]
        self.l_pick_a.options = opts
        self.l_pick_b.options = opts
        if len(opts) > 1:
            self.l_pick_b.value = opts[1][1]
        dec = person_decisions(self.db, prof)
        items = []
        for r in dec.itertuples():
            label = W.HTML(f"{_esc(source_label(r.source_system_a))} {_esc(r.source_id_a)} &harr; "
                           f"{_esc(source_label(r.source_system_b))} {_esc(r.source_id_b)}: <b>{_esc(r.decision)}</b> "
                           f"by {_esc(r.decision_method)}{' (' + _esc(r.rule_name) + ')' if not _blank(r.rule_name) else ''}, "
                           f"{_esc(r.decided_by)} {_esc(r.decided_datetime)}")
            if r.decision == "match":
                b = W.Button(description="Withdraw", tooltip="Unlink: not sure they are the same person")
                b.on_click(self._act("Withdrawing the link...", lambda r=r: self.withdraw(r)))
                items.append(W.HBox([label, b]))
            else:
                items.append(label)
        self.l_decisions.children = [W.HTML("<b>Decisions</b>")] + items if items else [W.HTML("<i>No decisions: every profile stands alone.</i>")]

    def withdraw(self, r):
        outcome, detail = self.db.record_decision(r.source_system_a, r.source_id_a, r.source_system_b,
                                                  r.source_id_b, "unsure", "Withdrawn in person lookup")
        self._log("Withdrawn", _pair_text(r.source_system_a, r.source_id_a, r.source_system_b, r.source_id_b),
                  outcome, detail)
        self.open_person(self.person_key)
        self.say(f"{outcome}: {detail}")

    def split_selected(self):
        a, b = self.l_pick_a.value, self.l_pick_b.value
        if not a or not b or a == b:
            self.say("Pick two different profiles.", ok=False)
            return
        outcome, detail = self.db.record_decision(a[0], a[1], b[0], b[1], "no_match", self.l_diff_note.value.strip())
        self._log("Not the same person", _pair_text(a[0], a[1], b[0], b[1]), outcome, detail)
        self.open_person(self.person_key)
        self.say(f"{outcome}: {detail}")

    # ---- conflicts and Apricot worklist
    def _conflicts_tab(self):
        self.c_out = W.HTML()
        b = self.c_refresh = W.Button(description="Refresh")
        b.on_click(self._act("Loading conflicts...", self.show_conflicts))
        self.show_conflicts()
        return W.VBox([W.HTML("A reviewer said these two are different people, but other confirmed links still join "
                              "them. Open either profile in Person lookup and withdraw the link that is wrong."),
                       b, self.c_out])

    def show_conflicts(self):
        df = load_conflicts(self.db)
        if self.busy:
            self.say(f"{len(df)} conflict(s).")
        self.c_out.value = ("<p>No conflicts.</p>" if not len(df) else
                            "<table cellpadding=4><tr><th>Decision</th><th>Pair</th><th>Found</th></tr>" +
                            "".join(f"<tr><td>{_esc(r.decision_key)}</td><td>{_esc(r.pair)}</td>"
                                    f"<td>{_esc(r.detected_datetime)}</td></tr>" for r in df.itertuples()) + "</table>")

    def _worklist_tab(self):
        self.w_out = W.HTML()
        b = self.w_refresh = W.Button(description="Refresh")
        b.on_click(self._act("Loading the Apricot worklist...", self.show_worklist))
        self.show_worklist()
        return W.VBox([W.HTML("Do these in Apricot. A task disappears after the next Apricot import records the change."),
                       b, self.w_out])

    def show_worklist(self):
        df = load_worklist(self.db)
        if self.busy:
            self.say(f"{len(df)} Apricot task row(s).")
        self.w_out.value = ("<p>Nothing to do in Apricot.</p>" if not len(df) else
                            "<table cellpadding=4><tr><th>Task</th><th>Apricot profile</th><th>Do this</th>"
                            "<th>Source profile</th><th>Enrollments</th><th>Services</th><th>Notes</th></tr>" +
                            "".join(f"<tr><td>{_esc(r.task_id)}</td><td>{_esc(r.apricot_record_id)}</td><td>{_esc(r.action)}</td>"
                                    f"<td>{_esc(source_label(r.source_system))} {_esc(r.source_id)}</td>"
                                    f"<td>{_esc(r.live_enrollments)}</td><td>{_esc(r.live_services)}</td>"
                                    f"<td>{_esc(r.live_case_notes)}</td></tr>" for r in df.itertuples()) + "</table>")


# ---------------- CELL 4: RUN ----------------
if __name__ == "__main__":
    app = ReviewApp(WarehouseDB(WAREHOUSE_SERVER, WAREHOUSE_NAME))
    display(app.widget)
