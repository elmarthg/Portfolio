"""Generate two weeks of synthetic source exports for the demo, plus the answer key.

    python data/generate.py [OUT_DIR]          (default: data/bronze)

Writes
    <OUT>/week1/hmis_lac/<entity>/<file>_<date>.csv   Clarity HMIS export (Looker CSVs)
    <OUT>/week1/sf_cis/contact.csv                    Salesforce Contact pull
    <OUT>/week2/...                                   the same sources one week later, with changes
    <OUT>/zips.csv                                    ZIP -> county reference
    <OUT>/truth.json                                  which profiles are really the same person, and what
                                                      was planted, so the demo can score the matching

The data is always generated against one fixed anchor date, so every run produces the same people,
records and planted cases. Then every date except birthdates is shifted so that week 1 is a week ago and
week 2 is today, and the notebooks' freshness gates pass the same way they would in production.

What is planted, so each part of the pipeline has something to do:
  * Clarity duplicates: the same person entered twice in Clarity under two Personal IDs (typos, nicknames).
  * Cross-system people: veterans served in Clarity who are also Salesforce contacts.
  * Salesforce duplicates: a second contact for the same person with the same phone (exact-rule link).
  * Look-alikes: different people who share a name and a near date of birth (must NOT be linked).
  * A duplicate the matcher can't see (different name entirely), merged by a Clarity admin in week 2.
  * Week 2 changes: name and address edits (client history), exits, new people, records deleted at
    the source, and an enrollment with a mistyped year (held back for correction, not loaded).
"""
import copy, csv, datetime as dt, json, os, random, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import clarity_export as cx
import salesforce_export as sx

# Clarity client row positions (see clarity_export.CLI_H)
C_UPDATED, C_DOB, C_FIRST, C_MIDDLE, C_PID, C_UID, C_VET, C_CITY, C_ZIP, C_LAST = 2, 1, 4, 6, 8, 11, 16, 17, 19, 20
# Clarity enrollment row positions (see clarity_export.ENR_H)
E_ACTIVE, E_CREATED, E_ID, E_HH, E_START, E_EXIT, E_PID, E_UID, E_PROG, E_UPDATED = 0, 2, 3, 4, 8, 9, 14, 15, 16, 17
E_DEST, E_DEST_CAT, E_REASON, E_SCREEN_ADDED = 26, 27, 28, 29

NICK = {"Robert": "Bob", "Michael": "Mike", "James": "Jim", "Anthony": "Tony", "David": "Dave",
        "Patricia": "Patty", "Angela": "Angie", "Teresa": "Terry", "Carlos": "Charlie", "Luis": "Lou",
        "William": "Bill", "Gregory": "Greg", "Raymond": "Ray", "Kevin": "Kev", "Victor": "Vic",
        "Sandra": "Sandy", "Brenda": "Bren", "Janet": "Jan", "Alicia": "Ally", "Denise": "Dee"}


def typo(rnd, name):
    """A plausible data-entry variant of a name: a nickname, a dropped letter or a wrong letter."""
    if name in NICK and rnd.random() < 0.5:
        return NICK[name]
    i = rnd.randrange(1, len(name))
    if rnd.random() < 0.5 or len(name) <= 3:
        return name[:i] + rnd.choice("aeiourstln") + name[i + 1:]
    return name[:i] + name[i + 1:]


def iso(d):
    return d.isoformat() if d else ""


ANCHOR = dt.date(2026, 11, 23)        # the data is built as if today were this date, then shifted
_DATE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")


def shift_dates(value, days):
    """Move every YYYY-MM-DD inside a value by `days` (time parts like 'T17:00:00' are left as they are)."""
    if not days or not isinstance(value, str) or not value:
        return value
    return _DATE.sub(lambda m: (dt.date(int(m[1]), int(m[2]), int(m[3])) + dt.timedelta(days=days)).isoformat(), value)


def generate(out, today=None):
    real_today = today or dt.date.today()
    shift = (real_today - ANCHOR).days
    today = ANCHOR
    w1, w2 = today - dt.timedelta(days=7), today
    rnd = random.Random(42)
    truth, planted = {}, {}

    # ------------------------------------------------------------------ week 1: Clarity
    tables, clients = cx.build(iso(w1))
    cli = tables["client"][2]
    enr = tables["enrollment"][2]
    hm = tables["household_member"][2]
    for c in clients:
        truth[f"hmis_lac:{c['pid']}"] = f"P-{c['pid']}"
    template = next(r for r in enr if r[E_PROG] == "2107")      # street outreach: one grant, no beds
    counters = {"pid": 499000, "enr": 990000, "hh": 390000}

    def new_enrollment(pid, uid, start, exit_=None):
        counters["enr"] += 1
        counters["hh"] += 1
        r = list(template)
        r[E_ACTIVE] = "No" if exit_ else "Yes"
        r[E_CREATED] = iso(start)
        r[E_ID] = f"E{counters['enr']}"
        r[E_HH] = f"H{counters['hh']}"
        r[E_START], r[E_EXIT] = iso(start), iso(exit_)
        r[E_PID], r[E_UID] = pid, uid
        r[E_UPDATED] = iso(min(exit_ or w1, w1))
        r[20], r[21] = "Self (head of household)", "Yes"
        r[E_DEST] = r[E_DEST_CAT] = r[E_REASON] = r[E_SCREEN_ADDED] = ""
        if exit_:
            r[E_DEST], r[E_DEST_CAT], r[E_REASON], r[E_SCREEN_ADDED] = (
                "Staying or living with family, temporary tenure", "Temporary", "Completed program", iso(exit_))
        hm_row = [iso(start), "Individual", "Head of Household", "No" if exit_ else "Yes", uid, r[E_HH], "Adult only"]
        return r, hm_row

    def new_client(base_row, first, last, dob, zip_, city, created):
        counters["pid"] += 1
        r = list(base_row)
        r[C_PID] = str(counters["pid"])
        r[C_UID] = f"{rnd.getrandbits(40):010x}"
        r[C_FIRST], r[C_LAST], r[C_DOB] = first, last, dob
        r[C_ZIP], r[C_CITY] = zip_, city
        r[0], r[C_UPDATED] = iso(created), iso(created)
        r[10], r[21] = "", "Data not collected"
        return r

    adults = [c for c in clients if c["kind"] == "adult" and c["row"][C_DOB]]
    rnd.shuffle(adults)

    # Clarity duplicates: a second profile for the same person, entered later by someone else
    planted["clarity_duplicates"] = []
    for c in adults[:12]:
        o = c["row"]
        first, last = o[C_FIRST], o[C_LAST]
        if rnd.random() < 0.5:
            first = typo(rnd, first)
        else:
            last = typo(rnd, last)
        created = w1 - dt.timedelta(days=rnd.randint(20, 200))
        d = new_client(o, first, last, o[C_DOB], o[C_ZIP], o[C_CITY], created)
        cli.append(d)
        e, h = new_enrollment(d[C_PID], d[C_UID], created, created + dt.timedelta(days=rnd.randint(5, 40)))
        enr.append(e)
        hm.append(h)
        truth[f"hmis_lac:{d[C_PID]}"] = truth[f"hmis_lac:{o[C_PID]}"]
        planted["clarity_duplicates"].append([o[C_PID], d[C_PID]])

    # A duplicate no matcher could find (entered under another name), merged by a Clarity admin in week 2
    hidden = adults[12]
    o = hidden["row"]
    created = w1 - dt.timedelta(days=90)
    hidden_dup = new_client(o, "Sam" if o[C_FIRST] != "Sam" else "Alex", "Okonkwo", o[C_DOB], "", "", created)
    cli.append(hidden_dup)
    e, h = new_enrollment(hidden_dup[C_PID], hidden_dup[C_UID], created, created + dt.timedelta(days=30))
    enr.append(e)
    hm.append(h)
    truth[f"hmis_lac:{hidden_dup[C_PID]}"] = truth[f"hmis_lac:{o[C_PID]}"]
    planted["clarity_admin_merge"] = {"kept": o[C_PID], "merged_away": hidden_dup[C_PID]}

    # ------------------------------------------------------------------ week 1: Salesforce
    contacts, n = [], 0

    def add_contact(identity=None, **override):
        nonlocal n
        n += 1
        c = sx.contact(n, rnd, w1, identity)
        c.update(override)
        contacts.append(c)
        return c

    # veterans served in Clarity who are also Salesforce contacts
    planted["cross_system"] = []
    vets = [c for c in adults[13:] if c["row"][C_VET] == "Yes"][:40]
    for v in vets:
        r = v["row"]
        first, last, dob = r[C_FIRST], r[C_LAST], dt.date.fromisoformat(r[C_DOB])
        roll = rnd.random()
        if roll < 0.25:
            first = typo(rnd, first)
        elif roll < 0.35 and dob.day <= 12 and dob.day != dob.month:
            dob = dob.replace(month=dob.day, day=dob.month)       # day and month swapped at entry
        sex = {"Woman (Girl, if child)": "Female", "Man (Boy, if child)": "Male"}.get(r[5])
        c = add_contact(dict(first=first, last=last, dob=dob, zip=r[C_ZIP], city=r[C_CITY]), Sex__c=sex)
        truth[f"sf_cis:{c['Id']}"] = truth[f"hmis_lac:{r[C_PID]}"]
        planted["cross_system"].append([r[C_PID], c["Id"]])

    # people only in Salesforce
    for _ in range(340):
        c = add_contact()
        truth[f"sf_cis:{c['Id']}"] = f"S-{c['Id']}"

    # Salesforce duplicates: a second contact, same name, date of birth and phone
    planted["salesforce_duplicates"] = []
    sf_only = [c for c in contacts if truth[f"sf_cis:{c['Id']}"].startswith("S-")]
    for orig in sf_only[:5]:
        dob = dt.date.fromisoformat(orig["Birthdate"])
        same = {k: orig[k] for k in ("Phone", "Sex__c", "Race__c", "Hispanic_or_Latino__c", "Service_Branch__c")}
        c = add_contact(dict(first=orig["FirstName"], last=orig["LastName"], dob=dob, zip=orig["MailingPostalCode"],
                             city=orig["MailingCity"]), **same)
        truth[f"sf_cis:{c['Id']}"] = truth[f"sf_cis:{orig['Id']}"]
        planted["salesforce_duplicates"].append([orig["Id"], c["Id"]])

    # look-alikes: same name, birth year one apart, different phone and address (a father and son)
    planted["look_alikes"] = []
    for orig in sf_only[5:9]:
        dob = dt.date.fromisoformat(orig["Birthdate"])
        dob2 = dob.replace(year=dob.year + 1) if not (dob.month == 2 and dob.day == 29) else dob + dt.timedelta(days=366)
        c = add_contact(dict(first=orig["FirstName"], last=orig["LastName"], dob=dob2, zip=orig["MailingPostalCode"],
                             city=orig["MailingCity"]))
        truth[f"sf_cis:{c['Id']}"] = f"S-{c['Id']}"
        planted["look_alikes"].append([orig["Id"], c["Id"]])

    week1 = {"tables": tables, "contacts": contacts}

    # ------------------------------------------------------------------ week 2: the same sources, a week later
    t2 = copy.deepcopy(tables)
    c2 = copy.deepcopy(contacts)
    cli2, enr2, hm2, svc2 = t2["client"][2], t2["enrollment"][2], t2["household_member"][2], t2["service"][2]
    changes = {}
    protected = {p for pair in planted["clarity_duplicates"] for p in pair} | {o[C_PID], hidden_dup[C_PID]}

    # name and address changes (each makes a new client version; the old one is kept)
    edited = [c for c in adults[60:] if c["row"][C_PID] not in protected][:6]
    for c in edited:
        for r in cli2:
            if r[C_PID] == c["row"][C_PID]:
                if rnd.random() < 0.5:
                    r[C_LAST] = rnd.choice(["Castillo", "Bennett", "Morales", "Foster"])
                else:
                    r[C_ZIP], r[C_CITY] = "90044", "Los Angeles"
                r[C_UPDATED] = iso(w2 - dt.timedelta(days=1))
    changes["clarity_clients_edited"] = len(edited)

    # exits: open enrollments of one-person households with no update screens
    by_eid = {}
    for r in enr2:
        by_eid.setdefault(r[E_ID], []).append(r)
    hh_size = {}
    for r in enr2:
        hh_size.setdefault(r[E_HH], set()).add(r[E_ID])
    open_single = [eid for eid, rows in by_eid.items()
                   if not rows[0][E_EXIT] and len(hh_size[rows[0][E_HH]]) == 1
                   and all(not x[E_SCREEN_ADDED] for x in rows) and rows[0][E_PID] not in protected]
    exit_day = w2 - dt.timedelta(days=2)
    for eid in sorted(open_single)[:12]:
        for r in by_eid[eid]:
            r[E_ACTIVE], r[E_EXIT], r[E_UPDATED] = "No", iso(exit_day), iso(exit_day)
            r[E_DEST], r[E_DEST_CAT], r[E_REASON], r[E_SCREEN_ADDED] = (
                "Rental by client, with ongoing housing subsidy", "Permanent", "Completed program", iso(exit_day))
    changes["clarity_exits"] = min(12, len(open_single))

    # new people this week
    base = adults[0]["row"]
    for i in range(15):
        first = rnd.choice(["Nadia", "Omar", "Priya", "Tomas", "Wendell", "Ingrid", "Kofi", "Lucia"])
        last = rnd.choice(["Abara", "Brandt", "Chavez", "Duarte", "Esposito", "Fujita", "Gill", "Haddad"])
        dob = iso(dt.date(1960, 1, 1) + dt.timedelta(days=rnd.randint(0, 14000)))
        start = w2 - dt.timedelta(days=rnd.randint(1, 6))
        r = new_client(base, first, last, dob, "90013", "Los Angeles", start)
        cli2.append(r)
        e, h = new_enrollment(r[C_PID], r[C_UID], start)
        enr2.append(e)
        hm2.append(h)
        truth[f"hmis_lac:{r[C_PID]}"] = f"P-{r[C_PID]}"
    changes["clarity_new_people"] = 15

    # an enrollment typed with the wrong year: held back by the loader and listed for correction
    r = new_client(base, "Yesenia", "Quintero", "1984-04-02", "90011", "Los Angeles", w2 - dt.timedelta(days=1))
    cli2.append(r)
    bad_start = (w2 - dt.timedelta(days=3)).replace(year=2104)
    e, h = new_enrollment(r[C_PID], r[C_UID], bad_start)
    e[E_CREATED] = e[E_UPDATED] = iso(w2 - dt.timedelta(days=1))
    enr2.append(e)
    hm2.append(h)
    truth[f"hmis_lac:{r[C_PID]}"] = f"P-{r[C_PID]}"
    planted["held_back_enrollment"] = e[E_ID]

    # services deleted at the source (flagged deleted in the warehouse, never physically removed)
    gone = sorted({r[0] for r in svc2})[:4]
    t2["service"] = (t2["service"][0], t2["service"][1], [r for r in svc2 if r[0] not in gone])
    changes["clarity_services_removed"] = len(gone)

    # the admin merge: the hidden duplicate's enrollments move to the kept Personal ID and it leaves the export
    keep = next(r for r in cli2 if r[C_PID] == o[C_PID])
    for r in enr2:
        if r[E_PID] == hidden_dup[C_PID]:
            r[E_PID], r[E_UID] = keep[C_PID], keep[C_UID]
    for r in hm2:
        if r[4] == hidden_dup[C_UID]:
            r[4] = keep[C_UID]
    t2["client"] = (t2["client"][0], t2["client"][1], [r for r in cli2 if r[C_PID] != hidden_dup[C_PID]])

    # Salesforce: edits, exits, new contacts, contacts deleted in Salesforce
    shared = {p[1] for p in planted["cross_system"]} | {x for p in planted["salesforce_duplicates"] + planted["look_alikes"] for x in p}
    plain = [c for c in c2 if c["Id"] not in shared]
    for c in plain[:6]:
        c["MobilePhone"] = f"(323) 555-{rnd.randint(1000, 9999)}"
        c["MailingStreet"] = f"{rnd.randint(100, 9999)} Harbor Way"
        c["LastModifiedDate"] = f"{w2 - dt.timedelta(days=1)}T17:00:00.000+0000"
    exits = [c for c in plain[6:] if c["Date_Enrolled__c"] and not c["Exited_Date__c"]][:8]
    for c in exits:
        c["Exited_Date__c"], c["Contact_Status__c"] = iso(w2 - dt.timedelta(days=2)), "Exited"
        c["LastModifiedDate"] = f"{w2 - dt.timedelta(days=1)}T17:00:00.000+0000"
    deleted = [c["Id"] for c in plain[40:43]]
    c2 = [c for c in c2 if c["Id"] not in deleted]
    for _ in range(10):
        c = sx.contact(n + 1, rnd, w2)
        n += 1
        c2.append(c)
        truth[f"sf_cis:{c['Id']}"] = f"S-{c['Id']}"
    changes.update(salesforce_contacts_edited=6, salesforce_exits=len(exits), salesforce_contacts_deleted=3,
                   salesforce_new_contacts=10)
    week2 = {"tables": t2, "contacts": c2}

    # ------------------------------------------------------------------ write
    def shifted_tables(tables):
        out_t = {}
        for entity, (stem, header, rows) in tables.items():
            keep = {C_DOB} if entity == "client" else set()      # birthdates are not moved
            out_t[entity] = (stem, header, [[v if i in keep else shift_dates(v, shift) for i, v in enumerate(r)] for r in rows])
        return out_t

    def shifted_contacts(contacts):
        return [{k: v if k == "Birthdate" else shift_dates(v, shift) for k, v in c.items()} for c in contacts]

    w1, w2 = w1 + dt.timedelta(days=shift), w2 + dt.timedelta(days=shift)
    for label, day, wk in (("week1", w1, week1), ("week2", w2, week2)):
        cx.write(shifted_tables(wk["tables"]), os.path.join(out, label, "hmis_lac"), iso(day))
        os.makedirs(os.path.join(out, label, "sf_cis"), exist_ok=True)
        with open(os.path.join(out, label, "sf_cis", "contact.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=sx.COLUMNS)
            w.writeheader()
            w.writerows(shifted_contacts(wk["contacts"]))
    with open(os.path.join(out, "zips.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["zip", "city", "state_id", "county_name"])
        w.writerows(sx.ZIPS)
    json.dump({"week1": iso(w1), "week2": iso(w2), "truth": truth, "planted": planted, "week2_changes": changes},
              open(os.path.join(out, "truth.json"), "w"), indent=1)
    return {"week1": iso(w1), "week2": iso(w2), "clarity_clients": len(cli), "salesforce_contacts": len(contacts),
            "people": len(set(truth.values()))}


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "bronze")
    print(generate(out))
