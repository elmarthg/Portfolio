"""Synthetic Clarity HMIS export (Looker-style CSVs) for the demo.

Every person, name, ID, phone and note here is invented. The fictional agency is Northwind Community
Services. Column headers and the export's quirks match a real Clarity Looker export: one enrollment row
per funding source and per update/exit screen, one program row per site, and repeated client rows.
No SSNs: the SSN column holds only blanks and masked partials (XXX-XX-1234), so nothing in the demo can
be a real person's number.
"""
import csv, os, random, datetime as dt

HEADERS = {}


def build(export_date, n_clients=600, seed=20261113):
    """Return ({entity: (file_stem, header, rows)}, clients). clients carry the identity fields the
    demo's other generators reuse (Salesforce overlap, planted duplicates)."""
    EXPORT_DATE = export_date
    N_CLIENTS = n_clients
    rnd = random.Random(seed)
    D = dt.date

    def day(d): return d.isoformat() if d else ""
    def stamp(d): return f"{d.isoformat()} 00:00:00" if d else ""
    def rdate(a, b): return a + dt.timedelta(days=rnd.randint(0, max(0, (b - a).days)))
    EXPORT = D.fromisoformat(EXPORT_DATE)

    # ---------------- organization ----------------
    AGENCY = ["66", "Northwind Community Services", "Common", "CA-600", "Los Angeles County",
              "100 Harbor Way, Ste. 200", "Long Beach", "CA", "90802"]
    ORG_H = ("Agencies Agency ID,Agencies Agency Name,Agencies Agency Type,Agencies CoC Code,Agencies County Name,"
             "Agencies Address,Agencies City,Agencies State,Agencies ZIP Code")

    # ---------------- programs ----------------
    # id, name, HUD project type code, start, end, active, target population, sites, beds, family share
    PROGRAMS = [
        ("2101", "Northwind Emergency Shelter", "0", D(2015, 7, 1), None, "Yes", "", [("200 Cedar Ave", "90201"), ("210 Cedar Ave", "90201")], 60, 0.0),
        ("2102", "Northwind Youth Shelter", "0", D(2016, 1, 1), None, "Yes", "Youth", [("300 Maple St", "90028")], 24, 0.0),
        ("2103", "Northwind Transitional Village", "2", D(2012, 10, 1), None, "Yes", "", [("400 Birch Blvd", "90025")], 30, 0.6),
        ("2104", "Northwind Veterans Rapid Rehousing", "13", D(2019, 10, 1), None, "Yes", "Veterans", [("500 Oak St", "90015")], 0, 0.35),
        ("2105", "Northwind Veterans Homelessness Prevention", "12", D(2019, 10, 1), None, "Yes", "Veterans", [("500 Oak St", "90015")], 0, 0.35),
        ("2106", "Northwind Permanent Supportive Housing", "3", D(2017, 7, 1), None, "Yes", "", [("600 Elm St", "90015")], 0, 0.2),
        ("2107", "Northwind Street Outreach", "4", D(2020, 1, 1), None, "Yes", "", [("700 Pine Ave", "90042")], 0, 0.0),
        ("2108", "Northwind Coordinated Entry Access", "14", D(2018, 7, 1), D(2025, 6, 30), "No", "", [("700 Pine Ave", "90042")], 0, 0.1),
    ]
    PROG = {p[0]: p for p in PROGRAMS}
    PRG_H = ("Programs Added Date,Programs Agency CoC Code,Programs Agency Name,Programs Address,Programs Address2,"
             "Programs State,Programs ZIP Code,Programs Full Name,Programs Operating End Date,Programs Operating Start Date,"
             "Programs Program Active,Programs Program ID,Program Sites Address,Program Sites Address2,Program Sites State,"
             "Program Sites ZIP Code,Programs Target Population,Agency Creating Profile Agency Name,Programs Project Type Code")
    prog_rows = []
    for pid, name, ptype, start, end, active, pop, sites, beds, fam in PROGRAMS:
        for addr, zip_ in sites:
            prog_rows.append([day(start), "CA-600", "Northwind Community Services", sites[0][0], "", "CA", sites[0][1], name,
                              day(end), day(start), active, pid, addr, "", "CA", zip_, pop, "Northwind Community Services", ptype])

    # ---------------- funding sources (grants belong to programs) ----------------
    FUNDING = {
        "2101": [("G2101A", "COUNTY-ES-2526", "County Emergency Shelter Grant", "Local or Other Funding Source", "46", 1850000)],
        "2102": [("G2102A", "COUNTY-YOUTH-2526", "County Youth Shelter Grant", "Local or Other Funding Source", "46", 720000)],
        "2103": [("G2103A", "HUD-TH-DEMO-01", "HUD CoC Transitional Housing", "HUD: CoC - Transitional Housing", "4", 640000),
                 ("G2103B", "CITY-TH-26", "City Bridge Funding", "Local or Other Funding Source", "46", 210000)],
        "2104": [("G2104A", "SSVF-DEMO-RRH", "SSVF Rapid Re-Housing", "VA: SSVF - Rapid Re-Housing", "33", 2400000)],
        "2105": [("G2105A", "SSVF-DEMO-HP", "SSVF Homelessness Prevention", "VA: SSVF - Homelessness Prevention", "34", 900000)],
        "2106": [("G2106A", "HUD-PSH-DEMO-02", "HUD CoC Permanent Supportive Housing", "HUD: CoC - Permanent Supportive Housing", "2", 1300000),
                 ("G2106B", "CITY-PSH-26", "County Supportive Services", "Local or Other Funding Source", "46", 380000)],
        "2107": [("G2107A", "COUNTY-SO-26", "County Street Outreach", "Local or Other Funding Source", "46", 560000)],
        "2108": [("G2108A", "COUNTY-CES-2425", "County CES Access Center", "Local or Other Funding Source", "46", 300000)],
    }
    FUND_H = ("Program Funding Sources ID,Program Funding Sources Grant ID,Program Funding Sources Grant Identifier,"
              "Program Funding Sources Fund Name,Program Funding Sources Funding Source,Program Funding Sources Funding Source Code,"
              "Program Funding Sources Grant Amount,Program Funding Sources Grant Start Date,Program Funding Sources Grant End Date,"
              "Program Funding Sources Amount,Program Funding Sources Currently Active (Yes / No),"
              "Program Funding Sources Direct Expense (Yes / No),Program Funding Sources Eligibility (Yes / No)")
    fund_rows, fid = [], 7000
    for pid, grants in FUNDING.items():
        for gid, ident, fund, source, code, amt in grants:
            fid += 1
            active = "No" if PROG[pid][5] == "No" else "Yes"
            g_start = max(PROG[pid][3], D(2018, 7, 1))
            g_end = PROG[pid][4] or D(2027, 6, 30)
            fund_rows.append([f"F{fid}", gid, ident, fund, source, code, f"${amt:,.2f}", day(g_start), day(g_end),
                              f"{amt * 0.5:.0f}", active, rnd.choice(["Yes", "No"]), "Yes"])

    # ---------------- clients ----------------
    FIRST_W = ["Maria", "Angela", "Denise", "Rosa", "Keisha", "Linda", "Patricia", "Yolanda", "Teresa", "Monique",
               "Sandra", "Lupe", "Tanya", "Grace", "Carmen", "Brenda", "Alicia", "Diane", "Erica", "Janet"]
    FIRST_M = ["James", "Robert", "Carlos", "Michael", "Darnell", "Jose", "David", "Anthony", "Marcus", "Luis",
               "William", "Terrence", "Juan", "Kevin", "Raymond", "Andre", "Miguel", "Gregory", "Victor", "Dwayne"]
    FIRST_K = ["Aiden", "Sofia", "Jayden", "Mia", "Elijah", "Camila", "Noah", "Zoe", "Mateo", "Aaliyah"]
    LAST = ["Garcia", "Johnson", "Hernandez", "Williams", "Lopez", "Brown", "Martinez", "Jackson", "Gonzalez", "Davis",
            "Rodriguez", "Washington", "Perez", "Thomas", "Sanchez", "Harris", "Ramirez", "Robinson", "Torres", "Lewis",
            "Flores", "Walker", "Rivera", "Young", "Nguyen", "Kim", "Reyes", "Moore", "Cruz", "Allen"]
    RACES = (["Black, African American, or African"] * 34 + ["White"] * 14 + ["Hispanic/Latina/e/o"] * 12
             + ["White; Hispanic/Latina/e/o"] * 18 + ["Asian or Asian American"] * 4
             + ["American Indian, Alaska Native, or Indigenous"] * 2 + ["Native Hawaiian or Pacific Islander"] * 1
             + ["Black, African American, or African; Hispanic/Latina/e/o"] * 4 + ["Middle Eastern or North African"] * 1
             + ["Client doesn't know"] * 3 + ["Client prefers not to answer"] * 3 + ["Data not collected"] * 4)
    LA_ZIPS = ["90001", "90002", "90003", "90011", "90013", "90014", "90015", "90021", "90028", "90037", "90042",
               "90044", "90201", "90255", "90262", "90280", "90731", "90802", "90813", "91331", "91402", "91601"]
    CITY = {"90201": "Bell", "90255": "Huntington Park", "90262": "Lynwood", "90280": "South Gate", "90731": "San Pedro",
            "90802": "Long Beach", "90813": "Long Beach", "91331": "Pacoima", "91402": "Panorama City", "91601": "North Hollywood"}
    CLI_H = ("Clients Date Created Date,Clients Date of Birth Date,Clients Date Updated Date,Clients DoB Data Quality,"
             "Clients First Name,Clients Gender,Clients Middle Name,Clients Name Data Quality,Clients Personal ID,"
             "Clients Primary Language,Clients SSN,Clients Unique Identifier,Clients User Created,Clients User Updating,"
             "Clients Branch of Military,Clients Discharge Status,Clients Veteran Status,Clients City,Clients State,"
             "Clients ZIP Code,Clients Last Name,Clients SSN Data Quality,Clients Race and Ethnicity,Clients Marital Status")
    STAFF = ["jmorales", "kchen", "dwilliams", "aortiz", "tnguyen", "rbanks", "lsantos", "mkhan"]
    clients = []

    def new_client(kind, last=None):
        pid = str(400001 + len(clients))
        uid = f"{rnd.getrandbits(40):010x}"
        if kind == "child":
            sexw = rnd.random() < 0.5
            first, dob = rnd.choice(FIRST_K), rdate(D(2009, 1, 1), D(2024, 6, 30))
            gender = "Woman (Girl, if child)" if sexw else "Man (Boy, if child)"
        else:
            sexw = rnd.random() < 0.38
            first = rnd.choice(FIRST_W if sexw else FIRST_M)
            lo = D(1955, 1, 1) if kind == "adult" else D(2001, 1, 1)
            hi = D(2000, 12, 31) if kind == "adult" else D(2008, 11, 1)
            dob = rdate(lo, hi)
            gender = rnd.choices(["Woman (Girl, if child)", "Man (Boy, if child)", "Transgender", "Non-Binary"],
                                 [0.36 if sexw else 0.0, 0.0 if sexw else 0.6, 0.02, 0.02])[0]
            if gender == "Transgender" or gender == "Non-Binary":
                pass
            elif sexw:
                gender = "Woman (Girl, if child)"
            else:
                gender = "Man (Boy, if child)"
        last = last or rnd.choice(LAST)
        middle = rnd.choice(["", "", "", "A", "J", "M", "L", "R"])
        roll = rnd.random()
        if roll < 0.60:   # demo data never holds a full SSN: masked partials and blanks only
            ssn, ssn_dq = f"XXX-XX-{rnd.randint(1000, 9999)}", "Approximate or partial SSN reported"
        else:
            ssn, ssn_dq = "", rnd.choice(["Client doesn't know", "Client prefers not to answer", "Data not collected"])
        dob_dq = "Full DOB reported"
        if kind != "child" and rnd.random() < 0.04:
            dob, dob_dq = None, "Client doesn't know"
        elif rnd.random() < 0.03:
            dob_dq = "Approximate or partial DOB reported"
        vet = "No"
        branch = discharge = ""
        if kind == "adult" and rnd.random() < 0.3:
            vet = "Yes"
            branch = rnd.choice(["Army", "Navy", "Marines", "Air Force", "Coast Guard"])
            discharge = rnd.choices(["Honorable", "General under honorable conditions", "Other than honorable conditions (OTH)",
                                     "Client doesn't know"], [0.7, 0.18, 0.08, 0.04])[0]
        elif kind == "child":
            vet = ""
        zip_ = rnd.choice(LA_ZIPS) if rnd.random() < 0.75 else ""
        if zip_ and rnd.random() < 0.1:
            zip_ = f"{zip_}-{rnd.randint(1000, 9999)}"
        city = CITY.get(zip_[:5], "Los Angeles") if zip_ else ""
        created = rdate(D(2018, 6, 1), D(2026, 10, 15))
        updated = rdate(created, min(EXPORT - dt.timedelta(days=1), created + dt.timedelta(days=700)))
        lang = rnd.choices(["English", "Spanish", "Korean", "Armenian", ""], [0.72, 0.2, 0.02, 0.02, 0.04])[0]
        marital = "" if kind == "child" else rnd.choice(["Single", "Single", "Single", "Married", "Divorced", "Separated", "Widowed"])
        c = dict(pid=pid, uid=uid, kind=kind, created=created, updated=updated, row=[
            day(created), day(dob), day(updated), dob_dq, first, gender, middle, "Full name reported", pid, lang, ssn, uid,
            rnd.choice(STAFF), rnd.choice(STAFF), branch, discharge, vet, city, "CA" if city else "", zip_, last, ssn_dq,
            "" if kind == "child" and rnd.random() < 0.5 else rnd.choice(RACES), marital])
        if c["row"][22] == "":
            c["row"][22] = "Data not collected"
        clients.append(c)
        return c

    # ---------------- enrollments, households, services, notes, beds ----------------
    ENR_H = ("Enrollments Active in Project,Enrollments Assigned Staff,Enrollments Date Created Date,Enrollments Enrollment ID,"
             "Enrollments Household ID,Enrollments Household Move-In Date,Enrollments Household Type,Enrollments Private (Yes / No),"
             "Enrollments Project Start Date,Enrollments Project Exit Date,Enrollments User Creating,Enrollments User Home Agency,"
             "Enrollments User Updating,Enrollments Individual or Family,Clients Personal ID,Clients Unique Identifier,"
             "Programs Program ID,Enrollments Last Updated Date,Programs CoC,Programs Project Type Code,"
             "Entry Screen Relationship to Head of Household,Entry Screen Head of Household (Yes / No),"
             "Entry Screen Housing Move-in Date,Entry Screen Prior Living Situation Category,Program Funding Sources Grant ID,"
             "Program Funding Sources Grant Identifier,Update/Exit Screen Destination,Update/Exit Screen Destination Category,"
             "Update/Exit Screen Exit Reason,Update/Exit Screen Added Date")
    HM_H = ("Clients Household Member Start,Clients Household Type,Clients Member Type,Clients Active in Household?,"
            "Clients Unique Identifier,Enrollments Household ID,Enrollments Household Type")
    SVC_H = ("Services Service ID,Enrollments Enrollment ID,Clients Personal ID,Clients Unique Identifier,Services Start Date Date,"
             "Services End Date Date,Services Service Category,Service Items Service Item ID,Service Items Service Item Name,"
             "Service Items Delivery Type,Services Service Added Date,Services User Creating,Services User Updating,"
             "Services Service Updated Date,Services Services Notes,Services Aftercare Project?,"
             "Services Financial Assistance Start Date,Services Financial Assistance End Date,Service Expenses Amount")
    NOTE_H = ("Client Notes - Enrollment Level ID,Enrollments Enrollment ID,Clients Personal ID,Clients Unique Identifier,"
              "Client Notes - Enrollment Level Case Note Date,Client Notes - Enrollment Level Date Added Date,"
              "Client Notes - Enrollment Level Date Updated Date,Client Notes - Enrollment Level Note,"
              "Client Notes - Enrollment Level Title,Client Notes - Enrollment Level Tracking Hour,"
              "Client Notes - Enrollment Level Tracking Minute,Client Notes - Enrollment Level Private (Yes / No),"
              "Client Notes - Enrollment Level Staff Full Name,Client Notes - Enrollment Level Category")
    BED_H = ("Programs Program ID,Inventory - Building Units ID,Inventory - Unit Configuration Beds ID,"
             "Inventory - Unit Configuration Beds Name,Inventory - Unit Configuration Beds Added Date,"
             "Inventory - Unit Configuration History Status Start Date,Inventory - Building Unit Configurations Household Type,"
             "Inventory - Unit Configuration Beds Last Updated Date,Bed Availability Status Current Availability,"
             "Bed Availability Status Current Status Start Date,Bed Availability Status Current Status End Date,Bed Occupancy ID,"
             "Bed Occupancy Assignment Date,Bed Occupancy Occupation Start Date,Bed Occupancy Occupation End Date,"
             "Bed Occupancy Pending Occupation End Date,Bed Occupancy Type,Bed Status Status,Bed Status Offline Reason,"
             "Bed Status Is Current Bed Status,Bed Status Status Start Date,Bed Status Status End Date,Enrollments Enrollment ID,"
             "Clients Personal ID")
    ROI_H = ("Clients Unique Identifier,Release of Information Active ROI?,Release of Information Added Date,"
             "Release of Information Agency Name,Release of Information Documentation,Release of Information End Date,"
             "Release of Information ID,Release of Information Last Updated Date,Release of Information Start Date,"
             "Release of Information User Creating,Release of Information User Updating")

    PRIOR = [("Homeless situations", 0.62), ("Temporary housing situations", 0.2), ("Institutional situations", 0.08),
             ("Permanent housing situations", 0.1)]
    DEST = [("Rental by client, no ongoing housing subsidy", "Permanent"), ("Rental by client, with ongoing housing subsidy", "Permanent"),
            ("Staying or living with family, permanent tenure", "Permanent"), ("Staying or living with family, temporary tenure", "Temporary"),
            ("Emergency shelter, including hotel or motel paid for with emergency shelter voucher", "Homeless"),
            ("Place not meant for habitation", "Homeless"), ("Hospital or other residential non-psychiatric medical facility", "Institutional"),
            ("Client doesn't know", "Other"), ("No exit interview completed", "Other")]
    EXIT_REASON = ["Completed program", "Left for housing opp. before completing program", "Non-compliance with program",
                   "Unknown/disappeared", "Reached maximum time allowed by program"]
    SERVICE_ITEMS = {
        "0": [("301", "Shelter bed night", "Case Management", "Individual"), ("302", "Meal", "Basic Needs", "Individual"),
              ("303", "Case management session", "Case Management", "Individual")],
        "2": [("303", "Case management session", "Case Management", "Individual"), ("304", "Life skills class", "Education", "Group")],
        "13": [("303", "Case management session", "Case Management", "Individual"), ("401", "Rental assistance", "Financial Assistance", "Payment"),
               ("402", "Security deposit", "Financial Assistance", "Payment"), ("403", "Utility deposit", "Financial Assistance", "Payment")],
        "12": [("303", "Case management session", "Case Management", "Individual"), ("401", "Rental assistance", "Financial Assistance", "Payment"),
               ("404", "Utility payment", "Financial Assistance", "Payment")],
        "3": [("303", "Case management session", "Case Management", "Individual"), ("305", "Housing stability check-in", "Case Management", "Individual")],
        "4": [("306", "Outreach contact", "Outreach", "Individual"), ("302", "Meal", "Basic Needs", "Individual")],
        "14": [("307", "CES assessment", "Coordinated Entry", "Individual")],
    }
    NOTE_TEXT = ["Met with client to review housing plan.", "Client reported progress on document readiness.",
                 "Follow-up call; client did not answer, left voicemail.", "Completed intake paperwork with client.",
                 "Discussed budgeting and next steps for lease-up.", "Client attended group session.",
                 "Coordinated with landlord on unit inspection.", "Referred client to benefits enrollment clinic."]
    NOTE_TITLES = ["Intake", "Follow-up", "Housing plan", "Phone contact", "Group", "Landlord contact"]

    enr_rows, hm_rows, svc_rows, note_rows, bed_occ = [], [], [], [], []
    counters = {"enr": 0, "hh": 0, "svc": 0, "note": 0}
    LAST_DATE = EXPORT - dt.timedelta(days=2)

    def program_weights():
        return [("2101", 0.24), ("2102", 0.08), ("2103", 0.1), ("2104", 0.14), ("2105", 0.08),
                ("2106", 0.06), ("2107", 0.24), ("2108", 0.06)]

    def stay_dates(pid):
        p = PROG[pid]
        lo = max(p[3], D(2022, 7, 1) if pid != "2106" else D(2018, 3, 1))
        hi = min(p[4] or LAST_DATE, LAST_DATE)
        start = rdate(lo, hi)
        typical = {"0": 75, "2": 300, "13": 240, "12": 120, "3": 1500, "4": 150, "14": 30}[p[2]]
        length = int(rnd.expovariate(1 / typical)) + 1
        end = start + dt.timedelta(days=length)
        if p[4] and end > p[4]:
            end = p[4]
        exit_ = end if end <= LAST_DATE and rnd.random() < 0.9 else None
        if p[4]:
            exit_ = exit_ or p[4]
        return start, exit_

    def add_services(eid, c, pid, start, exit_):
        ptype = PROG[pid][2]
        items = SERVICE_ITEMS[ptype]
        span_end = exit_ or LAST_DATE
        n = min(rnd.randint(0, 8) + (2 if ptype in ("13", "12") else 0), 1 + (span_end - start).days // 7)
        for _ in range(n):
            counters["svc"] += 1
            sid = f"S{800000 + counters['svc']}"
            item = rnd.choice(items)
            sday = rdate(start, span_end)
            fa = item[2] == "Financial Assistance"
            amount = f"${rnd.choice([450, 900, 1250, 1500, 1850, 2100]):,.2f}" if fa else ""
            fa_end = day(sday + dt.timedelta(days=29)) if fa else ""
            added = min(sday + dt.timedelta(days=rnd.randint(0, 5)), LAST_DATE)
            svc_rows.append([sid, eid, c["pid"], c["uid"], day(sday), day(sday), item[2], item[0], item[1], item[3],
                             day(added), rnd.choice(STAFF), rnd.choice(STAFF), day(added), "", "No",
                             day(sday) if fa else "", fa_end, amount])
            if fa and rnd.random() < 0.15:   # a second expense line on the same service, as Clarity exports it
                svc_rows.append(svc_rows[-1][:-1] + [f"${rnd.choice([50, 75, 120]):,.2f}"])

    def add_notes(eid, c, start, exit_):
        span_end = exit_ or LAST_DATE
        for _ in range(min(rnd.randint(0, 6), 1 + (span_end - start).days // 10)):
            counters["note"] += 1
            nday = rdate(start, span_end)
            added = min(nday + dt.timedelta(days=rnd.randint(0, 3)), LAST_DATE)
            note_rows.append([f"N{500000 + counters['note']}", eid, c["pid"], c["uid"],
                              day(nday) if rnd.random() > 0.03 else "", day(added), day(added), rnd.choice(NOTE_TEXT),
                              rnd.choice(NOTE_TITLES), str(rnd.randint(0, 1)), str(rnd.choice([0, 15, 30, 45])),
                              rnd.choice(["No", "No", "No", "Yes"]), f"{rnd.choice(FIRST_W + FIRST_M)} {rnd.choice(LAST)}", "Case Note"])

    def enroll(household, pid, start, exit_, hh_type):
        counters["hh"] += 1
        hid = f"H{300000 + counters['hh']}"
        family = len(household) > 1
        move_in = ""
        if PROG[pid][2] in ("13", "3") and rnd.random() < 0.75:
            mi = start + dt.timedelta(days=rnd.randint(14, 120))
            if mi <= (exit_ or LAST_DATE):
                move_in = day(mi)
        prior = rnd.choices([p[0] for p in PRIOR], [p[1] for p in PRIOR])[0]
        grants = FUNDING[pid]
        exit_screens = []
        if exit_:
            dest, cat = rnd.choice(DEST)
            exit_screens.append((dest, cat, rnd.choice(EXIT_REASON), exit_))
        if (exit_ or LAST_DATE) - start > dt.timedelta(days=365) and rnd.random() < 0.6:
            exit_screens.insert(0, ("", "", "", start + dt.timedelta(days=365)))   # annual assessment update screen
        for i, (c, rel) in enumerate(household):
            counters["enr"] += 1
            eid = f"E{900000 + counters['enr']}"
            created = start + dt.timedelta(days=rnd.randint(0, 3))
            base = ["No" if exit_ else "Yes", rnd.choice(STAFF), day(created), eid, hid, move_in, hh_type,
                    rnd.choice(["No", "No", "Yes"]), day(start), day(exit_), rnd.choice(STAFF), "Northwind Community Services",
                    rnd.choice(STAFF), "Family" if family else "Individual", c["pid"], c["uid"], pid,
                    day(min(exit_ or LAST_DATE, LAST_DATE)), "CA-600", PROG[pid][2], rel, "Yes" if i == 0 else "No",
                    move_in, prior]
            screens = exit_screens or [("", "", "", None)]
            for gid, ident, *_ in grants:
                for dest, cat, reason, added in screens:
                    enr_rows.append(base + [gid, ident, dest, cat, reason, day(added)])
            hm_rows.append([day(start), "Family" if family else "Individual", "Head of Household" if i == 0 else rel,
                            "No" if exit_ else "Yes", c["uid"], hid, hh_type])
            add_services(eid, c, pid, start, exit_)
            add_notes(eid, c, start, exit_)
            if PROG[pid][8]:
                bed_occ.append((pid, eid, c["pid"], start, exit_))

    # Build households until we reach N_CLIENTS people, then give some people a second or third stay.
    households = []
    while len(clients) < N_CLIENTS:
        pid = rnd.choices([p for p, _ in program_weights()], [w for _, w in program_weights()])[0]
        if PROG[pid][9] and rnd.random() < PROG[pid][9]:
            head = new_client("adult")
            members = [(head, "Self (head of household)")]
            if rnd.random() < 0.5:
                members.append((new_client("adult", head["row"][20]), "Spouse or partner"))
            for _ in range(rnd.randint(1, 3)):
                members.append((new_client("child", head["row"][20]), "Child"))
            hh_type = "Adults and children"
        else:
            head = new_client("youth" if pid == "2102" else "adult")
            members, hh_type = [(head, "Self (head of household)")], "Adult only"
        households.append((members, hh_type))
        start, exit_ = stay_dates(pid)
        enroll(members, pid, start, exit_, hh_type)

    for members, hh_type in rnd.sample(households, k=len(households) // 4):   # returning clients
        pid = rnd.choices([p for p, _ in program_weights()], [w for _, w in program_weights()])[0]
        if pid == "2102" and members[0][0]["kind"] != "youth":
            pid = "2101"
        start, exit_ = stay_dates(pid)
        enroll(members, pid, start, exit_, hh_type)

    # ---------------- bed inventory and occupancy ----------------
    bed_rows, occ_id = [], 0
    for pid, *_rest in PROGRAMS:
        beds = PROG[pid][8]
        if not beds:
            continue
        stays = [s for s in bed_occ if s[0] == pid]
        for b in range(1, beds + 1):
            unit = f"U{pid}{(b - 1) // 4 + 1:02d}"
            bid = f"B{pid}{b:03d}"
            added = PROG[pid][3]
            offline = rnd.random() < 0.05
            current = "Offline" if offline else "Online"
            my = stays[b - 1::beds]
            occupied_now = any(s[4] is None for s in my)
            avail = "Occupied" if occupied_now else ("Unavailable" if offline else "Available")
            status_rows = [(current, "Maintenance" if offline else "", "Yes", rdate(D(2025, 1, 1), LAST_DATE), "")]
            if rnd.random() < 0.2:
                status_rows.append(("Offline", "Repair", "No", D(2024, 3, 1), D(2024, 4, 15)))
            occs = my or [None]
            for s in occs:
                if s:
                    occ_id += 1
                    occ = [f"O{occ_id:06d}", day(s[3]), day(s[3]), day(s[4]), "", "Assigned", ]
                    enr, cli = s[1], s[2]
                else:
                    occ, enr, cli = ["", "", "", "", "", ""], "", ""
                for st in status_rows:
                    bed_rows.append([pid, unit, bid, f"Bed {b}", day(added), day(added),
                                     "Adults and children" if PROG[pid][9] else "Adult only", day(LAST_DATE - dt.timedelta(days=30)),
                                     avail, day(rdate(D(2026, 1, 1), LAST_DATE)), ""] + occ
                                    + [st[0], st[1], st[2], day(st[3]), day(st[4]) if st[4] else "", enr, cli])

    # ---------------- ROIs ----------------
    roi_rows, roi_id = [], 1200000
    for c in clients:
        if rnd.random() < 0.08:
            continue
        roi_id += 1
        start = max(c["created"], D(2019, 1, 1))
        end = start + dt.timedelta(days=365 * 7 - 1)
        upd = min(start + dt.timedelta(days=rnd.randint(0, 200)), LAST_DATE)
        roi_rows.append([c["uid"], "Yes" if end > EXPORT else "No", stamp(start), "Northwind Community Services",
                         rnd.choice(["Electronic Signature", "Attached PDF", "Verbal Consent"]), stamp(end), str(roi_id),
                         stamp(upd), stamp(start), rnd.choice(STAFF), rnd.choice(STAFF)])
        if rnd.random() < 0.05:   # the same ROI exported twice with different update dates
            roi_rows.append(roi_rows[-1][:7] + [stamp(min(upd + dt.timedelta(days=30), LAST_DATE))] + roi_rows[-1][8:])

    # ---------------- client rows, with Clarity's repeated rows ----------------
    cli_rows = []
    for c in clients:
        cli_rows.append(c["row"])
        if rnd.random() < 0.04:   # older copy of the same client, earlier update date and an older last name
            older = list(c["row"])
            older[2] = day(c["created"])
            older[20] = rnd.choice(LAST)
            cli_rows.append(older)
    rnd.shuffle(cli_rows)

    tables = {
        "organization": ("organization", ORG_H, [AGENCY]),
        "program": ("program", PRG_H, prog_rows),
        "client": ("client", CLI_H, cli_rows),
        "enrollment": ("enrollment", ENR_H, enr_rows),
        "service": ("service", SVC_H, svc_rows),
        "case_note": ("client_notes", NOTE_H, note_rows),
        "household_member": ("household", HM_H, hm_rows),
        "bed_unit": ("bed_and_units", BED_H, bed_rows),
        "funding": ("funding", FUND_H, fund_rows),
        "roi": ("roi", ROI_H, roi_rows),
    }
    return tables, clients


def write(tables, out_dir, export_date):
    """Write each entity to <out_dir>/<entity>/<stem>_<export_date>.csv, as the landing notebook expects."""
    counts = {}
    for entity, (stem, header, rows) in tables.items():
        path = os.path.join(out_dir, entity)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, f"{stem}_{export_date}.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header.split(","))
            w.writerows(rows)
        counts[entity] = len(rows)
    return counts
