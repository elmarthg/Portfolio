"""Synthetic Salesforce Contact pull (Bronze) for the demo.

Every contact is invented. Column names match the Salesforce Contact object the Silver notebook reads,
including the custom fields for enrollment, assessment and military service. Each row is one contact,
holding that contact's current enrollment and latest assessment, as Salesforce stores them.
"""
import datetime as dt

COLUMNS = [
    "Id", "IsDeleted", "AccountId", "LastName", "FirstName", "MiddleName", "MailingStreet", "MailingCity",
    "MailingState", "MailingPostalCode", "Phone", "MobilePhone", "HomePhone", "Email", "LeadSource", "Birthdate",
    "LastModifiedDate", "OwnerId", "CreatedDate", "CreatedById", "Discharge_Status__c", "Service_Connected__c",
    "Education_Level__c", "Conviction__c", "Conviction_Type__c", "Conviction_Date__c", "Probation_Parole__c",
    "Homeless__c", "How_Long_Homeless__c", "Hispanic_or_Latino__c", "External_ID__c", "Military_from__c",
    "Military_Service_to__c", "Race__c", "Service_Branch__c", "Campaign_Badge__c", "Type_of_Service__c",
    "Military_Service_Component__c", "Post_9_11__c", "Current_Marital_Status__c", "HasChildren_c__c",
    "Housing_Status__c", "Times_Homeless_in_3_years__c", "Last_Released_from_Incarceration__c",
    "Experience_During_Incarceration__c", "Probation_or_Parole_end_date__c", "Substance_Abuse__c",
    "Mental_Health_Condition__c", "Gender_Identity__c", "Sex__c", "Disability_Status__c",
    "Program__c", "Contact_Status__c", "HVRP_Grant_Name__c", "Referred_by_Agency__c",
    "Referred_to_Workforce_EDD_AJCC__c", "Date_Enrolled__c", "Referred_to_Workforce_Name__c", "Date_Placed__c",
    "Carry_Over_Enrollment__c", "Starting_Pay__c", "Placed_Flag__c", "Co_Enrolled_In__c", "Exited_Date__c",
    "Date_Assessed__c", "Pre_Assessment_ID__c",
]

# ZIP reference for the notebook's ZIP-to-county lookup (a small stand-in for the full US ZIP list)
ZIPS = [("90001", "Los Angeles", "CA", "Los Angeles"), ("90002", "Los Angeles", "CA", "Los Angeles"),
        ("90003", "Los Angeles", "CA", "Los Angeles"), ("90011", "Los Angeles", "CA", "Los Angeles"),
        ("90013", "Los Angeles", "CA", "Los Angeles"), ("90014", "Los Angeles", "CA", "Los Angeles"),
        ("90015", "Los Angeles", "CA", "Los Angeles"), ("90021", "Los Angeles", "CA", "Los Angeles"),
        ("90028", "Los Angeles", "CA", "Los Angeles"), ("90037", "Los Angeles", "CA", "Los Angeles"),
        ("90042", "Los Angeles", "CA", "Los Angeles"), ("90044", "Los Angeles", "CA", "Los Angeles"),
        ("90201", "Bell", "CA", "Los Angeles"), ("90255", "Huntington Park", "CA", "Los Angeles"),
        ("90262", "Lynwood", "CA", "Los Angeles"), ("90280", "South Gate", "CA", "Los Angeles"),
        ("90731", "San Pedro", "CA", "Los Angeles"), ("90802", "Long Beach", "CA", "Los Angeles"),
        ("90813", "Long Beach", "CA", "Los Angeles"), ("91101", "Pasadena", "CA", "Los Angeles"),
        ("91331", "Pacoima", "CA", "Los Angeles"), ("91402", "Panorama City", "CA", "Los Angeles"),
        ("91601", "North Hollywood", "CA", "Los Angeles"), ("92701", "Santa Ana", "CA", "Orange"),
        ("92501", "Riverside", "CA", "Riverside"), ("92101", "San Diego", "CA", "San Diego")]

FIRST = ["James", "Maria", "Robert", "Linda", "Michael", "Patricia", "David", "Jennifer", "Carlos", "Angela",
         "Tyrone", "Mei", "Darnell", "Rosa", "Kevin", "Denise", "Luis", "Grace", "Andre", "Teresa"]
LAST = ["Alvarez", "Johnson", "Nguyen", "Williams", "Garcia", "Brown", "Lee", "Martinez", "Davis", "Lopez",
        "Wilson", "Kim", "Okafor", "Castillo", "Bennett", "Reyes", "Foster", "Patel", "Morgan", "Silva"]
RACES = ["White", "Black or African American", "Asian", "White;Black or African American", "Decline to state",
         "Middle Eastern or North African", "American Indian or Alaska Native", "Other"]
CO = [None, None, None, "SSVF", "AJCC - American Job Center of CA", "SSVF;AJCC - American Job Center of CA",
      "No Co-Enrollments", "GPD - VA Grant and Per Diem"]


def contact(i, rnd, export, identity=None):
    """One Contact row as a dict. identity (first, last, dob, zip, city) makes the contact a person who
    also exists elsewhere (in Clarity, or as a second Salesforce contact)."""
    z = rnd.choice(ZIPS)
    first, last = rnd.choice(FIRST), rnd.choice(LAST)
    dob = dt.date(1950, 1, 1) + dt.timedelta(days=rnd.randint(0, 17000))
    zip_, city = z[0], z[1]
    if identity:
        first, last, dob = identity["first"], identity["last"], identity["dob"]
        if identity.get("zip"):
            zip_, city = identity["zip"][:5], identity.get("city") or city
    start = export - dt.timedelta(days=rnd.randint(60, 760))
    exited = start + dt.timedelta(days=rnd.randint(30, 300)) if rnd.random() < 0.35 else None
    if exited and exited > export - dt.timedelta(days=7):
        exited = None
    r = dict.fromkeys(COLUMNS)
    r.update(Id=f"003DEMO{i:011d}", IsDeleted="false", AccountId=f"001DEMO{i:011d}",
             LastName=last, FirstName=first, MailingStreet=f"{rnd.randint(100, 9999)} {rnd.choice(['Main', 'Oak', 'Lake', 'Hill'])} St",
             MailingCity=city, MailingState="California", MailingPostalCode=zip_,
             Phone=f"213555{rnd.randint(1000, 9999)}", MobilePhone=f"(323) 555-{rnd.randint(1000, 9999)}",
             Email=f"client{i}@example.org", LeadSource="Referral", Birthdate=str(dob),
             LastModifiedDate=f"{export - dt.timedelta(days=rnd.randint(1, 20))}T17:00:00.000+0000",
             OwnerId="005DEMO00000000001", CreatedDate=f"{start - dt.timedelta(days=30)}T17:00:00.000+0000",
             CreatedById="005DEMO00000000001",
             Discharge_Status__c="Honorable", Hispanic_or_Latino__c=rnd.choice(["No", "Hispanic or Latino"]),
             Race__c=rnd.choice(RACES), Service_Branch__c=rnd.choice(["Army", "Navy", "Marines"]),
             Program__c=rnd.choice(["HVRP", "Non-HVRP", "At Risk (CODE)"]),
             Contact_Status__c="Exited" if exited else "Enrolled",
             Date_Enrolled__c=str(start), Exited_Date__c=str(exited) if exited else None,
             Starting_Pay__c=f"${rnd.randint(16, 30)}.50" if rnd.random() < 0.3 else None,
             Placed_Flag__c=rnd.choice(["true", "false"]), Co_Enrolled_In__c=rnd.choice(CO),
             Disability_Status__c=rnd.choice(["Yes", "No", None]), Sex__c=rnd.choice(["Male", "Female"]))
    # A realistic mix: most contacts enrolled; about a third assessed only (most with no assessment date);
    # a few with no status at all; a handful marked enrolled or exited with no enrollment date (data-entry
    # errors the loader reports); a few still marked Assessed after enrolling.
    if rnd.random() < 0.7:
        r["Date_Assessed__c"] = str(start - dt.timedelta(days=rnd.randint(1, 30)))
        r["Pre_Assessment_ID__c"] = f"PA-{i:05d}"
    kind = rnd.random()
    if identity:
        kind = 0.9           # people shared with another system are enrolled, so they show up in reports
    if kind < 0.34:
        r.update(Contact_Status__c="Assessed", Date_Enrolled__c=None, Exited_Date__c=None, Co_Enrolled_In__c=None,
                 Date_Assessed__c=None if rnd.random() < 0.72 else str(start), Placed_Flag__c="false")
    elif kind < 0.42:
        r.update(Contact_Status__c=None, Date_Enrolled__c=None, Exited_Date__c=None, Co_Enrolled_In__c=None,
                 Date_Assessed__c=None, Pre_Assessment_ID__c=None, Placed_Flag__c="false")
    elif kind < 0.44:
        r.update(Contact_Status__c=rnd.choice(["Exited", "Retention", "Enrolled"]), Date_Enrolled__c=None)
    elif kind < 0.46:
        r.update(Contact_Status__c="Assessed")
    return r
