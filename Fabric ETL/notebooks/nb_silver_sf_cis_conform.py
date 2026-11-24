# nb_silver_sf_cis_conform v1.4
# v1.4: assessments are not enrollments. CIS assesses people first and enrolls them only once they qualify,
#       so a contact with no Date_Enrolled__c is an assessed client, not an enrollment with a missing date.
#       Enrollment rows are made only for contacts with an enrollment date, or whose status says they were
#       enrolled (Enrolled, Exited, Retention, Placed); the latter with no date are real data errors and the
#       loader rejects them. New table conf_sf_cis_assessment: one row per contact with an assessment date or
#       status Assessed (Salesforce keeps only the latest assessment on the contact).
#       The first run after this change drops the enrollment row count sharply: rerun with
#       ALLOW_DROP = "enrollment" after checking that the drop is the assessed-only contacts.
# v1.3: stops if lh_bronze.dbo.sf_cis_contact holds more than one Salesforce pull (the Dataflow
#       destination must use update method Replace; this notebook keeps the history itself in
#       sf_cis_contact_history). An appending Bronze table means contacts deleted in Salesforce
#       never disappear and every past pull is copied into the history table again each week.
# Salesforce Contact (Bronze) to conf_sf_cis_client, conf_sf_cis_enrollment and conf_sf_cis_program (Silver).

# ---------------- CELL 0: PARAMETERS (mark as the parameter cell) ----------------
SOURCE_SYSTEM = "sf_cis"
WAREHOUSE_NAME = "REPLACE_WITH_WAREHOUSE_NAME"   # Fabric warehouse that holds etl_source_config
SOURCE_CONFIG_JSON = ""      # set by the pipeline; blank on manual runs
MAX_DROP_PCT = 10
ALLOW_DROP = ""              # e.g. "client" or "all" when a drop is real
SCOPE_START = "2024-10-01"   # enrollments from this date (FY2025+). "" loads full history.
ALLOW_CHECKS = ""            # other gates to let through after checking: "schema", "stale", "duplicates"
MAX_EXPORT_AGE_DAYS = 8      # stop if the Bronze contact table hasn't been refreshed in this many days


# ---------------- CELL 1: CONFIG ----------------
BRONZE_CONTACT = "lh_bronze.dbo.sf_cis_contact"
USZIPS_TABLE = "lh_bronze.dbo.ref_uszips"
USZIPS_FILE = "/lakehouse/default/Files/reference/uszips.xlsx"

TGT = {
    "client":     "lh_silver.dbo.conf_sf_cis_client",
    "enrollment": "lh_silver.dbo.conf_sf_cis_enrollment",
    "program":    "lh_silver.dbo.conf_sf_cis_program",
    "assessment": "lh_silver.dbo.conf_sf_cis_assessment",
}
GUARD_MIN_ROWS = 20
BRONZE_HISTORY = "lh_bronze.dbo.sf_cis_contact_history"
QLOG_TABLE = "lh_silver.dbo.etl_quality_log"
KEYS = {"program": ["program_id"], "client": ["salesforce_id"], "enrollment": ["source_record_id"],
        "assessment": ["source_record_id"]}

PROGRAMS = [
    (10001, "VES",          "VES",                                             "Veteran Employment Services"),
    (10002, "SSVF",         "SSVF",                                            "Supportive Services for Veteran Families"),
    (10003, "SSVF_SS",      "SSVF - Supportive Services",                      "SSVF Supportive Services"),
    (10004, "SSVF_SS_FULL", "SSVF - Supportive Services for Veteran Families", "SSVF Supportive Services (Full)"),
    (10005, "SSVF_SHALLOW", "SSVF - Shallow Subsidy",                          "SSVF Shallow Subsidy"),
    (10006, "SSVF_RENTAL",  "SSVF - Shallow Subsidy (Rental Assistance Only)", "SSVF Shallow Subsidy (Rental Only)"),
    (10007, "GPD",          "GPD - VA Grant and Per Diem",                     "VA Grant and Per Diem"),
    (10008, "VASH",         "VASH - VA Supportive Housing",                    "VA Supportive Housing"),
    (10009, "AJCC",         "AJCC - American Job Center of CA",                "American Job Center of California"),
]
PRIMARY_PROGRAM_LABEL = "VES"
# Contact_Status__c values that mean the person was enrolled. A contact with one of these and no
# Date_Enrolled__c is a data-entry error (kept, so the loader reports it); any other status without an
# enrollment date (Assessed, blank) is not an enrollment.
ENROLLED_STATUSES = ["Enrolled", "Exited", "Retention", "Placed"]
ASSESSED_STATUS = "Assessed"
ASSESSMENT_TYPE = "Pre-assessment"
CO_ENROLLMENT_DELIMITER = ";"

HUD_RACE_RULES = [
    ("American Indian, Alaska Native, or Indigenous", ["american indian", "alaska", "indigenous", "native american"]),
    ("Asian or Asian American",                       ["asian"]),
    ("Middle Eastern or North African",               ["middle eastern", "north african"]),
    ("Black, African American, or African",           ["black", "african"]),
    ("Hispanic/Latina/e/o",                           ["hispanic", "latin"]),
    ("Native Hawaiian or Pacific Islander",           ["hawaiian", "pacific islander"]),
    ("White",                                         ["white", "caucasian"]),
]
HUD_RACE_NOT_COLLECTED = [
    ("Client prefers not to answer", ["prefer", "refused", "declin"]),
    ("Client doesn't know",          ["doesn't know", "don't know", "unknown"]),
    ("Data not collected",           ["not collected"]),
]
RACE_SEPARATOR = "; "
US_STATES = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR", "CALIFORNIA": "CA", "COLORADO": "CO",
    "CONNECTICUT": "CT", "DELAWARE": "DE", "DISTRICT OF COLUMBIA": "DC", "FLORIDA": "FL", "GEORGIA": "GA",
    "HAWAII": "HI", "IDAHO": "ID", "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS",
    "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD", "MASSACHUSETTS": "MA", "MICHIGAN": "MI",
    "MINNESOTA": "MN", "MISSISSIPPI": "MS", "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY", "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK", "OREGON": "OR", "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX",
    "UTAH": "UT", "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI", "WYOMING": "WY", "PUERTO RICO": "PR", "GUAM": "GU", "VIRGIN ISLANDS": "VI",
    "US VIRGIN ISLANDS": "VI", "AMERICAN SAMOA": "AS", "NORTHERN MARIANA ISLANDS": "MP", "CALIF": "CA", "CAL": "CA",
}


# ---------------- CELL 2: GUARD + HELPERS ----------------
import json, datetime as dt
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
RUN_ID = f"{SOURCE_SYSTEM}_{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"
ALLOWED = {x.strip().lower() for x in ALLOW_CHECKS.split(",") if x.strip()}
QLOG = []

def qlog(check, table, value, status, detail=""):
    QLOG.append((RUN_ID, SOURCE_SYSTEM, "nb_silver_sf_cis_conform", check, table,
                 int(value) if value is not None else None, status, detail[:1000]))

def flush_qlog():
    if not QLOG:
        return
    schema = StructType([StructField(n, t) for n, t in [
        ("run_id", StringType()), ("source_system", StringType()), ("notebook", StringType()),
        ("check_name", StringType()), ("table_name", StringType()), ("value", LongType()),
        ("status", StringType()), ("detail", StringType())]])
    (spark.createDataFrame(QLOG, schema).withColumn("logged_at", F.current_timestamp())
          .write.format("delta").mode("append").saveAsTable(QLOG_TABLE))
    QLOG.clear()

def stop(message):
    flush_qlog()
    raise ValueError(message)

def load_source_config():
    if SOURCE_CONFIG_JSON.strip():
        row = json.loads(SOURCE_CONFIG_JSON)
        row = row[0] if isinstance(row, list) else row
        return row if row.get("source_system") == SOURCE_SYSTEM else None
    import com.microsoft.spark.fabric
    rows = (spark.read.synapsesql(f"{WAREHOUSE_NAME}.dbo.etl_source_config")
                 .filter(F.col("source_system") == SOURCE_SYSTEM).collect())
    return rows[0].asDict() if rows else None

CFG = load_source_config()
if not CFG:
    raise ValueError(f"{SOURCE_SYSTEM} has no row in etl_source_config.")
if CFG.get("block_reason"):
    raise ValueError(f"{SOURCE_SYSTEM} is blocked: {CFG['block_reason']}")
if not CFG.get("is_enabled"):
    print(f"NOTE: {SOURCE_SYSTEM} is_enabled = 0, so the weekly pipeline skips it. This manual run continues.")

def table_exists(name):
    try:
        spark.read.table(name).limit(0).collect()
        return True
    except Exception:
        return False

def s(col):
    """Trimmed text; empty becomes NULL."""
    c = F.trim(F.col(col).cast("string"))
    return F.when(c == "", None).otherwise(c)

def d(col):
    """Date from a Salesforce date or datetime value."""
    c = s(col)
    return F.coalesce(F.to_date(F.substring(c, 1, 10), "yyyy-MM-dd"), F.to_date(c, "M/d/yyyy"))

def ts(col):
    """Timestamp. Already-typed columns are kept as is; Salesforce text
    ("2025-01-03T17:00:00.000+0000") keeps its UTC offset."""
    if dict(raw.dtypes).get(col) == "timestamp":
        return F.col(col)
    c = s(col)
    return F.coalesce(F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
                      F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ssZ"),
                      F.to_timestamp(c))

def bool_(col):
    c = F.lower(s(col))
    return (F.when(c.isin("true", "1", "yes", "y"), F.lit(True))
             .when(c.isin("false", "0", "no", "n"), F.lit(False)))

def phone(col):
    """(xxx) xxx-xxxx for 10-digit numbers (or 11 starting with 1); else NULL."""
    digits = F.regexp_replace(F.coalesce(F.col(col).cast("string"), F.lit("")), r"[^0-9]", "")
    digits = F.when((F.length(digits) == 11) & digits.startswith("1"), digits.substr(2, 10)).otherwise(digits)
    return F.when(F.length(digits) == 10,
                  F.concat(F.lit("("), digits.substr(1, 3), F.lit(") "), digits.substr(4, 3),
                           F.lit("-"), digits.substr(7, 4)))

def clean_name(col):
    """Control characters removed, then capitalized (Text.Clean + Text.Proper)."""
    c = F.regexp_replace(s(col), r"[\x00-\x1F\x7F]", "")
    return F.initcap(F.lower(c))

def state_code(c):
    """2-letter USPS code from a code or a full state name (already upper-cased); NULL otherwise."""
    out = F.when(c.isin(*sorted(set(US_STATES.values()))), c)
    for name, code in US_STATES.items():
        out = out.when(c == name, F.lit(code))
    return out

def money(col):
    c = F.regexp_replace(s(col), r"[$,\s]", "")
    return F.when(c.rlike(r"^-?[0-9]*\.?[0-9]+$"), c.cast("decimal(10,2)"))

PENDING = {}

def write(df, key):
    PENDING[key] = df

def _write_now(df, key):
    out = df.withColumn("source_system", F.lit(SOURCE_SYSTEM)).withColumn("load_datetime", F.current_timestamp())
    w = out.write.format("delta")
    if "schema" in ALLOWED:
        w = w.option("mergeSchema", "true")
    if table_exists(TGT[key]):
        w.mode("overwrite").option("replaceWhere", f"source_system = '{SOURCE_SYSTEM}'").saveAsTable(TGT[key])
    else:
        w.mode("overwrite").partitionBy("source_system").saveAsTable(TGT[key])
    n = spark.read.table(TGT[key]).filter(F.col("source_system") == SOURCE_SYSTEM).count()
    print(f"{TGT[key]}: {n:,} rows")

raw = spark.read.table(BRONZE_CONTACT)
missing_cols = [c_ for c_ in ["Id", "LastModifiedDate", "Race__c", "Date_Enrolled__c"] if c_ not in raw.columns]
if missing_cols:
    raise ValueError(f"{BRONZE_CONTACT} is missing {missing_cols}. Check the Bronze Dataflow.")

def _append_history(df, batch_date):
    w = df.write.format("delta").option("mergeSchema", "true")
    if table_exists(BRONZE_HISTORY):
        w.mode("overwrite").option("replaceWhere", f"bronze_batch_date = '{batch_date}'").saveAsTable(BRONZE_HISTORY)
    else:
        w.mode("overwrite").saveAsTable(BRONZE_HISTORY)


# ---------------- CELL 2b: BRONZE FRESHNESS AND HISTORY ----------------
now = spark.sql("SELECT current_timestamp() AS t, current_date() AS d").first()
try:
    last_refresh = spark.sql(f"DESCRIBE HISTORY {BRONZE_CONTACT} LIMIT 1").first()["timestamp"]
except Exception:
    last_refresh = None
    print(f"NOTE: couldn't read the history of {BRONZE_CONTACT}; Bronze freshness not checked.")
if last_refresh is not None:
    age = (now["t"] - last_refresh).days
    if age > MAX_EXPORT_AGE_DAYS and "stale" not in ALLOWED:
        qlog("bronze_age_days", "sf_cis_contact", age, "stop", f"last refresh {last_refresh}")
        stop(f"{BRONZE_CONTACT} was last refreshed {last_refresh} ({age} days ago). Check the Dataflow, "
             f"or rerun with ALLOW_CHECKS = 'stale' after checking.")
    qlog("bronze_age_days", "sf_cis_contact", age, "override" if age > MAX_EXPORT_AGE_DAYS else "pass",
         f"last refresh {last_refresh}")

# v1.3: one Salesforce pull only. Contact Id is unique within a pull, so repeated Ids mean the
# Dataflow appended. Checked before the history snapshot so the history isn't polluted.
_ids = raw.agg(F.count(F.lit(1)).alias("rows"), F.countDistinct("Id").alias("ids")).first()
_extra = _ids["rows"] - _ids["ids"]
if _extra > 0 and "duplicates" not in ALLOWED:
    qlog("bronze_repeated_ids", "sf_cis_contact", _extra, "stop", f"{_ids['rows']} rows for {_ids['ids']} contacts")
    stop(f"{BRONZE_CONTACT} has {_ids['rows']:,} rows for {_ids['ids']:,} contacts, so it holds more than one "
         f"Salesforce pull. Set the Dataflow destination's update method to Replace and refresh it. "
         f"History is kept by this notebook in {BRONZE_HISTORY}. To run anyway after checking, "
         f"rerun with ALLOW_CHECKS = 'duplicates'.")
qlog("bronze_repeated_ids", "sf_cis_contact", _extra, "override" if _extra > 0 else "pass",
     f"{_ids['rows']} rows for {_ids['ids']} contacts")

if raw.limit(1).count():
    _append_history(raw.withColumn("bronze_batch_date", F.lit(now["d"]).cast("date"))
                       .withColumn("bronze_batch_id", F.lit(RUN_ID))
                       .withColumn("bronze_load_datetime", F.current_timestamp()), now["d"])
    print(f"{BRONZE_HISTORY}: snapshot for {now['d']} saved")


# ---------------- CELL 3: PROGRAM ----------------
program = spark.createDataFrame(PROGRAMS, "program_id INT, program_code STRING, sf_picklist_value STRING, program_name STRING")
write(program, "program")


# ---------------- CELL 4: CLIENT ----------------
if table_exists(USZIPS_TABLE):
    zips_raw = spark.read.table(USZIPS_TABLE)
else:
    import pandas as pd
    zips_raw = spark.createDataFrame(pd.read_excel(USZIPS_FILE, dtype=str,
                                                   usecols=["zip", "city", "state_id", "county_name"]))
zips = (zips_raw
             .select(F.lpad(F.regexp_replace(F.col("zip").cast("string"), r"\.0$", ""), 5, "0").alias("ref_zip5"),
                     F.col("city").alias("zip_city"), F.col("state_id").alias("zip_state"),
                     F.col("county_name").alias("zip_county"))
             .dropDuplicates(["ref_zip5"]))

pc = s("MailingPostalCode")
tokens = F.filter(F.split(F.trim(F.regexp_replace(F.coalesce(pc, F.lit("")), r"[^0-9 ]", "")), r"\s+"),
                  lambda t: F.length(t) == 5)
zip5 = F.coalesce(
    F.when(pc.rlike(r"^\d{5}(-?\d{4})?$"), pc.substr(1, 5)),
    F.when(F.size(tokens) == 1, tokens.getItem(0)))

cl = (raw.select(
        s("Id").alias("salesforce_id"),
        bool_("IsDeleted").alias("is_deleted"),
        s("AccountId").alias("salesforce_account_id"),
        clean_name("LastName").alias("last_name"),
        clean_name("FirstName").alias("first_name"),
        clean_name("MiddleName").alias("middle_name"),
        clean_name("MailingStreet").alias("address_line1"),
        zip5.alias("zip5"),
        s("MailingCity").alias("mailing_city"),
        s("MailingState").alias("mailing_state"),
        phone("Phone").alias("home_phone"),
        phone("MobilePhone").alias("mobile_phone"),
        s("Email").alias("email"),
        s("LeadSource").alias("lead_source"),
        d("Birthdate").alias("date_of_birth"),
        ts("LastModifiedDate").alias("last_modified_date"),
        s("OwnerId").alias("owner_id"),
        ts("CreatedDate").alias("created_date"),
        s("CreatedById").alias("created_by_id"),
        s("Discharge_Status__c").alias("discharge_status"),
        s("Service_Connected__c").alias("service_connected"),
        s("Education_Level__c").alias("education_level"),
        s("Conviction__c").alias("has_conviction"),
        s("Conviction_Type__c").alias("conviction_type"),
        d("Conviction_Date__c").alias("conviction_date"),
        s("Probation_Parole__c").alias("probation_parole_status"),
        s("Homeless__c").alias("homeless_status"),
        s("How_Long_Homeless__c").alias("length_of_homelessness"),
        s("Hispanic_or_Latino__c").alias("ethnicity_raw"),
        s("External_ID__c").alias("external_id"),
        d("Military_from__c").alias("military_entry_date"),
        d("Military_Service_to__c").alias("military_discharge_date"),
        s("Race__c").alias("race_detail"),
        s("Service_Branch__c").alias("military_branch"),
        s("Campaign_Badge__c").alias("period_of_service"),
        s("Type_of_Service__c").alias("service_type"),
        s("Military_Service_Component__c").alias("military_service_component"),
        s("Post_9_11__c").alias("is_post_9_11"),
        s("Current_Marital_Status__c").alias("marital_status"),
        s("HasChildren_c__c").alias("number_of_children"),
        s("Housing_Status__c").alias("housing_status"),
        s("Times_Homeless_in_3_years__c").alias("times_homeless_3yr"),
        d("Last_Released_from_Incarceration__c").alias("last_release_date"),
        s("Experience_During_Incarceration__c").alias("experience_during_incarceration"),
        d("Probation_or_Parole_end_date__c").alias("parole_end_date"),
        s("Substance_Abuse__c").alias("substance_abuse"),
        s("Mental_Health_Condition__c").alias("mental_health_condition"),
        s("Gender_Identity__c").alias("gender"),
        s("Sex__c").alias("sex"),
        s("Disability_Status__c").alias("disability_status"),
      )
      .filter(F.col("salesforce_id").isNotNull())
      .join(zips, F.col("zip5") == F.col("ref_zip5"), "left").drop("ref_zip5")
      .withColumn("postal_code", F.col("zip5"))
      .withColumn("city", F.coalesce("zip_city", "mailing_city"))
      .withColumn("_ms", F.upper(F.trim(F.regexp_replace(F.coalesce(F.col("mailing_state"), F.lit("")), r"[.]", ""))))
      .withColumn("_ms_code", state_code(F.col("_ms")))
      .withColumn("state", F.coalesce("zip_state", "_ms_code"))
      .withColumn("_state_unmatched", F.when(F.col("zip_state").isNull() & (F.col("_ms") != "") & F.col("_ms_code").isNull(),
                                             F.col("mailing_state")))
      .drop("_ms", "_ms_code")
      .withColumn("county_name", F.col("zip_county"))
      .drop("zip5", "zip_city", "zip_state", "zip_county", "mailing_city", "mailing_state")
      .withColumn("veteran_status", F.lit("Yes")))

low = F.lower(F.coalesce(F.col("race_detail"), F.lit("")))
eth = F.lower(F.coalesce(F.col("ethnicity_raw"), F.lit("")))
ethnicity = (F.when(eth.rlike(r"^(no|not)\b|non[- ]hispanic|not hispanic"), F.lit("No"))
              .when(eth.rlike(r"^(yes|y)$|hispanic|latin"), F.lit("Yes")))
cats = []
for label, words in HUD_RACE_RULES:
    hit = F.lit(False)
    for w in words:
        hit = hit | low.contains(w)
    if label == "Black, African American, or African":
        hit = hit & ~(low.contains("north african") & ~low.contains("black") & ~low.rlike(r"(^|;)\s*african"))
    if label == "Hispanic/Latina/e/o":
        hit = hit | (ethnicity == "Yes")
    cats.append(F.when(hit, F.lit(label)))
not_collected = F.lit(None).cast("string")
for label, words in reversed(HUD_RACE_NOT_COLLECTED):
    hit = F.lit(False)
    for w in words:
        hit = hit | low.contains(w)
    not_collected = F.when(hit, F.lit(label)).otherwise(not_collected)
race_hud = F.concat_ws(RACE_SEPARATOR, *cats)
cl = (cl.withColumn("ethnicity", ethnicity)
        .withColumn("race", F.when(race_hud != "", race_hud).otherwise(not_collected))
        .drop("ethnicity_raw"))

w_c = Window.partitionBy("salesforce_id").orderBy(F.col("last_modified_date").desc_nulls_last())
client = cl.withColumn("_rn", F.row_number().over(w_c)).filter("_rn = 1").drop("_rn")
unmatched_state = [r[0] for r in client.filter(F.col("_state_unmatched").isNotNull())
                                       .select("_state_unmatched").distinct().limit(25).collect()]
n_unmatched_state = client.filter(F.col("_state_unmatched").isNotNull()).count()
client = client.drop("_state_unmatched")
write(client, "client")


# ---------------- CELL 5: ENROLLMENT ----------------
en = (raw.select(
        s("Id").alias("salesforce_id"),
        s("Program__c").alias("hvrp_participant_type"),
        s("Contact_Status__c").alias("enrollment_status"),
        s("HVRP_Grant_Name__c").alias("grant_name"),
        s("Referred_by_Agency__c").alias("referral_agency"),
        s("Referred_to_Workforce_EDD_AJCC__c").alias("referred_to_workforce"),
        s("Type_of_Service__c").alias("service_type"),
        d("Date_Enrolled__c").alias("enrollment_start_date"),
        s("Referred_to_Workforce_Name__c").alias("workforce_referral_name"),
        d("Date_Placed__c").alias("placement_date"),
        s("Carry_Over_Enrollment__c").alias("is_carry_over"),
        money("Starting_Pay__c").alias("starting_hourly_wage"),
        bool_("Placed_Flag__c").alias("is_placed"),
        s("Co_Enrolled_In__c").alias("co_enrolled_in"),
        d("Exited_Date__c").alias("enrollment_exit_date"),
        ts("LastModifiedDate").alias("_lm"),
      )
      .filter(F.col("salesforce_id").isNotNull()))
en = (en.withColumn("_rn", F.row_number().over(Window.partitionBy("salesforce_id").orderBy(F.col("_lm").desc_nulls_last())))
        .filter("_rn = 1").drop("_rn", "_lm"))
if SCOPE_START:
    en = en.filter(F.col("enrollment_start_date").isNull() | (F.col("enrollment_start_date") >= F.lit(SCOPE_START).cast("date")))
# Only people who were enrolled get enrollment rows (assessed-only contacts become assessments below)
en = en.filter(F.col("enrollment_start_date").isNotNull() | F.col("enrollment_status").isin(ENROLLED_STATUSES))

primary = (en.drop("co_enrolled_in")
             .withColumn("program_label", F.lit(PRIMARY_PROGRAM_LABEL))
             .withColumn("is_co_enrollment", F.lit(0)))
co = (en.filter(F.col("co_enrolled_in").isNotNull())
        .withColumn("program_label", F.explode(F.split("co_enrolled_in", CO_ENROLLMENT_DELIMITER)))
        .withColumn("program_label", F.trim("program_label"))
        .filter((F.col("program_label") != "") & (F.lower("program_label") != "no co-enrollments"))
        .drop("co_enrolled_in")
        .withColumn("is_co_enrollment", F.lit(1))
        .dropDuplicates(["salesforce_id", "program_label"]))
combined = primary.unionByName(co)

prog_map = program.select(F.col("sf_picklist_value").alias("program_label"), "program_id", "program_code")
start_txt = F.coalesce(F.date_format("enrollment_start_date", "yyyy-MM-dd"), F.lit("NO_START_DATE"))
enrollment = (combined.join(prog_map, "program_label", "left")
                .withColumn("source_record_id",
                            F.when(F.col("is_co_enrollment") == 1,
                                   F.concat_ws("|", "salesforce_id", start_txt, "program_label"))
                             .otherwise(F.concat_ws("|", "salesforce_id", start_txt))))
write(enrollment, "enrollment")


# ---------------- CELL 5b: ASSESSMENT ----------------
# One row per contact with an assessment date or status Assessed. Salesforce keeps only the latest
# assessment on the contact, so the ID is '<Contact Id>|assessment' and a new assessment updates the row.
ASSESSMENT_FIELDS = ["Date_Assessed__c", "Pre_Assessment_ID__c", "LeadSource"]
missing_asm = [c_ for c_ in ASSESSMENT_FIELDS if c_ not in raw.columns]
if missing_asm:
    print(f"NOTE  assessment fields not in Bronze (add them to the Dataflow): {missing_asm}")
    qlog("assessment_fields_missing", "assessment", len(missing_asm), "warn", ",".join(missing_asm))
def opt_s(col):
    return s(col) if col in raw.columns else F.lit(None).cast("string")
def opt_d(col):
    return d(col) if col in raw.columns else F.lit(None).cast("date")
asm = (raw.select(
        s("Id").alias("salesforce_id"),
        opt_d("Date_Assessed__c").alias("assessment_date"),
        opt_s("Pre_Assessment_ID__c").alias("source_assessment_id"),
        opt_s("LeadSource").alias("assessment_source"),
        s("Contact_Status__c").alias("contact_status"),
        d("Date_Enrolled__c").alias("enrollment_start_date"),
        ts("CreatedDate").alias("source_added_date"),
        ts("LastModifiedDate").alias("source_last_updated"),
      )
      .filter(F.col("salesforce_id").isNotNull()))
asm = (asm.withColumn("_rn", F.row_number().over(Window.partitionBy("salesforce_id")
                                                 .orderBy(F.col("source_last_updated").desc_nulls_last())))
          .filter("_rn = 1").drop("_rn"))
asm = asm.filter(F.col("assessment_date").isNotNull() | (F.col("contact_status") == ASSESSED_STATUS))
if SCOPE_START:
    asm = asm.filter(F.col("assessment_date").isNull() | (F.col("assessment_date") >= F.lit(SCOPE_START).cast("date")))
primary_prog = program.filter(F.col("sf_picklist_value") == PRIMARY_PROGRAM_LABEL).select("program_id", "program_code")
assessment = (asm.crossJoin(primary_prog)
                 .withColumn("source_record_id", F.concat_ws("|", "salesforce_id", F.lit("assessment")))
                 .withColumn("assessment_type", F.lit(ASSESSMENT_TYPE))
                 .withColumn("assessment_outcome",
                             F.when(F.col("enrollment_start_date").isNotNull(), F.lit("Enrolled")).otherwise(F.lit("Not enrolled"))))
write(assessment, "assessment")


# ---------------- CELL 6: GATES, THEN WRITE ----------------
allow = {x.strip() for x in ALLOW_DROP.split(",") if x.strip()}
plan, problems = [], []
for key, df in PENDING.items():
    df = df.persist()
    new_n = df.count()
    exists = table_exists(TGT[key])
    old = spark.read.table(TGT[key]).filter(F.col("source_system") == SOURCE_SYSTEM) if exists else None
    old_n = old.count() if exists else 0
    plan.append((key, df, old_n, new_n))

    dropped = (old_n >= GUARD_MIN_ROWS and new_n < old_n * (1 - MAX_DROP_PCT / 100)) or (old_n > 0 and new_n == 0)
    if dropped and not ("all" in allow or key in allow):
        problems.append(f"  {key}: {old_n:,} -> {new_n:,} rows")
        qlog("row_count", key, new_n, "stop", f"was {old_n}")
    else:
        qlog("row_count", key, new_n, "override" if dropped else "pass", f"was {old_n}")

    if exists:
        cur = {f.name: f.dataType.simpleString() for f in old.schema if f.name not in ("source_system", "load_datetime")}
        new = {f.name: f.dataType.simpleString() for f in df.schema}
        if cur != new:
            retyped = sorted(c for c in set(cur) & set(new) if cur[c] != new[c])
            diff = (f"added {sorted(set(new) - set(cur))}, removed {sorted(set(cur) - set(new))}, "
                    f"retyped {retyped}")
            if retyped:
                problems.append(f"  {key}: column type changed ({diff}). A type change can't be merged into the "
                                f"shared table; rebuild {TGT[key]} on purpose, then rerun every source")
                qlog("schema", key, None, "stop", diff)
            elif "schema" in ALLOWED:
                qlog("schema", key, None, "override", diff)
            else:
                problems.append(f"  {key}: schema changed ({diff})")
                qlog("schema", key, None, "stop", diff)

    if KEYS.get(key) and new_n:
        dups = df.groupBy(*KEYS[key]).count().filter("count > 1").count()
        qlog("duplicate_keys", key, dups, "stop" if dups else "pass", ",".join(KEYS[key]))
        if dups:
            problems.append(f"  {key}: {dups:,} duplicate {'+'.join(KEYS[key])}")

if problems:
    stop("Quality gates stopped sf_cis before anything was written:\n" + "\n".join(problems)
         + "\nCheck the Bronze Dataflow refresh. If a drop is real, rerun with ALLOW_DROP; "
           "for a schema change, ALLOW_CHECKS = 'schema'.")
for key, df, old_n, new_n in plan:
    _write_now(df, key)
    print(f"    was {old_n:,}")
flush_qlog()


# ---------------- CELL 7: QUICK CHECKS ----------------
cli = spark.read.table(TGT["client"]).filter(F.col("source_system") == SOURCE_SYSTEM)
enr = spark.read.table(TGT["enrollment"]).filter(F.col("source_system") == SOURCE_SYSTEM)
asm_w = spark.read.table(TGT["assessment"]).filter(F.col("source_system") == SOURCE_SYSTEM)
checks = {
    "contacts with no county (ZIP blank or not in uszips)":
        cli.filter(F.col("county_name").isNull()).count(),
    "status says enrolled but no enrollment date (loader rejects these)":
        enr.filter(F.col("enrollment_start_date").isNull()).count(),
    "assessments with no assessment date":
        asm_w.filter(F.col("assessment_date").isNull()).count(),
    "status still Assessed but has an enrollment date":
        asm_w.filter((F.col("contact_status") == ASSESSED_STATUS) & F.col("enrollment_start_date").isNotNull()).count(),
    "co-enrollments whose program isn't in PROGRAMS (program_id NULL)":
        enr.filter(F.col("program_id").isNull()).count(),
    "contacts with race not mapped to HUD":
        cli.filter(F.col("race_detail").isNotNull() & F.col("race").isNull()).count(),
    "contacts whose Mailing State isn't a US state (state left blank)": n_unmatched_state,
}
for k, v in checks.items():
    print(f"{'OK ' if v == 0 else 'CHECK'}  {k}: {v:,}")
    qlog("quick_check", k, v, "pass" if v == 0 else "warn")
flush_qlog()

unmapped_prog = [r[0] for r in enr.filter(F.col("program_id").isNull()).select("program_label").distinct().collect()]
if unmapped_prog:
    print("Add these to PROGRAMS (new program_id each):", unmapped_prog)
unmapped_race = [r[0] for r in cli.filter(F.col("race_detail").isNotNull() & F.col("race").isNull())
                              .select("race_detail").distinct().collect()]
if unmapped_race:
    print("Race values with no HUD category (add words to HUD_RACE_RULES):", unmapped_race)
if unmatched_state:
    print("Mailing State values that aren't a US state or code (add to US_STATES if real):", unmatched_state)



# ---------------- CELL 8: EXIT ----------------
summary = {"run_id": RUN_ID, "source_system": SOURCE_SYSTEM,
           "rows": {k: n for k, _, _, n in plan}, "checks": checks}
try:
    import notebookutils
except ImportError:
    notebookutils = None
if notebookutils is not None:
    notebookutils.notebook.exit(json.dumps(summary))
else:
    print(json.dumps(summary))
