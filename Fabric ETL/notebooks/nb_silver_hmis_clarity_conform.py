# nb_silver_hmis_clarity_conform v2.4
# One Clarity county's Bronze tables to the shared conf_hmis_clarity_* Silver tables.

# ---------------- CELL 0: PARAMETERS (mark as the parameter cell) ----------------
SOURCE_SYSTEM = "hmis_lac"      # hmis_lac | hmis_sandiego | hmis_orange | hmis_sanbern | hmis_kern
WAREHOUSE_NAME = "REPLACE_WITH_WAREHOUSE_NAME"  # Fabric warehouse that holds etl_source_config
SOURCE_CONFIG_JSON = ""         # set by the pipeline; blank on manual runs
MAX_DROP_PCT = 10               # stop if any table has this % fewer rows than last run
ALLOW_DROP = ""                 # tables allowed to shrink this run, e.g. "service,case_note", or "all"
ALLOW_CHECKS = ""               # other gates to let through after checking: "schema", "stale", "orphans"
MAX_EXPORT_AGE_DAYS = 8         # stop if the newest export is older than this
MAX_ORPHAN_PCT = 1              # stop if more enrollments than this % have no client or no program
KEY_VAULT_URL = ""              # e.g. "https://<your-key-vault>.vault.azure.net/"; blank leaves ssn_hash empty
SSN_HASH_SECRET_NAME = "ssn-hash-key"


# ---------------- CELL 1: CONFIG ----------------

GUARD_MIN_ROWS = 20
QLOG_TABLE = "lh_silver.dbo.etl_quality_log"
CORE = ["agency", "program", "client", "enrollment"]
KEYS = {
    "agency": ["agency_id"], "program": ["hmis_program_id"], "client": ["personal_id"],
    "enrollment": ["enrollment_id"], "grant": ["grant_id"], "service": ["service_id"],
    "service_item": ["service_item_code"], "case_note": ["note_id"],
    "household_member": ["household_id", "unique_identifier"], "bed": ["bed_id"],
    "bed_status": ["status_record_id"], "bed_night": ["occupancy_id", "bed_night_date"],
    "funding_source": ["funding_source_id"], "roi": ["roi_id"],
    "enrollment_grant": ["enrollment_id", "grant_id"],
}
DERIVED = {"service_item": "service", "bed_status": "bed", "bed_night": "bed",
           "enrollment_grant": "enrollment", "grant": "enrollment"}

BRONZE_PATTERN = "lh_bronze.dbo.{src}_{entity}"

SRC = {
    "agency":     BRONZE_PATTERN.format(src=SOURCE_SYSTEM, entity="organization"),
    "program":    BRONZE_PATTERN.format(src=SOURCE_SYSTEM, entity="program"),
    "client":     BRONZE_PATTERN.format(src=SOURCE_SYSTEM, entity="client"),
    "enrollment": BRONZE_PATTERN.format(src=SOURCE_SYSTEM, entity="enrollment"),
    "service":          BRONZE_PATTERN.format(src=SOURCE_SYSTEM, entity="service"),
    "case_note":        BRONZE_PATTERN.format(src=SOURCE_SYSTEM, entity="case_note"),
    "household_member": BRONZE_PATTERN.format(src=SOURCE_SYSTEM, entity="household_member"),
    "bed_unit":         BRONZE_PATTERN.format(src=SOURCE_SYSTEM, entity="bed_unit"),
    "funding":          BRONZE_PATTERN.format(src=SOURCE_SYSTEM, entity="funding"),
    "roi":              BRONZE_PATTERN.format(src=SOURCE_SYSTEM, entity="roi"),
}

TGT = {
    "agency":     "lh_silver.dbo.conf_hmis_clarity_agency",
    "program":    "lh_silver.dbo.conf_hmis_clarity_program",
    "client":     "lh_silver.dbo.conf_hmis_clarity_client",
    "enrollment": "lh_silver.dbo.conf_hmis_clarity_enrollment",
    "grant":      "lh_silver.dbo.conf_hmis_clarity_grant",
    "service":          "lh_silver.dbo.conf_hmis_clarity_service",
    "service_item":     "lh_silver.dbo.conf_hmis_clarity_service_item",
    "case_note":        "lh_silver.dbo.conf_hmis_clarity_case_note",
    "household_member": "lh_silver.dbo.conf_hmis_clarity_household_member",
    "bed":              "lh_silver.dbo.conf_hmis_clarity_bed",
    "bed_status":       "lh_silver.dbo.conf_hmis_clarity_bed_status",
    "bed_night":        "lh_silver.dbo.conf_hmis_clarity_bed_night",
    "funding_source":   "lh_silver.dbo.conf_hmis_clarity_funding_source",
    "roi":              "lh_silver.dbo.conf_hmis_clarity_roi",
    "enrollment_grant": "lh_silver.dbo.conf_hmis_clarity_enrollment_grant",
}

BED_NIGHT_START = "2020-01-01"

FILE_NAME_COLS = ["source_file_name", "_source_file_name", "file_name"]
FILE_DATE_COLS = ["source_file_date", "_source_file_date", "file_date", "load_date"]

AGENCY_MAP = {
    "agency_id":     ["agencies_agency_id", "agency_id"],
    "agency_name":   ["agencies_agency_name", "agency_name"],
    "organization_type": ["agencies_agency_type", "agency_type"],
    "coc_code":      ["agencies_coc_code", "coc_code"],
    "county":        ["agencies_county_name", "county_name"],
    "address_line1": ["agencies_address", "address"],
    "city":          ["agencies_city", "city"],
    "state":         ["agencies_state", "state"],
    "postal_code":   ["agencies_zip_code", "zip_code"],
}

PROGRAM_MAP = {
    "hmis_program_id":      ["programs_program_id", "program_id"],
    "program_name":         ["programs_full_name", "full_name"],
    "agency_name":          ["programs_agency_name", "agency_name"],
    "coc_code":             ["programs_agency_coc_code", "agency_coc_code"],
    "project_type_code":    ["programs_project_type_code", "project_type_code"],
    "target_population":    ["programs_target_population", "target_population"],
    "address_line1":        ["programs_address", "address"],
    "state":                ["programs_state", "state"],
    "postal_code":          ["programs_zip_code", "zip_code"],
    "is_active_raw":        ["programs_program_active", "program_active"],
    "operating_start_date": ["programs_operating_start_date", "operating_start_date"],
    "operating_end_date":   ["programs_operating_end_date", "operating_end_date"],
    "added_date":           ["programs_added_date", "added_date"],
}

CLIENT_MAP = {
    "personal_id":         ["clients_personal_id", "personal_id"],
    "unique_identifier":   ["clients_unique_identifier", "unique_identifier"],
    "first_name":          ["clients_first_name", "first_name"],
    "middle_name":         ["clients_middle_name", "middle_name"],
    "last_name":           ["clients_last_name", "last_name"],
    "date_of_birth":       ["clients_date_of_birth_date", "date_of_birth_date"],
    "sex":                 ["clients_sex", "sex"],
    "gender":              ["clients_gender", "gender"],
    "race_ethnicity_raw":  ["clients_race_and_ethnicity", "race_and_ethnicity"],
    "marital_status":      ["clients_marital_status", "marital_status"],
    "primary_language":    ["clients_primary_language", "primary_language"],
    "ssn_in":              ["clients_ssn", "ssn"],
    "name_data_quality":   ["clients_name_data_quality", "name_data_quality"],
    "ssn_data_quality":    ["clients_ssn_data_quality", "ssn_data_quality"],
    "dob_data_quality":    ["clients_dob_data_quality", "dob_data_quality"],
    "email":               ["clients_email", "email"],
    "home_phone":          ["clients_phone1", "phone1"],
    "mobile_phone":        ["clients_phone2", "phone2"],
    "address_line1":       ["clients_address", "address"],
    "city":                ["clients_city", "city"],
    "state":               ["clients_state", "state"],
    "postal_code":         ["clients_zip_code", "zip_code"],
    "veteran_status":      ["clients_veteran_status", "veteran_status"],
    "military_branch":     ["clients_branch_of_military", "branch_of_military"],
    "discharge_status":    ["clients_discharge_status", "discharge_status"],
    "disability_status":   ["clients_any_disability", "any_disability"],
    "substance_abuse":     ["clients_substance_use_disorder", "substance_use_disorder"],
    "mental_health_condition": ["clients_mental_health", "mental_health"],
    "source_added_date":   ["clients_date_created_date", "date_created_date"],
    "source_last_updated": ["clients_date_updated_date", "date_updated_date"],
    "created_by":          ["clients_user_created", "user_created"],
    "updated_by":          ["clients_user_updating", "user_updating"],
}

ENROLLMENT_MAP = {
    "enrollment_id":          ["enrollments_enrollment_id"],
    "personal_id":            ["clients_personal_id"],
    "unique_identifier":      ["clients_unique_identifier"],
    "hmis_program_id":        ["programs_program_id"],
    "household_id":           ["enrollments_household_id"],
    "household_type":         ["enrollments_household_type"],
    "individual_or_family":   ["enrollments_individual_or_family"],
    "active_raw":             ["enrollments_active_in_project"],
    "private_raw":            ["enrollments_private_yes_no"],
    "assigned_staff":         ["enrollments_assigned_staff"],
    "enrollment_start_date":  ["enrollments_project_start_date"],
    "enrollment_exit_date":   ["enrollments_project_exit_date"],
    "household_move_in_date": ["enrollments_household_move_in_date"],
    "entry_move_in_date":     ["entry_screen_housing_move_in_date"],
    "relationship_to_hoh":    ["entry_screen_relationship_to_head_of_household"],
    "hoh_raw":                ["entry_screen_head_of_household_yes_no"],
    "prior_living_category":  ["entry_screen_prior_living_situation_category"],
    "continuum_code":         ["programs_coc"],
    "project_type_code":      ["programs_project_type_code"],
    "grant_id":               ["program_funding_sources_grant_id"],
    "grant_identifier":       ["program_funding_sources_grant_identifier"],
    "destination_at_exit":    ["update_exit_screen_destination"],
    "destination_category":   ["update_exit_screen_destination_category"],
    "exit_reason":            ["update_exit_screen_exit_reason"],
    "exit_screen_added_date": ["update_exit_screen_added_date"],
    "source_added_date":      ["enrollments_date_created_date"],
    "source_last_updated":    ["enrollments_last_updated_date"],
    "created_by":             ["enrollments_user_creating"],
    "updated_by":             ["enrollments_user_updating"],
}

SERVICE_MAP = {
    "service_id":        ["services_service_id"],
    "enrollment_id":     ["enrollments_enrollment_id"],
    "personal_id":       ["clients_personal_id"],
    "unique_identifier": ["clients_unique_identifier"],
    "service_start_date": ["services_start_date_date"],
    "service_end_date":  ["services_end_date_date"],
    "service_category":  ["services_service_category"],
    "service_item_id":   ["service_items_service_item_id", "services_service_item_id"],
    "service_item_name": ["service_items_service_item_name", "services_service_item_name", "services_name"],
    "delivery_type":     ["service_items_delivery_type", "services_delivery_type"],
    "service_notes":     ["services_services_notes"],
    "aftercare_project": ["services_aftercare_project"],
    "fa_start_date":     ["services_financial_assistance_start_date"],
    "fa_end_date":       ["services_financial_assistance_end_date"],
    "expense_amount":    ["service_expenses_amount"],
    "service_active_raw": ["services_service_active"],
    "source_added_date": ["services_service_added_date"],
    "source_last_updated": ["services_service_updated_date"],
    "created_by":        ["services_user_creating"],
    "updated_by":        ["services_user_updating"],
}

CASE_NOTE_MAP = {
    "note_id":           ["client_notes_enrollment_level_id"],
    "enrollment_id":     ["enrollments_enrollment_id"],
    "personal_id":       ["clients_personal_id"],
    "unique_identifier": ["clients_unique_identifier"],
    "note_date":         ["client_notes_enrollment_level_case_note_date",
                          "client_notes_enrollment_level_date_added_date"],
    "note_title":        ["client_notes_enrollment_level_title"],
    "note_content":      ["client_notes_enrollment_level_note"],
    "note_category":     ["client_notes_enrollment_level_category"],
    "note_author":       ["client_notes_enrollment_level_staff_full_name"],
    "tracking_hours":    ["client_notes_enrollment_level_tracking_hour"],
    "tracking_minutes":  ["client_notes_enrollment_level_tracking_minute"],
    "private_raw":       ["client_notes_enrollment_level_private_yes_no"],
    "source_added_date": ["client_notes_enrollment_level_date_added_date"],
    "source_last_updated": ["client_notes_enrollment_level_date_updated_date"],
}

HOUSEHOLD_MEMBER_MAP = {
    "household_id":      ["enrollments_household_id",
                          "clients_global_household_id_profile_household"],
    "unique_identifier": ["clients_unique_identifier"],
    "member_type":       ["clients_member_type"],
    "household_type":    ["enrollments_household_type", "clients_household_type"],
    "member_start_date": ["clients_household_member_start"],
    "member_end_date":   ["clients_household_member_end"],
    "active_raw":        ["clients_active_in_household"],
}

BED_UNIT_MAP = {
    "hmis_program_id":   ["programs_program_id"],
    "unit_id":           ["inventory_building_units_id"],
    "unit_name":         ["inventory_building_units_name"],
    "bed_id":            ["inventory_unit_configuration_beds_id", "inventory_building_units_bed_id"],
    "bed_name":          ["inventory_unit_configuration_beds_name"],
    "household_type":    ["inventory_building_unit_configurations_household_type"],
    "inventory_start_date": ["inventory_unit_configuration_history_status_start_date",
                             "inventory_building_units_start_date"],
    "added_date":        ["inventory_unit_configuration_beds_added_date",
                          "inventory_building_unit_configurations_added_date"],
    "last_updated_date": ["inventory_unit_configuration_beds_last_updated_date",
                          "inventory_building_units_last_updated_date"],
    "availability_status": ["bed_availability_status_current_availability"],
    "availability_start":  ["bed_availability_status_current_status_start_date"],
    "availability_end":    ["bed_availability_status_current_status_end_date"],
    "bed_status":        ["bed_status_status"],
    "offline_reason":    ["bed_status_offline_reason"],
    "bed_status_current_raw": ["bed_status_is_current_bed_status"],
    "bed_status_start":  ["bed_status_status_start_date"],
    "bed_status_end":    ["bed_status_status_end_date"],
    "unit_status":       ["unit_status_status"],
    "occupancy_id":      ["bed_occupancy_id"],
    "assignment_date":   ["bed_occupancy_assignment_date"],
    "occupation_start":  ["bed_occupancy_occupation_start_date"],
    "occupation_end":    ["bed_occupancy_occupation_end_date"],
    "pending_end":       ["bed_occupancy_pending_occupation_end_date"],
    "occupancy_type":    ["bed_occupancy_type"],
    "enrollment_id":     ["enrollments_enrollment_id"],
    "personal_id":       ["clients_personal_id"],
    "unique_identifier": ["clients_unique_identifier"],
}

FUNDING_MAP = {
    "funding_source_id": ["program_funding_sources_id"],
    "hmis_program_id":   ["programs_program_id"],
    "grant_id":          ["program_funding_sources_grant_id"],
    "grant_identifier":  ["program_funding_sources_grant_identifier"],
    "fund_name":         ["program_funding_sources_fund_name"],
    "funding_source":    ["program_funding_sources_funding_source"],
    "funding_source_code": ["program_funding_sources_funding_source_code"],
    "amount":            ["program_funding_sources_amount"],
    "grant_amount":      ["program_funding_sources_grant_amount"],
    "original_amount":   ["program_funding_sources_original_amount"],
    "total_available":   ["program_funding_sources_total_available"],
    "fs_start_date":     ["program_funding_sources_funding_source_start_date_date"],
    "fs_end_date":       ["program_funding_sources_funding_source_end_date_date"],
    "grant_start_date":  ["program_funding_sources_grant_start_date"],
    "grant_end_date":    ["program_funding_sources_grant_end_date"],
    "active_raw":        ["program_funding_sources_currently_active_yes_no"],
    "direct_expense_raw": ["program_funding_sources_direct_expense_yes_no"],
    "eligibility_raw":   ["program_funding_sources_eligibility_yes_no"],
    "source_added_date": ["program_funding_sources_grant_added_date"],
    "source_last_updated": ["program_funding_sources_grant_updated_date"],
}

ROI_MAP = {
    "roi_id":            ["release_of_information_id"],
    "unique_identifier": ["clients_unique_identifier"],
    "personal_id":       ["clients_personal_id"],
    "agency_name":       ["release_of_information_agency_name"],
    "documentation":     ["release_of_information_documentation"],
    "start_date":        ["release_of_information_start_date"],
    "end_date":          ["release_of_information_end_date"],
    "active_raw":        ["release_of_information_active_roi"],
    "permission_raw":    ["release_of_information_permission"],
    "source_added_date": ["release_of_information_added_date"],
    "source_last_updated": ["release_of_information_last_updated_date"],
    "created_by":        ["release_of_information_user_creating"],
    "updated_by":        ["release_of_information_user_updating"],
}

SSN_PLACEHOLDERS = ["123456789", "987654321"] + [str(d) * 9 for d in range(10)]


# ---------------- CELL 2: GUARD + HELPERS ----------------
import re, json, datetime as dt
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
RUN_ID = f"{SOURCE_SYSTEM}_{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"
ALLOWED = {x.strip().lower() for x in ALLOW_CHECKS.split(",") if x.strip()}
QLOG = []

def qlog(check, table, value, status, detail=""):
    QLOG.append((RUN_ID, SOURCE_SYSTEM, "nb_silver_hmis_clarity_conform", check, table,
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
    """This county's row from etl_source_config: from the pipeline's
    parameter if given, otherwise read from the warehouse."""
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
    raise ValueError(f"{SOURCE_SYSTEM} has no row in etl_source_config. Add one only after the "
                     f"CoC confirms in writing that client-level data may be stored.")
if CFG.get("block_reason"):
    raise ValueError(f"{SOURCE_SYSTEM} is blocked: {CFG['block_reason']}")
if not CFG.get("is_enabled"):
    print(f"NOTE: {SOURCE_SYSTEM} is not enabled for the weekly run (manual run only).")
if CFG.get("bronze_prefix"):
    SRC = {k: v.replace(BRONZE_PATTERN.format(src=SOURCE_SYSTEM, entity=""), CFG["bronze_prefix"], 1)
           for k, v in SRC.items()}
print(f"Conforming {SOURCE_SYSTEM} ({CFG.get('notes') or 'approved in etl_source_config'})")

def norm(name: str) -> str:
    n = re.sub(r"[^0-9a-z]+", "_", name.strip().lower())
    return re.sub(r"_+", "_", n).strip("_")

def read_normalized(table: str):
    """Read a table and normalize column names, suffixing duplicates
    (Client.csv repeats City/State/ZIP/Marital Status)."""
    df = spark.read.table(table)
    seen, new_cols = {}, []
    for c in df.columns:
        n = norm(c)
        if n in seen:
            seen[n] += 1
            n = f"{n}_{seen[n]}"
        else:
            seen[n] = 0
        new_cols.append(n)
    return df.toDF(*new_cols)

def read_optional(key, mapping):
    """Read an optional Bronze table. If it's missing, return an EMPTY table
    with the expected columns, so this county's rows in Silver are cleared
    and the Silver table always exists for the warehouse loader."""
    if table_exists(SRC[key]):
        return read_normalized(SRC[key])
    print(f"NO DATA {key}: {SRC[key]} not found. This county's {key} rows will be empty. "
          f"Land it in Bronze to include it.")
    from pyspark.sql.types import StructType, StructField, StringType
    cols = list(dict.fromkeys(c[0] for c in mapping.values()))
    return spark.createDataFrame([], StructType([StructField(c, StringType()) for c in cols]))

def clean_money(col):
    c = F.regexp_replace(clean_str(col), r"[$,\s]", "")
    return F.when(c.rlike(r"^-?[0-9]*\.?[0-9]+$"), c.cast("decimal(18,2)"))

def pick(df, candidates):
    for c in candidates:
        if c in df.columns:
            return F.col(c)
    return F.lit(None)

def clean_str(col):
    c = F.trim(col.cast("string"))
    return F.when(c == "", None).otherwise(c)

def clean_date(col):
    c = clean_str(col)
    return F.coalesce(
        F.to_date(F.substring(c, 1, 10), "yyyy-MM-dd"),
        F.to_date(c, "M/d/yyyy"),
        F.to_date(c, "MM/dd/yyyy"),
    )

def yes_no_bit(col):
    c = F.lower(clean_str(col))
    return (F.when(c.isin("yes", "y", "true", "1"), F.lit(True))
             .when(c.isin("no", "n", "false", "0"), F.lit(False)))

def apply_map(df, mapping, date_cols=()):
    out = []
    for tgt, cands in mapping.items():
        expr = pick(df, cands)
        expr = clean_date(expr) if tgt in date_cols else clean_str(expr)
        out.append(expr.alias(tgt))
    return out

def lineage(df):
    return [
        clean_str(pick(df, FILE_NAME_COLS)).alias("source_file_name"),
        clean_date(pick(df, FILE_DATE_COLS)).alias("source_file_date"),
    ]

def table_exists(name):
    try:
        spark.read.table(name).limit(0).collect()
        return True
    except Exception:
        return False

PENDING = {}

def write(df, key):
    """Queue a table. Nothing is written until the guard cell has checked
    every table (see WRITE cell near the end)."""
    PENDING[key] = df

def _write_now(df, key):
    """Replace only this county's rows in a shared table."""
    out = (df.withColumn("source_system", F.lit(SOURCE_SYSTEM))
             .withColumn("load_datetime", F.current_timestamp()))
    w = out.write.format("delta")
    if "schema" in ALLOWED:
        w = w.option("mergeSchema", "true")
    if table_exists(TGT[key]):
        (w.mode("overwrite")
          .option("replaceWhere", f"source_system = '{SOURCE_SYSTEM}'")
          .saveAsTable(TGT[key]))
    else:
        w.mode("overwrite").partitionBy("source_system").saveAsTable(TGT[key])
    n = spark.read.table(TGT[key]).filter(F.col("source_system") == SOURCE_SYSTEM).count()
    print(f"{TGT[key]} [{SOURCE_SYSTEM}]: {n:,} rows")


# ---------------- CELL 2b: EXPORT FRESHNESS ----------------
missing_core = [k for k in CORE if not table_exists(SRC[k])]
if missing_core:
    qlog("core_tables_present", ",".join(missing_core), len(missing_core), "stop")
    stop(f"{SOURCE_SYSTEM}: Bronze is missing {', '.join(SRC[k] for k in missing_core)}. "
         f"Land all four core exports (organization, program, client, enrollment) with nb_bronze_land_files.")
today = spark.sql("SELECT current_date() AS d").first()["d"]
newest, batches = {}, set()
for k in CORE:
    b = read_normalized(SRC[k])
    date_col = next((c for c in FILE_DATE_COLS if c in b.columns), None)
    newest[k] = b.agg(F.max(clean_date(F.col(date_col)))).first()[0] if date_col else None
    if "bronze_batch_id" in b.columns:
        batches.add(b.agg(F.max("bronze_batch_id")).first()[0])
stale = {k: d for k, d in newest.items() if d is not None and (today - d).days > MAX_EXPORT_AGE_DAYS}
undated = [k for k, d in newest.items() if d is None]
mixed = len(batches) > 1
for k, d in newest.items():
    qlog("export_age_days", k, (today - d).days if d else None,
         "warn" if d is None else ("stop" if k in stale else "pass"), f"newest export {d}")
if mixed:
    qlog("core_same_batch", "core", len(batches), "stop", f"batches {sorted(batches)}")
if undated:
    print(f"NOTE: no export date on {', '.join(undated)}; land files through nb_bronze_land_files.")
if (stale or mixed) and "stale" not in ALLOWED:
    stop(f"{SOURCE_SYSTEM}: export is stale or mixed. "
         + (f"Older than {MAX_EXPORT_AGE_DAYS} days: {', '.join(f'{k} ({d})' for k, d in stale.items())}. " if stale else "")
         + ("Core tables come from different landing batches. " if mixed else "")
         + "Land a complete new export, or rerun with ALLOW_CHECKS = 'stale' after checking.")
if stale or mixed:
    qlog("stale_override", "core", None, "override", "ALLOW_CHECKS stale")


# ---------------- CELL 3: AGENCY ----------------
a_raw = read_normalized(SRC["agency"])
agency = (a_raw.select(*apply_map(a_raw, AGENCY_MAP))
               .filter(F.col("agency_id").isNotNull())
               .dropDuplicates(["agency_id"]))
write(agency, "agency")


# ---------------- CELL 4: PROGRAM ----------------
p_raw = read_normalized(SRC["program"])
p = p_raw.select(
    *apply_map(p_raw, PROGRAM_MAP,
               date_cols=("operating_start_date", "operating_end_date", "added_date")),
    *lineage(p_raw),
).filter(F.col("hmis_program_id").isNotNull())

program = (p.groupBy("hmis_program_id")
            .agg(*[F.first(c, ignorenulls=True).alias(c)
                   for c in p.columns if c != "hmis_program_id"])
            .withColumn("project_type_code", F.col("project_type_code").cast("int"))
            .withColumn("is_active", yes_no_bit(F.col("is_active_raw")))
            .drop("is_active_raw"))
write(program, "program")


# ---------------- CELL 5: CLIENT ----------------
c_raw = read_normalized(SRC["client"])
c = c_raw.select(
    *apply_map(c_raw, CLIENT_MAP,
               date_cols=("date_of_birth", "source_added_date", "source_last_updated")),
    *lineage(c_raw),
).filter(F.col("personal_id").isNotNull())

digits = F.regexp_replace(F.coalesce(F.col("ssn_in"), F.lit("")), r"[^0-9]", "")
import hmac, hashlib
SSN_KEY = None
if KEY_VAULT_URL.strip():
    import notebookutils
    SSN_KEY = notebookutils.credentials.getSecret(KEY_VAULT_URL, SSN_HASH_SECRET_NAME)
else:
    print("NOTE: KEY_VAULT_URL is not set, so ssn_hash stays empty. Matching on SSN needs the keyed hash.")
_ssn_key = SSN_KEY.encode() if SSN_KEY else None

@F.udf("string")
def hmac_sha256(value):
    if not value or _ssn_key is None:
        return None
    return hmac.new(_ssn_key, value.encode(), hashlib.sha256).hexdigest()

ssn_valid = (
    (F.length(digits) == 9)
    & ~digits.substr(1, 3).isin("000", "666")
    & (digits.substr(1, 1) != "9")
    & (digits.substr(4, 2) != "00")
    & (digits.substr(6, 4) != "0000")
    & ~digits.isin(*SSN_PLACEHOLDERS)
)

rx = F.col("race_ethnicity_raw")
not_collected = F.lower(rx).rlike("doesn.t know|prefers not|refused|not collected")

c = (c
     .withColumn("ssn_raw", F.when(F.length(digits) == 9,
                 F.concat_ws("-", digits.substr(1, 3), digits.substr(4, 2), digits.substr(6, 4))))
     .withColumn("ssn_hash", F.when(ssn_valid, hmac_sha256(digits)))
     .withColumn("ssn_last4", F.when(ssn_valid, digits.substr(6, 4)))
     .withColumn("race", rx)
     .withColumn("race_detail", rx)
     .withColumn("ethnicity",
                 F.when(rx.isNull() | not_collected, F.lit(None))
                  .when(F.lower(rx).contains("hispanic"), F.lit("Yes"))
                  .otherwise(F.lit("No")))
     .drop("ssn_in", "race_ethnicity_raw"))

w = Window.partitionBy("personal_id").orderBy(
    F.col("source_last_updated").desc_nulls_last(),
    F.col("source_added_date").desc_nulls_last())
client = c.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
dupes = c.count() - client.count()
if dupes:
    print(f"NOTE: {dupes:,} duplicate client rows collapsed by Personal ID")
write(client, "client")


# ---------------- CELL 6: ENROLLMENT ----------------
e_raw = read_normalized(SRC["enrollment"])
e = e_raw.select(
    *apply_map(e_raw, ENROLLMENT_MAP, date_cols=(
        "enrollment_start_date", "enrollment_exit_date", "household_move_in_date",
        "entry_move_in_date", "exit_screen_added_date",
        "source_added_date", "source_last_updated")),
    *lineage(e_raw),
).filter(F.col("enrollment_id").isNotNull())

raw_rows = e.count()

exit_w = Window.partitionBy("enrollment_id").orderBy(
    F.col("destination_at_exit").isNull().asc(),
    F.col("exit_screen_added_date").desc_nulls_last())
exit_fields = (e.withColumn("_rn", F.row_number().over(exit_w))
                .filter("_rn = 1")
                .select("enrollment_id", "destination_at_exit",
                        "destination_category", "exit_reason"))

exit_cols = {"destination_at_exit", "destination_category", "exit_reason",
             "exit_screen_added_date", "grant_id", "grant_identifier"}
base_cols = [c_ for c_ in e.columns if c_ != "enrollment_id" and c_ not in exit_cols]

enrollment = (
    e.groupBy("enrollment_id")
     .agg(*[F.first(c_, ignorenulls=True).alias(c_) for c_ in base_cols],
          F.array_join(F.array_sort(F.collect_set("grant_identifier")), "; ").alias("grant_name"),
          F.array_join(F.array_sort(F.collect_set("grant_id")), "; ").alias("grant_ids"),
          F.size(F.collect_set("grant_id")).alias("grant_count"),
          F.first("grant_id", ignorenulls=True).alias("_first_grant_id"))
     .join(exit_fields, "enrollment_id", "left")
     .withColumn("grant_name", F.when(F.col("grant_name") == "", None).otherwise(F.col("grant_name")))
     .withColumn("grant_ids", F.when(F.col("grant_ids") == "", None).otherwise(F.col("grant_ids")))
     .withColumn("source_grant_id",
                 F.when(F.col("grant_count") == 1, F.col("_first_grant_id")))
     .withColumnRenamed("individual_or_family", "household_composition_type")
     .drop("_first_grant_id")
     .withColumn("housing_move_in_date",
                 F.coalesce("household_move_in_date", "entry_move_in_date"))
     .withColumn("is_head_of_household", yes_no_bit(F.col("hoh_raw")))
     .withColumn("is_private", yes_no_bit(F.col("private_raw")))
     .withColumn("enrollment_status",
                 F.when(F.col("enrollment_exit_date").isNotNull(), F.lit("Exited"))
                  .otherwise(F.lit("Active")))
     .withColumn("length_of_stay_days",
                 F.datediff(F.coalesce("enrollment_exit_date", F.current_date()),
                            "enrollment_start_date"))
     .withColumn("project_type_code", F.col("project_type_code").cast("int"))
     .drop("household_move_in_date", "entry_move_in_date", "hoh_raw",
           "private_raw", "active_raw")
)

print(f"Enrollment export rows: {raw_rows:,} -> {enrollment.count():,} enrollments after collapsing fan-out")
write(enrollment, "enrollment")

grant = (e.filter(F.col("grant_id").isNotNull())
          .groupBy("grant_id")
          .agg(F.first("grant_identifier", ignorenulls=True).alias("grant_identifier"),
               F.array_join(F.array_sort(F.collect_set("hmis_program_id")), "; ")
                .alias("hmis_program_ids")))
write(grant, "grant")


# ---------------- CELL 7: SERVICES (optional) ----------------
s_raw = read_optional("service", SERVICE_MAP)
sv = s_raw.select(
    *apply_map(s_raw, SERVICE_MAP, date_cols=(
        "service_start_date", "service_end_date", "fa_start_date", "fa_end_date",
        "source_added_date", "source_last_updated")),
    *lineage(s_raw),
).filter(F.col("service_id").isNotNull())
sv = sv.withColumn("expense_amount", clean_money(F.col("expense_amount")))
keep = [c_ for c_ in sv.columns if c_ not in ("service_id", "expense_amount")]
service = (sv.groupBy("service_id")
             .agg(*[F.first(c_, ignorenulls=True).alias(c_) for c_ in keep],
                  F.sum("expense_amount").alias("service_amount"))
             .withColumn("is_active", yes_no_bit(F.col("service_active_raw")))
             .drop("service_active_raw"))
write(service, "service")

service_item = (sv.filter(F.col("service_item_id").isNotNull() | F.col("service_item_name").isNotNull())
                  .withColumn("service_item_code",
                              F.coalesce("service_item_id", "service_item_name"))
                  .groupBy("service_item_code")
                  .agg(F.first("service_item_name", ignorenulls=True).alias("service_item_name"),
                       F.first("service_category", ignorenulls=True).alias("service_category"),
                       F.first("delivery_type", ignorenulls=True).alias("delivery_type")))
write(service_item, "service_item")


# ---------------- CELL 8: CASE NOTES (optional) ----------------
n_raw = read_optional("case_note", CASE_NOTE_MAP)
cn = n_raw.select(
    *apply_map(n_raw, CASE_NOTE_MAP, date_cols=(
        "note_date", "source_added_date", "source_last_updated")),
    *lineage(n_raw),
).filter(F.col("note_id").isNotNull())
w_n = Window.partitionBy("note_id").orderBy(F.col("source_last_updated").desc_nulls_last())
case_note = (cn.withColumn("_rn", F.row_number().over(w_n)).filter("_rn = 1").drop("_rn")
               .withColumn("note_date", F.coalesce("note_date", "source_added_date"))
               .withColumn("time_tracked_minutes",
                           (F.coalesce(F.col("tracking_hours").cast("int"), F.lit(0)) * 60
                            + F.coalesce(F.col("tracking_minutes").cast("int"), F.lit(0))))
               .withColumn("time_tracked_minutes",
                           F.when(F.col("tracking_hours").isNull() & F.col("tracking_minutes").isNull(), None)
                            .otherwise(F.col("time_tracked_minutes")))
               .withColumn("is_private", yes_no_bit(F.col("private_raw")))
               .drop("tracking_hours", "tracking_minutes", "private_raw"))
write(case_note, "case_note")


# ---------------- CELL 9: HOUSEHOLD MEMBERS (optional) ----------------
h_raw = read_optional("household_member", HOUSEHOLD_MEMBER_MAP)
hm = h_raw.select(
    *apply_map(h_raw, HOUSEHOLD_MEMBER_MAP, date_cols=("member_start_date", "member_end_date")),
    *lineage(h_raw),
).filter(F.col("household_id").isNotNull() & F.col("unique_identifier").isNotNull())
w_h = Window.partitionBy("household_id", "unique_identifier").orderBy(
    F.col("member_start_date").desc_nulls_last())
household_member = (hm.withColumn("_rn", F.row_number().over(w_h)).filter("_rn = 1").drop("_rn")
                      .withColumn("is_active", yes_no_bit(F.col("active_raw")))
                      .drop("active_raw"))
write(household_member, "household_member")


# ---------------- CELL 10: BEDS AND UNITS (optional) ----------------
b_raw = read_optional("bed_unit", BED_UNIT_MAP)
bu = b_raw.select(
    *apply_map(b_raw, BED_UNIT_MAP, date_cols=(
        "inventory_start_date", "added_date", "last_updated_date",
        "availability_start", "availability_end", "bed_status_start", "bed_status_end",
        "assignment_date", "occupation_start", "occupation_end", "pending_end")),
    *lineage(b_raw),
).filter(F.col("bed_id").isNotNull() | F.col("unit_id").isNotNull())
bu = bu.withColumn("bed_id", F.coalesce("bed_id", F.concat(F.lit("unit-"), F.col("unit_id"))))

bed_cols = ["hmis_program_id", "unit_id", "unit_name", "bed_name", "household_type",
            "inventory_start_date", "added_date", "last_updated_date",
            "source_file_name", "source_file_date"]
bed = bu.groupBy("bed_id").agg(*[F.first(c_, ignorenulls=True).alias(c_) for c_ in bed_cols])
write(bed, "bed")

bed_status = (bu.filter(F.col("bed_status").isNotNull() | F.col("availability_status").isNotNull())
                .withColumn("is_current", yes_no_bit(F.col("bed_status_current_raw")))
                .select("bed_id", "hmis_program_id", "availability_status", "availability_start",
                        "availability_end", "bed_status", "offline_reason", "bed_status_start",
                        "bed_status_end", "unit_status", "is_current",
                        "source_file_name", "source_file_date")
                .dropDuplicates(["bed_id", "bed_status", "bed_status_start", "availability_status",
                                 "availability_start"])
                .withColumn("status_record_id", F.concat_ws("|", "bed_id",
                            F.coalesce(F.col("bed_status_start").cast("string"), F.lit("")),
                            F.coalesce("bed_status", F.lit("")),
                            F.coalesce(F.col("availability_start").cast("string"), F.lit("")))))
write(bed_status, "bed_status")

occ = (bu.filter(F.col("occupancy_id").isNotNull() & F.col("occupation_start").isNotNull())
         .dropDuplicates(["occupancy_id"])
         .withColumn("night_from", F.greatest(F.col("occupation_start"), F.lit(BED_NIGHT_START).cast("date")))
         .withColumn("night_to", F.date_sub(F.coalesce("occupation_end", "pending_end",
                                                       F.date_add(F.current_date(), 1)), 1))
         .withColumn("night_to", F.least(F.col("night_to"), F.current_date()))
         .filter(F.col("night_to") >= F.col("night_from")))
bed_night = (occ.withColumn("bed_night_date", F.explode(F.sequence("night_from", "night_to")))
                .select("occupancy_id", "bed_id", "hmis_program_id", "enrollment_id", "personal_id",
                        "unique_identifier", "bed_night_date", "assignment_date", "occupation_start",
                        "occupation_end", "pending_end", "occupancy_type",
                        "source_file_name", "source_file_date"))
write(bed_night, "bed_night")


# ---------------- CELL 11: FUNDING SOURCES (optional) ----------------
f_raw = read_optional("funding", FUNDING_MAP)
fs = f_raw.select(
    *apply_map(f_raw, FUNDING_MAP, date_cols=(
        "fs_start_date", "fs_end_date", "grant_start_date", "grant_end_date",
        "source_added_date", "source_last_updated")),
    *lineage(f_raw),
).filter(F.col("funding_source_id").isNotNull())
for m in ("amount", "grant_amount", "original_amount", "total_available"):
    fs = fs.withColumn(m, clean_money(F.col(m)))
funding_source = (fs.dropDuplicates(["funding_source_id"])
                    .withColumn("is_active", yes_no_bit(F.col("active_raw")))
                    .withColumn("is_direct_expense", yes_no_bit(F.col("direct_expense_raw")))
                    .withColumn("is_eligibility", yes_no_bit(F.col("eligibility_raw")))
                    .drop("active_raw", "direct_expense_raw", "eligibility_raw"))
write(funding_source, "funding_source")


# ---------------- CELL 12: ENROLLMENT GRANTS ----------------
grant_period = (fs.filter(F.col("grant_id").isNotNull())
                  .groupBy("grant_id")
                  .agg(F.min(F.coalesce("grant_start_date", "fs_start_date")).alias("grant_start"),
                       F.max(F.coalesce("grant_end_date", "fs_end_date")).alias("grant_end")))
program_fs = (fs.filter(F.col("grant_id").isNotNull() & F.col("hmis_program_id").isNotNull())
                .select("grant_id", "hmis_program_id", "funding_source_id")
                .dropDuplicates(["grant_id", "hmis_program_id"]))
eg = (e.filter(F.col("grant_id").isNotNull())
        .select("enrollment_id", "grant_id", "grant_identifier").dropDuplicates(["enrollment_id", "grant_id"])
        .join(enrollment.select("enrollment_id", "hmis_program_id", "enrollment_start_date", "enrollment_exit_date"),
              "enrollment_id")
        .join(grant_period, "grant_id", "left")
        .join(program_fs, ["grant_id", "hmis_program_id"], "left"))
enr_end = F.coalesce("enrollment_exit_date", F.current_date())
eg = (eg.withColumn("period_checked", F.col("grant_start").isNotNull() & F.col("grant_end").isNotNull())
        .withColumn("overlaps", ~F.col("period_checked") |
                    ((F.col("enrollment_start_date") <= F.col("grant_end")) & (enr_end >= F.col("grant_start")))))
dropped = eg.filter(~F.col("overlaps")).count()
if dropped:
    print(f"NOTE: {dropped:,} enrollment-grant pairs dropped: grant period doesn't overlap the enrollment")
enrollment_grant = (eg.filter("overlaps")
    .withColumn("link_start", F.greatest("enrollment_start_date", F.coalesce("grant_start", "enrollment_start_date")))
    .withColumn("link_end", F.when(F.col("grant_end").isNull(), F.col("enrollment_exit_date"))
                             .otherwise(F.least(F.coalesce("enrollment_exit_date", "grant_end"), F.col("grant_end"))))
    .select("enrollment_id", "grant_id", "grant_identifier", "hmis_program_id", "funding_source_id",
            "link_start", "link_end", "period_checked"))
write(enrollment_grant, "enrollment_grant")


# ---------------- CELL 13: RELEASES OF INFORMATION (optional) ----------------
r_raw = read_optional("roi", ROI_MAP)
ro = r_raw.select(
    *apply_map(r_raw, ROI_MAP, date_cols=(
        "start_date", "end_date", "source_added_date", "source_last_updated")),
    *lineage(r_raw),
).filter(F.col("roi_id").isNotNull())
w_r = Window.partitionBy("roi_id").orderBy(F.col("source_last_updated").desc_nulls_last())
ro = ro.withColumn("_rn", F.row_number().over(w_r)).filter("_rn = 1").drop("_rn")
w_c = Window.partitionBy("unique_identifier", "agency_name").orderBy(
    F.col("start_date").desc_nulls_last(), F.col("source_last_updated").desc_nulls_last())
roi = (ro.withColumn("is_current", F.row_number().over(w_c) == 1)
         .withColumn("consent_status",
                     F.when(F.col("end_date") < F.current_date(), "Expired")
                      .when(F.col("start_date") > F.current_date(), "Not started")
                      .when(F.col("start_date").isNotNull(), "Active"))
         .withColumn("is_active_flag", yes_no_bit(F.col("active_raw")))
         .withColumn("permission_granted", yes_no_bit(F.col("permission_raw")))
         .drop("active_raw", "permission_raw"))
write(roi, "roi")


# ---------------- CELL 14: GATES, THEN WRITE ----------------
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
    if dropped and not ("all" in allow or key in allow or DERIVED.get(key) in allow):
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

enr_q, cli_q, prg_q = PENDING["enrollment"], PENDING["client"], PENDING["program"]
n_enr = enr_q.count()
for label, n in [("enrollments with no client", enr_q.join(cli_q, "personal_id", "left_anti").count()),
                 ("enrollments with no program", enr_q.join(prg_q, "hmis_program_id", "left_anti").count())]:
    pct = 100 * n / n_enr if n_enr else 0
    over = pct > MAX_ORPHAN_PCT
    status = ("override" if "orphans" in ALLOWED else "stop") if over else ("warn" if n else "pass")
    qlog("orphans", label, n, status, f"{pct:.1f}% of {n_enr} enrollments")
    if status == "stop":
        problems.append(f"  {label}: {n:,} ({pct:.1f}%)")

if problems:
    stop(f"Quality gates stopped {SOURCE_SYSTEM} before anything was written:\n" + "\n".join(problems)
         + "\nCheck the export. If a drop is real, rerun with ALLOW_DROP set to those tables; "
           "for a schema change or orphans, ALLOW_CHECKS = 'schema' or 'orphans'.")

for key, df, old_n, new_n in plan:
    _write_now(df, key)
    print(f"    was {old_n:,}")
flush_qlog()


# ---------------- CELL 15: QUICK CHECKS (this county only) ----------------
mine = F.col("source_system") == SOURCE_SYSTEM
enr = spark.read.table(TGT["enrollment"]).filter(mine)
cli = spark.read.table(TGT["client"]).filter(mine)
prg = spark.read.table(TGT["program"]).filter(mine)

checks = {
    "enrollments with no matching client": enr.join(cli, "personal_id", "left_anti").count(),
    "enrollments with no matching program": enr.join(prg, "hmis_program_id", "left_anti").count(),
    "enrollments missing start date": enr.filter(F.col("enrollment_start_date").isNull()).count(),
    "enrollments starting before 2000 (outside dim_date)":
        enr.filter(F.col("enrollment_start_date") < F.lit("2000-01-01")).count(),
    "enrollments missing household_id": enr.filter(F.col("household_id").isNull()).count(),
    "enrollments in multi-grant programs (grant_key will be NULL)":
        enr.filter(F.col("grant_count") > 1).count(),
    "clients with SSN kept but not hashed (placeholder, invalid, or no key)":
        cli.filter(F.col("ssn_raw").isNotNull() & F.col("ssn_hash").isNull()).count(),
}
def optional_check(key, label, fn):
    if table_exists(TGT[key]):
        df = spark.read.table(TGT[key]).filter(mine)
        if df.limit(1).count():
            checks[label] = fn(df)

optional_check("service", "services with no matching enrollment",
               lambda d: d.join(enr, "enrollment_id", "left_anti").count())
optional_check("case_note", "case notes with no matching enrollment",
               lambda d: d.join(enr, "enrollment_id", "left_anti").count())
optional_check("bed_night", "bed nights with no matching enrollment",
               lambda d: d.filter(F.col("enrollment_id").isNotNull())
                          .join(enr, "enrollment_id", "left_anti").count())
optional_check("funding_source", "funding sources with no program ID (add Programs Program ID to the export)",
               lambda d: d.filter(F.col("hmis_program_id").isNull()).count())
optional_check("roi", "ROIs with no matching client",
               lambda d: d.join(cli.select("unique_identifier"), "unique_identifier", "left_anti").count())

for k, v in checks.items():
    print(f"{'OK ' if v == 0 else 'CHECK'}  {k}: {v:,}")
    qlog("quick_check", k, v, "pass" if v == 0 else "warn")
flush_qlog()


# ---------------- CELL 16: EXIT ----------------
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
