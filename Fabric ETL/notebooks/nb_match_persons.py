# nb_match_persons v1.3
# v1.3: the trained model is kept in memory and each model file is written once. v1.2 read the file back
#       right after writing it, and Fabric's lakehouse folder can return a just-written file empty
#       (JSONDecodeError on the first Fabric run, 2026-11-22). A saved model that is empty or damaged now
#       stops the run with a clear message instead of a parsing error; it is never silently retrained.
# v1.2: u is measured from every pair when the population allows (U_MAX_PAIRS, was a fixed 2 million), and a
#       comparison level never seen in the sample gets a cautious estimate (3 / pairs sampled, the statistical
#       upper bound for something seen 0 times) instead of Splink's generic default, which made SSN and exact
#       name agreement count for far too little. The levels estimated this way are logged (u_levels_estimated).
# v1.1 notes:
# v1.1: SSNs are compared without a stored key. The notebook reads the full SSN from Clarity Silver
#       (ssn_raw, already there for the Apricot import), turns each valid SSN into a token with a random
#       key made for this run only, and compares tokens. The SSN, the tokens and the key are never written
#       anywhere and the key is discarded when the run ends, so no Key Vault is needed (docs/design_decisions.md, decision 12).
#       If Silver has no ssn_raw column, the keyed ssn_hash from Silver is used instead.
# Finds profiles that may be the same person, across Salesforce CIS and every Clarity county and within
# each one. Reads Silver only; writes IDs, scores and plain-language evidence to lh_silver, never names,
# dates or contact details. The warehouse turns the output into decisions and the review queue
# (dbo.usp_apply_match_suggestions), so this notebook never changes the warehouse itself.
#
# Two layers:
#   1. Exact rules (deterministic, checked by hand on real data before going live):
#        same_ssn_dob      same valid SSN (compared as a per-run token) and the same full date of birth or the same name
#        name_dob_contact  same first and last name, same full date of birth, and a shared phone or email
#      An identifier shared by more than SHARED_LIMIT profiles (a shelter's front-desk phone, an agency
#      email, a placeholder SSN) is never used as evidence.
#      An exact-rule pair with contrary evidence (two different valid SSNs, or different recorded sex)
#      goes to review instead.
#   2. A Splink model (probabilistic, Fellegi-Sunter, with term-frequency adjustments so a common name
#      counts for less than a rare one). Pairs scoring at or above REVIEW_THRESHOLD go to the review
#      queue. Pairs at or above AUTO_LINK_THRESHOLD are linked without review only when that parameter is
#      set; it is off by default and should stay off until reviewed pairs show the model is reliable.
#
# The model is trained once, saved under Files/matching and reused, so scores do not drift week to week.
# Retrain on purpose (TRAIN_MODEL = True), look at the match weights chart, then point MODEL_FILE at the new
# file. Every suggestion row carries the model_id that scored it.
#
# Pipeline: after the Silver notebooks and before nb_refresh_sql_endpoint.
# Default lakehouse: lh_silver (the model folder is /lakehouse/default/Files/matching).
# Library: splink==4.0.17, pinned in the Environment env_matching on Fabric Runtime 2.0 (Spark 4.1, Python 3.13).
# Not 5.0.0: that first 5.x release stops training with an error when a comparison level's estimate reaches
# zero, which a small population can trigger (seen in testing). Move to 5.x only after rerunning the tests.

# ---------------- CELL 0: PARAMETERS (mark as the parameter cell) ----------------
SOURCES = "sf_cis,hmis_lac"          # every enabled source; hmis_* read from conf_hmis_clarity_client
REVIEW_THRESHOLD = 0.80              # Splink match probability that puts a pair in the review queue
AUTO_LINK_THRESHOLD = ""             # e.g. "0.999" to link without review; blank = off (recommended)
MODEL_DIR = "/lakehouse/default/Files/matching"
MODEL_FILE = "person_model_current.json"
TRAIN_MODEL = False                  # True: train a new model from this run's data and save it
SHARED_LIMIT = 3                     # a phone, email, address or SSN on more profiles than this is not evidence
MAX_INPUT_DROP_PCT = 10              # stop if a source has this % fewer profiles than the last run
MAX_LINK_PCT = 2                     # stop if exact-rule links exceed this % of profiles
MAX_REVIEW_PCT = 10                  # stop if review pairs exceed this % of profiles
MAX_COMPARISONS = 20_000_000         # stop if the blocking rules would create more pairs than this
ALLOW_CHECKS = ""                    # gates to let through after checking: "input_drop,link_volume,review_volume"
KEEP_RUNS = 8                        # suggestion runs kept in lh_silver.dbo.match_suggestion
LABELS_PATH = ""                     # with TRAIN_MODEL: OneLake path of the warehouse table dbo.match_decision
                                     # (abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<warehouse>.Warehouse/Tables/dbo/match_decision)
MIN_LABELS = 200                     # reviewer "same person" decisions needed before m is learned from them
U_MAX_PAIRS = 20_000_000             # random pairs sampled to measure u; at least every pair up to ~6,300 profiles


# ---------------- CELL 1: CONFIG ----------------
import datetime as dt, hashlib, hmac, json, os, re, secrets, unicodedata
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, LongType,
                               TimestampType, BooleanType)

NOTEBOOK = "nb_match_persons"
SF_TABLE = "lh_silver.dbo.conf_sf_cis_client"
HMIS_TABLE = "lh_silver.dbo.conf_hmis_clarity_client"
SUGGESTION_TABLE = "lh_silver.dbo.match_suggestion"
RUN_TABLE = "lh_silver.dbo.match_run"
QLOG_TABLE = "lh_silver.dbo.etl_quality_log"

RUN_TIME = dt.datetime.now(dt.timezone.utc)      # timezone-aware, so Delta stores the true UTC instant
RUN_ID = f"match_{RUN_TIME:%Y%m%dT%H%M%SZ}"
ALLOWED = {x.strip().lower() for x in ALLOW_CHECKS.split(",") if x.strip()}
SRC_LIST = [s.strip().lower() for s in SOURCES.split(",") if s.strip()]
AUTO_T = float(AUTO_LINK_THRESHOLD) if str(AUTO_LINK_THRESHOLD).strip() else None
if AUTO_T is not None and AUTO_T < REVIEW_THRESHOLD:
    raise ValueError("AUTO_LINK_THRESHOLD must be at least REVIEW_THRESHOLD.")

# SSN tokens: a random key for this run only. Never printed, logged or saved; gone when the session ends.
_RUN_SSN_KEY = secrets.token_bytes(32)
SSN_PLACEHOLDERS = {"123456789", "987654321"} | {str(d) * 9 for d in range(10)}

def ssn_token(v):
    """Token for a valid SSN (same rules as the Clarity Silver notebook); None for anything else."""
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return None
    d = re.sub(r"\D", "", str(v))
    if (len(d) != 9 or d[:3] in ("000", "666") or d[0] == "9" or d[3:5] == "00" or d[5:] == "0000"
            or d in SSN_PLACEHOLDERS):
        return None
    return hmac.new(_RUN_SSN_KEY, d.encode(), hashlib.sha256).hexdigest()

QLOG = []
def qlog(check, table, value, status, detail=""):
    QLOG.append((RUN_ID, "all", NOTEBOOK, check, table, int(value) if value is not None else None,
                 status, detail[:1000]))

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

def table_exists(name):
    try:
        spark.read.table(name).limit(1).collect()
        return True
    except Exception:
        return False

RUN_INFO = {"profiles": 0, "profiles_by_source": {}, "pairs_scored": 0, "exact_links": 0,
            "auto_links": 0, "review_pairs": 0, "model_id": None, "model_file": None, "trained": False}

RUN_SCHEMA = StructType([StructField(n, t) for n, t in [
    ("match_run_id", StringType()), ("run_datetime", TimestampType()), ("status", StringType()),
    ("model_id", StringType()), ("model_file", StringType()), ("trained_this_run", BooleanType()),
    ("profiles", LongType()), ("profiles_by_source", StringType()), ("pairs_scored", LongType()),
    ("exact_links", LongType()), ("auto_links", LongType()), ("review_pairs", LongType()),
    ("review_threshold", DoubleType()), ("auto_link_threshold", DoubleType()), ("detail", StringType())]])

def write_run(status, detail=""):
    row = (RUN_ID, RUN_TIME, status, RUN_INFO["model_id"], RUN_INFO["model_file"], RUN_INFO["trained"],
           RUN_INFO["profiles"], json.dumps(RUN_INFO["profiles_by_source"], sort_keys=True),
           RUN_INFO["pairs_scored"], RUN_INFO["exact_links"], RUN_INFO["auto_links"], RUN_INFO["review_pairs"],
           float(REVIEW_THRESHOLD), AUTO_T, detail[:4000])
    spark.createDataFrame([row], RUN_SCHEMA).write.format("delta").mode("append").saveAsTable(RUN_TABLE)

def stop(check, message):
    """Record the stop and fail the pipeline activity. Nothing reaches the warehouse."""
    qlog(check, None, None, "stop", message)
    flush_qlog()
    write_run("stopped", f"{check}: {message}")
    raise RuntimeError(f"{NOTEBOOK} stopped at '{check}': {message}")

def gate(check, ok, message, value=None):
    if ok:
        qlog(check, None, value, "pass")
    elif check in ALLOWED or "all" in ALLOWED:
        qlog(check, None, value, "override", message)
    else:
        stop(check, message)


# ---------------- CELL 2: READ AND STANDARDISE ----------------
def blank(v):
    return v is None or (not isinstance(v, (list, tuple)) and bool(pd.isna(v)))

JUNK_NAMES = {"UNKNOWN", "UNK", "ANONYMOUS", "ANON", "NONE", "NA", "N A", "NULL", "TEST", "CLIENT",
              "REFUSED", "DOE", "BABY", "NOT PROVIDED", "NO NAME", "XXX", "X"}
JUNK_EMAIL = re.compile(r"^(none|noemail|no\.?email|na|n\.a|test|unknown|declined|refused|noreply|nomail)@|@(none|example\.com|test\.com)$")
WEAK_DOB = re.compile(r"^(approximate|client|data not collected|2$|8$|9$|99$)", re.I)

def clean_name(v):
    if blank(v):
        return None
    s = unicodedata.normalize("NFKD", str(v)).encode("ascii", "ignore").decode().upper()
    s = re.sub(r"[^A-Z ]+", " ", s.replace("-", " "))
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\b(JR|SR|II|III|IV)$", "", s).strip()
    return None if not s or s in JUNK_NAMES else s

def clean_phone(v):
    if blank(v):
        return None
    d = re.sub(r"\D", "", str(v))
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    if len(d) != 10 or len(set(d)) == 1 or d in ("1234567890", "0123456789") or d[0] in "01":
        return None
    return d

def clean_email(v):
    if blank(v):
        return None
    s = str(v).strip().lower()
    return s if "@" in s and not JUNK_EMAIL.search(s) else None

def clean_address(v):
    if blank(v):
        return None
    s = re.sub(r"[^A-Z0-9 ]+", " ", str(v).upper())
    for a, b in [(" STREET", " ST"), (" AVENUE", " AVE"), (" BOULEVARD", " BLVD"), (" ROAD", " RD"),
                 (" DRIVE", " DR"), (" APARTMENT", " APT"), (" NORTH ", " N "), (" SOUTH ", " S "),
                 (" EAST ", " E "), (" WEST ", " W ")]:
        s = s.replace(a, b)
    s = re.sub(r"\s+", " ", s).strip()
    return s or None

def clean_sex(v):
    if blank(v):
        return None
    s = str(v).strip().lower()
    return "F" if s.startswith(("f", "woman", "girl")) else "M" if s.startswith(("m", "man", "boy")) else None

# Every column is given its type here, so the union of the sources never depends on how each Silver
# table happened to type a column. Spark 4 (Fabric Runtime 2.0) runs in ANSI mode, where a mismatched
# union would cast text to a number and fail.
PROFILE_TYPES = [("source_system", "string"), ("source_id", "string"), ("first_name", "string"),
                 ("middle_name", "string"), ("last_name", "string"), ("date_of_birth", "date"),
                 ("dob_data_quality", "string"), ("sex", "string"), ("email", "string"),
                 ("home_phone", "string"), ("mobile_phone", "string"), ("address_line1", "string"),
                 ("postal_code", "string"), ("ssn_hash", "string"), ("updated", "timestamp")]

def typed(df):
    return df.select(*[F.col(c).cast(t).alias(c) for c, t in PROFILE_TYPES])

SSN_SOURCE = [None]

def read_profiles():
    frames, by_source = [], {}
    if "sf_cis" in SRC_LIST:
        sf = (spark.read.table(SF_TABLE)
              .filter((F.col("source_system") == "sf_cis") & ~F.coalesce(F.col("is_deleted").cast("boolean"), F.lit(False)))
              .select(F.lit("sf_cis").alias("source_system"), F.col("salesforce_id").alias("source_id"),
                      "first_name", "middle_name", "last_name", "date_of_birth",
                      F.lit(None).alias("dob_data_quality"), "sex", "email", "home_phone",
                      "mobile_phone", "address_line1", "postal_code",
                      F.lit(None).alias("ssn_hash"), F.col("last_modified_date").alias("updated")))
        frames.append(typed(sf))
    hmis = [s for s in SRC_LIST if s.startswith("hmis_")]
    if hmis:
        hm_all = spark.read.table(HMIS_TABLE)
        # the full SSN is read only to make this run's tokens (see standardise); ssn_hash is the fallback
        ssn_col = F.col("ssn_raw") if "ssn_raw" in hm_all.columns else F.col("ssn_hash")
        SSN_SOURCE[0] = "ssn_raw" if "ssn_raw" in hm_all.columns else "ssn_hash"
        hm = (hm_all.filter(F.col("source_system").isin(hmis))
              .select("source_system", F.col("personal_id").alias("source_id"),
                      "first_name", "middle_name", "last_name", "date_of_birth", "dob_data_quality", "sex",
                      "email", "home_phone", "mobile_phone", "address_line1", "postal_code",
                      ssn_col.alias("ssn_hash"),
                      F.col("source_last_updated").alias("updated")))
        frames.append(typed(hm))
    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f)
    p = df.toPandas()
    p["source_id"] = p["source_id"].astype(str).str.strip()
    p = p[p["source_id"].ne("") & p["source_id"].ne("None")]
    before = len(p)
    p = (p.sort_values(["source_system", "source_id", "updated"], na_position="first")
          .drop_duplicates(["source_system", "source_id"], keep="last"))
    if len(p) < before:
        qlog("duplicate_source_ids", None, before - len(p), "warn",
             "Silver held more than one row for a source ID; the newest was used")
    for s, n in p.groupby("source_system").size().items():
        by_source[s] = int(n)
    return p, by_source

raw, by_source = read_profiles()
RUN_INFO["profiles"] = int(len(raw))
RUN_INFO["profiles_by_source"] = by_source

# Gate: a source that shrank sharply means a broken export or a failed Silver run
if table_exists(RUN_TABLE):
    last = (spark.read.table(RUN_TABLE).filter("status = 'success'")
                 .orderBy(F.col("run_datetime").desc()).limit(1).collect())
    if last:
        prev = json.loads(last[0]["profiles_by_source"] or "{}")
        drops = [f"{s}: {prev[s]} -> {by_source.get(s, 0)}" for s in prev
                 if s in SRC_LIST and prev[s] >= 20 and by_source.get(s, 0) < prev[s] * (1 - MAX_INPUT_DROP_PCT / 100)]
        gate("input_drop", not drops, "Profiles dropped by more than "
             f"{MAX_INPUT_DROP_PCT}% since the last run ({'; '.join(drops)}). Check the Silver run.")
missing = [s for s in SRC_LIST if by_source.get(s, 0) == 0]
if missing:
    stop("input_missing", f"No profiles for {', '.join(missing)}. Check SOURCES and the Silver tables.")

def standardise(p):
    s = pd.DataFrame({"unique_id": p["source_system"] + ":" + p["source_id"],
                      "source_system": p["source_system"], "source_id": p["source_id"]})
    s["first_name"] = p["first_name"].map(clean_name)
    s["first_name"] = s["first_name"].map(lambda x: x.split(" ")[0] if x else None)
    s["last_name"] = p["last_name"].map(clean_name)
    s["name_concat"] = (s["first_name"].fillna("") + " " + s["last_name"].fillna("")).str.strip().replace("", None)
    dob = pd.to_datetime(p["date_of_birth"], errors="coerce")
    bad = dob.isna() | (dob.dt.year < 1900) | (dob > pd.Timestamp(RUN_TIME.date())) | \
          dob.isin([pd.Timestamp("1900-01-01"), pd.Timestamp("1901-01-01")])
    s["dob"] = dob.where(~bad).dt.date
    s["dob_full"] = s["dob"].where(~p["dob_data_quality"].fillna("").str.strip().str.match(WEAK_DOB))
    s["sex"] = p["sex"].map(clean_sex)
    s["email"] = p["email"].map(clean_email)
    ph = pd.DataFrame({"m": p["mobile_phone"].map(clean_phone), "h": p["home_phone"].map(clean_phone)})
    s["phones"] = [sorted({x for x in (a, b) if isinstance(x, str)}) for a, b in zip(ph["m"], ph["h"])]
    s["address_norm"] = p["address_line1"].map(clean_address)
    s["postal5"] = p["postal_code"].astype("string").str.extract(r"^(\d{5})", expand=False)
    if SSN_SOURCE[0] == "ssn_raw":
        s["ssn_hash"] = p["ssn_hash"].map(ssn_token).values      # p["ssn_hash"] holds the full SSN here
    else:
        s["ssn_hash"] = p["ssn_hash"].where(p["ssn_hash"].notna() & (p["ssn_hash"].astype(str).str.len() > 0))
    s = s.reset_index(drop=True)

    # identifiers shared by more than SHARED_LIMIT profiles carry no identity
    def drop_shared(col, label):
        vc = s[col].value_counts()
        shared = set(vc[vc > SHARED_LIMIT].index)
        if shared:
            s.loc[s[col].isin(shared), col] = None
            qlog(f"shared_{label}", None, len(shared), "warn",
                 f"{len(shared)} {label} value(s) appear on more than {SHARED_LIMIT} profiles and are not used for matching")
    drop_shared("email", "email")
    drop_shared("address_norm", "address")
    drop_shared("ssn_hash", "ssn")
    exploded = s[["unique_id", "phones"]].explode("phones").dropna()
    vc = exploded.groupby("phones")["unique_id"].nunique()
    shared_ph = set(vc[vc > SHARED_LIMIT].index)
    if shared_ph:
        s["phones"] = s["phones"].map(lambda xs: [x for x in xs if x not in shared_ph])
        qlog("shared_phone", None, len(shared_ph), "warn",
             f"{len(shared_ph)} phone number(s) appear on more than {SHARED_LIMIT} profiles and are not used for matching")
    s["phone1"] = s["phones"].map(lambda xs: xs[0] if xs else None)
    s["phones"] = s["phones"].map(lambda xs: xs if xs else None)
    s = s.astype(object)
    return s.where(s.notna(), None)

prof = standardise(raw)
raw = raw.drop(columns=["ssn_hash"])     # the full SSN is not kept past this point
qlog("ssn_compare_source", None, int(prof["ssn_hash"].notna().sum()), "pass",
     "per-run tokens from ssn_raw" if SSN_SOURCE[0] == "ssn_raw" else f"from {SSN_SOURCE[0]}")
qlog("profiles", None, len(prof), "pass", json.dumps(by_source, sort_keys=True))


# ---------------- CELL 3: EXACT RULES ----------------
def pair_key(a, b):
    return (a, b) if a <= b else (b, a)

def exact_pairs(s):
    out = {}
    base = s[["unique_id", "first_name", "last_name", "dob_full", "phones", "email", "ssn_hash", "sex", "dob"]]

    # same_ssn_dob: valid SSN hash (shared ones already removed) plus the same full DOB or the same name
    x = base[base["ssn_hash"].notna()]
    j = x.merge(x, on="ssn_hash", suffixes=("_l", "_r"))
    j = j[j["unique_id_l"] < j["unique_id_r"]]
    ok = ((j["dob_full_l"].notna() & (j["dob_full_l"] == j["dob_full_r"]))
          | (j["first_name_l"].notna() & (j["first_name_l"] == j["first_name_r"])
             & (j["last_name_l"] == j["last_name_r"])))
    for a, b in zip(j.loc[ok, "unique_id_l"], j.loc[ok, "unique_id_r"]):
        out[pair_key(a, b)] = "same_ssn_dob"

    # name_dob_contact: same first name, last name and full DOB, plus a shared phone or email
    y = base[base["first_name"].notna() & base["last_name"].notna() & base["dob_full"].notna()]
    j = y.merge(y, on=["first_name", "last_name", "dob_full"], suffixes=("_l", "_r"))
    j = j[j["unique_id_l"] < j["unique_id_r"]]
    phone = [bool(set(a or []) & set(b or [])) for a, b in zip(j["phones_l"], j["phones_r"])]
    email = (j["email_l"].notna() & (j["email_l"] == j["email_r"])).tolist()
    for a, b, p_, e_ in zip(j["unique_id_l"], j["unique_id_r"], phone, email):
        if p_ or e_:
            out.setdefault(pair_key(a, b), "name_dob_contact")
    return out

exact = exact_pairs(prof)

# contrary evidence sends an exact-rule pair to review instead
idx = prof.set_index("unique_id")
def contrary(a, b):
    ra, rb = idx.loc[a], idx.loc[b]
    if not blank(ra["ssn_hash"]) and not blank(rb["ssn_hash"]) and ra["ssn_hash"] != rb["ssn_hash"]:
        return "two different SSNs"
    if not blank(ra["sex"]) and not blank(rb["sex"]) and ra["sex"] != rb["sex"]:
        return "different sex recorded"
    return None

exact_link, exact_review = {}, {}
for k, rule in exact.items():
    why = contrary(*k)
    (exact_review if why else exact_link)[k] = (rule, why)
qlog("exact_rule_links", None, len(exact_link), "pass")
if exact_review:
    qlog("exact_rule_contrary", None, len(exact_review), "warn",
         "Exact-rule pairs with contrary evidence were sent to review instead of linked")


# ---------------- CELL 4: SPLINK MODEL ----------------
# How the model learns, and why:
#   u (how often a value agrees by chance) is measured from this data by random sampling.
#   The share of pairs that are true matches is estimated from the exact name + DOB rule.
#   m (how often a value agrees for the SAME person) starts from the written priors in M_PRIORS.
#   Unsupervised EM training was tried and rejected: in a homeless-services population, family members
#   share phones, addresses, last names and sometimes DOBs (twins), and EM learned "household" instead of
#   "same person" (it scored juniors and seniors as matches). Once reviewers have confirmed enough pairs
#   (MIN_LABELS), set LABELS_PATH and retrain: m is then estimated from the reviewers' decisions.
import logging
from splink import Linker, SettingsCreator, block_on, DuckDBAPI
from splink.blocking_analysis import cumulative_comparisons_to_be_scored_from_blocking_rules_data
import splink.comparison_library as cl
import splink.comparison_level_library as cll

# m priors, one value per non-null level in the order the levels are listed below
M_PRIORS = {
    "name":    [0.80, 0.01, 0.07, 0.05, 0.025, 0.035, 0.01],  # exact, swapped, close, less close, last only, first only, other
    "dob":     [0.84, 0.03, 0.05, 0.025, 0.03, 0.022, 0.003],  # exact, day/month swapped, one digit, month, year, 10 years, other
    "contact": [0.45, 0.20, 0.10, 0.25],                       # phone or email and address, phone or email, address only, none shared
    "postal5": [0.60, 0.40],
    "ssn":     [0.98, 0.02],
    "sex":     [0.97, 0.03],
}
BLOCKING = [
    block_on("dob"),
    block_on("first_name", "last_name"),
    block_on("last_name", "year(dob)"),
    block_on("first_name", "year(dob)"),
    block_on("phone1"),
    block_on("email"),
    block_on("ssn_hash"),
]

def settings():
    dob = cl.CustomComparison(output_column_name="dob", comparison_levels=[
        cll.NullLevel("dob"),
        cll.ExactMatchLevel("dob"),
        cll.CustomLevel("year(dob_l) = year(dob_r) AND month(dob_l) = day(dob_r) AND day(dob_l) = month(dob_r)",
                        "Day and month swapped"),
        cll.CustomLevel("damerau_levenshtein(strftime(dob_l, '%Y%m%d'), strftime(dob_r, '%Y%m%d')) <= 1",
                        "One digit different"),
        cll.CustomLevel("abs(date_diff('day', dob_l, dob_r)) <= 31", "Within a month"),
        cll.CustomLevel("abs(date_diff('day', dob_l, dob_r)) <= 366", "Within a year"),
        cll.CustomLevel("abs(date_diff('day', dob_l, dob_r)) <= 3653", "Within 10 years"),
        cll.ElseLevel(),
    ])
    # Phone, email and street address are one comparison: family members share them, so counting each
    # one separately would treat a household as strong evidence of one person.
    contact = cl.CustomComparison(output_column_name="contact", comparison_levels=[
        cll.CustomLevel("(phones_l IS NULL AND email_l IS NULL AND address_norm_l IS NULL) OR "
                        "(phones_r IS NULL AND email_r IS NULL AND address_norm_r IS NULL)",
                        "No contact details on one side").configure(is_null_level=True),
        cll.CustomLevel("(len(list_intersect(phones_l, phones_r)) >= 1 OR email_l = email_r) "
                        "AND address_norm_l = address_norm_r", "Phone or email, and street address"),
        cll.CustomLevel("len(list_intersect(phones_l, phones_r)) >= 1 OR email_l = email_r", "Phone or email"),
        cll.CustomLevel("address_norm_l = address_norm_r", "Street address only"),
        cll.ElseLevel(),
    ])
    return SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="unique_id",
        blocking_rules_to_generate_predictions=BLOCKING,
        comparisons=[
            cl.ForenameSurnameComparison("first_name", "last_name", forename_surname_concat_col_name="name_concat")
              .configure(m_probabilities=M_PRIORS["name"]),
            dob.configure(m_probabilities=M_PRIORS["dob"]),
            contact.configure(m_probabilities=M_PRIORS["contact"]),
            cl.ExactMatch("postal5").configure(term_frequency_adjustments=True, m_probabilities=M_PRIORS["postal5"]),
            cl.ExactMatch("ssn_hash").configure(m_probabilities=M_PRIORS["ssn"]),
            cl.ExactMatch("sex").configure(m_probabilities=M_PRIORS["sex"]),
        ],
        retain_intermediate_calculation_columns=False,
        retain_matching_columns=True,
    )

model_cols = ["unique_id", "first_name", "last_name", "name_concat", "dob", "email", "phones", "phone1",
              "address_norm", "postal5", "ssn_hash", "sex"]
model_df = prof[model_cols].copy()

counts = cumulative_comparisons_to_be_scored_from_blocking_rules_data(
    table_or_tables=model_df, blocking_rules=BLOCKING, link_type="dedupe_only", db_api=DuckDBAPI())
n_cmp = int(counts["cumulative_rows"].iloc[-1]) if len(counts) else 0
gate("comparison_volume", n_cmp <= MAX_COMPARISONS,
     f"The blocking rules would score {n_cmp:,} pairs (limit {MAX_COMPARISONS:,}). A placeholder value is "
     "probably filling a blocking column; check the shared-identifier warnings.", n_cmp)

def reviewer_labels():
    if not LABELS_PATH:
        return None
    d = (spark.read.format("delta").load(LABELS_PATH)
         .filter((F.col("is_active").cast("int") == 1) & (F.col("decision_method") == "reviewer")
                 & (F.col("decision") == "match"))
         .select(F.concat_ws(":", "source_system_a", "source_id_a").alias("unique_id_l"),
                 F.concat_ws(":", "source_system_b", "source_id_b").alias("unique_id_r")).toPandas())
    known = set(model_df["unique_id"])
    d = d[d["unique_id_l"].isin(known) & d["unique_id_r"].isin(known)]
    if len(d) < MIN_LABELS:
        qlog("model_labels", None, len(d), "warn",
             f"Only {len(d)} reviewer-confirmed pairs (need {MIN_LABELS}); the m priors were kept")
        return None
    return d

os.makedirs(MODEL_DIR, exist_ok=True)
model_path = os.path.join(MODEL_DIR, MODEL_FILE)
had_model = os.path.exists(model_path)
if TRAIN_MODEL or not had_model:
    try:
        trainer = Linker(model_df, settings(), DuckDBAPI(), set_up_basic_logging=False)
        trainer.training.estimate_probability_two_random_records_match(
            [block_on("first_name", "last_name", "dob")], recall=0.8)
        n_prof = len(model_df)
        all_pairs = n_prof * (n_prof - 1) // 2
        u_pairs = int(min(U_MAX_PAIRS, all_pairs))
        trainer.training.estimate_u_using_random_sampling(max_pairs=u_pairs, seed=20261115)
        qlog("u_sampling", None, u_pairs, "pass", f"{u_pairs:,} of {all_pairs:,} possible pairs sampled for u")
        labels = reviewer_labels()
        if labels is not None:
            trainer.table_management.register_table(labels, "__reviewer_labels", overwrite=True)
            trainer.training.estimate_m_from_pairwise_labels("__reviewer_labels")
            qlog("model_labels", None, len(labels), "pass", "m estimated from reviewer decisions")
    except Exception as e:
        stop("model_training", f"Training failed ({e}). Keep the saved model and check the input.")
    versioned = os.path.join(MODEL_DIR, f"person_model_{RUN_TIME:%Y%m%dT%H%M%SZ}.json")
    mj = trainer.misc.save_model_to_json()          # the model as a dict, in memory; nothing read back from disk
    # A level never seen among u_pairs random pairs has a chance of at most about 3 / u_pairs (the
    # "rule of three"). Use that instead of Splink's default, and take it from the comparison's ELSE level
    # so each comparison's u values still add up to 1.
    filled = []
    for comp in mj["comparisons"]:
        levels = [lv for lv in comp["comparison_levels"] if not lv.get("is_null_level")]
        missing = [lv for lv in levels if lv.get("u_probability") is None]
        if not missing:
            continue
        u_floor = 3.0 / max(u_pairs, 1)
        for lv in missing:
            lv["u_probability"] = u_floor
            filled.append(f"{comp['output_column_name']}: {lv.get('label_for_charts') or lv.get('sql_condition', '')[:40]}")
        known = sum(lv["u_probability"] for lv in levels)
        else_lv = levels[-1]
        if known > 1 and else_lv not in missing:
            else_lv["u_probability"] = max(else_lv["u_probability"] - (known - 1), u_floor)
    if filled:
        qlog("u_levels_estimated", None, len(filled), "warn",
             "Never seen in the u sample, estimated as 3 / pairs sampled: " + "; ".join(filled))
    trained_text = json.dumps(mj, indent=2)
    with open(versioned, "w") as fh:
        fh.write(trained_text)
    RUN_INFO["trained"] = True
    if not had_model:
        with open(model_path, "w") as fh:
            fh.write(trained_text)
        qlog("model_trained", None, None, "warn",
             f"No saved model was found, so one was trained and saved as {MODEL_FILE} (copy: "
             f"{os.path.basename(versioned)}). Review the match weights before turning on AUTO_LINK_THRESHOLD.")
    else:
        qlog("model_trained", None, None, "warn",
             f"A new model was saved as {os.path.basename(versioned)}. This run scored with {MODEL_FILE} as "
             "usual; to switch, review the new model and set MODEL_FILE to the new file.")

# Score with the pinned model, so a retrain never changes production scores by itself
if RUN_INFO["trained"] and not had_model:
    model_json = trained_text                         # just trained and saved as MODEL_FILE: use it from memory
else:
    with open(model_path) as fh:
        model_json = fh.read()
    try:
        json.loads(model_json)
    except ValueError:
        stop("model_file", f"{MODEL_FILE} in {MODEL_DIR} is empty or damaged ({len(model_json)} bytes). "
             "Restore the previous model by setting MODEL_FILE to a dated person_model_*.json file in that folder, "
             "or delete it to train a new model on purpose.")
linker = Linker(model_df, json.loads(model_json), DuckDBAPI(), set_up_basic_logging=False)
RUN_INFO["model_file"] = MODEL_FILE
RUN_INFO["model_id"] = hashlib.sha256(model_json.encode()).hexdigest()[:12]

pred = linker.inference.predict(threshold_match_probability=min(REVIEW_THRESHOLD, 0.5)).as_pandas_dataframe()
RUN_INFO["pairs_scored"] = n_cmp


# ---------------- CELL 5: SUGGESTIONS ----------------
def same(a, b):
    return not blank(a) and not blank(b) and bool(a == b)

def evidence(a, b):
    ra, rb = idx.loc[a], idx.loc[b]
    e = []
    fn, ln = same(ra["first_name"], rb["first_name"]), same(ra["last_name"], rb["last_name"])
    if fn and ln:
        e.append("same first and last name")
    elif same(ra["first_name"], rb["last_name"]) and same(ra["last_name"], rb["first_name"]):
        e.append("first and last name swapped")
    elif ln:
        e.append("same last name, first name differs")
    elif fn:
        e.append("same first name, last name differs")
    else:
        e.append("names differ")
    da, db_ = ra["dob"], rb["dob"]
    if blank(da) or blank(db_):
        e.append("date of birth missing on one profile")
    elif da == db_:
        e.append("same date of birth" + ("" if not blank(ra["dob_full"]) and not blank(rb["dob_full"]) else
                                         " (one is marked approximate)"))
    elif (da.year, da.day, da.month) == (db_.year, db_.month, db_.day):
        e.append("date of birth with day and month swapped")
    elif abs((da - db_).days) <= 366:
        e.append("date of birth within a year")
    else:
        e.append("date of birth differs")
    if set(ra["phones"] or []) & set(rb["phones"] or []):
        e.append("shared phone")
    if same(ra["email"], rb["email"]):
        e.append("same email")
    if same(ra["address_norm"], rb["address_norm"]):
        e.append("same street address")
    elif same(ra["postal5"], rb["postal5"]):
        e.append("same ZIP")
    if same(ra["ssn_hash"], rb["ssn_hash"]):
        e.append("same SSN")
    elif not blank(ra["ssn_hash"]) and not blank(rb["ssn_hash"]):
        e.append("different SSN")
    if not blank(ra["sex"]) and not blank(rb["sex"]) and ra["sex"] != rb["sex"]:
        e.append("different sex recorded")
    return "; ".join(e)

gamma_cols = [c for c in pred.columns if c.startswith("gamma_")]
scores = {}
for r in pred.itertuples(index=False):
    k = pair_key(r.unique_id_l, r.unique_id_r)
    scores[k] = (float(r.match_probability), float(r.match_weight),
                 json.dumps({c[6:]: int(getattr(r, c)) for c in gamma_cols}, sort_keys=True))

rows = []
def add(k, action, rule, extra=""):
    p, w, levels = scores.get(k, (None, None, None))
    a_sys, a_id = k[0].split(":", 1)
    b_sys, b_id = k[1].split(":", 1)
    ev = evidence(*k) + (f". {extra}" if extra else "")
    rows.append((RUN_ID, RUN_TIME, RUN_INFO["model_id"], a_sys, a_id, b_sys, b_id, p, w, action, rule, ev, levels))

for k, (rule, _) in exact_link.items():
    add(k, "link", rule)
for k, (rule, why) in exact_review.items():
    add(k, "review", rule, f"Exact rule {rule} held but {why}, so it needs a reviewer")
for k, (p, w, levels) in scores.items():
    if k in exact_link or k in exact_review or p < REVIEW_THRESHOLD:
        continue
    add(k, "auto_link" if AUTO_T is not None and p >= AUTO_T else "review", "score")

sug = pd.DataFrame(rows, columns=["match_run_id", "run_datetime", "model_id", "source_system_a", "source_id_a",
                                  "source_system_b", "source_id_b", "match_probability", "match_weight",
                                  "suggested_action", "rule_name", "evidence", "comparison_levels"])
RUN_INFO["exact_links"] = int((sug["suggested_action"] == "link").sum())
RUN_INFO["auto_links"] = int((sug["suggested_action"] == "auto_link").sum())
RUN_INFO["review_pairs"] = int((sug["suggested_action"] == "review").sum())

n = max(RUN_INFO["profiles"], 1)
gate("link_volume", RUN_INFO["exact_links"] + RUN_INFO["auto_links"] <= max(10, n * MAX_LINK_PCT / 100),
     f"{RUN_INFO['exact_links'] + RUN_INFO['auto_links']} automatic links for {n} profiles is more than "
     f"{MAX_LINK_PCT}%. A rule or a placeholder value is probably joining unrelated people.",
     RUN_INFO["exact_links"] + RUN_INFO["auto_links"])
gate("review_volume", RUN_INFO["review_pairs"] <= max(25, n * MAX_REVIEW_PCT / 100),
     f"{RUN_INFO['review_pairs']} review pairs for {n} profiles is more than {MAX_REVIEW_PCT}%. "
     "Check the model and REVIEW_THRESHOLD.", RUN_INFO["review_pairs"])


# ---------------- CELL 6: WRITE (suggestions first; the success row in match_run is the commit marker) ----------------
SUG_SCHEMA = StructType([StructField(n, t) for n, t in [
    ("match_run_id", StringType()), ("run_datetime", TimestampType()), ("model_id", StringType()),
    ("source_system_a", StringType()), ("source_id_a", StringType()), ("source_system_b", StringType()),
    ("source_id_b", StringType()), ("match_probability", DoubleType()), ("match_weight", DoubleType()),
    ("suggested_action", StringType()), ("rule_name", StringType()), ("evidence", StringType()),
    ("comparison_levels", StringType())]])
records = [tuple(None if blank(v) else v for v in r) for r in rows]
(spark.createDataFrame(records, SUG_SCHEMA).write.format("delta").mode("append")
      .saveAsTable(SUGGESTION_TABLE))
write_run("success", f"{RUN_INFO['exact_links']} exact-rule links, {RUN_INFO['auto_links']} score links, "
                     f"{RUN_INFO['review_pairs']} pairs for review")
flush_qlog()

def prune_old_runs():
    keep = [r["match_run_id"] for r in spark.read.table(RUN_TABLE).filter("status = 'success'")
            .orderBy(F.col("run_datetime").desc()).limit(KEEP_RUNS).collect()]
    ids = ", ".join("'" + k.replace("'", "") + "'" for k in keep)
    spark.sql(f"DELETE FROM {SUGGESTION_TABLE} WHERE match_run_id NOT IN ({ids})")

try:
    prune_old_runs()
except Exception as e:   # pruning is housekeeping; a failure here must not fail the run
    print(f"Pruning old suggestion runs failed and will be retried next run: {e}")

print(json.dumps({k: RUN_INFO[k] for k in ("profiles", "profiles_by_source", "pairs_scored", "exact_links",
                                           "auto_links", "review_pairs", "model_id", "trained")}, indent=1))
