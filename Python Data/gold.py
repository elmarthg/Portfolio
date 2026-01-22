from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Tuple

import pandas as pd


# ----------------------------
# Helpers (null-safe text/date)
# ----------------------------

def _s(x) -> str:
    """Null-safe string."""
    if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
        return ""
    return str(x)


def _norm_text(x) -> str:
    """Normalize common punctuation/encoding artifacts for deterministic contains checks."""
    s = _s(x).strip()
    # common “smart quote” and mis-encoded apostrophe patterns
    s = s.replace("’", "'").replace("â€™", "'").replace("\ufeff", "")
    return s


def _contains(haystack: str, needle: str) -> bool:
    return needle.lower() in haystack.lower()


def _to_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def _pull_date(pull_date: str) -> date:
    return pd.to_datetime(pull_date, errors="coerce").date()


def housing_threshold(survey_name) -> int | None:
    """
    Threshold rules:
      - survey contains 'CES' (case-insensitive) -> 8
      - exactly 'Los Angeles Housing Assessment Tool (LA HAT)' -> 17
      - otherwise -> None
    Null-safe.
    """
    s = _norm_text(survey_name)
    if not s:
        return None
    if "CES" in s.upper():
        return 8
    if s == "Los Angeles Housing Assessment Tool (LA HAT)":
        return 17
    return None


def ces_status(assessment_date: date | None, pull_date_str: str) -> str:
    if assessment_date is None or pd.isna(assessment_date):
        return "Assessment Not Done"
    pdte = _pull_date(pull_date_str)
    try:
        age_days = (pdte - assessment_date).days
    except Exception:
        return "Assessment Not Done"

    if age_days > 730:
        return "Over 2 Years, Review for life change"
    return "Current"


def _program_short_name(program_name: str) -> str:
    p = _norm_text(program_name)
    if not p:
        return ""

    # Split only on the FIRST " - "
    parts = p.split(" - ", 1)

    # If delimiter not found, return full string
    if len(parts) == 1:
        return p

    # Return everything after the first delimiter
    return parts[1].strip()



# ----------------------------
# Document parsing (file list)
# ----------------------------

def _doc_flags_from_file_list(file_list_value) -> dict:
    """
    Replicates your Power Query flags.
    Returns flags + text fields like 'Proof of Income' and 'Health Insurance Column Flag'.
    """
    txt = _norm_text(file_list_value)

    ssn = 1 if _contains(txt, "Social Security Card") else 0

    # Matches your exact pattern phrase
    id_pattern = "Driver's License/State ID Card/Photo ID/ School Identification Card"
    cdl_id = 1 if _contains(txt, id_pattern) else 0

    homeless_ver = 1 if _contains(txt, "Form 6053 - Los Angeles CoC Homelessness Verification") else 0
    disability = 1 if _contains(txt, "Disability Verification") else 0

    # Health insurance flag: returns "1 - ..." else "0"
    health_matches = []
    if _contains(txt, "Medicaid or Medicare Card"):
        health_matches.append("Medicare/Medicaid")
    if _contains(txt, "Health Insurance Documentation"):
        health_matches.append("Other Health Insurance")

    health_flag = f"1 - {', '.join(health_matches)}" if health_matches else "0"

    # Proof of Income: returns "1 - ..." else "0"
    income_matches = []
    def add_if(needle: str, label: str):
        if _contains(txt, needle):
            income_matches.append(label)

    add_if("Pay Stub", "Pay Stub")
    add_if("Supplemental Security Disability Income (SSDI) Forms", "SSDI")
    add_if("Supplemental Security Income (SSI) Forms", "SSI")
    add_if("General Relief (GR) Form", "GR")
    add_if("Food Stamp Card or Award Letter", "Food Stamp")
    add_if("CalWORKS Forms", "CalWORKS")
    add_if("Form 1087 - Self Declaration of Income/No Income Form", "Form 1087")
    add_if("Form 1084 - 3rd Party Income Verification", "Form 1084")
    add_if("Alimony Agreement", "Alimony Agreement")
    add_if("Social Security (NUMI) Printout", "Social Security (NUMI) Printout")
    add_if("Tax Return", "Tax Return")
    add_if("Veterans Affairs (VA) Benefits Award Letter", "Veterans Affairs (VA) Benefits Award Letter")
    add_if("Self Employment Document", "Self Employment Document")
    add_if("Other Financial Document", "Other Financial Document")

    income_flag = f"1 - {', '.join(income_matches)}" if income_matches else "0"

    # Document Ready rule (matches your PQ)
    doc_ready = "Document Ready" if (cdl_id == 1 and ssn == 1 and income_flag.startswith("1")) else "Not Document Ready"

    # Missing documents list (matches your PQ behavior)
    missing = []
    if ssn == 0:
        missing.append("SSN Card")
    if cdl_id == 0:
        missing.append("CDL or State ID")
    if disability == 0:
        missing.append("Disability Verification (If Applicable)")
    if health_flag == "0":
        missing.append("Health Insurance")
    if income_flag == "0":
        missing.append("Proof of Income")

    missing_text = "All documents present" if not missing else ", ".join(missing)

    return {
        "SSN Card": ssn,
        "CDL or State ID": cdl_id,
        "Homelessness Verification": homeless_ver,
        "Disability Verification": disability,
        "Health Insurance Column Flag": health_flag,
        "Proof of Income": income_flag,
        "Document Ready": doc_ready,
        "Missing Documents": missing_text,
    }


# ----------------------------
# Gold builders
# ----------------------------

def build_gold_client_program_readiness(
    *,
    pull_date: str,
    spine_client_program: pd.DataFrame,
    case_notes_latest: pd.DataFrame,
    services_latest: pd.DataFrame,
    ces_unified_latest: pd.DataFrame,
) -> pd.DataFrame:
    """
    Gold fact table at grain: (client_id, program_name)

    Inputs are expected from Silver:
      - spine_client_program: one row per (client_id, program_name)
      - case_notes_latest: one row per (client_id, program_name) for latest month <= pull_date
      - services_latest: one row per (client_id, program_name) for latest month <= pull_date
      - ces_unified_latest: one row per client_id (external overrides HMIS)
    """
    df = spine_client_program.copy()

    # Standardize key columns (robust to upstream naming)
    if "client_id" not in df.columns and "Clients Unique Identifier" in df.columns:
        df["client_id"] = df["Clients Unique Identifier"]
    if "program_name" not in df.columns and "Programs Full Name" in df.columns:
        df["program_name"] = df["Programs Full Name"]

    df["client_id"] = df["client_id"].astype(str).str.strip()
    df["program_name"] = df["program_name"].astype(str).str.strip()

    # Join Case Notes (latest-asof)
    cn = case_notes_latest.copy()
    if len(cn) > 0:
        cn["client_id"] = cn["client_id"].astype(str).str.strip()
        cn["program_name"] = cn["program_name"].astype(str).str.strip()
        df = df.merge(cn, on=["client_id", "program_name"], how="left")
    else:
        df["case_notes_month"] = pd.NA
        df["case_notes_count"] = pd.NA

    # Join Services (latest-asof)
    sv = services_latest.copy()
    if len(sv) > 0:
        sv["client_id"] = sv["client_id"].astype(str).str.strip()
        sv["program_name"] = sv["program_name"].astype(str).str.strip()
        df = df.merge(sv, on=["client_id", "program_name"], how="left")
    else:
        df["services_month"] = pd.NA
        df["services_count"] = pd.NA

    # Join CES (client-level)
    ces = ces_unified_latest.copy()
    if len(ces) > 0:
        ces["client_id"] = ces["client_id"].astype(str).str.strip()
        df = df.merge(ces, on=["client_id"], how="left", suffixes=("", "_ces"))
    else:
        df["survey_name"] = pd.NA
        df["assessment_date"] = pd.NA
        df["assessment_score"] = pd.NA
        df["ces_source"] = pd.NA

    # Normalize CES columns to the names you used in Power BI
    df["Survey Name"] = df.get("survey_name", pd.NA)
    df["Latest CES Assessment Date"] = df.get("assessment_date", pd.NA)
    df["Assessment Score"] = df.get("assessment_score", pd.NA)
    df["CES Source"] = df.get("ces_source", pd.NA)

    # Ensure assessment date is date type for stable diff
    df["Latest CES Assessment Date"] = _to_date_series(df["Latest CES Assessment Date"])

    # CES Status
    df["CES Status"] = df["Latest CES Assessment Date"].apply(lambda d: ces_status(d, pull_date))
    df["Assessment Status"] = df["CES Status"]  # keep the name your PQ ended with

    # Document flags from file list (List of Client File Name)
    file_col = None
    for c in ["file_list", "List of Client File Name", "List of Client File Name "]:
        if c in df.columns:
            file_col = c
            break
    if file_col is None:
        df["List of Client File Name"] = ""
        file_col = "List of Client File Name"

    flags = df[file_col].apply(_doc_flags_from_file_list)
    flags_df = pd.DataFrame(list(flags))
    df = pd.concat([df.reset_index(drop=True), flags_df.reset_index(drop=True)], axis=1)

    # Housing Matching Ready
    # Null-safe: Survey Name may be float/NaN -> handled by housing_threshold()
    thr_series = df["Survey Name"].apply(housing_threshold)
    df["_housing_threshold"] = pd.to_numeric(thr_series, errors="coerce")

    score = pd.to_numeric(df["Assessment Score"], errors="coerce")
    docs_ready = (
        (pd.to_numeric(df["CDL or State ID"], errors="coerce").fillna(0).astype(int) == 1)
        & (pd.to_numeric(df["SSN Card"], errors="coerce").fillna(0).astype(int) == 1)
        & (df["Proof of Income"].fillna("").astype(str).str.startswith("1"))
    )

    matching_ready = docs_ready & df["_housing_threshold"].notna() & score.notna() & (score >= df["_housing_threshold"])
    df["Housing Matching Ready"] = matching_ready.map(
        {True: "Housing Matching Ready", False: "Not Housing Matching Ready"}
    )

    # Intervention Alert (same intent as your PQ)
    days = pd.to_numeric(df.get("Enrollments Days in Project", pd.NA), errors="coerce")
    long_stay = days.notna() & (days >= 120)
    mid_stay = days.notna() & (days >= 75) & (days < 120)

    assessment_expired = df["CES Status"].isin(["Over 2 Years, Review for life change", "Assessment Not Done"])
    low_assessment = df["_housing_threshold"].notna() & score.notna() & (score < df["_housing_threshold"])

    def _intervention(row) -> str:
        d = row.get("Enrollments Days in Project", None)
        try:
            d = float(d)
        except Exception:
            d = None

        docs_ok = row.get("Document Ready", "") == "Document Ready"
        expired = row.get("CES Status", "") in ["Over 2 Years, Review for life change", "Assessment Not Done"]

        thr = row.get("_housing_threshold", None)
        sc = row.get("Assessment Score", None)
        try:
            thr = float(thr) if thr is not None and not pd.isna(thr) else None
            sc = float(sc) if sc is not None and not pd.isna(sc) else None
        except Exception:
            thr, sc = None, None

        low = (thr is not None) and (sc is None or sc < thr)

        if d is not None and d >= 120 and (not docs_ok):
            return "≥120 days & missing docs – ESCALATE"
        if d is not None and d >= 120 and expired:
            return "≥120 days in interim housing & CES assessment >2 years old — ESCALATE for review"
        if d is not None and d >= 120 and low:
            return "≥120 days & score below threshold – ESCALATE"
        if d is not None and d >= 120:
            return "≥120 days & docs/assessment OK – monitor"
        if d is not None and 75 <= d < 120 and (not docs_ok):
            return "75–119 days & missing docs – ACTION"
        if d is not None and 75 <= d < 120 and (expired or low):
            return "75–119 days & assessment risk – ACTION"
        return "No Action Needed"

    df["Intervention Alert"] = df.apply(_intervention, axis=1)

    # Case Count Category (based on latest case_notes_count)
    cn_count = pd.to_numeric(df.get("case_notes_count", 0), errors="coerce").fillna(0).astype(int)
    df["Case Count Category"] = cn_count.apply(
        lambda n: "5 or more case notes" if n >= 5 else f"{n} case note" + ("" if n == 1 else "s")
    )

    # Enrollment Days Tier
    def _tier(v) -> str | None:
        try:
            d = float(v)
        except Exception:
            return None
        if d < 60:
            return "Tier 1 - <60 Days"
        if d <= 120:
            return "Tier 2 - 60-120 Days"
        if d <= 365:
            return "Tier 3 - 120-365 Days"
        return "Tier 4 - >365 Days"

    df["Enrollment Days Tier"] = df.get("Enrollments Days in Project", pd.NA).apply(_tier)

    # Program short name
    df["Programs Name"] = df["program_name"].apply(_program_short_name)

    # Keep stable column order (you can add/remove here without breaking the pipeline)
      # -------------------------
    # STRICT GOLD SCHEMA (drop duplicates + internal helpers)
    # -------------------------
    gold_cols = [
        "client_id",
        "program_name",
        "Programs Name",

        "Enrollments Active in Project",
        "Clients Client Full Name",
        "Clients Active ROI?",
        "Enrollments Days in Project",
        "Enrollments Project Start Date",

        "Client Custom Point of Contact Name",
        "Client Custom Point of Contact Phone",
        "Client Custom Point of Contact Email",
        "Client Custom Point of Contact Date",

        "Clients DoB Data Quality",
        "Clients SSN Data Quality",
        "Clients SSN - Last 4",
        "Enrollments Deleted (Yes / No)",
        "Client Assessment Custom TB Clearance Date",

        "Client Assessments Last Assessment ID",
        "Client Assessments Last Assessment Date",
        "List of Client File Name",
        "List of Assigned Staff",

        "case_notes_month",
        "case_notes_count",
        "services_month",
        "services_count",

        "Survey Name",
        "Latest CES Assessment Date",
        "Assessment Score",
        "CES Source",
        "CES Status",
        "Assessment Status",

        "SSN Card",
        "CDL or State ID",
        "Homelessness Verification",
        "Disability Verification",
        "Health Insurance Column Flag",
        "Proof of Income",

        "Document Ready",
        "Housing Matching Ready",
        "Intervention Alert",
        "Case Count Category",
        "Enrollment Days Tier",
        "Missing Documents",
    ]

    # keep only columns that exist (non-breaking)
    gold_cols = [c for c in gold_cols if c in df.columns]
    df = df.reindex(columns=gold_cols).copy()

    return df



def build_gold_client_readiness_current(gold_client_program: pd.DataFrame) -> pd.DataFrame:
    """
    Collapses (client_id, program) grain to a single row per client_id.
    Deterministic pick:
      1) Active in project = YES
      2) Not deleted
      3) Highest days in project
      4) Most recent project start date
    """
    df = gold_client_program.copy()

    # Safe ranks
    active = df.get("Enrollments Active in Project", "").fillna("").astype(str).str.upper().eq("YES").astype(int)
    not_deleted = ~df.get("Enrollments Deleted (Yes / No)", "").fillna("").astype(str).str.upper().eq("YES")
    not_deleted = not_deleted.astype(int)

    days = pd.to_numeric(df.get("Enrollments Days in Project", pd.NA), errors="coerce").fillna(-1)
    start = pd.to_datetime(df.get("Enrollments Project Start Date", pd.NaT), errors="coerce")

    df["_r_active"] = active
    df["_r_not_deleted"] = not_deleted
    df["_r_days"] = days
    df["_r_start"] = start

    df = df.sort_values(
        ["client_id", "_r_active", "_r_not_deleted", "_r_days", "_r_start"],
        ascending=[True, False, False, False, False],
    )

    out = df.drop_duplicates(["client_id"], keep="first").copy()
    out = out.drop(columns=["_r_active", "_r_not_deleted", "_r_days", "_r_start"], errors="ignore")
    return out


def active_multi_programs(gold_client_program: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      - detail: all rows for clients active in >1 program
      - summary: one row per client with active_program_count
    """
    df = gold_client_program.copy()

    active_flag = df.get("Enrollments Active in Project", "").fillna("").astype(str).str.upper().eq("YES")
    active_df = df[active_flag].copy()

    if len(active_df) == 0:
        return (
            pd.DataFrame(columns=df.columns),
            pd.DataFrame(columns=["client_id", "active_program_count"]),
        )

    counts = (
        active_df.groupby("client_id", dropna=False)["program_name"]
        .nunique()
        .reset_index(name="active_program_count")
    )
    multi = counts[counts["active_program_count"] > 1].copy()

    detail = active_df.merge(multi[["client_id", "active_program_count"]], on="client_id", how="inner")
    detail = detail.sort_values(["active_program_count", "client_id", "program_name"], ascending=[False, True, True])

    summary = multi.sort_values(["active_program_count", "client_id"], ascending=[False, True]).copy()

    return detail, summary
