import re
from pathlib import Path

import pandas as pd

from .config import EXCLUDE_PROGRAMS

MONTH_RE = re.compile(r"^\d{4}-\d{2}$")


def canonical_id(x) -> str:
    """
    Canonicalize HMIS IDs to prevent invisible duplicates.
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x)
    s = (s.replace("\ufeff", "")   # BOM
           .replace("\u200b", "")  # zero-width space
           .replace("\xa0", "")    # NBSP
         )
    s = re.sub(r"\s+", "", s)      # remove all whitespace
    return s.strip()


def _month_label(yyyy_mm: str) -> str:
    dt = pd.to_datetime(yyyy_mm + "-01")
    return dt.strftime("%B %Y")


# -------------------------
# ProgramClientData
# -------------------------

def program_client_data(df: pd.DataFrame, exclude_programs: bool = False) -> pd.DataFrame:
    out = df.copy()

    if exclude_programs and "Programs Full Name" in out.columns:
        out = out[~out["Programs Full Name"].isin(EXCLUDE_PROGRAMS)].copy()

    out = out.rename(columns={
        "Clients Unique Identifier": "client_id",
        "Programs Full Name": "program_name",
        "Clients Active ROI?": "roi_active",
        "Enrollments Deleted (Yes / No)": "enrollment_deleted",
        "Enrollments Days in Project": "days_in_project",
        "Enrollments Project Start Date": "project_start_date",
        "Client Assessments Last Assessment ID": "last_assessment_id",
        "Client Assessments Last Assessment Date": "last_assessment_date",
        "List of Client File Name": "file_list",
        "List of Assigned Staff": "assigned_staff",
        "Clients Client Full Name": "client_full_name",
        "Enrollments Active in Project": "active_in_project",
    })

    out["client_id"] = out["client_id"].apply(canonical_id)
    out["program_name"] = out["program_name"].astype(str).str.strip()

    if "days_in_project" in out.columns:
        out["days_in_project"] = pd.to_numeric(out["days_in_project"], errors="coerce").astype("Int64")
    if "project_start_date" in out.columns:
        out["project_start_date"] = pd.to_datetime(out["project_start_date"], errors="coerce")
    if "last_assessment_id" in out.columns:
        out["last_assessment_id"] = pd.to_numeric(out["last_assessment_id"], errors="coerce").astype("Int64")
    if "last_assessment_date" in out.columns:
        out["last_assessment_date"] = pd.to_datetime(out["last_assessment_date"], errors="coerce")

    out["roi_active_flag"] = out.get("roi_active", "").astype(str).str.upper().eq("YES").astype("int64")
    out["enrollment_deleted_flag"] = out.get("enrollment_deleted", "").astype(str).str.upper().eq("YES").astype("int64")

    out = out.sort_values(
        ["client_id", "program_name", "project_start_date"],
        ascending=[True, True, False],
        kind="mergesort"
    ).reset_index(drop=True)

    return out


def enrollment_duplicates(pcd: pd.DataFrame) -> pd.DataFrame:
    g = pcd.groupby(["client_id", "program_name"], dropna=False).size().reset_index(name="count")
    dup_keys = g[g["count"] >= 2][["client_id", "program_name"]]
    out = pcd.merge(dup_keys, on=["client_id", "program_name"], how="inner")
    return out.sort_values(["client_id", "program_name", "project_start_date"], ascending=[True, True, False], kind="mergesort")


def current_client_program_snapshot(pcd: pd.DataFrame) -> pd.DataFrame:
    df = pcd.sort_values(
        ["client_id", "program_name", "project_start_date"],
        ascending=[True, True, False],
        kind="mergesort",
    ).drop_duplicates(["client_id", "program_name"], keep="first")
    return df.reset_index(drop=True)


# -------------------------
# Case Notes
# -------------------------

def case_notes(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in [".xlsx", ".xls"]:
        return _case_notes_excel_two_header(path)
    return _case_notes_legacy_csv(path)


def _case_notes_excel_two_header(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, header=[0, 1], dtype=str)

    new_cols = []
    month_cols = []

    for top, bottom in df.columns:
        top_s = "" if pd.isna(top) else str(top).strip()
        bot_s = "" if pd.isna(bottom) else str(bottom).strip()

        if bot_s in ("Clients Unique Identifier", "Programs Full Name"):
            new_cols.append(bot_s)
        elif MONTH_RE.match(top_s):
            colname = f"{_month_label(top_s)} Case Note Count"
            new_cols.append(colname)
            month_cols.append(colname)
        else:
            new_cols.append(bot_s or top_s)

    df.columns = new_cols
    df = df.rename(columns={"Clients Unique Identifier": "client_id", "Programs Full Name": "program_name"})

    out = df.melt(
        id_vars=["client_id", "program_name"],
        value_vars=month_cols,
        var_name="month_label",
        value_name="case_note_count",
    )

    out["month"] = pd.to_datetime(
        out["month_label"].str.replace(" Case Note Count", "", regex=False),
        format="%B %Y",
        errors="coerce",
    ).dt.strftime("%Y-%m")

    out["client_id"] = out["client_id"].apply(canonical_id)
    out["program_name"] = out["program_name"].astype(str).str.strip()
    out["case_note_count"] = pd.to_numeric(out["case_note_count"], errors="coerce").fillna(0).astype("int64")

    out = out.dropna(subset=["month"])
    out = out[["client_id", "program_name", "month", "case_note_count"]]
    return out.sort_values(["program_name", "client_id", "month"], kind="mergesort").reset_index(drop=True)


def _case_notes_legacy_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, header=None, dtype=str, encoding="cp1252", engine="python")
    df = df.iloc[2:].copy()

    df.columns = [
        "client_id",
        "program_name",
        "Sep Case Note Count",
        "Oct Case Note Count",
        "Nov Case Note Count",
        "Dec Case Note Count",
    ]

    month_map = {
        "Sep Case Note Count": "2025-09",
        "Oct Case Note Count": "2025-10",
        "Nov Case Note Count": "2025-11",
        "Dec Case Note Count": "2025-12",
    }

    out = df.melt(
        id_vars=["client_id", "program_name"],
        value_vars=list(month_map.keys()),
        var_name="month_col",
        value_name="case_note_count",
    )
    out["month"] = out["month_col"].map(month_map)
    out["case_note_count"] = pd.to_numeric(out["case_note_count"], errors="coerce").fillna(0).astype("int64")

    out["client_id"] = out["client_id"].apply(canonical_id)
    out["program_name"] = out["program_name"].astype(str).str.strip()

    return out[["client_id", "program_name", "month", "case_note_count"]].reset_index(drop=True)


# -------------------------
# Services
# -------------------------

def services(df: pd.DataFrame) -> pd.DataFrame:
    out = df.rename(columns={
        "Clients Unique Identifier": "client_id",
        "Programs Full Name": "program_name",
        "Services Start Date Month": "month",
        "Services Count": "services_count",
    }).copy()

    out["client_id"] = out["client_id"].apply(canonical_id)
    out["program_name"] = out["program_name"].astype(str).str.strip()

    out["month"] = pd.to_datetime(out["month"].astype(str).str.strip() + "-01", errors="coerce").dt.strftime("%Y-%m")
    out["services_count"] = pd.to_numeric(out["services_count"], errors="coerce").fillna(0).astype("int64")

    out = out.dropna(subset=["month"])
    out = out[["client_id", "program_name", "month", "services_count"]]
    return out.sort_values(["program_name", "client_id", "month"], kind="mergesort").reset_index(drop=True)


# -------------------------
# CES (by client)
# -------------------------

def ces_hmis_latest_by_client(df: pd.DataFrame, pull_date: str) -> pd.DataFrame:
    out = df.rename(columns={
        "Clients Unique Identifier": "client_id",
        "Client Assessments Assessment ID": "assessment_id",
        "Client Assessments Assessment Date": "assessment_date",
        "Client Assessments Assessment Score": "assessment_score",
        "Client Assessments Name": "survey_name",
    }).copy()

    out["client_id"] = out["client_id"].apply(canonical_id)
    out["survey_name"] = out["survey_name"].astype(str).str.strip()

    out["assessment_id"] = pd.to_numeric(out["assessment_id"], errors="coerce")
    out["assessment_date"] = pd.to_datetime(out["assessment_date"], errors="coerce")
    out["assessment_score"] = pd.to_numeric(out["assessment_score"], errors="coerce")

    pull_dt = pd.to_datetime(pull_date)
    out = out[out["assessment_date"].notna() & (out["assessment_date"] <= pull_dt)].copy()

    out = out.sort_values(
        ["client_id", "assessment_date", "assessment_id"],
        ascending=[True, False, False],
        kind="mergesort",
    ).drop_duplicates(["client_id"], keep="first")

    out["ces_source"] = "hmis"
    out["pull_date"] = pull_date
    return out[["client_id", "survey_name", "assessment_date", "assessment_score", "ces_source", "pull_date"]].reset_index(drop=True)


def ces_external_latest_by_client(df: pd.DataFrame, pull_date: str) -> pd.DataFrame:
    out = df.rename(columns={
        "Clients Unique Identifier": "client_id",
        "Survey Name": "survey_name",
        "Latest CES Assessment Date": "assessment_date",
        "Assessment Score": "assessment_score",
        "Last Updated": "last_updated",
    }).copy()

    out["client_id"] = out["client_id"].apply(canonical_id)
    out["survey_name"] = out["survey_name"].astype(str).str.strip()

    out["assessment_date"] = pd.to_datetime(out["assessment_date"], errors="coerce")
    out["assessment_score"] = pd.to_numeric(out["assessment_score"], errors="coerce")
    out["last_updated"] = pd.to_datetime(out.get("last_updated"), errors="coerce")

    pull_dt = pd.to_datetime(pull_date)
    out = out[out["assessment_date"].notna() & (out["assessment_date"] <= pull_dt)].copy()

    out = out.sort_values(
        ["client_id", "assessment_date", "last_updated"],
        ascending=[True, False, False],
        kind="mergesort",
    ).drop_duplicates(["client_id"], keep="first")

    out["ces_source"] = "external"
    out["pull_date"] = pull_date
    return out[["client_id", "survey_name", "assessment_date", "assessment_score", "ces_source", "pull_date"]].reset_index(drop=True)


def ces_unified_latest_by_client(ces_external: pd.DataFrame, ces_hmis: pd.DataFrame) -> pd.DataFrame:
    """
    External overrides HMIS. Handles empty frames without FutureWarning.
    """
    cols = ["client_id", "survey_name", "assessment_date", "assessment_score", "ces_source", "pull_date"]

    if ces_external is None or ces_external.empty:
        return ces_hmis.reindex(columns=cols).sort_values(["client_id"], kind="mergesort").reset_index(drop=True)

    if ces_hmis is None or ces_hmis.empty:
        return ces_external.reindex(columns=cols).sort_values(["client_id"], kind="mergesort").reset_index(drop=True)

    ext_ids = set(ces_external["client_id"].astype(str).tolist())
    hmis_keep = ces_hmis[~ces_hmis["client_id"].astype(str).isin(ext_ids)].copy()

    ces_external = ces_external.reindex(columns=cols)
    hmis_keep = hmis_keep.reindex(columns=cols)

    out = pd.concat([ces_external, hmis_keep], ignore_index=True)
    return out.sort_values(["client_id"], kind="mergesort").reset_index(drop=True)
