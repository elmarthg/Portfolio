from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

import pandas as pd


def _canon_text(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace("\ufeff", "", regex=False).str.strip()


def _to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def _pull_month(pull_date: str) -> str:
    return pull_date[:7]


EXCLUDE_PROGRAMS = {
    "Weingart Center Association - Downtown Access Center",
    "Weingart Center Association - Problem-Solving Families",
    "Weingart Center Association - Problem-Solving Individuals",
}


def _pcd_dupe_detail_standard(pcd_clean: pd.DataFrame, dupe_summary: pd.DataFrame) -> pd.DataFrame:
    """
    Returns the actual duplicated rows (detail) for enrollment duplicates by (client_id, program_name),
    with standardized columns for operational review.
    """
    df = pcd_clean.copy()
    key = ["client_id", "program_name"]

    dupe_mask = df.duplicated(key, keep=False)
    detail = df.loc[dupe_mask].copy()

    if len(detail) == 0:
        # return empty but with expected columns
        cols = [
            "client_id", "active_in_project", "client_full_name", "roi_active", "days_in_project",
            "project_start_date", "Client Custom Point of Contact Name", "Client Custom Point of Contact Phone",
            "Client Custom Point of Contact Email", "Client Custom Point of Contact Date",
            "Clients DoB Data Quality", "Clients SSN Data Quality", "Clients SSN - Last 4",
            "enrollment_deleted", "Client Assessment Custom TB Clearance Date", "program_name",
            "last_assessment_id", "last_assessment_date", "file_list", "assigned_staff",
            "roi_active_flag", "enrollment_deleted_flag", "row_count"
        ]
        return pd.DataFrame(columns=cols)

    # Attach row_count per key from the summary
    detail = detail.merge(dupe_summary, on=key, how="left")

    def col(name: str) -> pd.Series:
        return detail[name] if name in detail.columns else pd.Series([pd.NA] * len(detail), index=detail.index)

    out = pd.DataFrame(
        {
            "client_id": col("client_id"),
            "active_in_project": col("Enrollments Active in Project"),
            "client_full_name": col("Clients Client Full Name"),
            "roi_active": col("Clients Active ROI?"),
            "days_in_project": col("Enrollments Days in Project"),
            "project_start_date": col("Enrollments Project Start Date"),
            "Client Custom Point of Contact Name": col("Client Custom Point of Contact Name"),
            "Client Custom Point of Contact Phone": col("Client Custom Point of Contact Phone"),
            "Client Custom Point of Contact Email": col("Client Custom Point of Contact Email"),
            "Client Custom Point of Contact Date": col("Client Custom Point of Contact Date"),
            "Clients DoB Data Quality": col("Clients DoB Data Quality"),
            "Clients SSN Data Quality": col("Clients SSN Data Quality"),
            "Clients SSN - Last 4": col("Clients SSN - Last 4"),
            "enrollment_deleted": col("Enrollments Deleted (Yes / No)"),
            "Client Assessment Custom TB Clearance Date": col("Client Assessment Custom TB Clearance Date"),
            "program_name": col("program_name"),
            "last_assessment_id": col("Client Assessments Last Assessment ID"),
            "last_assessment_date": col("Client Assessments Last Assessment Date"),
            "file_list": col("List of Client File Name"),
            "assigned_staff": col("List of Assigned Staff"),
            "roi_active_flag": (col("Clients Active ROI?").astype(str).str.upper() == "YES").astype("int64"),
            "enrollment_deleted_flag": (col("Enrollments Deleted (Yes / No)").astype(str).str.upper() == "YES").astype(
                "int64"
            ),
            "row_count": col("row_count"),
        }
    )

    # Clean up types
    out["client_id"] = _canon_text(out["client_id"])
    out["program_name"] = _canon_text(out["program_name"])

    # Keep stable ordering for deterministic review
    out = out.sort_values(["client_id", "program_name", "project_start_date"], ascending=[True, True, False])
    return out


def build_program_client_data(
    pcd_raw: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      - pcd_clean: cleaned raw ProgramClientData (still contains original columns)
      - dupe_summary: duplicates grouped by (client_id, program_name) with row_count
      - dupe_detail: actual duplicated rows with standardized columns
    """
    df = pcd_raw.copy()

    df["client_id"] = _canon_text(df["Clients Unique Identifier"])
    df["program_name"] = _canon_text(df["Programs Full Name"])

    df = df[~df["Programs Full Name"].isin(EXCLUDE_PROGRAMS)].copy()

    # Type normalization (keeps original columns intact for downstream use)
    if "Enrollments Project Start Date" in df.columns:
        df["Enrollments Project Start Date"] = _to_date(df["Enrollments Project Start Date"])
    if "Client Assessments Last Assessment Date" in df.columns:
        df["Client Assessments Last Assessment Date"] = _to_date(df["Client Assessments Last Assessment Date"])
    if "Client Custom Point of Contact Date" in df.columns:
        df["Client Custom Point of Contact Date"] = _to_date(df["Client Custom Point of Contact Date"])

    # Flags used for deterministic ranking later
    if "Enrollments Active in Project" in df.columns:
        df["enrollment_active_in_project"] = _canon_text(df["Enrollments Active in Project"]).str.upper()
    else:
        df["enrollment_active_in_project"] = ""

    if "Enrollments Deleted (Yes / No)" in df.columns:
        df["enrollment_deleted_flag"] = _canon_text(df["Enrollments Deleted (Yes / No)"]).str.upper()
    else:
        df["enrollment_deleted_flag"] = ""

    if "Enrollments Days in Project" in df.columns:
        df["enrollment_days_in_project"] = pd.to_numeric(df["Enrollments Days in Project"], errors="coerce")
    else:
        df["enrollment_days_in_project"] = pd.NA

    # Duplicate summary
    key = ["client_id", "program_name"]
    dupe_summary = (
        df.groupby(key, dropna=False)
        .size()
        .reset_index(name="row_count")
        .query("row_count > 1")
        .sort_values(["row_count", "client_id", "program_name"], ascending=[False, True, True])
    )

    # Duplicate detail (full rows)
    dupe_detail = _pcd_dupe_detail_standard(df, dupe_summary)

    return df, dupe_summary, dupe_detail


def choose_current_client_program(pcd_clean: pd.DataFrame) -> pd.DataFrame:
    df = pcd_clean.copy()

    df["active_rank"] = (df["enrollment_active_in_project"] == "YES").astype(int)
    df["deleted_rank"] = (df["enrollment_deleted_flag"] != "YES").astype(int)
    df["start_rank"] = pd.to_datetime(df.get("Enrollments Project Start Date"), errors="coerce")
    df["days_rank"] = pd.to_numeric(df["enrollment_days_in_project"], errors="coerce").fillna(-1)

    df = df.sort_values(
        ["client_id", "program_name", "active_rank", "deleted_rank", "start_rank", "days_rank"],
        ascending=[True, True, False, False, False, False],
    )

    out = df.drop_duplicates(["client_id", "program_name"], keep="first").copy()
    out = out.drop(columns=["active_rank", "deleted_rank", "start_rank", "days_rank"], errors="ignore")
    return out


def build_case_notes_monthly(case_notes_raw: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = case_notes_raw.copy()

    rename = {
        "Clients Unique Identifier": "client_id",
        "Programs Full Name": "program_name",
        "Client Notes - Enrollment Level Case Note Month": "month",
        "Client Notes - Enrollment Level Count": "case_note_count",
        "List of Staff Full Name": "staff_full_name",
    }
    df = df.rename(columns=rename)

    required = {"client_id", "program_name", "month", "case_note_count"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CaseNotes missing required columns: {sorted(missing)}")

    df["client_id"] = _canon_text(df["client_id"])
    df["program_name"] = _canon_text(df["program_name"])
    df["month"] = _canon_text(df["month"])

    bad_month = ~df["month"].str.match(r"^\d{4}-\d{2}$", na=False)
    if bad_month.any():
        examples = df.loc[bad_month, "month"].dropna().unique()[:10]
        raise ValueError(f"CaseNotes month not in YYYY-MM format. Examples: {examples}")

    df["case_note_count"] = pd.to_numeric(df["case_note_count"], errors="coerce").fillna(0).astype("int64")

    key = ["client_id", "program_name", "month"]
    qa_dupes = (
        df.groupby(key, dropna=False)
        .size()
        .reset_index(name="raw_row_count")
        .query("raw_row_count > 1")
    )

    if "staff_full_name" in df.columns:
        df["staff_full_name"] = _canon_text(df["staff_full_name"])
        out = (
            df.groupby(key, as_index=False)
            .agg(
                case_note_count=("case_note_count", "sum"),
                case_note_staff_list=("staff_full_name", lambda s: ", ".join(sorted(set([x for x in s if x and x.lower() != "nan"])))),
            )
        )
    else:
        out = df.groupby(key, as_index=False).agg(case_note_count=("case_note_count", "sum"))

    return out, qa_dupes


def latest_month_asof_pull(df_monthly: pd.DataFrame, pull_date: str, value_col: str, prefix: str) -> pd.DataFrame:
    pull_month = _pull_month(pull_date)

    df = df_monthly.copy()
    df = df[df["month"] <= pull_month].copy()

    df = df.sort_values(["client_id", "program_name", "month"], ascending=[True, True, False])
    latest = df.drop_duplicates(["client_id", "program_name"], keep="first").copy()

    latest = latest.rename(columns={"month": f"{prefix}_month", value_col: f"{prefix}_count"})
    return latest


def build_services_monthly(services_raw: pd.DataFrame) -> pd.DataFrame:
    df = services_raw.copy()

    rename = {
        "Clients Unique Identifier": "client_id",
        "Programs Full Name": "program_name",
        "Services Start Date Month": "month",
        "Services Count": "services_count",
    }
    df = df.rename(columns=rename)

    required = {"client_id", "program_name", "month", "services_count"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Services missing required columns: {sorted(missing)}")

    df["client_id"] = _canon_text(df["client_id"])
    df["program_name"] = _canon_text(df["program_name"])
    df["month"] = _canon_text(df["month"])

    bad_month = ~df["month"].str.match(r"^\d{4}-\d{2}$", na=False)
    if bad_month.any():
        examples = df.loc[bad_month, "month"].dropna().unique()[:10]
        raise ValueError(f"Services month not in YYYY-MM format. Examples: {examples}")

    df["services_count"] = pd.to_numeric(df["services_count"], errors="coerce").fillna(0).astype("int64")

    out = (
        df.groupby(["client_id", "program_name", "month"], as_index=False)
        .agg(services_count=("services_count", "sum"))
    )
    return out


def build_ces_hmis_latest(ces_hmis_raw: pd.DataFrame, pull_date: str) -> pd.DataFrame:
    df = ces_hmis_raw.copy()

    rename = {
        "Clients Unique Identifier": "client_id",
        "Client Assessments Assessment ID": "assessment_id",
        "Client Assessments Assessment Date": "assessment_date",
        "Client Assessments Assessment Score": "assessment_score",
        "Client Assessments Is Coordinated Entry": "is_coordinated_entry",
        "Client Assessments Assessing Agency Name": "assessing_agency",
        "Client Assessments Name": "survey_name",
    }
    df = df.rename(columns=rename)

    required = {"client_id", "assessment_date", "assessment_score", "survey_name"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CES_HMIS missing required columns: {sorted(missing)}")

    df["client_id"] = _canon_text(df["client_id"])
    df["survey_name"] = _canon_text(df["survey_name"])
    df["assessment_date"] = pd.to_datetime(df["assessment_date"], errors="coerce").dt.date
    df["assessment_score"] = pd.to_numeric(df["assessment_score"], errors="coerce")

    pull_dt = pd.to_datetime(pull_date).date()
    df = df[df["assessment_date"].notna() & (df["assessment_date"] <= pull_dt)].copy()

    if "assessment_id" in df.columns:
        df["assessment_id"] = pd.to_numeric(df["assessment_id"], errors="coerce")

    df = df.sort_values(
        ["client_id", "assessment_date", "assessment_id", "assessment_score"],
        ascending=[True, False, False, False],
    )

    out = df.drop_duplicates(["client_id"], keep="first").copy()
    out["ces_source"] = "HMIS"

    keep = [
        "client_id",
        "survey_name",
        "assessment_date",
        "assessment_score",
        "is_coordinated_entry",
        "assessing_agency",
        "ces_source",
    ]
    for c in keep:
        if c not in out.columns:
            out[c] = pd.NA

    return out[keep]


def build_ces_external_latest(ces_external_raw: pd.DataFrame, pull_date: str) -> pd.DataFrame:
    df = ces_external_raw.copy()

    rename = {
        "Clients Unique Identifier": "client_id",
        "Survey Name": "survey_name",
        "Latest CES Assessment Date": "assessment_date",
        "Assessment Score": "assessment_score",
        "Last Updated": "last_updated",
    }
    df = df.rename(columns=rename)

    required = {"client_id", "survey_name", "assessment_date", "assessment_score"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CES_External missing required columns: {sorted(missing)}")

    df["client_id"] = _canon_text(df["client_id"])
    df["survey_name"] = _canon_text(df["survey_name"])

    df["assessment_date"] = pd.to_datetime(df["assessment_date"], errors="coerce").dt.date
    df["assessment_score"] = pd.to_numeric(df["assessment_score"], errors="coerce")

    pull_dt = pd.to_datetime(pull_date).date()
    df = df[df["assessment_date"].notna() & (df["assessment_date"] <= pull_dt)].copy()

    if "last_updated" in df.columns:
        df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")
    else:
        df["last_updated"] = pd.NaT

    df = df.sort_values(
        ["client_id", "assessment_date", "last_updated", "assessment_score"],
        ascending=[True, False, False, False],
    )

    out = df.drop_duplicates(["client_id"], keep="first").copy()
    out["ces_source"] = "EXTERNAL"

    keep = ["client_id", "survey_name", "assessment_date", "assessment_score", "ces_source"]
    for c in keep:
        if c not in out.columns:
            out[c] = pd.NA

    return out[keep]


def unify_ces_external_over_hmis(ces_external_latest: pd.DataFrame, ces_hmis_latest: pd.DataFrame) -> pd.DataFrame:
    ext = ces_external_latest.copy()
    hmis = ces_hmis_latest.copy()

    if len(ext) == 0:
        return hmis
    if len(hmis) == 0:
        return ext

    ext_ids = set(ext["client_id"].astype(str))
    hmis_keep = hmis[~hmis["client_id"].astype(str).isin(ext_ids)].copy()

    cols = sorted(set(ext.columns) | set(hmis_keep.columns))
    ext = ext.reindex(columns=cols)
    hmis_keep = hmis_keep.reindex(columns=cols)

    return pd.concat([ext, hmis_keep], ignore_index=True)


@dataclass(frozen=True)
class SilverOutputs:
    pcd_clean: pd.DataFrame
    pcd_current_client_program: pd.DataFrame
    qa_enrollment_duplicates_summary: pd.DataFrame
    qa_enrollment_duplicates_detail: pd.DataFrame

    case_notes_monthly: pd.DataFrame
    qa_case_notes_duplicates: pd.DataFrame
    case_notes_latest: pd.DataFrame

    services_monthly: pd.DataFrame
    services_latest: pd.DataFrame

    ces_hmis_latest: pd.DataFrame
    ces_external_latest: pd.DataFrame
    ces_unified_latest: pd.DataFrame


def build_silver(
    *,
    pull_date: str,
    pcd_raw: pd.DataFrame,
    case_notes_raw: pd.DataFrame,
    services_raw: pd.DataFrame,
    ces_hmis_raw: pd.DataFrame,
    ces_external_raw: pd.DataFrame,
) -> SilverOutputs:
    pcd_clean, dupe_summary, dupe_detail = build_program_client_data(pcd_raw)
    pcd_current = choose_current_client_program(pcd_clean)

    cn_monthly, cn_dupes = build_case_notes_monthly(case_notes_raw)
    cn_latest = latest_month_asof_pull(cn_monthly, pull_date, "case_note_count", "case_notes")

    svc_monthly = build_services_monthly(services_raw)
    svc_latest = latest_month_asof_pull(svc_monthly, pull_date, "services_count", "services")

    ces_hmis_latest = build_ces_hmis_latest(ces_hmis_raw, pull_date)

    if len(ces_external_raw) > 0:
        ces_external_latest = build_ces_external_latest(ces_external_raw, pull_date)
    else:
        ces_external_latest = pd.DataFrame(
            columns=["client_id", "survey_name", "assessment_date", "assessment_score", "ces_source"]
        )

    ces_unified = unify_ces_external_over_hmis(ces_external_latest, ces_hmis_latest)

    return SilverOutputs(
        pcd_clean=pcd_clean,
        pcd_current_client_program=pcd_current,
        qa_enrollment_duplicates_summary=dupe_summary,
        qa_enrollment_duplicates_detail=dupe_detail,
        case_notes_monthly=cn_monthly,
        qa_case_notes_duplicates=cn_dupes,
        case_notes_latest=cn_latest,
        services_monthly=svc_monthly,
        services_latest=svc_latest,
        ces_hmis_latest=ces_hmis_latest,
        ces_external_latest=ces_external_latest,
        ces_unified_latest=ces_unified,
    )
