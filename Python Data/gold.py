from typing import Dict, List, Optional
import pandas as pd


def normalize_text(s) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s)
    return (
        s.replace("\u2019", "'")
         .replace("â€™", "'")
         .strip()
    )


def contains_ci(haystack, needle: str) -> bool:
    hs = normalize_text(haystack).casefold()
    nd = normalize_text(needle).casefold()
    return nd in hs


DOC_PATTERNS: Dict[str, List] = {
    "health_insurance": [
        ("Medicare/Medicaid", "Medicaid or Medicare Card"),
        ("Other Health Insurance", "Health Insurance Documentation"),
    ],
    "proof_of_income": [
        ("Pay Stub", "Pay Stub"),
        ("SSDI", "Supplemental Security Disability Income (SSDI) Forms"),
        ("SSI", "Supplemental Security Income (SSI) Forms"),
        ("GR", "General Relief (GR) Form"),
        ("Food Stamp", "Food Stamp Card or Award Letter"),
        ("CalWORKS", "CalWORKS Forms"),
        ("Form 1087", "Form 1087 - Self Declaration of Income/No Income Form"),
        ("Form 1084", "Form 1084 - 3rd Party Income Verification"),
        ("Alimony Agreement", "Alimony Agreement"),
        ("Social Security (NUMI) Printout", "Social Security (NUMI) Printout"),
        ("Tax Return", "Tax Return"),
        ("Veterans Affairs (VA) Benefits Award Letter", "Veterans Affairs (VA) Benefits Award Letter"),
        ("Self Employment Document", "Self Employment Document"),
        ("Other Financial Document", "Other Financial Document"),
    ],
}


def build_health_insurance_flag(file_list) -> str:
    matches = []
    for label, pattern in DOC_PATTERNS["health_insurance"]:
        if contains_ci(file_list, pattern):
            matches.append(label)
    return f"1 - {', '.join(matches)}" if matches else "0"


def build_proof_of_income(file_list) -> str:
    matches = []
    for label, pattern in DOC_PATTERNS["proof_of_income"]:
        if contains_ci(file_list, pattern):
            matches.append(label)
    return f"1 - {', '.join(matches)}" if matches else "0"


def ces_status(assessment_date, pull_date: str) -> str:
    if pd.isna(assessment_date):
        return "Assessment Not Done"
    days = (pd.to_datetime(pull_date) - pd.to_datetime(assessment_date)).days
    if days > 730:
        return "Over 2 Years, Review for life change"
    return "Current"


def threshold_for_survey(survey_name: Optional[str]) -> Optional[int]:
    s = "" if survey_name is None else str(survey_name).strip()
    if "CES" in s.upper():
        return 8
    if s == "Los Angeles Housing Assessment Tool (LA HAT)":
        return 17
    return None


def housing_matching_ready(survey_name, score, docs_ready: bool) -> str:
    th = threshold_for_survey(survey_name)
    score_val = pd.to_numeric(score, errors="coerce")
    is_ready = bool(docs_ready) and (th is not None) and pd.notna(score_val) and (score_val >= th)
    return "Housing Matching Ready" if is_ready else "Not Housing Matching Ready"


def intervention_alert(survey_name, score, assessment_status, days_in_project, docs_ready: bool) -> str:
    status = "" if assessment_status is None else str(assessment_status).strip()
    d = pd.to_numeric(days_in_project, errors="coerce")
    s = pd.to_numeric(score, errors="coerce")
    th = threshold_for_survey(survey_name)

    long_stay = pd.notna(d) and d >= 120
    mid_stay = pd.notna(d) and d >= 75
    low_assessment = (th is not None) and (pd.isna(s) or s < th)
    assessment_expired = status in {"Renewal Overdue", "Expired", "Over 2 Years, Review for life change"}

    if long_stay and (not docs_ready):
        return "≥120 days & missing docs – ESCALATE"
    if long_stay and assessment_expired:
        return "≥120 days & assessment expired – ESCALATE"
    if long_stay and low_assessment:
        return "≥120 days & score below threshold – ESCALATE"
    if long_stay:
        return "≥120 days & docs/assessment OK – monitor"
    if mid_stay and (not docs_ready):
        return "75–119 days & missing docs – ACTION"
    if mid_stay and (assessment_expired or low_assessment):
        return "75–119 days & assessment risk – ACTION"
    return "No Action Needed"


def case_count_category(n) -> str:
    v = pd.to_numeric(n, errors="coerce")
    if pd.isna(v):
        v = 0
    v = int(v)
    if v >= 5:
        return "5 or more case notes"
    return f"{v} case note" + ("" if v == 1 else "s")


def enrollment_days_tier(days) -> Optional[str]:
    d = pd.to_numeric(days, errors="coerce")
    if pd.isna(d):
        return None
    d = float(d)
    if d < 60:
        return "Tier 1 - <60 Days"
    if d <= 120:
        return "Tier 2 - 60-120 Days"
    if d <= 365:
        return "Tier 3 - 120-365 Days"
    return "Tier 4 - >365 Days"


def program_name_short(program_full) -> str:
    s = normalize_text(program_full)
    parts = s.split("-", 1)
    if len(parts) == 2:
        return parts[1].strip()
    return s.strip()


def missing_documents_row(ssn_flag: int, id_flag: int, dis_flag: int, hi_flag_text: str, income_text: str) -> str:
    missing = []
    if int(ssn_flag) == 0:
        missing.append("SSN Card")
    if int(id_flag) == 0:
        missing.append("CDL or State ID")
    if int(dis_flag) == 0:
        missing.append("Disability Verification (If Applicable)")
    if normalize_text(hi_flag_text) in {"0", ""}:
        missing.append("Health Insurance")
    if income_text is None or normalize_text(income_text) in {"", "0"}:
        missing.append("Proof of Income")
    return "All documents present" if not missing else ", ".join(missing)


def latest_month_case_notes(case_notes_monthly: pd.DataFrame) -> pd.DataFrame:
    df = case_notes_monthly.copy()
    df = df.sort_values(["client_id", "program_name", "month"], ascending=[True, True, False], kind="mergesort")
    df = df.drop_duplicates(["client_id", "program_name"], keep="first")
    return df.rename(columns={"month": "latest_case_note_month", "case_note_count": "latest_case_note_count"})


def latest_month_services(services_monthly: pd.DataFrame) -> pd.DataFrame:
    df = services_monthly.copy()
    df = df.sort_values(["client_id", "program_name", "month"], ascending=[True, True, False], kind="mergesort")
    df = df.drop_duplicates(["client_id", "program_name"], keep="first")
    return df.rename(columns={"month": "latest_services_month", "services_count": "latest_services_count"})


def client_program_readiness_current(
    pcd_current_client_program: pd.DataFrame,
    case_notes_monthly: pd.DataFrame,
    services_monthly: pd.DataFrame,
    ces_latest_by_client: pd.DataFrame,
    pull_date: str
) -> pd.DataFrame:
    out = pcd_current_client_program.copy()

    cn_latest = latest_month_case_notes(case_notes_monthly)
    svc_latest = latest_month_services(services_monthly)

    out = out.merge(
        cn_latest[["client_id", "program_name", "latest_case_note_month", "latest_case_note_count"]],
        on=["client_id", "program_name"],
        how="left",
        validate="1:1",
    )
    out = out.merge(
        svc_latest[["client_id", "program_name", "latest_services_month", "latest_services_count"]],
        on=["client_id", "program_name"],
        how="left",
        validate="1:1",
    )

    out = out.merge(
        ces_latest_by_client.rename(columns={
            "survey_name": "Survey Name",
            "assessment_date": "Latest CES Assessment Date",
            "assessment_score": "Assessment Score",
        })[["client_id", "Survey Name", "Latest CES Assessment Date", "Assessment Score"]],
        on=["client_id"],
        how="left",
        validate="m:1",
    )

    fl = out.get("file_list", "")

    out["SSN Card"] = fl.apply(lambda x: int(contains_ci(x, "Social Security Card")))
    out["CDL or State ID"] = fl.apply(lambda x: int(contains_ci(x, "Driver's License/State ID Card/Photo ID/ School Identification Card")))
    out["Homelessness Verification"] = fl.apply(lambda x: int(contains_ci(x, "Form 6053 - Los Angeles CoC Homelessness Verification")))
    out["Disability Verification"] = fl.apply(lambda x: int(contains_ci(x, "Disability Verification")))

    out["Health Insurance Column Flag"] = fl.apply(build_health_insurance_flag)
    out["Proof of Income"] = fl.apply(build_proof_of_income)

    out["CES Status"] = out["Latest CES Assessment Date"].apply(lambda d: ces_status(d, pull_date))
    out["Assessment Status"] = out["CES Status"]

    out["Document Ready"] = out.apply(
        lambda r: "Document Ready"
        if (r.get("CDL or State ID", 0) == 1 and r.get("SSN Card", 0) == 1 and str(r.get("Proof of Income", "")).startswith("1"))
        else "Not Document Ready",
        axis=1
    )

    out["Housing Matching Ready"] = out.apply(
        lambda r: housing_matching_ready(
            r.get("Survey Name"),
            r.get("Assessment Score"),
            docs_ready=(r.get("Document Ready") != "Not Document Ready")
        ),
        axis=1
    )

    out["Intervention Alert"] = out.apply(
        lambda r: intervention_alert(
            r.get("Survey Name"),
            r.get("Assessment Score"),
            r.get("Assessment Status"),
            r.get("days_in_project"),
            docs_ready=(r.get("Document Ready") != "Not Document Ready")
        ),
        axis=1
    )

    out["Case Count Category"] = out["latest_case_note_count"].apply(case_count_category)
    out["Enrollment Days Tier"] = out["days_in_project"].apply(enrollment_days_tier)

    out["Missing Documents"] = out.apply(
        lambda r: missing_documents_row(
            ssn_flag=r.get("SSN Card", 0),
            id_flag=r.get("CDL or State ID", 0),
            dis_flag=r.get("Disability Verification", 0),
            hi_flag_text=r.get("Health Insurance Column Flag", "0"),
            income_text=r.get("Proof of Income", "0"),
        ),
        axis=1
    )

    out["Programs Name"] = out["program_name"].apply(program_name_short)
    out["pull_date"] = pull_date

    out = out.sort_values(["program_name", "client_id"], kind="mergesort").reset_index(drop=True)
    return out


def client_readiness_current(client_program_ready: pd.DataFrame) -> pd.DataFrame:
    df = client_program_ready.copy()
    if "project_start_date" in df.columns:
        df = df.sort_values(["client_id", "project_start_date"], ascending=[True, False], kind="mergesort")
    else:
        df = df.sort_values(["client_id"], kind="mergesort")
    df = df.drop_duplicates(["client_id"], keep="first").reset_index(drop=True)
    return df
