from __future__ import annotations

import argparse
from datetime import datetime

import pandas as pd

from .config import resolve_paths
from .io_utils import (
    read_table,
    write_csv,
    write_json,
    write_excel_sheets,
    quality_report,
    sha256_file,
)
from . import bronze, silver, gold


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="HMIS Medallion ETL (Bronze -> Silver -> Gold)")
    p.add_argument("--pull-date", required=True, help="YYYY-MM-DD pull date token used to select Bronze inputs")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    pull_date = args.pull_date

    paths = resolve_paths()

    # -------------------------
    # Resolve bronze inputs
    # -------------------------
    inputs = bronze.resolve_inputs(paths.bronze, pull_date)
    singles = inputs.singles
    ces_external_files = inputs.ces_external_files

    # -------------------------
    # Read bronze
    # -------------------------
    pcd_raw = read_table(singles["program_client_data"])
    case_notes_raw = read_table(singles["case_notes"])
    services_raw = read_table(singles["services"])
    ces_hmis_raw = read_table(singles["ces_hmis"])

    # CES external: concatenate all program files (skip empty frames)
    frames = []
    for f in sorted(ces_external_files, key=lambda p: p.name.lower()):
        try:
            df = read_table(f)
            if df is not None and len(df) > 0:
                frames.append(df)
        except Exception:
            # deterministic behavior: skip unreadable file rather than crash
            continue

    ces_external_raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # -------------------------
    # Build silver
    # -------------------------
    s = silver.build_silver(
        pull_date=pull_date,
        pcd_raw=pcd_raw,
        case_notes_raw=case_notes_raw,
        services_raw=services_raw,
        ces_hmis_raw=ces_hmis_raw,
        ces_external_raw=ces_external_raw,
    )

    # -------------------------
    # Output folders
    # -------------------------
    silver_out = paths.silver / f"pull_date={pull_date}"
    gold_out = paths.gold / f"pull_date={pull_date}"
    gold_current = paths.gold / "current"

    silver_out.mkdir(parents=True, exist_ok=True)
    gold_out.mkdir(parents=True, exist_ok=True)
    gold_current.mkdir(parents=True, exist_ok=True)

    # -------------------------
    # Write silver tables
    # -------------------------
    write_csv(s.pcd_clean, silver_out / "program_client_data_clean.csv")
    write_csv(s.pcd_current_client_program, silver_out / "program_client_data_current_client_program.csv")

    # NEW: duplicates summary + detail
    write_csv(s.qa_enrollment_duplicates_summary, silver_out / "qa_enrollment_duplicates_summary.csv")
    write_csv(s.qa_enrollment_duplicates_detail, silver_out / "qa_enrollment_duplicates_detail.csv")

    write_csv(s.case_notes_monthly, silver_out / "case_notes_monthly.csv")
    write_csv(s.qa_case_notes_duplicates, silver_out / "qa_case_notes_duplicates.csv")
    write_csv(s.case_notes_latest, silver_out / "case_notes_latest_asof_pull.csv")

    write_csv(s.services_monthly, silver_out / "services_monthly.csv")
    write_csv(s.services_latest, silver_out / "services_latest_asof_pull.csv")

    write_csv(s.ces_hmis_latest, silver_out / "ces_hmis_latest_by_client.csv")
    write_csv(s.ces_external_latest, silver_out / "ces_external_latest_by_client.csv")
    write_csv(s.ces_unified_latest, silver_out / "ces_unified_latest_by_client.csv")

    # -------------------------
    # Build gold
    # -------------------------
    gold_client_program = gold.build_gold_client_program_readiness(
        pull_date=pull_date,
        spine_client_program=s.pcd_current_client_program,
        case_notes_latest=s.case_notes_latest,
        services_latest=s.services_latest,
        ces_unified_latest=s.ces_unified_latest,
    )

    gold_client = gold.build_gold_client_readiness_current(gold_client_program)

    # -------------------------
    # Gold workbook extracts
    # -------------------------
    multi_detail, multi_summary = gold.active_multi_programs(gold_client_program)

    # Use DETAIL duplicates (full rows) as requested
    dup_enroll_detail = s.qa_enrollment_duplicates_detail.copy()

    # Gold duplicate client_id in client-level table (should be 0)
    gold_dupe_client = gold_client[gold_client.duplicated(["client_id"], keep=False)].copy()

    wb = {
        "Active_Multi_Programs_Detail": multi_detail,
        "Active_Multi_Programs_Summary": multi_summary,
        "Duplicate_Enrollments_Detail": dup_enroll_detail,
        "Gold_Duplicate_ClientID": gold_dupe_client,
    }

    # -------------------------
    # Write gold outputs (ARCHIVE: pull_date=...)
    # -------------------------
    write_csv(gold_client_program, gold_out / "client_program_readiness_current.csv")
    write_csv(gold_client, gold_out / "client_readiness_current.csv")

    # NEW: also publish duplicates detail CSV in Gold archive
    write_csv(dup_enroll_detail, gold_out / "duplicate_enrollments.csv")

    extract_path = gold_out / f"gold_extracts_{pull_date}.xlsx"
    write_excel_sheets(extract_path, wb)

    # -------------------------
    # Manifest + QA (ARCHIVE)
    # -------------------------
    manifest = {
        "pull_date": pull_date,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "paths": {
            "base": str(paths.base),
            "bronze": str(paths.bronze),
            "silver_out": str(silver_out),
            "gold_out": str(gold_out),
            "gold_current": str(gold_current),
        },
        "bronze_used": {
            "program_client_data": str(singles["program_client_data"]),
            "case_notes": str(singles["case_notes"]),
            "services": str(singles["services"]),
            "ces_hmis": str(singles["ces_hmis"]),
            "ces_external_files_count": len(ces_external_files),
        },
        "bronze_hashes": {
            "program_client_data": sha256_file(singles["program_client_data"]),
            "case_notes": sha256_file(singles["case_notes"]),
            "services": sha256_file(singles["services"]),
            "ces_hmis": sha256_file(singles["ces_hmis"]),
        },
        "outputs": {
            "extract_workbook": str(extract_path),
        },
    }
    write_json(manifest, gold_out / "_manifest.json")

    qa = {
        "silver": [
            quality_report(
                s.pcd_current_client_program,
                name="program_client_data_current_client_program",
                key_cols=["client_id", "program_name"],
            ),
            quality_report(
                s.case_notes_monthly,
                name="case_notes_monthly",
                key_cols=["client_id", "program_name", "month"],
            ),
            quality_report(
                s.services_monthly,
                name="services_monthly",
                key_cols=["client_id", "program_name", "month"],
            ),
            quality_report(
                s.ces_unified_latest,
                name="ces_unified_latest_by_client",
                key_cols=["client_id"],
            ),
        ],
        "gold": [
            quality_report(
                gold_client_program,
                name="client_program_readiness_current",
                key_cols=["client_id", "program_name"],
            ),
            quality_report(
                gold_client,
                name="client_readiness_current",
                key_cols=["client_id"],
            ),
        ],
    }
    write_json(qa, gold_out / "_quality.json")

    # -------------------------
    # OPTION A: publish STABLE "current" outputs for Power BI Desktop
    # -------------------------
    write_csv(gold_client_program, gold_current / "client_program_readiness_current.csv")
    write_csv(gold_client, gold_current / "client_readiness_current.csv")

    # NEW: stable duplicates CSV for Power BI
    write_csv(dup_enroll_detail, gold_current / "duplicate_enrollments.csv")

    write_excel_sheets(gold_current / "gold_extracts_current.xlsx", wb)
    write_json(manifest, gold_current / "_manifest.json")
    write_json(qa, gold_current / "_quality.json")

    # -------------------------
    # Console summary
    # -------------------------
    print("DONE")
    print(f"Pull date: {pull_date}")
    print("Bronze used:")
    print(f"  program_client_data: {singles['program_client_data']}")
    print(f"  case_notes: {singles['case_notes']}")
    print(f"  services: {singles['services']}")
    print(f"  ces_hmis: {singles['ces_hmis']}")
    print(f"  ces_external_files: {len(ces_external_files)}")
    print(f"Silver outputs: {silver_out}")
    print(f"Gold outputs: {gold_out}")
    print(f"Gold current: {gold_current}")
    print(f"Extract workbook (archived): {extract_path}")
    print(f"Extract workbook (current): {gold_current / 'gold_extracts_current.xlsx'}")


if __name__ == "__main__":
    main()
