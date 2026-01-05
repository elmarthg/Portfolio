import argparse
import uuid
from pathlib import Path

import pandas as pd

from . import bronze, silver, gold
from .config import Paths, default_base
from .io_utils import read_table, write_csv, write_json, write_excel_sheets, quality_report


def active_in_multiple_programs(pcd_current_client_program: pd.DataFrame) -> pd.DataFrame:
    counts = pcd_current_client_program.groupby("client_id")["program_name"].nunique().reset_index(name="program_count")
    multi_ids = counts[counts["program_count"] >= 2][["client_id"]]
    detail = pcd_current_client_program.merge(multi_ids, on="client_id", how="inner")
    detail = detail.merge(counts, on="client_id", how="left")
    return detail.sort_values(["program_count", "client_id", "program_name"], ascending=[False, True, True], kind="mergesort")


def active_in_multiple_programs_summary(pcd_current_client_program: pd.DataFrame) -> pd.DataFrame:
    g = pcd_current_client_program.groupby("client_id")["program_name"].agg(
        program_count="nunique",
        programs=lambda s: " | ".join(sorted(set(map(str, s))))
    ).reset_index()
    g = g[g["program_count"] >= 2].copy()
    return g.sort_values(["program_count", "client_id"], ascending=[False, True], kind="mergesort")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=str(default_base()), help="Base Staging Area path")
    ap.add_argument("--pull-date", required=True, help="YYYY-MM-DD date token for HMIS filenames")
    ap.add_argument("--exclude-programs", action="store_true", help="Exclude specific programs (Power BI filter)")
    args = ap.parse_args()

    base = Path(args.base)
    paths = Paths(base=base)
    pull_date = args.pull_date
    run_id = uuid.uuid4().hex

    # -------------------------
    # BRONZE: resolve inputs + manifest
    # -------------------------
    singles, ces_external_files = bronze.resolve_inputs(paths.bronze, pull_date)
    manifest = bronze.build_manifest(run_id, pull_date, singles, ces_external_files)

    # -------------------------
    # READ BRONZE
    # -------------------------
    pcd_raw = read_table(singles["program_client_data"])
    svc_raw = read_table(singles["services"])
    ces_hmis_raw = read_table(singles["ces_hmis"])

    cn_path = singles["case_notes"]

    if ces_external_files:
        frames = [read_table(p) for p in ces_external_files]
        ces_ext_raw = pd.concat(frames, ignore_index=True)
    else:
        ces_ext_raw = pd.DataFrame()

    # -------------------------
    # SILVER: transforms
    # -------------------------
    pcd_s = silver.program_client_data(pcd_raw, exclude_programs=args.exclude_programs)
    pcd_current_cp = silver.current_client_program_snapshot(pcd_s)
    qa_dups = silver.enrollment_duplicates(pcd_s)

    cn_s = silver.case_notes(cn_path)
    svc_s = silver.services(svc_raw)

    ces_hmis_s = silver.ces_hmis_latest_by_client(ces_hmis_raw, pull_date)

    if len(ces_ext_raw) > 0:
        ces_ext_s = silver.ces_external_latest_by_client(ces_ext_raw, pull_date)
    else:
        ces_ext_s = pd.DataFrame(columns=["client_id","survey_name","assessment_date","assessment_score","ces_source","pull_date"])

    ces_unified_s = silver.ces_unified_latest_by_client(ces_ext_s, ces_hmis_s)

    # -------------------------
    # WRITE SILVER
    # -------------------------
    s_root = paths.silver / f"pull_date={pull_date}"
    write_json(manifest, s_root / "_manifest.json")

    write_csv(pcd_s, s_root / "program_client_data.csv")
    write_json(quality_report(pcd_s, ["client_id","program_name"]), s_root / "program_client_data._quality.json")

    write_csv(pcd_current_cp, s_root / "program_client_data_current_client_program.csv")
    write_json(quality_report(pcd_current_cp, ["client_id","program_name"]), s_root / "program_client_data_current_client_program._quality.json")

    write_csv(qa_dups, s_root / "qa_enrollment_duplicates.csv")
    write_json(quality_report(qa_dups, ["client_id","program_name"]), s_root / "qa_enrollment_duplicates._quality.json")

    write_csv(cn_s, s_root / "case_notes_monthly.csv")
    write_json(quality_report(cn_s, ["client_id","program_name","month"]), s_root / "case_notes_monthly._quality.json")

    write_csv(svc_s, s_root / "services_monthly.csv")
    write_json(quality_report(svc_s, ["client_id","program_name","month"]), s_root / "services_monthly._quality.json")

    write_csv(ces_hmis_s, s_root / "ces_hmis_latest_by_client.csv")
    write_json(quality_report(ces_hmis_s, ["client_id"]), s_root / "ces_hmis_latest_by_client._quality.json")

    write_csv(ces_ext_s, s_root / "ces_external_latest_by_client.csv")
    write_json(quality_report(ces_ext_s, ["client_id"]), s_root / "ces_external_latest_by_client._quality.json")

    write_csv(ces_unified_s, s_root / "ces_unified_latest_by_client.csv")
    write_json(quality_report(ces_unified_s, ["client_id"]), s_root / "ces_unified_latest_by_client._quality.json")

    # -------------------------
    # GOLD: marts
    # -------------------------
    client_program_ready = gold.client_program_readiness_current(
        pcd_current_client_program=pcd_current_cp,
        case_notes_monthly=cn_s,
        services_monthly=svc_s,
        ces_latest_by_client=ces_unified_s,
        pull_date=pull_date
    )

    client_ready = gold.client_readiness_current(client_program_ready)

    g_root = paths.gold / f"pull_date={pull_date}"
    write_json(manifest, g_root / "_manifest.json")

    write_csv(client_program_ready, g_root / "client_program_readiness_current.csv")
    write_json(quality_report(client_program_ready, ["client_id","program_name"]),
               g_root / "client_program_readiness_current._quality.json")

    write_csv(client_ready, g_root / "client_readiness_current.csv")
    write_json(quality_report(client_ready, ["client_id"]),
               g_root / "client_readiness_current._quality.json")

    # -------------------------
    # EXTRACTS (Excel workbook)
    # -------------------------
    multi_detail = active_in_multiple_programs(pcd_current_cp)
    multi_summary = active_in_multiple_programs_summary(pcd_current_cp)

    dup_enrollments = qa_dups.copy()
    gold_client_dups = client_ready[client_ready.duplicated(["client_id"], keep=False)].copy()

    report_path = g_root / f"gold_extracts_{pull_date}.xlsx"
    write_excel_sheets(
        {
            "Active_Multi_Programs_Detail": multi_detail,
            "Active_Multi_Programs_Summary": multi_summary,
            "Duplicate_Enrollments": dup_enrollments,
            "Gold_Duplicate_ClientID": gold_client_dups,
        },
        report_path
    )

    # CSV copies (optional but useful)
    write_csv(multi_detail, g_root / "active_multi_programs_detail.csv")
    write_csv(multi_summary, g_root / "active_multi_programs_summary.csv")
    write_csv(dup_enrollments, g_root / "duplicate_enrollments.csv")

    print("DONE")
    print("Pull date:", pull_date)
    print("Bronze used:")
    for k, p in singles.items():
        print(f"  {k}: {p}")
    print(f"  ces_external_files: {len(ces_external_files)}")
    print("Silver outputs:", s_root)
    print("Gold outputs:", g_root)
    print("Extract workbook:", report_path)


if __name__ == "__main__":
    main()
