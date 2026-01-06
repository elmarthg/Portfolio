# hmis_orchestrator.py
from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Optional

import dagster as dg


# -----------------------------
# CONFIG
# -----------------------------

# Default base folder (used if HMIS_BASE env var is not set)
DEFAULT_BASE = Path(
    r"C:\Users\ElmarthyJanettyGalla\OneDrive - Weingart Center Association\QA Data Hub - Documents\Staging Area"
)

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


# -----------------------------
# HELPERS
# -----------------------------

def _parse_date(text: str) -> Optional[str]:
    m = DATE_RE.search(text)
    return m.group(1) if m else None


def _bronze_program_client_dir(base: Path) -> Path:
    return base / "Bronze Stage" / "ProgramClientData"


def latest_pull_date_from_bronze(base: Path) -> str:
    """
    Derive the latest pull_date by scanning Bronze/ProgramClientData filenames like:
      ProgramClientData_2026-01-05.csv
    """
    pcd_dir = _bronze_program_client_dir(base)
    if not pcd_dir.exists():
        raise FileNotFoundError(f"Expected directory not found: {pcd_dir}")

    dates: list[str] = []
    for p in pcd_dir.glob("ProgramClientData_*.*"):
        d = _parse_date(p.name)
        if d:
            dates.append(d)

    if not dates:
        raise FileNotFoundError(
            f"No pull-dated ProgramClientData files found in {pcd_dir} "
            f"(expected names like ProgramClientData_YYYY-MM-DD.csv)"
        )

    return max(dates)  # YYYY-MM-DD lexicographically sortable


# -----------------------------
# OP + JOB
# -----------------------------

@dg.op(
    config_schema={
        "base": str,       # Staging Area root
        "pull_date": str,  # YYYY-MM-DD
    }
)
def run_hmis_etl(context) -> None:
    """
    Orchestrates your existing ETL CLI as one unit:
      python -m etl.run --pull-date YYYY-MM-DD
    """
    base = Path(context.op_config["base"]).resolve()
    pull_date = context.op_config["pull_date"]

    cmd = [sys.executable, "-m", "etl.run", "--pull-date", pull_date]

    context.log.info(f"Running ETL: {' '.join(cmd)}")
    context.log.info(f"Base: {base}")
    context.log.info(f"Pull date: {pull_date}")

    env = os.environ.copy()
    env["HMIS_BASE"] = str(base)  # harmless if your ETL doesn't use it

    # Ensure imports resolve the same way as your manual run from _ops
    ops_dir = Path(__file__).resolve().parent

    subprocess.run(cmd, cwd=str(ops_dir), env=env, check=True)


@dg.job
def hmis_etl_job():
    run_hmis_etl()


# -----------------------------
# SENSOR (optional)
# -----------------------------

@dg.sensor(job=hmis_etl_job, minimum_interval_seconds=60)
def new_bronze_pull_date_sensor(context):
    """
    Triggers a run when a newer pull_date appears in Bronze.
    Uses cursor to avoid re-processing the same pull_date.
    """
    base = Path(os.environ.get("HMIS_BASE", str(DEFAULT_BASE))).resolve()
    if not base.exists():
        context.log.warning(f"Base path does not exist: {base}")
        return

    newest = latest_pull_date_from_bronze(base)
    last = context.cursor or ""

    if last and newest <= last:
        return

    run_config = {
        "ops": {
            "run_hmis_etl": {
                "config": {"base": str(base), "pull_date": newest}
            }
        }
    }

    yield dg.RunRequest(run_key=newest, run_config=run_config, tags={"pull_date": newest, "trigger": "sensor"})
    context.update_cursor(newest)


# -----------------------------
# SCHEDULE: 1st + 3rd Wednesday (9:00 AM PT)
# -----------------------------

@dg.schedule(
    job=hmis_etl_job,
    cron_schedule=[
        "0 9 1-7 * 3",     # 1st Wednesday
        "0 9 15-21 * 3",   # 3rd Wednesday
    ],
    execution_timezone="America/Los_Angeles",
)
def hmis_etl_1st_3rd_wednesday_schedule(context):
    """
    Runs on the 1st and 3rd Wednesday of each month at 9:00 AM Pacific.
    Executes the latest pull_date found in Bronze.
    """
    base = Path(os.environ.get("HMIS_BASE", str(DEFAULT_BASE))).resolve()
    if not base.exists():
        return dg.SkipReason(f"Base path does not exist: {base}")

    pull_date = latest_pull_date_from_bronze(base)

    run_config = {
        "ops": {
            "run_hmis_etl": {
                "config": {"base": str(base), "pull_date": pull_date}
            }
        }
    }

    return dg.RunRequest(
        run_key=pull_date,
        run_config=run_config,
        tags={"pull_date": pull_date, "trigger": "schedule"},
    )


# -----------------------------
# DEFINITIONS
# -----------------------------

defs = dg.Definitions(
    jobs=[hmis_etl_job],
    sensors=[new_bronze_pull_date_sensor],
    schedules=[hmis_etl_1st_3rd_wednesday_schedule],
)
