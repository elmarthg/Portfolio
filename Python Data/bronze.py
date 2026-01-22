from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


def select_one_for_pull_date(folder: Path, pull_date: str, patterns: List[str]) -> Path:
    """
    Select a single file in `folder` that contains the pull_date token either:
      *_YYYY-MM-DD.*  OR  YYYY-MM-DD_*.*
    """
    folder = Path(folder)
    if not folder.exists():
        raise FileNotFoundError(f"Bronze folder not found: {folder}")

    candidates: List[Path] = []
    for pat in patterns:
        candidates.extend(folder.glob(pat))

    candidates = sorted([p for p in candidates if p.is_file()], key=lambda p: p.name.lower())

    def matches(p: Path) -> bool:
        name = p.name
        return (f"_{pull_date}." in name) or name.startswith(f"{pull_date}_") or (pull_date in name)

    hits = [p for p in candidates if matches(p)]
    if not hits:
        raise FileNotFoundError(
            f"No file found for pull_date={pull_date} in {folder}. "
            f"Expected date token like '*_{pull_date}.*' or '{pull_date}_*' matching {patterns}."
        )

    return sorted(hits, key=lambda p: p.name.lower())[0]


def discover_ces_external_files(folder: Path) -> List[Path]:
    """
    Loads all program-level CES external files with naming like:
      Weingart CES Data_AB109.xlsx
      Weingart CES Data_B7.xlsx

    Date token is optional.
    """
    folder = Path(folder)
    if not folder.exists():
        return []

    out: List[Path] = []
    for p in folder.rglob("*"):
        if not p.is_file():
            continue

        parts = {x.lower() for x in p.parts}
        if "archive" in parts or "_archive" in parts:
            continue
        if p.name.startswith("~$"):
            continue

        if not p.name.lower().startswith("weingart ces data_"):
            continue
        if p.suffix.lower() not in [".xlsx", ".xls", ".csv"]:
            continue

        out.append(p)

    return sorted(out, key=lambda x: x.name.lower())


@dataclass(frozen=True)
class BronzeInputs:
    singles: Dict[str, Path]
    ces_external_files: List[Path]


def resolve_inputs(bronze_root: Path, pull_date: str) -> BronzeInputs:
    bronze_root = Path(bronze_root)

    singles = {
        "program_client_data": select_one_for_pull_date(
            bronze_root / "ProgramClientData", pull_date, ["*.csv", "*.xlsx", "*.xls"]
        ),
        "case_notes": select_one_for_pull_date(
            bronze_root / "CaseNotes", pull_date, ["*.csv", "*.xlsx", "*.xls"]
        ),
        "services": select_one_for_pull_date(
            bronze_root / "Services", pull_date, ["*.csv", "*.xlsx", "*.xls"]
        ),
        "ces_hmis": select_one_for_pull_date(
            bronze_root / "CES" / "HMIS", pull_date, ["*.csv", "*.xlsx", "*.xls"]
        ),
    }

    ces_external_files = discover_ces_external_files(bronze_root / "CES" / "External")

    return BronzeInputs(singles=singles, ces_external_files=ces_external_files)
