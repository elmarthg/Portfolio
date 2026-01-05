from pathlib import Path
from typing import Dict, List, Tuple
import re

from .io_utils import sha256_file


def _date_in_name(name: str, pull_date: str) -> bool:
    """
    Supports BOTH:
      - YYYY-MM-DD_FileName.ext
      - FileName_YYYY-MM-DD.ext
      - FileName_YYYY-MM-DD_anything.ext
    """
    pattern = rf"(^|_){re.escape(pull_date)}(_|\.|$)"
    return re.search(pattern, name) is not None


def files_for_pull_date(folder: Path, pull_date: str, patterns: List[str], recursive: bool = False) -> List[Path]:
    globber = folder.rglob if recursive else folder.glob
    matches: List[Path] = []

    for pat in patterns:
        for p in globber(pat):
            if p.name.startswith("~$"):
                continue
            if _date_in_name(p.name, pull_date):
                matches.append(p)

    return sorted(matches)


def files_for_prefix(
    folder: Path,
    prefixes: List[str],
    patterns: List[str],
    recursive: bool = False,
    ignore_folder_names: set = None,
) -> List[Path]:
    """
    Finds files whose names start with any prefix in `prefixes`, e.g.:
      Weingart CES Data_AB109.xlsx
    """
    ignore_folder_names = {n.lower() for n in (ignore_folder_names or set())}
    globber = folder.rglob if recursive else folder.glob
    matches: List[Path] = []

    for pat in patterns:
        for p in globber(pat):
            if p.name.startswith("~$"):
                continue

            # Optional ignore: Archive folders
            if ignore_folder_names:
                if any(part.lower() in ignore_folder_names for part in p.parts):
                    continue

            for pref in prefixes:
                if p.name.startswith(pref):
                    matches.append(p)
                    break

    return sorted(set(matches))


def select_one_for_pull_date(folder: Path, pull_date: str, patterns: List[str]) -> Path:
    matches = files_for_pull_date(folder, pull_date, patterns, recursive=False)
    if not matches:
        raise FileNotFoundError(
            f"No file found for pull_date={pull_date} in {folder}. "
            f"Expected date token like '*_{pull_date}.*' or '{pull_date}_*' matching {patterns}."
        )
    return matches[-1]  # deterministic


def resolve_inputs(bronze_root: Path, pull_date: str) -> Tuple[Dict[str, Path], List[Path]]:
    """
    Bronze structure:
      Bronze Stage\ProgramClientData
      Bronze Stage\CaseNotes
      Bronze Stage\Services
      Bronze Stage\CES\HMIS
      Bronze Stage\CES\External
    """
    singles = {
        "program_client_data": select_one_for_pull_date(
            bronze_root / "ProgramClientData", pull_date, ["*.xlsx", "*.xls", "*.csv"]
        ),
        "case_notes": select_one_for_pull_date(
            bronze_root / "CaseNotes", pull_date, ["*.xlsx", "*.xls", "*.csv"]
        ),
        "services": select_one_for_pull_date(
            bronze_root / "Services", pull_date, ["*.xlsx", "*.xls", "*.csv"]
        ),
        "ces_hmis": select_one_for_pull_date(
            bronze_root / "CES" / "HMIS", pull_date, ["*.xlsx", "*.xls", "*.csv"]
        ),
    }

    # CES External: may NOT include date token. Preferred: date-token files (if present),
    # fallback: files starting with "Weingart CES Data_".
    ces_ext_folder = bronze_root / "CES" / "External"

    ces_external_files = files_for_pull_date(
        ces_ext_folder, pull_date, ["*.xlsx", "*.xls", "*.csv"], recursive=True
    )

    if not ces_external_files:
        ces_external_files = files_for_prefix(
            ces_ext_folder,
            prefixes=["Weingart CES Data_"],
            patterns=["*.xlsx", "*.xls", "*.csv"],
            recursive=True,
            ignore_folder_names={"archive", "_archive"},
        )

    return singles, ces_external_files


def build_manifest(run_id: str, pull_date: str, singles: Dict[str, Path], ces_external_files: List[Path]) -> Dict:
    inputs = list(singles.values()) + list(ces_external_files)
    return {
        "run_id": run_id,
        "pull_date": pull_date,
        "inputs": [
            {
                "dataset_hint": _dataset_hint(singles, p, ces_external_files),
                "path": str(p),
                "file_name": p.name,
                "sha256": sha256_file(p),
                "size_bytes": p.stat().st_size,
                "modified_ts": p.stat().st_mtime,
            }
            for p in inputs
        ],
    }


def _dataset_hint(singles: Dict[str, Path], p: Path, ces_external_files: List[Path]) -> str:
    for k, v in singles.items():
        if v == p:
            return k
    if p in ces_external_files:
        return "ces_external"
    return "unknown"
