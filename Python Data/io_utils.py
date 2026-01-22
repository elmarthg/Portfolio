from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def read_table(path: Path) -> pd.DataFrame:
    """
    Robust reader for CSV/Excel (HMIS exports vary in encoding).
    """
    path = Path(path)
    suf = path.suffix.lower()

    if suf in [".xlsx", ".xls"]:
        return pd.read_excel(path)

    if suf == ".csv":
        for enc in ["utf-8-sig", "utf-8", "cp1252", "latin1"]:
            try:
                return pd.read_csv(path, encoding=enc)
            except UnicodeDecodeError:
                continue
        return pd.read_csv(path, encoding="latin1", errors="replace")

    raise ValueError(f"Unsupported file type: {path}")


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, default=str)


def write_excel_sheets(path: Path, sheets: Dict[str, pd.DataFrame]) -> None:
    """
    Writes an xlsx workbook with multiple sheets.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        for name, df in sheets.items():
            safe = (name or "Sheet1")[:31]
            df.to_excel(xw, sheet_name=safe, index=False)


def quality_report(
    df: pd.DataFrame,
    *,
    name: str,
    key_cols: Optional[List[str]] = None,
    sample_dupe_keys: int = 50,
) -> Dict[str, Any]:
    """
    Minimal quality report for auditability. Intended for JSON.
    """
    out: Dict[str, Any] = {
        "name": name,
        "rows": int(len(df)),
        "cols": int(df.shape[1]),
        "null_rates": {},
    }

    for c in df.columns:
        out["null_rates"][c] = float(df[c].isna().mean()) if len(df) else 0.0

    if key_cols:
        for k in key_cols:
            if k not in df.columns:
                raise ValueError(f"quality_report: key col missing: {k}")

        dupe_mask = df.duplicated(key_cols, keep=False)
        dupe_rows = df.loc[dupe_mask, key_cols].copy()
        out["duplicate_key_rows"] = int(dupe_mask.sum())

        if len(dupe_rows):
            sample = (
                dupe_rows.value_counts()
                .head(sample_dupe_keys)
                .reset_index(name="count")
            )
            out["duplicate_key_sample"] = sample.to_dict(orient="records")
        else:
            out["duplicate_key_sample"] = []

    return out
