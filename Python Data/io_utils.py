import hashlib
import json
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def read_table(path: Path) -> pd.DataFrame:
    """
    Reads .xlsx/.xls via pandas Excel reader; .csv via read_csv with encoding fallback.
    """
    if path.suffix.lower() in [".xlsx", ".xls"]:
        return pd.read_excel(path)

    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="cp1252")


def write_csv(df: pd.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    df.to_csv(path, index=False, encoding="utf-8")


def write_json(obj: Dict, path: Path) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")


def write_excel_sheets(sheets: Dict[str, pd.DataFrame], path: Path) -> None:
    """
    Write multiple DataFrames to one Excel workbook with multiple sheets.
    """
    ensure_dir(path.parent)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            safe_name = str(sheet_name)[:31]  # Excel sheet name limit
            df.to_excel(writer, sheet_name=safe_name, index=False)


def quality_report(df: pd.DataFrame, key_cols: Optional[List[str]] = None) -> Dict:
    rep: Dict = {
        "row_count": int(len(df)),
        "column_count": int(df.shape[1]),
        "null_rate_by_column": {c: float(df[c].isna().mean()) for c in df.columns},
    }
    if key_cols:
        rep["key_cols"] = key_cols
        rep["duplicate_key_rows"] = int(df.duplicated(key_cols).sum())
    return rep
