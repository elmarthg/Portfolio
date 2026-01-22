from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    base: Path
    bronze: Path
    silver: Path
    gold: Path
    ops: Path


def resolve_paths() -> Paths:
    r"""
    Determines base folder for the Staging Area.

    Resolution order:
      1) HMIS_BASE env var (recommended for Task Scheduler / Dagster)
      2) derive from this file location:
         ...\Staging Area\_ops\etl\config.py  -> base is ...\Staging Area
    """
    env = os.environ.get("HMIS_BASE", "").strip()
    if env:
        base = Path(env).expanduser().resolve()
    else:
        # config.py is at ...\Staging Area\_ops\etl\config.py
        # parents[0]=etl, [1]=_ops, [2]=Staging Area
        base = Path(__file__).resolve().parents[2]

    ops = base / "_ops"
    bronze = base / "Bronze Stage"
    silver = base / "Silver Stage"
    gold = base / "Gold Stage"

    return Paths(base=base, bronze=bronze, silver=silver, gold=gold, ops=ops)
