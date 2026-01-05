from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    base: Path

    @property
    def bronze(self) -> Path:
        return self.base / "Bronze Stage"

    @property
    def silver(self) -> Path:
        return self.base / "Silver Stage"

    @property
    def gold(self) -> Path:
        return self.base / "Gold Stage"


def default_base() -> Path:
    return Path(
        r"C:\Users\ElmarthyJanettyGalla\OneDrive - Weingart Center Association\QA Data Hub - Documents\Staging Area"
    )


EXCLUDE_PROGRAMS = {
    "Weingart Center Association - Downtown Access Center",
    "Weingart Center Association - Problem-Solving Families",
    "Weingart Center Association - Problem-Solving Individuals",
}
