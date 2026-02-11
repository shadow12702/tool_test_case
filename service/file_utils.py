from __future__ import annotations

from pathlib import Path


def looks_like_xlsx(path: Path) -> bool:
    """
    Some files in this workspace are actually XLSX (ZIP) but named as .csv.
    Detect by ZIP magic bytes "PK".
    """
    try:
        with path.open("rb") as f:
            sig = f.read(2)
        return sig == b"PK"
    except OSError:
        return False

