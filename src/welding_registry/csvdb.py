from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


BASE_DIR = Path("warehouse/csv")
ASOF_DIR = BASE_DIR / "asof"
LOG_FILE = BASE_DIR / "display_log.csv"


def ensure_dirs() -> None:
    ASOF_DIR.mkdir(parents=True, exist_ok=True)
    BASE_DIR.mkdir(parents=True, exist_ok=True)


def asof_csv_path(date: str) -> Path:
    ensure_dirs()
    return ASOF_DIR / f"{pd.to_datetime(date).date().isoformat()}.csv"


def write_asof_csv(df: pd.DataFrame, *, date: str) -> Path:
    ensure_dirs()
    path = asof_csv_path(date)
    # Normalize string columns and order a bit
    cols = [
        "name",
        "license_no",
        "qualification",
        "category",
        "first_issue_date",
        "issue_date",
        "expiry_date",
        "valid_from",
        "valid_to",
    ]
    cols = [c for c in cols if c in df.columns] + [c for c in df.columns if c not in cols]
    out = df.copy()[cols]
    for c in out.columns:
        if out[c].dtype == "datetime64[ns]":
            out[c] = out[c].dt.date
    out.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def read_asof_csv(date: str) -> Optional[pd.DataFrame]:
    path = asof_csv_path(date)
    if not path.exists():
        return None
    df = pd.read_csv(path)
    # Coerce date-like columns back
    for c in ["first_issue_date", "issue_date", "expiry_date", "valid_from", "valid_to"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    return df


def get_person_list(date: str) -> list[tuple[str, int]]:
    df = read_asof_csv(date)
    if df is None or "name" not in df.columns:
        return []
    s = df["name"].dropna().astype(str).value_counts()
    return [(str(idx), int(val)) for idx, val in s.sort_index().items()]


def get_qualification_list(date: str) -> list[str]:
    df = read_asof_csv(date)
    if df is None or "qualification" not in df.columns:
        return []
    vals = sorted(set(df["qualification"].dropna().astype(str)))
    return vals


def _append_csv(path: Path, row: dict) -> None:
    import csv

    ensure_dirs()
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            w.writeheader()
        w.writerow(row)


def log_display_selection(
    *,
    date: str,
    mode: str,  # 'person' or 'qualification'
    persons: Iterable[str] | None,
    qualifications: Iterable[str] | None,
    operator: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Path:
    ensure_dirs()
    row = {
        "timestamp": pd.Timestamp.utcnow().isoformat(timespec="seconds"),
        "date": pd.to_datetime(date).date().isoformat(),
        "mode": mode,
        "persons": ";".join([str(x) for x in (persons or [])]),
        "qualifications": ";".join([str(x) for x in (qualifications or [])]),
        "operator": operator or "",
        "session_id": session_id or "",
    }
    _append_csv(LOG_FILE, row)
    return LOG_FILE


__all__ = [
    "ensure_dirs",
    "write_asof_csv",
    "read_asof_csv",
    "get_person_list",
    "get_qualification_list",
    "log_display_selection",
    "asof_csv_path",
]


def read_csv_robust(path: Path) -> pd.DataFrame:
    """Read CSV with tolerant encoding handling (UTF-8/UTF-8-SIG/CP932).
    Returns a pandas DataFrame, or raises on total failure.
    """
    # Quick BOM sniff
    encodings = ["utf-8", "utf-8-sig", "cp932", "shift_jis"]
    try:
        with path.open("rb") as f:
            head = f.read(4)
        if head.startswith(b"\xef\xbb\xbf"):
            encodings = ["utf-8-sig", "utf-8", "cp932", "shift_jis"]
    except Exception:
        pass
    last_err: Exception | None = None
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
            continue
    if last_err is not None:
        raise last_err
    # Fallback (should not reach)
    return pd.read_csv(path)
