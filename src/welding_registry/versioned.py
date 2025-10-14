from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from .io_excel import read_sheet, to_canonical
from .normalize import name_key


# ------------------------
# Data model (DuckDB)
# ------------------------

DDL = r"""
CREATE TABLE IF NOT EXISTS ver_snapshots (
  snapshot_id BIGINT PRIMARY KEY,
  snapshot_date DATE NOT NULL,
  imported_at TIMESTAMP DEFAULT now(),
  source_path TEXT,
  sheet TEXT,
  header_row INT,
  content_hash TEXT,
  row_count INT
);

CREATE SEQUENCE IF NOT EXISTS ver_snapshots_seq;

CREATE TABLE IF NOT EXISTS ver_persons (
  person_id BIGINT PRIMARY KEY,
  name TEXT NOT NULL,
  name_key TEXT NOT NULL UNIQUE
);
CREATE SEQUENCE IF NOT EXISTS ver_persons_seq;

-- Raw records per snapshot (audit trail / comparisons)
CREATE TABLE IF NOT EXISTS ver_snapshot_records (
  snapshot_id BIGINT NOT NULL,
  rec_key TEXT NOT NULL,
  name TEXT,
  license_no TEXT,
  qualification TEXT,
  category TEXT,
  first_issue_date DATE,
  issue_date DATE,
  expiry_date DATE
);

-- Validity ranges built from successive snapshots
CREATE TABLE IF NOT EXISTS ver_assignments (
  assign_id BIGINT PRIMARY KEY,
  person_id BIGINT NOT NULL,
  rec_key TEXT NOT NULL,
  license_no TEXT,
  qualification TEXT,
  category TEXT,
  first_issue_date DATE,
  issue_date DATE,
  expiry_date DATE,
  valid_from DATE NOT NULL,
  valid_to DATE
);
CREATE SEQUENCE IF NOT EXISTS ver_assignments_seq;

-- DuckDB does not support partial indexes; create general indexes instead
CREATE INDEX IF NOT EXISTS idx_ver_assign_rec ON ver_assignments(rec_key);
CREATE INDEX IF NOT EXISTS idx_ver_assign_asof ON ver_assignments(valid_from, valid_to);
"""


@dataclass
class SnapshotMeta:
    snapshot_id: int
    snapshot_date: pd.Timestamp
    row_count: int
    content_hash: str


CANON_COLS = [
    "name",
    "license_no",
    "qualification",
    "category",
    "first_issue_date",
    "issue_date",
    "expiry_date",
]


def _normalize_snapshot_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Collapse duplicate-named columns by coalescing left-to-right
    if out.columns.duplicated().any():
        dup_names = [n for n, c in out.columns.value_counts().items() if c > 1]
        for name in dup_names:
            cols = [i for i, c in enumerate(out.columns) if c == name]
            base = out.iloc[:, cols].bfill(axis=1).iloc[:, 0]
            out[name] = base
        out = out.loc[:, ~out.columns.duplicated()]
    # Keep only known columns; others are preserved only in snapshot_records for audit if needed later
    keep = [c for c in CANON_COLS if c in out.columns]
    out = out[keep]
    # Normalize dates to pandas datetime (date)
    for c in ("first_issue_date", "issue_date", "expiry_date"):
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce").dt.date
    # Coerce strings
    for c in ("name", "license_no", "qualification", "category"):
        if c in out.columns:
            out[c] = out[c].map(
                lambda x: None if (pd.isna(x) or str(x).strip() == "") else str(x).strip()
            )
    # Drop fully empty rows (no name and no license_no and no qualification)
    core = [c for c in ("name", "license_no", "qualification") if c in out.columns]
    if core:
        mask = out[core].isna().all(axis=1)
        out = out[~mask].reset_index(drop=True)
    return out


def _record_key(row: pd.Series) -> str:
    nkey = name_key(str(row.get("name", "") or ""))
    lic = str(row.get("license_no", "") or "").strip().lower()
    q = str(row.get("qualification", "") or "").strip().lower()
    return f"{nkey}|{lic}|{q}"


def _content_hash(df: pd.DataFrame) -> str:
    # Stable hash of normalized content
    # Sort by key and include only canonical columns
    tmp = df.copy()
    tmp["_rec_key"] = tmp.apply(_record_key, axis=1)
    cols = ["_rec_key"] + [c for c in CANON_COLS if c in tmp.columns]
    tmp = tmp[cols].sort_values("_rec_key", kind="stable").reset_index(drop=True)
    blob = tmp.to_csv(index=False).encode("utf-8")
    return sha256(blob).hexdigest()


def _ensure_schema(con) -> None:
    con.execute(DDL)


def read_snapshot_xls(
    path: Path, sheet: Optional[str | int] = None
) -> tuple[pd.DataFrame, Optional[int]]:
    # Reuse robust detection from io_excel
    if sheet is None:
        # Read first sheet name
        df0, hdr = read_sheet(path, 0)
        df = df0
        header_row = hdr
    else:
        df, hdr = read_sheet(path, sheet)
        header_row = hdr
    df = to_canonical(df)
    return _normalize_snapshot_df(df), header_row


def _connect_duckdb(path: Path):
    import duckdb  # type: ignore

    return duckdb.connect(str(path))


def ingest_snapshot_df(
    df: pd.DataFrame,
    *,
    duckdb_path: Path,
    snapshot_date: Optional[str | pd.Timestamp] = None,
    source_path: Optional[Path] = None,
) -> SnapshotMeta:
    df_norm = _normalize_snapshot_df(df)
    ts = pd.to_datetime(snapshot_date) if snapshot_date is not None else pd.Timestamp.utcnow()
    con = _connect_duckdb(duckdb_path)
    try:
        _ensure_schema(con)
        meta = _insert_snapshot_meta(
            con,
            snapshot_date=ts,
            source_path=(source_path or Path("<csv-upload>")),
            sheet=None,
            header_row=None,
            df_norm=df_norm,
        )
        _write_snapshot_records(con, meta.snapshot_id, df_norm)

        # Prepare rec_keys and ensure persons
        df_norm["_rec_key"] = df_norm.apply(_record_key, axis=1)
        for nm in sorted(set(df_norm.get("name", pd.Series(dtype=str)).dropna().astype(str))):
            if nm.strip():
                _get_or_create_person(con, nm)

        current_keys: set[str] = set(df_norm["_rec_key"].astype(str).tolist())
        open_map = _open_assignments_for(con, current_keys)

        new_rows = []
        for _, r in df_norm.iterrows():
            rk = r["_rec_key"]
            if rk in open_map:
                aid = open_map[rk]
                con.execute(
                    "UPDATE ver_assignments SET license_no = ?, qualification = ?, category = ?, first_issue_date = ?, issue_date = ?, expiry_date = ? WHERE assign_id = ?",
                    [
                        r.get("license_no"),
                        r.get("qualification"),
                        r.get("category"),
                        r.get("first_issue_date"),
                        r.get("issue_date"),
                        r.get("expiry_date"),
                        aid,
                    ],
                )
                continue
            nm = str(r.get("name") or "")
            if not nm.strip():
                continue
            pid = _get_or_create_person(con, nm)
            new_rows.append(
                [
                    int(con.execute("SELECT nextval('ver_assignments_seq')").fetchone()[0]),
                    pid,
                    rk,
                    r.get("license_no"),
                    r.get("qualification"),
                    r.get("category"),
                    r.get("first_issue_date"),
                    r.get("issue_date"),
                    r.get("expiry_date"),
                    ts.date(),
                    None,
                ]
            )
        if new_rows:
            con.executemany(
                "INSERT INTO ver_assignments(assign_id, person_id, rec_key, license_no, qualification, category, first_issue_date, issue_date, expiry_date, valid_from, valid_to) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                new_rows,
            )
        _close_missing_assignments(con, meta.snapshot_id, ts, current_keys)
        return meta
    finally:
        con.close()


def _get_or_create_person(con, name: str) -> int:
    nk = name_key(name)
    row = con.execute("SELECT person_id FROM ver_persons WHERE name_key = ?", [nk]).fetchone()
    if row:
        return int(row[0])
    pid = int(con.execute("SELECT nextval('ver_persons_seq')").fetchone()[0])
    con.execute(
        "INSERT INTO ver_persons(person_id, name, name_key) VALUES (?, ?, ?)", [pid, name, nk]
    )
    return pid


def _insert_snapshot_meta(
    con,
    *,
    snapshot_date: pd.Timestamp,
    source_path: Path,
    sheet: Optional[str],
    header_row: Optional[int],
    df_norm: pd.DataFrame,
) -> SnapshotMeta:
    ch = _content_hash(df_norm)
    sid = int(con.execute("SELECT nextval('ver_snapshots_seq')").fetchone()[0])
    con.execute(
        "INSERT INTO ver_snapshots(snapshot_id, snapshot_date, source_path, sheet, header_row, content_hash, row_count) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            sid,
            pd.to_datetime(snapshot_date).date(),
            str(source_path),
            (str(sheet) if sheet is not None else None),
            (int(header_row) if header_row is not None else None),
            ch,
            int(len(df_norm)),
        ],
    )
    return SnapshotMeta(
        snapshot_id=sid,
        snapshot_date=pd.to_datetime(snapshot_date),
        row_count=int(len(df_norm)),
        content_hash=ch,
    )


def _write_snapshot_records(con, sid: int, df: pd.DataFrame) -> None:
    rows = []
    for _, r in df.iterrows():
        rows.append(
            [
                sid,
                _record_key(r),
                r.get("name"),
                r.get("license_no"),
                r.get("qualification"),
                r.get("category"),
                r.get("first_issue_date"),
                r.get("issue_date"),
                r.get("expiry_date"),
            ]
        )
    con.executemany(
        "INSERT INTO ver_snapshot_records(snapshot_id, rec_key, name, license_no, qualification, category, first_issue_date, issue_date, expiry_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def _open_assignments_for(con, rec_keys: Iterable[str]) -> dict[str, int]:
    # Return map rec_key -> assign_id for currently-open intervals
    keys = list({*rec_keys})
    if not keys:
        return {}
    ph = ",".join(["?"] * len(keys))
    rows = con.execute(
        f"SELECT rec_key, assign_id FROM ver_assignments WHERE valid_to IS NULL AND rec_key IN ({ph})",
        keys,
    ).fetchall()
    return {str(k): int(aid) for (k, aid) in rows}


def _close_missing_assignments(
    con, sid: int, snapshot_date: pd.Timestamp, current_keys: set[str]
) -> int:
    # Close any open assignments whose rec_key is not present in current snapshot
    open_rows = con.execute(
        "SELECT rec_key, assign_id FROM ver_assignments WHERE valid_to IS NULL"
    ).fetchall()
    to_close = [int(aid) for (rk, aid) in open_rows if str(rk) not in current_keys]
    if not to_close:
        return 0
    ph = ",".join(["?"] * len(to_close))
    # Close as of the day before snapshot_date
    # Close the day BEFORE the new snapshot takes effect
    dt = (pd.to_datetime(snapshot_date) - pd.Timedelta(days=1)).date()
    con.execute(
        f"UPDATE ver_assignments SET valid_to = ? WHERE assign_id IN ({ph})", [dt, *to_close]
    )
    return len(to_close)


def ingest_snapshot(
    xls_path: Path,
    *,
    duckdb_path: Path,
    snapshot_date: Optional[str | pd.Timestamp] = None,
    sheet: Optional[str | int] = None,
) -> SnapshotMeta:
    df_raw, header_row = read_snapshot_xls(xls_path, sheet)
    df = df_raw.copy()
    # Default snapshot_date: file mtime
    if snapshot_date is None:
        ts = pd.Timestamp(Path(xls_path).stat().st_mtime, unit="s")
    else:
        ts = pd.to_datetime(snapshot_date)

    con = _connect_duckdb(duckdb_path)
    try:
        _ensure_schema(con)
        meta = _insert_snapshot_meta(
            con,
            snapshot_date=ts,
            source_path=xls_path,
            sheet=(str(sheet) if sheet is not None else None),
            header_row=header_row,
            df_norm=df,
        )
        _write_snapshot_records(con, meta.snapshot_id, df)

        # Prepare rec_keys and ensure persons
        df["_rec_key"] = df.apply(_record_key, axis=1)
        # Minimal name backfill when missing
        df["_name_eff"] = df["name"].fillna("")
        # Upsert persons referenced in this snapshot
        for nm in sorted(set(df["_name_eff"].dropna().astype(str))):
            if nm.strip():
                _get_or_create_person(con, nm)

        # Open: rec_keys present now
        current_keys: set[str] = set(df["_rec_key"].astype(str).tolist())
        open_map = _open_assignments_for(con, current_keys)

        # Create new assignments for keys that are not open
        new_rows = []
        for _, r in df.iterrows():
            rk = r["_rec_key"]
            if rk in open_map:
                # Update metadata in-place to reflect any changes in the latest snapshot
                aid = open_map[rk]
                con.execute(
                    "UPDATE ver_assignments SET license_no = ?, qualification = ?, category = ?, first_issue_date = ?, issue_date = ?, expiry_date = ? WHERE assign_id = ?",
                    [
                        r.get("license_no"),
                        r.get("qualification"),
                        r.get("category"),
                        r.get("first_issue_date"),
                        r.get("issue_date"),
                        r.get("expiry_date"),
                        aid,
                    ],
                )
                continue
            nm = str(r.get("name") or "")
            if nm.strip():
                pid = _get_or_create_person(con, nm)
            else:
                # Anonymous row (should be rare); skip interval tracking but keep in snapshot audit
                continue
            new_rows.append(
                [
                    int(con.execute("SELECT nextval('ver_assignments_seq')").fetchone()[0]),
                    pid,
                    rk,
                    r.get("license_no"),
                    r.get("qualification"),
                    r.get("category"),
                    r.get("first_issue_date"),
                    r.get("issue_date"),
                    r.get("expiry_date"),
                    ts.date(),
                    None,
                ]
            )
        if new_rows:
            con.executemany(
                "INSERT INTO ver_assignments(assign_id, person_id, rec_key, license_no, qualification, category, first_issue_date, issue_date, expiry_date, valid_from, valid_to) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                new_rows,
            )

        # Close assignments missing in this snapshot
        _close_missing_assignments(con, meta.snapshot_id, ts, current_keys)
        return meta
    finally:
        con.close()


def asof_dataframe(*, duckdb_path: Path, date: str | pd.Timestamp) -> pd.DataFrame:
    con = _connect_duckdb(duckdb_path)
    try:
        dt = pd.to_datetime(date).date()
        sql = """
        SELECT p.name,
               a.license_no,
               a.qualification,
               a.category,
               a.first_issue_date,
               a.issue_date,
               a.expiry_date,
               a.valid_from,
               a.valid_to
          FROM ver_assignments a
          JOIN ver_persons p ON p.person_id = a.person_id
         WHERE a.valid_from <= ? AND (a.valid_to IS NULL OR a.valid_to >= ?)
         ORDER BY p.name, a.qualification, a.license_no
        """
        return con.execute(sql, [dt, dt]).df()
    finally:
        con.close()


def export_asof_report(
    *,
    duckdb_path: Path,
    date: str | pd.Timestamp,
    out_path: Path,
    format: str = "xlsx",
) -> Path:
    df = asof_dataframe(duckdb_path=duckdb_path, date=date)
    # Render with Japanese-like headers close to current ledger
    colmap = {
        "name": "氏名",
        "license_no": "登録番号",
        "qualification": "資格",
        "category": "区分",
        "first_issue_date": "初回交付",
        "issue_date": "交付日",
        "expiry_date": "有効期限",
        "valid_from": "有効自",
        "valid_to": "有効至",
    }
    out_df = df.rename(columns=colmap)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if format.lower() == "xlsx":
        with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
            out_df.to_excel(xw, index=False, sheet_name="資格一覧")
        return out_path
    else:
        out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        return out_path


# ------------ Convenience entry points for CLI wiring ------------


def cli_snapshot(xls: str, *, duckdb: str, date: Optional[str], sheet: Optional[str | int]) -> int:
    meta = ingest_snapshot(Path(xls), duckdb_path=Path(duckdb), snapshot_date=date, sheet=sheet)
    print(
        f"snapshot_id={meta.snapshot_id} date={meta.snapshot_date.date()} rows={meta.row_count} hash={meta.content_hash[:12]}…"
    )
    return 0


def cli_asof(date: str, *, duckdb: str, out: Optional[str], fmt: str = "xlsx") -> int:
    if out:
        outp = Path(out)
        export_asof_report(duckdb_path=Path(duckdb), date=date, out_path=outp, format=fmt)
        print(f"wrote {outp}")
        return 0
    else:
        df = asof_dataframe(duckdb_path=Path(duckdb), date=date)
        # Minimal pretty print
        if df.empty:
            print("No rows.")
        else:
            print(df.to_string(index=False))
        return 0


def cli_diff(date_from: str, date_to: str, *, duckdb: str) -> int:
    con = _connect_duckdb(Path(duckdb))
    try:
        # Pick snapshots nearest to or on the provided dates
        s_from = con.execute(
            "SELECT snapshot_id FROM ver_snapshots WHERE snapshot_date <= ? ORDER BY snapshot_date DESC LIMIT 1",
            [pd.to_datetime(date_from).date()],
        ).fetchone()
        s_to = con.execute(
            "SELECT snapshot_id FROM ver_snapshots WHERE snapshot_date <= ? ORDER BY snapshot_date DESC LIMIT 1",
            [pd.to_datetime(date_to).date()],
        ).fetchone()
        if not s_from or not s_to:
            print("Snapshots for the specified dates not found.")
            return 2
        sid_from, sid_to = int(s_from[0]), int(s_to[0])
        a = con.execute(
            "SELECT rec_key FROM ver_snapshot_records WHERE snapshot_id = ?", [sid_from]
        ).df()
        b = con.execute(
            "SELECT rec_key FROM ver_snapshot_records WHERE snapshot_id = ?", [sid_to]
        ).df()
        set_a = set(a["rec_key"].astype(str))
        set_b = set(b["rec_key"].astype(str))
        added = sorted(set_b - set_a)
        removed = sorted(set_a - set_b)
        if not added and not removed:
            print("No differences.")
            return 0
        if added:
            print("[ADDED]")
            for k in added:
                print(k)
        if removed:
            print("\n[REMOVED]")
            for k in removed:
                print(k)
        return 0
    finally:
        con.close()
