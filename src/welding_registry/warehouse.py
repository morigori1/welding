from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha1
import getpass
import json
import uuid
from pathlib import Path
from typing import Iterable, Optional, Sequence

import pandas as pd

from .normalize import license_key as _license_key_normalized, name_key as _name_key

try:  # optional import until a caller actually uses DuckDB helpers
    import duckdb  # type: ignore
except Exception:  # pragma: no cover - lazily imported in helpers
    duckdb = None  # type: ignore


def _as_path(path: Path | str) -> Path:
    return Path(path).expanduser()


def _connect(db_path: Path):
    if duckdb is None:  # pragma: no cover - defer expensive import to runtime
        import duckdb as _duckdb  # type: ignore

        globals()['duckdb'] = _duckdb
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))  # type: ignore[return-value]


def ensure_issue_schema(db_path: Path | str) -> None:
    path = _as_path(db_path)
    with _connect(path) as con:
        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS issue_person_filter (
                person_key VARCHAR PRIMARY KEY,
                include BOOLEAN NOT NULL DEFAULT TRUE,
                notes VARCHAR,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS issue_license_filter (
                license_key VARCHAR PRIMARY KEY,
                person_key VARCHAR NOT NULL,
                include BOOLEAN NOT NULL DEFAULT TRUE,
                notes VARCHAR,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS issue_runs (
                run_id VARCHAR PRIMARY KEY,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR,
                comment VARCHAR,
                row_count INTEGER NOT NULL DEFAULT 0,
                due_version VARCHAR,
                filters_version VARCHAR
            )
            '''
        )
        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS issue_run_items (
                run_id VARCHAR NOT NULL,
                row_index INTEGER NOT NULL,
                person_key VARCHAR,
                license_key VARCHAR,
                payload JSON,
                PRIMARY KEY (run_id, row_index)
            )
            '''
        )
        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS issue_filters_audit (
                change_id VARCHAR PRIMARY KEY,
                changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                actor VARCHAR,
                person_key VARCHAR,
                license_key VARCHAR,
                include BOOLEAN,
                notes VARCHAR
            )
            '''
        )


def _table_exists(con, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?", [name]
    ).fetchone()
    return bool(row)


def _fetch_table(con, name: str) -> pd.DataFrame:
    if not _table_exists(con, name):
        return pd.DataFrame()
    return con.execute(f"SELECT * FROM {name}").df()


def _clean_token(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"nan", "none", "null"}:
        return ""
    return text


def _person_key(row: pd.Series) -> str:
    emp = _clean_token(row.get('employee_id'))
    if emp:
        return f"emp:{emp}"
    name = _clean_token(row.get('name'))
    if name:
        return f"name:{_name_key(name)}"
    lic = _clean_token(row.get('license_no'))
    if lic:
        return f"lic:{_license_key_normalized(lic)}"
    qual = _clean_token(row.get('qualification'))
    expiry = _clean_token(row.get('expiry_date'))
    basis = f"{qual}|{expiry}"
    digest = sha1(basis.encode('utf-8', 'ignore')).hexdigest()[:16]
    return f"anon:{digest}"


def _license_key(row: pd.Series, person_key: str) -> str:
    lic = _clean_token(row.get('license_no'))
    if lic:
        return f"lic:{_license_key_normalized(lic)}"
    qual = _clean_token(row.get('qualification'))
    expiry = _clean_token(row.get('expiry_date'))
    issue = _clean_token(row.get('issue_date'))
    basis = f"{person_key}|{qual}|{expiry}|{issue}"
    digest = sha1(basis.encode('utf-8', 'ignore')).hexdigest()[:16]
    return f"derived:{digest}"


def attach_identity_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=['person_key', 'license_key'])
    if df.empty:
        df = df.copy()
        df['person_key'] = pd.Series(dtype='string')
        df['license_key'] = pd.Series(dtype='string')
        return df
    df2 = df.copy()
    persons: list[str] = []
    licenses: list[str] = []
    for _, row in df2.iterrows():
        pk = _person_key(row)
        lk = _license_key(row, pk)
        persons.append(pk)
        licenses.append(lk)
    df2['person_key'] = pd.Series(persons, dtype='string')
    df2['license_key'] = pd.Series(licenses, dtype='string')
    return df2


def _write_table(con, name: str, df: pd.DataFrame) -> None:
    con.execute(f"DROP TABLE IF EXISTS {name}")
    con.register('_tmp_df', df)
    try:
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM _tmp_df")
    finally:
        con.unregister('_tmp_df')


def _ensure_person_filters(con, keys: Sequence[str]) -> None:
    for key in keys:
        if not key:
            continue
        exists = con.execute(
            "SELECT 1 FROM issue_person_filter WHERE person_key = ?", [key]
        ).fetchone()
        if exists:
            continue
        con.execute(
            "INSERT INTO issue_person_filter (person_key, include, notes, updated_at) VALUES (?, TRUE, NULL, CURRENT_TIMESTAMP)",
            [key],
        )


def _ensure_license_filters(con, pairs: Iterable[tuple[str, str]]) -> None:
    for license_key, person_key in pairs:
        if not license_key:
            continue
        row = con.execute(
            "SELECT person_key FROM issue_license_filter WHERE license_key = ?",
            [license_key],
        ).fetchone()
        if row is None:
            con.execute(
                "INSERT INTO issue_license_filter (license_key, person_key, include, notes, updated_at) VALUES (?, ?, TRUE, NULL, CURRENT_TIMESTAMP)",
                [license_key, person_key],
            )
        elif person_key and row[0] != person_key:
            con.execute(
                "UPDATE issue_license_filter SET person_key = ?, updated_at = CURRENT_TIMESTAMP WHERE license_key = ?",
                [person_key, license_key],
            )


def _seed_filters(con, df: pd.DataFrame) -> None:
    if df.empty:
        return
    persons = tuple(dict.fromkeys(df['person_key'].dropna().tolist()))
    _ensure_person_filters(con, persons)
    pairs = []
    for lk, pk in zip(df['license_key'].tolist(), df['person_key'].tolist()):
        pairs.append((lk, pk or ''))
    _ensure_license_filters(con, pairs)


def materialize_roster_all(db_path: Path | str) -> pd.DataFrame:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        base = _fetch_table(con, 'roster')
        manual = _fetch_table(con, 'roster_manual')
        frames: list[pd.DataFrame] = []
        if not base.empty:
            df = base.copy()
            df['source'] = 'ingest'
            frames.append(df)
        if not manual.empty:
            df = manual.copy()
            df['source'] = 'manual'
            frames.append(df)
        if not frames:
            con.execute('DROP TABLE IF EXISTS roster_all')
            return pd.DataFrame()
        combined = pd.concat(frames, ignore_index=True, sort=False)
        combined = attach_identity_columns(combined)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if 'created' in combined.columns:
            created = pd.to_datetime(combined['created'], errors='coerce')
        else:
            created = pd.Series(pd.NaT, index=combined.index)
        combined['last_updated'] = created.fillna(now)
        priority = combined['source'].map({'manual': 0, 'ingest': 1}).fillna(5)
        combined['_priority'] = priority
        combined = combined.sort_values(
            by=['license_key', '_priority', 'last_updated'],
            ascending=[True, True, False],
            kind='stable',
        )
        deduped = combined.drop_duplicates(subset=['license_key'], keep='first')
        deduped = deduped.drop(columns=['_priority'])
        deduped = deduped.reset_index(drop=True)
        _seed_filters(con, deduped)
        _write_table(con, 'roster_all', deduped)
        return deduped


def write_due_tables(db_path: Path | str, due_raw: pd.DataFrame) -> pd.DataFrame:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    due_enriched = attach_identity_columns(due_raw)
    with _connect(path) as con:
        _seed_filters(con, due_enriched)
        _write_table(con, 'due_raw', due_enriched)
        filtered = con.execute(
            '''
            SELECT d.*
            FROM due_raw d
            LEFT JOIN issue_person_filter pf ON d.person_key = pf.person_key
            LEFT JOIN issue_license_filter lf ON d.license_key = lf.license_key
            WHERE COALESCE(pf.include, TRUE) AND COALESCE(lf.include, TRUE)
            ORDER BY expiry_date
            '''
        ).df()
        _write_table(con, 'due', filtered)
        return filtered


def set_person_filter(db_path: Path | str, person_key: str, include: bool, notes: str | None = None) -> None:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        _ensure_person_filters(con, [person_key])
        assignments = ["include = ?"]
        params: list[object] = [bool(include)]
        if notes is not None:
            assignments.append("notes = ?")
            params.append(notes)
        assignments.append("updated_at = CURRENT_TIMESTAMP")
        params.append(person_key)
        sql = "UPDATE issue_person_filter SET " + ", ".join(assignments) + " WHERE person_key = ?"
        con.execute(sql, params)


def set_license_filter(
    db_path: Path | str,
    license_key: str,
    include: bool,
    *,
    person_key: str | None = None,
    notes: str | None = None,
) -> None:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        pair = (license_key, person_key or "")
        _ensure_license_filters(con, [pair])
        assignments = ["include = ?"]
        params: list[object] = [bool(include)]
        if notes is not None:
            assignments.append("notes = ?")
            params.append(notes)
        if person_key is not None:
            assignments.append("person_key = ?")
            params.append(person_key)
        assignments.append("updated_at = CURRENT_TIMESTAMP")
        params.append(license_key)
        sql = "UPDATE issue_license_filter SET " + ", ".join(assignments) + " WHERE license_key = ?"
        con.execute(sql, params)


def reapply_due_filters(db_path: Path | str) -> pd.DataFrame:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        if not _table_exists(con, 'due_raw'):
            return pd.DataFrame()
        due_raw = con.execute("SELECT * FROM due_raw").df()
    return write_due_tables(path, due_raw)


def record_issue_run(
    db_path: Path | str,
    data: pd.DataFrame,
    *,
    comment: str | None = None,
    created_by: str | None = None,
) -> str:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    run_id = uuid.uuid4().hex
    creator = created_by or getpass.getuser()
    needs_keys = 'person_key' not in data.columns or 'license_key' not in data.columns
    payload = attach_identity_columns(data) if needs_keys else data.copy()
    with _connect(path) as con:
        row_count = len(payload)
        con.execute(
            "INSERT INTO issue_runs (run_id, created_at, created_by, comment, row_count, due_version, filters_version) VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, NULL, NULL)",
            [run_id, creator, comment, row_count],
        )
        if row_count:
            records: list[dict[str, object]] = []
            payload = payload.reset_index(drop=True)
            for idx, row in payload.iterrows():
                snap = row.to_dict()
                records.append(
                    {
                        'run_id': run_id,
                        'row_index': int(idx),
                        'person_key': str(row.get('person_key') or ''),
                        'license_key': str(row.get('license_key') or ''),
                        'payload': json.dumps(snap, ensure_ascii=False, default=str),
                    }
                )
            df_items = pd.DataFrame.from_records(records)
            con.register('_issue_items', df_items)
            try:
                con.execute(
                    "INSERT INTO issue_run_items SELECT run_id, row_index, person_key, license_key, CAST(payload AS JSON) FROM _issue_items"
                )
            finally:
                con.unregister('_issue_items')
    return run_id


def load_issue_runs(db_path: Path | str) -> pd.DataFrame:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        if not _table_exists(con, 'issue_runs'):
            return pd.DataFrame()
        return con.execute(
            "SELECT run_id, created_at, created_by, comment, row_count FROM issue_runs ORDER BY created_at DESC"
        ).df()


def load_issue_run_items(db_path: Path | str, run_id: str) -> pd.DataFrame:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        if not _table_exists(con, 'issue_run_items'):
            return pd.DataFrame()
        df = con.execute(
            "SELECT row_index, person_key, license_key, payload FROM issue_run_items WHERE run_id = ? ORDER BY row_index",
            [run_id],
        ).df()
    if df.empty:
        return df
    payloads = []
    for value in df['payload']:
        if isinstance(value, str):
            try:
                payloads.append(json.loads(value))
            except json.JSONDecodeError:
                payloads.append({})
        elif value is None:
            payloads.append({})
        else:
            payloads.append(value)
    detail = pd.json_normalize(payloads) if payloads else pd.DataFrame()
    base = df[['row_index', 'person_key', 'license_key']].reset_index(drop=True)
    if detail.empty:
        return base
    detail = detail.fillna('')
    return pd.concat([base, detail.reset_index(drop=True)], axis=1)


__all__ = [
    'attach_identity_columns',
    'ensure_issue_schema',
    'materialize_roster_all',
    'reapply_due_filters',
    'record_issue_run',
    'load_issue_runs',
    'load_issue_run_items',
    'set_license_filter',
    'set_person_filter',
    'write_due_tables',
]


