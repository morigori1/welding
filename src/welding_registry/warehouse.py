from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha1
import getpass
import json
import math
import re
import os
import uuid
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from .normalize import license_key as _license_key_normalized, name_key as _name_key

try:  # optional import until a caller actually uses DuckDB helpers
    import duckdb  # type: ignore
except Exception:  # pragma: no cover - lazily imported in helpers
    duckdb = None  # type: ignore

DEFAULT_SHEET = "default"

def _unique_strings(values) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        label = "" if value is None else str(value)
        if label not in seen:
            seen.add(label)
            out.append(label)
    return out

ALL_SHEETS_LABEL = "(全体)"


def _as_path(path: Path | str | bytes) -> Path:
    if isinstance(path, bytes):
        path = os.fsdecode(path)
    return Path(path).expanduser()


def _connect(db_path: Path):
    if duckdb is None:  # pragma: no cover - defer expensive import to runtime
        import duckdb as _duckdb  # type: ignore

        globals()["duckdb"] = _duckdb
    db_path.parent.mkdir(parents=True, exist_ok=True)
    path_str = os.fspath(db_path)
    if isinstance(path_str, bytes):
        path_str = os.fsdecode(path_str)
    return duckdb.connect(path_str)  # type: ignore[return-value]


def ensure_issue_schema(db_path: Path | str) -> None:
    path = _as_path(db_path)
    with _connect(path) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS issue_person_filter (
                person_key VARCHAR PRIMARY KEY,
                include BOOLEAN NOT NULL DEFAULT TRUE,
                notes VARCHAR,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS issue_license_filter (
                license_key VARCHAR PRIMARY KEY,
                person_key VARCHAR NOT NULL,
                include BOOLEAN NOT NULL DEFAULT TRUE,
                notes VARCHAR,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS issue_sheet_filter (
                print_sheet VARCHAR PRIMARY KEY,
                include BOOLEAN NOT NULL DEFAULT TRUE,
                notes VARCHAR,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS issue_sheet_membership (
                license_key VARCHAR NOT NULL,
                person_key VARCHAR,
                print_sheet VARCHAR NOT NULL,
                include BOOLEAN NOT NULL DEFAULT TRUE,
                notes VARCHAR,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (license_key, print_sheet)
            )
            """
        )
        def _ensure_column_types(table: str, columns: dict[str, tuple[str, ...]]) -> None:
            try:
                info = con.execute(f"PRAGMA table_info('{table}')").fetchall()
            except Exception:
                return
            if not info:
                return
            current = {row[1]: (row[2] or '').upper() for row in info}
            for column, accepted in columns.items():
                dtype = current.get(column)
                if dtype and dtype not in accepted:
                    try:
                        con.execute(
                            f"ALTER TABLE {table} ALTER COLUMN {column} SET DATA TYPE VARCHAR"
                        )
                    except Exception:
                        pass

        _ensure_column_types('due_raw', {'name': ('VARCHAR', 'TEXT'), 'display_name': ('VARCHAR', 'TEXT'), 'employee_id': ('VARCHAR', 'TEXT')})
        _ensure_column_types('due', {'name': ('VARCHAR', 'TEXT'), 'display_name': ('VARCHAR', 'TEXT'), 'employee_id': ('VARCHAR', 'TEXT')})

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS issue_runs (
                run_id VARCHAR PRIMARY KEY,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR,
                comment VARCHAR,
                row_count INTEGER NOT NULL DEFAULT 0,
                due_version VARCHAR,
                filters_version VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS issue_run_items (
                run_id VARCHAR NOT NULL,
                row_index INTEGER NOT NULL,
                person_key VARCHAR,
                license_key VARCHAR,
                payload JSON,
                PRIMARY KEY (run_id, row_index)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS issue_filters_audit (
                change_id VARCHAR PRIMARY KEY,
                changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                actor VARCHAR,
                person_key VARCHAR,
                license_key VARCHAR,
                include BOOLEAN,
                notes VARCHAR
            )
            """
        )


        con.execute(
            """
            CREATE TABLE IF NOT EXISTS roster_person_override (
                person_key VARCHAR PRIMARY KEY,
                display_name VARCHAR,
                employee_id VARCHAR,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def _table_exists(con, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()
    return bool(row)


def _fetch_table(con, name: str) -> pd.DataFrame:
    if not _table_exists(con, name):
        return pd.DataFrame()
    return con.execute(f"SELECT * FROM {name}").df()


def _pragma_table_info(con, name: str) -> pd.DataFrame:
    if not _table_exists(con, name):  # pragma: no cover - convenience guard
        return pd.DataFrame()
    return con.execute(f"PRAGMA table_info('{name}')").df()


def _ensure_column(con, table: str, column: str, col_type: str) -> None:
    info = _pragma_table_info(con, table)
    if info.empty:
        return
    cols = {str(row["name"]) for _, row in info.iterrows()}
    if column not in cols:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


def _ensure_roster_manual(con) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS roster_manual (
            name VARCHAR,
            license_no VARCHAR,
            qualification VARCHAR,
            first_issue_date DATE,
            issue_date DATE,
            expiry_date DATE,
            print_sheet VARCHAR,
            created TIMESTAMP DEFAULT now()
        )
        """)
    _ensure_column(con, "roster_manual", "print_sheet", "VARCHAR")
    _ensure_column(con, "roster_manual", "registration_date", "DATE")
    _ensure_column(con, "roster_manual", "category", "VARCHAR")
    _ensure_column(con, "roster_manual", "continuation_status", "VARCHAR")
    _ensure_column(con, "roster_manual", "next_stage_label", "VARCHAR")
    _ensure_column(con, "roster_manual", "next_exam_period", "VARCHAR")
    _ensure_column(con, "roster_manual", "next_procedure_status", "VARCHAR")
    _ensure_column(con, "roster_manual", "employee_id", "VARCHAR")
    _ensure_column(con, "roster_manual", "birth_year_west", "VARCHAR")
    _ensure_column(con, "roster_manual", "source_sheet", "VARCHAR")
    _ensure_column(con, "roster_manual", "created", "TIMESTAMP")

def _ensure_report_table(con) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS qual_reports (
            report_id VARCHAR NOT NULL,
            license_key VARCHAR NOT NULL,
            person_key VARCHAR NOT NULL,
            note VARCHAR,
            created_at TIMESTAMP NOT NULL DEFAULT now(),
            PRIMARY KEY (report_id, license_key)
        )
        """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS report_definitions (
            report_id VARCHAR PRIMARY KEY,
            label VARCHAR,
            description VARCHAR,
            created_at TIMESTAMP NOT NULL DEFAULT now()
        )
        """)


def _coerce_optional_date(value):
    if value is None:
        return None
    if isinstance(value, str):
        if not value.strip():
            return None
        candidate = value
    else:
        candidate = value
    try:
        dt = pd.to_datetime(candidate, errors="coerce")
    except Exception:
        dt = pd.NaT
    if pd.isna(dt):
        return None
    return dt.date()


def _clean_token(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"nan", "none", "null", "na", "<na>"}:
        return ""
    return text


NAME_COLUMN_WHITELIST = {"name", "Unnamed: 8", "氏名", "氏名漢字", "氏名（漢字）", "氏名漢字"}


NAME_CHAR_PATTERN = re.compile(r"[A-Za-z぀-ヿ一-鿿]")


def _clean_name_value(value) -> str:
    text = _clean_token(value)
    if not text:
        return ""
    collapsed = ''.join(str(text).split())
    if not collapsed:
        return ""
    alpha_like = sum(ch.isalpha() or 0x3040 <= ord(ch) <= 0x30FF or 0x4E00 <= ord(ch) <= 0x9FFF for ch in collapsed)
    digit_count = sum(ch.isdigit() for ch in collapsed)
    if not alpha_like:
        return ""
    if digit_count and digit_count / max(len(collapsed), 1) > 0.3:
        return ""
    if all(ch in "0123456789./()-" for ch in collapsed):
        return ""
    return collapsed


def _normalize_employee_id(value) -> str:
    text = _clean_token(value)
    if not text:
        return ""
    try:
        num = float(text)
    except ValueError:
        return text
    if math.isfinite(num) and num.is_integer():
        return str(int(num))
    return text


def _normalize_license_no(value) -> str:
    text = _clean_token(value)
    if not text:
        return ""
    try:
        num = float(text)
    except ValueError:
        pass
    else:
        if math.isfinite(num) and num.is_integer():
            text = str(int(num))
    return _license_key_normalized(text)


def _load_worker_name_map(con) -> dict[str, str]:
    try:
        df = con.execute("SELECT employee_id, name FROM workers WHERE name IS NOT NULL").df()
    except Exception:
        return {}
    lookup: dict[str, str] = {}
    if df.empty:
        return lookup
    for _, row in df.iterrows():
        emp = _normalize_employee_id(row.get("employee_id"))
        name = _clean_name_value(row.get("name"))
        if emp and name and emp not in lookup:
            lookup[emp] = name
    return lookup


def _detect_name_columns(df: pd.DataFrame) -> list[str]:
    cols: list[str] = []
    for col in df.columns:
        raw = str(col).strip()
        norm = raw.replace(" ", "")
        lower = raw.lower()
        if not raw:
            continue
        if raw in NAME_COLUMN_WHITELIST or norm in NAME_COLUMN_WHITELIST or lower in NAME_COLUMN_WHITELIST:
            cols.append(col)
            continue
        if raw in {"Unnamed: 8"}:
            cols.append(col)
            continue
        series = df[col]
        if not isinstance(series, pd.Series):
            continue
        if not (pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)):
            continue
        sample = series.dropna()
        if sample.empty:
            continue
        cleaned = sample.astype(str).map(_clean_name_value)
        cleaned = cleaned[cleaned != ""]
        if cleaned.empty:
            continue
        if cleaned.nunique(dropna=True) < 20:
            continue
        digit_ratio = cleaned.str.contains(r"\d").mean()
        if digit_ratio > 0.1:
            continue
        hyphen_ratio = cleaned.str.contains(r"[-/]").mean()
        if hyphen_ratio > 0.2:
            continue
        char_ratio = cleaned.str.contains(NAME_CHAR_PATTERN).mean()
        if char_ratio < 0.7:
            continue
        cols.append(col)
    return cols


def _enrich_identity_fields(df: pd.DataFrame, con) -> pd.DataFrame:
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    frame = df.copy()
    if "employee_id" in frame.columns:
        emp_series = frame["employee_id"].astype("object").fillna("")
    else:
        emp_series = pd.Series([""] * len(frame), index=frame.index, dtype="object")
    if "Unnamed: 1" in frame.columns:
        fallback = frame["Unnamed: 1"].astype("object").fillna("")
        mask = emp_series.map(_clean_token) != ""
        emp_series = emp_series.where(mask, fallback)
    frame["employee_id"] = emp_series.map(_normalize_employee_id)

    name_columns: list[str] = []
    if "name" in frame.columns:
        name_columns.append("name")
    for col in _detect_name_columns(frame):
        if col not in name_columns:
            name_columns.append(col)

    worker_lookup = _load_worker_name_map(con)

    names = pd.Series([""] * len(frame), index=frame.index, dtype="object")
    for col in name_columns:
        series = frame[col].astype("object").fillna("").map(_clean_name_value)
        names = names.mask(names == "", series)

    if worker_lookup:
        worker_names = frame["employee_id"].map(lambda emp: worker_lookup.get(emp, "") if emp else "")
        names = names.mask(names == "", worker_names)

    license_series = frame.get("license_no")
    if license_series is not None:
        license_norm = (
            license_series.astype("string")
            .fillna("")
            .map(_normalize_license_no)
        )
    else:
        license_norm = pd.Series([""] * len(frame), index=frame.index, dtype="object")
    source_series = frame.get("source_seed")

    license_name_map: dict[str, str] = {}
    for lic, nm in zip(license_norm, names):
        if lic and nm and lic not in license_name_map:
            license_name_map[lic] = nm
    for emp, nm in zip(frame["employee_id"], names):
        if emp and nm and emp not in worker_lookup:
            worker_lookup[emp] = nm

    raw_license = frame.get("license_no")
    final_names: list[str] = []
    for idx, (nm, lic, emp) in enumerate(zip(names, license_norm, frame["employee_id"])):
        value = nm
        if not value:
            if lic and lic in license_name_map:
                value = license_name_map[lic]
            elif emp and emp in worker_lookup:
                value = worker_lookup[emp]
        if not value:
            if raw_license is not None and idx < len(raw_license):
                raw_val = _clean_token(raw_license.iloc[idx])
                if raw_val:
                    value = f"免許:{raw_val}"
        if not value and emp:
            value = f"ID:{emp}"
        if not value:
            src = source_series.iloc[idx] if source_series is not None and idx < len(source_series) else ""
            src_clean = _clean_token(src) or "unknown"
            value = f"{src_clean}:{idx}"
        final_names.append(value)

    frame["name"] = pd.Series(final_names, index=frame.index, dtype="string")
    return frame
def _person_key(row: pd.Series) -> str:
    emp = _clean_token(row.get("employee_id"))
    if emp:
        return f"emp:{emp}"
    name = _clean_token(row.get("name"))
    if name:
        return f"name:{_name_key(name)}"
    lic = _clean_token(row.get("license_no"))
    if lic:
        return f"lic:{_license_key_normalized(lic)}"
    qual = _clean_token(row.get("qualification"))
    expiry = _clean_token(row.get("expiry_date"))
    basis = f"{qual}|{expiry}"
    digest = sha1(basis.encode("utf-8", "ignore")).hexdigest()[:16]
    return f"anon:{digest}"


def _license_key(row: pd.Series, person_key: str) -> str:
    lic = _clean_token(row.get("license_no"))
    if lic:
        return f"lic:{_license_key_normalized(lic)}"
    qual = _clean_token(row.get("qualification"))
    expiry = _clean_token(row.get("expiry_date"))
    issue = _clean_token(row.get("issue_date"))
    basis = f"{person_key}|{qual}|{expiry}|{issue}"
    digest = sha1(basis.encode("utf-8", "ignore")).hexdigest()[:16]
    return f"derived:{digest}"


def _normalize_sheet(value) -> str:
    text = _clean_token(value)
    return text or DEFAULT_SHEET


def attach_identity_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=["person_key", "license_key"])
    if df.empty:
        df = df.copy()
        df["person_key"] = pd.Series(dtype="string")
        df["license_key"] = pd.Series(dtype="string")
        return df
    df2 = df.copy()
    persons: list[str] = []
    licenses: list[str] = []
    for _, row in df2.iterrows():
        pk = _person_key(row)
        lk = _license_key(row, pk)
        persons.append(pk)
        licenses.append(lk)
    df2["person_key"] = pd.Series(persons, index=df2.index, dtype="object").astype("string")
    df2["license_key"] = pd.Series(licenses, index=df2.index, dtype="object").astype("string")
    return df2


def _write_table(con, name: str, df: pd.DataFrame) -> None:
    con.execute(f"DROP TABLE IF EXISTS {name}")
    con.register("_tmp_df", df)
    try:
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM _tmp_df")
    finally:
        con.unregister("_tmp_df")


def _ensure_person_filters(con, keys: Sequence[str]) -> None:
    for key in keys:
        if not key:
            continue
        exists = con.execute(
            "SELECT 1 FROM issue_person_filter WHERE person_key = ?",
            [key],
        ).fetchone()
        if exists:
            continue
        con.execute(
            "INSERT INTO issue_person_filter (person_key, include, notes, updated_at) VALUES (?, TRUE, NULL, now())",
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
                "INSERT INTO issue_license_filter (license_key, person_key, include, notes, updated_at) VALUES (?, ?, TRUE, NULL, now())",
                [license_key, person_key],
            )
        elif person_key and row[0] != person_key:
            con.execute(
                "UPDATE issue_license_filter SET person_key = ?, updated_at = now() WHERE license_key = ?",
                [person_key, license_key],
            )


def _ensure_sheet_filters(con, sheets: Sequence[str]) -> None:
    for sheet in sheets:
        sheet = _normalize_sheet(sheet)
        row = con.execute(
            "SELECT 1 FROM issue_sheet_filter WHERE print_sheet = ?",
            [sheet],
        ).fetchone()
        if row is None:
            con.execute(
                "INSERT INTO issue_sheet_filter (print_sheet, include, notes, updated_at) VALUES (?, TRUE, NULL, now())",
                [sheet],
            )


def _ensure_sheet_membership(
    con,
    memberships: pd.DataFrame,
) -> None:
    if memberships.empty:
        return

    _ensure_column(con, "issue_sheet_membership", "person_key", "VARCHAR")
    _ensure_column(con, "issue_sheet_membership", "include", "BOOLEAN")
    _ensure_column(con, "issue_sheet_membership", "notes", "VARCHAR")
    _ensure_column(con, "issue_sheet_membership", "updated_at", "TIMESTAMP")
    con.execute("UPDATE issue_sheet_membership SET include = TRUE WHERE include IS NULL")
    con.execute("UPDATE issue_sheet_membership SET updated_at = now() WHERE updated_at IS NULL")

    df = memberships.copy()
    df["print_sheet"] = df["print_sheet"].map(_normalize_sheet)
    df["include"] = True
    df["notes"] = None
    df["updated_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    con.register("_sheet_membership_seed", df)
    try:
        con.execute(
            """
            INSERT INTO issue_sheet_membership (license_key, person_key, print_sheet, include, notes, updated_at)
            SELECT license_key, person_key, print_sheet, include, notes, updated_at
            FROM _sheet_membership_seed
            ON CONFLICT (license_key, print_sheet) DO NOTHING
            """
        )
    finally:
        con.unregister("_sheet_membership_seed")


def _seed_filters(con, df: pd.DataFrame) -> None:
    if df.empty:
        return
    persons = tuple(_unique_strings(df["person_key"].dropna().tolist()))
    _ensure_person_filters(con, persons)
    license_pairs: list[tuple[str, str]] = []
    for lk, pk in zip(df["license_key"], df["person_key"]):
        if pd.isna(lk):
            continue
        license_key = str(lk).strip()
        if not license_key:
            continue
        person_key = "" if pd.isna(pk) else str(pk).strip()
        license_pairs.append((license_key, person_key))
    _ensure_license_filters(con, license_pairs)


def _seed_sheet_state(con, roster: pd.DataFrame, memberships: pd.DataFrame) -> None:
    sheets = memberships["print_sheet"].dropna().map(_normalize_sheet).unique().tolist()
    _ensure_sheet_filters(con, sheets)
    _ensure_sheet_membership(con, memberships)


def _prepare_roster_frames(con) -> tuple[pd.DataFrame, pd.DataFrame]:
    base = _fetch_table(con, "roster")
    manual = _fetch_table(con, "roster_manual")
    if not manual.empty:
        _ensure_column(con, "roster_manual", "print_sheet", "VARCHAR")
        if "print_sheet" not in manual.columns:
            manual["print_sheet"] = DEFAULT_SHEET
    return base, manual


def materialize_roster_all(db_path: Path | str) -> pd.DataFrame:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        base, manual = _prepare_roster_frames(con)
        frames: list[pd.DataFrame] = []
        if not base.empty:
            df = _enrich_identity_fields(base.copy(), con)
            if "print_sheet" not in df.columns:
                df["print_sheet"] = DEFAULT_SHEET
            df["print_sheet"] = df["print_sheet"].map(_normalize_sheet)
            if "source_sheet" in df.columns:
                df["source_sheet"] = df["source_sheet"].astype("string").fillna("")
            else:
                df["source_sheet"] = pd.Series(["" for _ in range(len(df))], dtype="string")
            df["source"] = "ingest"
            frames.append(df)
        if not manual.empty:
            df = _enrich_identity_fields(manual.copy(), con)
            if "print_sheet" not in df.columns:
                df["print_sheet"] = DEFAULT_SHEET
            df["print_sheet"] = df["print_sheet"].map(_normalize_sheet)
            if "source_sheet" in df.columns:
                df["source_sheet"] = df["source_sheet"].astype("string").fillna("")
            else:
                df["source_sheet"] = pd.Series(["" for _ in range(len(df))], dtype="string")
            df["source"] = "manual"
            frames.append(df)
        if not frames:
            con.execute("DROP TABLE IF EXISTS roster_all")
            return pd.DataFrame()
        combined = pd.concat(frames, ignore_index=True, sort=False)
        combined = _enrich_identity_fields(combined, con)
        combined = attach_identity_columns(combined)
        if "print_sheet" not in combined.columns:
            combined["print_sheet"] = DEFAULT_SHEET
        combined["print_sheet"] = combined["print_sheet"].map(_normalize_sheet)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if "created" in combined.columns:
            created_series = pd.to_datetime(combined["created"], errors="coerce")
        else:
            created_series = pd.Series(pd.NaT, index=combined.index)
        combined["last_updated"] = created_series.fillna(now)
        combined["_registration_dt"] = pd.to_datetime(
            combined.get("registration_date"), errors="coerce"
        )
        combined["_issue_dt"] = pd.to_datetime(combined.get("issue_date"), errors="coerce")
        combined["_expiry_dt"] = pd.to_datetime(combined.get("expiry_date"), errors="coerce")
        combined["_first_issue_dt"] = pd.to_datetime(
            combined.get("first_issue_date"), errors="coerce"
        )
        combined["_effective_dt"] = combined["_registration_dt"]
        combined["_effective_dt"] = combined["_effective_dt"].fillna(combined["_issue_dt"])
        combined["_effective_dt"] = combined["_effective_dt"].fillna(combined["_expiry_dt"])
        combined["_effective_dt"] = combined["_effective_dt"].fillna(combined["_first_issue_dt"])
        combined["_effective_dt"] = combined["_effective_dt"].fillna(
            pd.to_datetime(combined["last_updated"], errors="coerce")
        )
        combined["_effective_dt"] = combined["_effective_dt"].fillna(now)
        combined["_source_rank"] = combined["source"].map({"ingest": 0, "manual": 2}).fillna(1)
        memberships = combined[["license_key", "person_key", "print_sheet"]].dropna(
            subset=["license_key"]
        )
        combined = combined.sort_values(
            by=["license_key", "_source_rank", "_effective_dt", "last_updated"],
            ascending=[True, True, False, False],
            kind="stable",
        )
        deduped = combined.drop_duplicates(subset=["license_key"], keep="first")
        manual_entries = combined[combined["source"] == "manual"].copy()
        manual_entries = manual_entries.sort_values(
            by=["license_key", "_effective_dt", "last_updated"],
            ascending=[True, False, False],
            kind="stable",
        )
        deduped = deduped.reset_index(drop=True)
        def _has_data(value: Any) -> bool:
            if value is None:
                return False
            if isinstance(value, str):
                return value.strip() != ""
            if isinstance(value, (list, tuple, set, dict)):
                return bool(value)
            if isinstance(value, pd.Timestamp):
                return not pd.isna(value)
            try:
                return not pd.isna(value)
            except TypeError:
                return True

        fallback_columns = [
            "registration_date",
            "first_issue_date",
            "issue_date",
            "expiry_date",
            "qualification",
            "category",
            "continuation_status",
            "next_stage_label",
            "next_exam_period",
            "next_procedure_status",
            "name",
            "display_name",
            "employee_id",
            "birth_year_west",
        ]
        if "license_key" in combined.columns:
            grouped = combined.groupby("license_key", sort=False)
            for column in fallback_columns:
                if column not in deduped.columns or column not in combined.columns:
                    continue

                fallback_map = grouped[column].apply(
                    lambda series: next((val for val in series if _has_data(val)), None)
                )
                fallback_values = deduped["license_key"].map(fallback_map)
                if pd.api.types.is_datetime64_any_dtype(deduped[column]):
                    fallback_values = pd.to_datetime(fallback_values, errors="coerce")
                mask = deduped[column].apply(lambda value: not _has_data(value))
                if mask.any():
                    deduped.loc[mask, column] = fallback_values.loc[mask]

        deduped["sheet_source"] = "auto"
        if not manual_entries.empty:
            manual_sheet = manual_entries[["license_key", "print_sheet"]].copy()
            if "print_sheet" in manual_sheet.columns:
                manual_sheet["print_sheet"] = manual_sheet["print_sheet"].astype("string")
                manual_sheet = manual_sheet[manual_sheet["print_sheet"].str.strip() != ""]
                if not manual_sheet.empty:
                    sheet_map = (
                        manual_sheet.drop_duplicates(subset=["license_key"], keep="first")
                        .set_index("license_key")["print_sheet"]
                    )
                    if not sheet_map.empty:
                        mask = deduped["license_key"].isin(sheet_map.index)
                        deduped.loc[mask, "print_sheet"] = deduped.loc[mask, "license_key"].map(sheet_map)
                        deduped.loc[mask, "sheet_source"] = "manual"
            if "source_sheet" in deduped.columns and "source_sheet" in manual_entries.columns:
                manual_source_sheet = manual_entries[["license_key", "source_sheet"]].copy()
                manual_source_sheet["source_sheet"] = manual_source_sheet["source_sheet"].astype(
                    "string"
                )
                manual_source_sheet = manual_source_sheet[
                    manual_source_sheet["source_sheet"].str.strip() != ""
                ]
                if not manual_source_sheet.empty:
                    source_map = (
                        manual_source_sheet.drop_duplicates(subset=["license_key"], keep="first")
                        .set_index("license_key")["source_sheet"]
                    )
                    if not source_map.empty:
                        mask = deduped["license_key"].isin(source_map.index)
                        deduped.loc[mask, "source_sheet"] = deduped.loc[mask, "license_key"].map(
                            source_map
                        )

        helper_cols = [
            "_source_rank",
            "_effective_dt",
            "_registration_dt",
            "_issue_dt",
            "_expiry_dt",
            "_first_issue_dt",
        ]
        deduped = deduped.drop(columns=[col for col in helper_cols if col in deduped.columns])

        text_columns = [
            "license_no",
            "name",
            "display_name",
            "qualification",
            "category",
            "continuation_status",
            "next_stage_label",
            "next_exam_period",
            "next_procedure_status",
            "birth_year_west",
            "print_sheet",
            "source_sheet",
            "sheet_source",
        ]
        for col in text_columns:
            if col in deduped.columns:
                deduped[col] = deduped[col].astype("string")

        if "display_name" not in deduped.columns:
            if "name" in deduped.columns:
                deduped["display_name"] = deduped["name"].astype("string")
            else:
                deduped["display_name"] = pd.Series([''] * len(deduped), dtype="string")
        else:
            deduped["display_name"] = deduped["display_name"].astype("string")
            if "name" in deduped.columns:
                name_series = deduped["name"].astype("string")
                mask = deduped["display_name"].isna() | (deduped["display_name"].str.strip() == '')
                if mask.any():
                    deduped.loc[mask, "display_name"] = name_series.loc[mask]

        if "employee_id" in deduped.columns:
            deduped["employee_id"] = deduped["employee_id"].astype("string")

        overrides_df = _load_person_override_df(con)
        deduped = _apply_person_overrides(deduped, overrides_df)
        _seed_filters(con, deduped)
        _seed_sheet_state(con, deduped, memberships)
        _write_table(con, "roster_all", deduped)
        _refresh_roster_views(con)
        return deduped



def list_qualifications(
    db_path: Path | str,
    *,
    refresh: bool = True,
    include_reports: bool = True,
    sort_by: Sequence[str] | str | None = None,
    ascending: Sequence[bool] | bool = True,
) -> pd.DataFrame:
    path = _as_path(db_path)
    if refresh:
        materialize_roster_all(path)

    with _connect(path) as con:
        roster = _fetch_table(con, "roster_all")
        if roster.empty:
            return roster

        sheet_field = "source_sheet" if "source_sheet" in roster.columns else ("print_sheet" if "print_sheet" in roster.columns else None)

        if sheet_field and sheet_field not in roster.columns:
            roster[sheet_field] = pd.Series([""] * len(roster), dtype="string")

        text_columns = [
            "name",
            "license_no",
            "qualification",
            "category",
            "continuation_status",
            "source",
            "display_name",
            "next_stage_label",
            "next_exam_period",
            "next_procedure_status",
            "employee_id",
            "birth_year_west",
        ]
        if sheet_field:
            text_columns.append(sheet_field)
        for col in text_columns:
            if col in roster.columns:
                roster[col] = roster[col].astype("string")

        date_columns = ["registration_date", "first_issue_date", "issue_date", "expiry_date"]
        for col in date_columns:
            if col in roster.columns:
                series = pd.to_datetime(roster[col], errors="coerce")
                roster[col] = series.dt.strftime("%Y-%m-%d").fillna("")

        if include_reports and _table_exists(con, "qual_reports"):
            report_df = _fetch_table(con, "qual_reports")
            if not report_df.empty:
                mapping = (
                    report_df.groupby("license_key")["report_id"]
                    .apply(lambda s: sorted({str(v) for v in s if v is not None}))
                )
                roster = roster.merge(
                    mapping.rename("report_ids"),
                    left_on="license_key",
                    right_index=True,
                    how="left",
                )
        if "report_ids" in roster.columns:
            roster["report_ids"] = roster["report_ids"].apply(
                lambda v: v if isinstance(v, list) else ([] if pd.isna(v) else [str(v)])
            )
        else:
            roster["report_ids"] = [[] for _ in range(len(roster))]

        sort_columns: list[str] = []
        if sort_by:
            if isinstance(sort_by, str):
                if sort_by in roster.columns:
                    sort_columns = [sort_by]
            else:
                sort_columns = [col for col in sort_by if col in roster.columns]

        if sort_columns:
            if isinstance(ascending, Sequence) and not isinstance(ascending, (str, bytes)):
                ascending_list = [bool(value) for value in ascending]
            else:
                ascending_list = [bool(ascending)] * len(sort_columns)
            if len(ascending_list) < len(sort_columns):
                ascending_list.extend([True] * (len(sort_columns) - len(ascending_list)))
            else:
                ascending_list = ascending_list[:len(sort_columns)]
            roster = roster.sort_values(by=sort_columns, ascending=ascending_list, kind="stable")
        else:
            sort_cols = [col for col in ("name", "license_no", "qualification") if col in roster.columns]
            if sort_cols:
                roster = roster.sort_values(sort_cols, kind="stable")
        return roster.reset_index(drop=True)



def add_manual_qualification(
    db_path: Path | str,
    *,
    name: str,
    license_no: str,
    qualification: str | None = None,
    registration_date: object | None = None,
    first_issue_date: object | None = None,
    issue_date: object | None = None,
    expiry_date: object | None = None,
    category: str | None = None,
    continuation_status: str | None = None,
    next_stage_label: str | None = None,
    next_exam_period: str | None = None,
    next_procedure_status: str | None = None,
    print_sheet: str | None = None,
    source_sheet: str | None = None,
    employee_id: str | None = None,
    birth_year_west: object | None = None,
) -> None:
    path = _as_path(db_path)
    name_clean = _clean_token(name)
    license_clean = _clean_token(license_no)
    if not name_clean:
        raise ValueError("name is required")
    if not license_clean:
        raise ValueError("license_no is required")
    sheet_value = _normalize_sheet(print_sheet) if print_sheet is not None else DEFAULT_SHEET
    qual_value = _clean_token(qualification) if qualification is not None else None
    source_value = _clean_token(source_sheet) if source_sheet is not None else None
    if source_value is None:
        source_value = sheet_value if sheet_value else None

    def _optional_text(value):
        text_value = _clean_token(value) if value is not None else ""
        return text_value if text_value else None

    registration_value = _coerce_optional_date(registration_date)
    first_issue_value = _coerce_optional_date(first_issue_date)
    issue_value = _coerce_optional_date(issue_date)
    expiry_value = _coerce_optional_date(expiry_date)
    category_value = _optional_text(category)
    continuation_value = _optional_text(continuation_status)
    next_stage_value = _optional_text(next_stage_label)
    next_exam_value = _optional_text(next_exam_period)
    next_procedure_value = _optional_text(next_procedure_status)
    employee_value = _normalize_employee_id(employee_id) if employee_id is not None else ""
    if not employee_value:
        employee_value = None
    birth_year_value = _optional_text(birth_year_west)

    record_items = [
        ("name", name_clean),
        ("license_no", license_clean),
        ("qualification", qual_value if qual_value else None),
        ("registration_date", registration_value),
        ("first_issue_date", first_issue_value),
        ("issue_date", issue_value),
        ("expiry_date", expiry_value),
        ("category", category_value),
        ("continuation_status", continuation_value),
        ("next_stage_label", next_stage_value),
        ("next_exam_period", next_exam_value),
        ("next_procedure_status", next_procedure_value),
        ("print_sheet", sheet_value),
        ("source_sheet", source_value),
        ("employee_id", employee_value),
        ("birth_year_west", birth_year_value),
    ]

    columns_clause = ", ".join(name for name, _ in record_items)
    placeholders = ", ".join(["?"] * len(record_items))
    values = [value for _, value in record_items]

    with _connect(path) as con:
        _ensure_roster_manual(con)
        con.execute(
            "DELETE FROM roster_manual WHERE license_no = ? AND name = ?",
            [license_clean, name_clean],
        )
        con.execute(
            f"""
            INSERT INTO roster_manual
                ({columns_clause}, created)
            VALUES ({placeholders}, now())
            """,
            values,
        )
    materialize_roster_all(path)



def delete_manual_qualification(
    db_path: Path | str,
    *,
    name: str,
    license_no: str,
    refresh: bool = True,
) -> None:
    path = _as_path(db_path)
    name_clean = _clean_token(name)
    license_clean = _clean_token(license_no)
    if not name_clean or not license_clean:
        raise ValueError("name and license_no are required")
    with _connect(path) as con:
        _ensure_roster_manual(con)
        con.execute(
            "DELETE FROM roster_manual WHERE license_no = ? AND name = ?",
            [license_clean, name_clean],
        )
    if refresh:
        materialize_roster_all(path)


def update_manual_qualification(
    db_path: Path | str,
    *,
    name: str,
    license_no: str,
    qualification: str | None = None,
    registration_date: object | None = None,
    first_issue_date: object | None = None,
    issue_date: object | None = None,
    expiry_date: object | None = None,
    category: str | None = None,
    continuation_status: str | None = None,
    next_stage_label: str | None = None,
    next_exam_period: str | None = None,
    next_procedure_status: str | None = None,
    print_sheet: str | None = None,
    source_sheet: str | None = None,
    employee_id: str | None = None,
    birth_year_west: object | None = None,
) -> None:
    path = _as_path(db_path)
    name_clean = _clean_token(name)
    license_clean = _clean_token(license_no)
    if not name_clean or not license_clean:
        raise ValueError("name and license_no are required")
    with _connect(path) as con:
        _ensure_roster_manual(con)
        existing = con.execute(
            """
            SELECT qualification, registration_date, first_issue_date, issue_date, expiry_date,
                   category, continuation_status, next_stage_label, next_exam_period,
                   next_procedure_status, print_sheet, source_sheet, employee_id, birth_year_west
            FROM roster_manual
            WHERE license_no = ? AND name = ?
            """,
            [license_clean, name_clean],
        ).df()
    if existing.empty:
        raise ValueError(f"Manual qualification not found for license_no={license_no} name={name}")
    row = existing.iloc[0].to_dict()
    delete_manual_qualification(path, name=name_clean, license_no=license_clean, refresh=False)
    add_manual_qualification(
        path,
        name=name_clean,
        license_no=license_clean,
        qualification=qualification if qualification is not None else row.get('qualification'),
        registration_date=registration_date if registration_date is not None else row.get('registration_date'),
        first_issue_date=first_issue_date if first_issue_date is not None else row.get('first_issue_date'),
        issue_date=issue_date if issue_date is not None else row.get('issue_date'),
        expiry_date=expiry_date if expiry_date is not None else row.get('expiry_date'),
        category=category if category is not None else row.get('category'),
        continuation_status=continuation_status if continuation_status is not None else row.get('continuation_status'),
        next_stage_label=next_stage_label if next_stage_label is not None else row.get('next_stage_label'),
        next_exam_period=next_exam_period if next_exam_period is not None else row.get('next_exam_period'),
        next_procedure_status=next_procedure_status if next_procedure_status is not None else row.get('next_procedure_status'),
        print_sheet=print_sheet if print_sheet is not None else row.get('print_sheet'),
        source_sheet=source_sheet if source_sheet is not None else row.get('source_sheet'),
        employee_id=employee_id if employee_id is not None else row.get('employee_id'),
        birth_year_west=birth_year_west if birth_year_west is not None else row.get('birth_year_west'),
    )



def list_report_definitions(db_path: Path | str) -> pd.DataFrame:
    path = _as_path(db_path)
    with _connect(path) as con:
        _ensure_report_table(con)
        df = _fetch_table(con, "report_definitions")
    if df.empty:
        return df
    for column in ("report_id", "label", "description"):
        if column in df.columns:
            df[column] = df[column].astype("string")
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    return df.sort_values(by="report_id", kind="stable").reset_index(drop=True)



def add_report_definition(
    db_path: Path | str,
    *,
    report_id: str,
    label: str | None = None,
    description: str | None = None,
) -> None:
    path = _as_path(db_path)
    report_clean = _clean_token(report_id)
    if not report_clean:
        raise ValueError("report_id is required")
    label_clean = _clean_token(label) if label is not None else ""
    label_value = label_clean if label_clean else report_clean
    description_value = None
    if description is not None:
        desc_text = str(description).strip()
        description_value = desc_text if desc_text else None
    with _connect(path) as con:
        _ensure_report_table(con)
        con.execute("""
            INSERT INTO report_definitions (report_id, label, description, created_at)
            VALUES (?, ?, ?, now())
            ON CONFLICT (report_id) DO UPDATE SET
                label = excluded.label,
                description = excluded.description
            """,
            [report_clean, label_value, description_value],
        )



def delete_report_definition(
    db_path: Path | str,
    *,
    report_id: str,
) -> None:
    path = _as_path(db_path)
    report_clean = _clean_token(report_id)
    if not report_clean:
        raise ValueError("report_id is required")
    with _connect(path) as con:
        _ensure_report_table(con)
        con.execute("DELETE FROM qual_reports WHERE report_id = ?", [report_clean])
        result = con.execute("DELETE FROM report_definitions WHERE report_id = ?", [report_clean])
        rowcount = getattr(result, "rowcount", None)
        if rowcount is not None and rowcount == 0:
            raise ValueError(f"report_id {report_id} not found")
        if rowcount is None:
            check = con.execute("SELECT 1 FROM report_definitions WHERE report_id = ?", [report_clean]).fetchone()
            if check:
                raise ValueError(f"failed to delete report_id {report_id}")



def add_report_entry(
    db_path: Path | str,
    *,
    report_id: str,
    license_no: str,
    note: str | None = None,
) -> None:
    path = _as_path(db_path)
    report_clean = _clean_token(report_id)
    license_clean = _clean_token(license_no)
    if not report_clean:
        raise ValueError("report_id is required")
    if not license_clean:
        raise ValueError("license_no is required")
    materialize_roster_all(path)
    with _connect(path) as con:
        _ensure_report_table(con)
        defined = con.execute("SELECT 1 FROM report_definitions WHERE report_id = ?", [report_clean]).fetchone()
        if defined is None:
            raise ValueError(f"report_id {report_id} is not defined")
        row = con.execute(
            """
            SELECT license_key, person_key
            FROM roster_all
            WHERE license_no = ?
            ORDER BY last_updated DESC
            LIMIT 1
            """,
            [license_clean],
        ).fetchone()
        if not row:
            raise ValueError(f"license_no {license_no} not found in roster_all")
        license_key, person_key = row
        con.execute(
            "DELETE FROM qual_reports WHERE report_id = ? AND license_key = ?",
            [report_clean, license_key],
        )
        con.execute(
            """
            INSERT INTO qual_reports (report_id, license_key, person_key, note, created_at)
            VALUES (?, ?, ?, ?, now())
            """,
            [report_clean, license_key, person_key, note],
        )


def remove_report_entry(
    db_path: Path | str,
    *,
    report_id: str,
    license_no: str,
) -> None:
    path = _as_path(db_path)
    report_clean = _clean_token(report_id)
    license_clean = _clean_token(license_no)
    if not report_clean:
        raise ValueError("report_id is required")
    if not license_clean:
        raise ValueError("license_no is required")
    license_key = f"lic:{_license_key_normalized(license_clean)}"
    with _connect(path) as con:
        _ensure_report_table(con)
        con.execute(
            "DELETE FROM qual_reports WHERE report_id = ? AND license_key = ?",
            [report_clean, license_key],
        )


def list_report_entries(db_path: Path | str, report_id: str | None = None) -> pd.DataFrame:
    path = _as_path(db_path)
    with _connect(path) as con:
        _ensure_report_table(con)
        if report_id:
            return con.execute(
                """
                SELECT qr.report_id, qr.license_key, qr.person_key, qr.note, qr.created_at,
                       rd.label AS report_label, rd.description AS report_description,
                       ra.name, ra.license_no, ra.qualification, ra.print_sheet
                FROM qual_reports qr
                LEFT JOIN roster_all ra ON qr.license_key = ra.license_key
                LEFT JOIN report_definitions rd ON qr.report_id = rd.report_id
                WHERE qr.report_id = ?
                ORDER BY qr.created_at DESC
                """,
                [report_id],
            ).df()
        return con.execute(
            """
            SELECT qr.report_id, qr.license_key, qr.person_key, qr.note, qr.created_at,
                   rd.label AS report_label, rd.description AS report_description,
                   ra.name, ra.license_no, ra.qualification, ra.print_sheet
            FROM qual_reports qr
            LEFT JOIN roster_all ra ON qr.license_key = ra.license_key
            LEFT JOIN report_definitions rd ON qr.report_id = rd.report_id
            ORDER BY qr.report_id, qr.created_at DESC
            """
        ).df()








def _refresh_roster_views(con) -> None:
    info = con.execute("PRAGMA table_info('roster_all')").fetchall()
    available = {row[1] for row in info}

    def _expr(column: str, default: str) -> str:
        return column if column in available else f"{default} AS {column}"

    people_select = ", ".join(
        [
            _expr("person_key", "NULL"),
            _expr("name", "NULL"),
            _expr("employee_id", "''"),
            _expr("birth_year_west", "''"),
        ]
    )
    con.execute(
        f"""
        CREATE OR REPLACE VIEW roster_people_current AS
        SELECT DISTINCT {people_select}
        FROM roster_all
        """
    )

    license_columns = [
        ("person_key", "NULL"),
        ("employee_id", "''"),
        ("name", "NULL"),
        ("license_key", "NULL"),
        ("license_no", "NULL"),
        ("qualification", "''"),
        ("category", "''"),
        ("continuation_status", "''"),
        ("registration_date", "NULL"),
        ("print_sheet", f"'{DEFAULT_SHEET}'"),
        ("issue_year", "NULL"),
        ("first_issue_date", "NULL"),
        ("issue_date", "NULL"),
        ("expiry_date", "NULL"),
        ("next_stage_label", "''"),
        ("next_exam_period", "''"),
        ("next_procedure_status", "''"),
        ("birth_year_west", "''"),
        ("source", "''"),
    ]
    license_select = ", ".join(_expr(col, default) for col, default in license_columns)
    con.execute(
        f"""
        CREATE OR REPLACE VIEW roster_license_current AS
        SELECT {license_select}
        FROM roster_all
        """
    )



def _normalize_due(df: pd.DataFrame) -> pd.DataFrame:
    if "print_sheet" not in df.columns:
        df["print_sheet"] = DEFAULT_SHEET
    df["print_sheet"] = df["print_sheet"].map(_normalize_sheet)
    return df


def _expand_due_sheets(con, df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "license_key" not in df.columns:
        return df
    membership = _fetch_table(con, "issue_sheet_membership")
    if membership.empty:
        return df
    membership = membership.copy()
    membership["license_key"] = (
        membership["license_key"].astype("string").fillna("").str.strip()
    )
    membership = membership[membership["license_key"] != ""]
    if membership.empty:
        return df
    if "include" in membership.columns:
        membership = membership[membership["include"].fillna(True)]
    membership["print_sheet"] = (
        membership["print_sheet"].astype("string").map(_normalize_sheet)
    )
    membership = membership[membership["print_sheet"] != ""]
    if membership.empty:
        return df

    def _unique_preserve(values):
        seen = []
        for val in values:
            if not val:
                continue
            if val not in seen:
                seen.append(val)
        return seen

    sheet_lookup = (
        membership.groupby("license_key", sort=False)["print_sheet"].agg(_unique_preserve)
    )
    if sheet_lookup.empty:
        return df

    df_copy = df.copy()
    df_copy["license_key"] = (
        df_copy["license_key"].astype("string").fillna("").str.strip()
    )
    if "print_sheet" in df_copy.columns:
        df_copy["print_sheet"] = df_copy["print_sheet"].astype("string").map(_normalize_sheet)
    else:
        df_copy["print_sheet"] = DEFAULT_SHEET

    expanded_frames = []
    for lic, rows in df_copy.groupby("license_key", sort=False):
        sheets = sheet_lookup.get(lic)
        if not sheets:
            expanded_frames.append(rows)
            continue
        replicated_rows = []
        for _, row in rows.iterrows():
            for sheet in sheets:
                new_row = row.copy()
                new_row["print_sheet"] = sheet
                replicated_rows.append(new_row)
        expanded_frames.append(pd.DataFrame(replicated_rows))
    result = pd.concat(expanded_frames, ignore_index=True, sort=False)
    if "print_sheet" in result.columns:
        result["print_sheet"] = result["print_sheet"].astype("string").map(_normalize_sheet)
    if "print_sheet" in df_copy.columns:
        result = result.drop_duplicates(
            subset=["license_key", "print_sheet", "person_key", "license_no", "qualification"],
            keep="first",
        ).reset_index(drop=True)
    return result


def _ensure_display_names(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "name" not in df.columns:
        return df
    result = df.copy()
    name_series = result["name"].astype("string")
    if "display_name" in result.columns:
        display_series = result["display_name"].astype("string")
        mask = display_series.isna() | (display_series.str.strip() == "")
        if mask.any():
            result.loc[mask, "display_name"] = name_series[mask]
        result["display_name"] = result["display_name"].astype("string")
    else:
        result["display_name"] = name_series
    return result


def write_due_tables(db_path: Path | str, due_raw: pd.DataFrame) -> pd.DataFrame:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    due_enriched = _normalize_due(attach_identity_columns(due_raw))
    with _connect(path) as con:
        due_enriched = _expand_due_sheets(con, due_enriched)
        text_columns = [
            "license_no",
            "name",
            "display_name",
            "qualification",
            "category",
            "continuation_status",
            "next_stage_label",
            "next_exam_period",
            "next_procedure_status",
            "birth_year_west",
            "print_sheet",
            "address",
            "web_publish_no",
        ]
        for col in text_columns:
            if col in due_enriched.columns:
                due_enriched[col] = due_enriched[col].astype("string")

        if "qualification_category" in due_enriched.columns:
            due_enriched["qualification_category"] = due_enriched["qualification_category"].astype("string")
        else:
            due_enriched["qualification_category"] = pd.Series([""] * len(due_enriched), dtype="string")

        mapped = None
        if "継続" in due_enriched.columns:
            mapped = (
                due_enriched["継続"]
                .astype("Int64")
                .map({0: "新規", 1: "継続", 2: "再試験"})
                .fillna("")
                .astype("string")
            )
        elif "continuation_status" in due_enriched.columns:
            mapped = due_enriched["continuation_status"].astype("string").fillna("")
        if mapped is not None:
            mask = due_enriched["qualification_category"].isna() | (
                due_enriched["qualification_category"].str.strip() == ""
            )
            if mask.any():
                due_enriched.loc[mask, "qualification_category"] = mapped.loc[mask]

        if "display_name" not in due_enriched.columns:
            if "name" in due_enriched.columns:
                due_enriched["display_name"] = due_enriched["name"].astype("string")
            else:
                due_enriched["display_name"] = pd.Series([''] * len(due_enriched), dtype="string")
        else:
            due_enriched["display_name"] = due_enriched["display_name"].astype("string")
            if "name" in due_enriched.columns:
                name_series = due_enriched["name"].astype("string")
                mask = due_enriched["display_name"].isna() | (due_enriched["display_name"].str.strip() == '')
                if mask.any():
                    due_enriched.loc[mask, "display_name"] = name_series.loc[mask]

        if "employee_id" in due_enriched.columns:
            due_enriched["employee_id"] = due_enriched["employee_id"].astype("string")

        overrides_df = _load_person_override_df(con)
        due_enriched = _apply_person_overrides(due_enriched, overrides_df)
        due_enriched = _ensure_display_names(due_enriched)
        _seed_filters(con, due_enriched)
        memberships = due_enriched[["license_key", "person_key", "print_sheet"]].dropna(
            subset=["license_key"]
        )
        _seed_sheet_state(con, due_enriched, memberships)
        _write_table(con, "due_raw", due_enriched)
        filtered = con.execute(
            """
            SELECT d.*
            FROM due_raw d
            LEFT JOIN issue_person_filter pf ON d.person_key = pf.person_key
            LEFT JOIN issue_license_filter lf ON d.license_key = lf.license_key
            LEFT JOIN issue_sheet_filter sf ON d.print_sheet = sf.print_sheet
            LEFT JOIN issue_sheet_membership sm
                ON d.license_key = sm.license_key AND d.print_sheet = sm.print_sheet
            WHERE COALESCE(pf.include, TRUE)
              AND COALESCE(lf.include, TRUE)
              AND COALESCE(sf.include, TRUE)
              AND COALESCE(sm.include, TRUE)
            ORDER BY d.print_sheet, d.expiry_date, d.name
            """
        ).df()
        filtered = _apply_person_overrides(filtered, overrides_df)
        filtered = _ensure_display_names(filtered)
        _write_table(con, "due", filtered)
        return filtered


def set_person_filter(
    db_path: Path | str, person_key: str, include: bool, notes: str | None = None
) -> None:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        _ensure_person_filters(con, [person_key])
        assignments = ["include = ?"]
        params: list[object] = [bool(include)]
        if notes is not None:
            assignments.append("notes = ?")
            params.append(notes)
        assignments.append("updated_at = now()")
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
        assignments.append("updated_at = now()")
        params.append(license_key)
        sql = "UPDATE issue_license_filter SET " + ", ".join(assignments) + " WHERE license_key = ?"
        con.execute(sql, params)


def load_sheet_filters(db_path: Path | str) -> pd.DataFrame:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        roster_all = _fetch_table(con, "roster_all")
        sheet_names = []
        if not roster_all.empty and "print_sheet" in roster_all.columns:
            sheet_names = roster_all["print_sheet"].dropna().map(_normalize_sheet).unique().tolist()
        df_filters = _fetch_table(con, "issue_sheet_filter")
        additional = []
        if not df_filters.empty:
            additional = df_filters["print_sheet"].tolist()
        all_names = sorted({*sheet_names, *additional})
        _ensure_sheet_filters(con, all_names)
        return con.execute(
            "SELECT print_sheet, include, notes FROM issue_sheet_filter ORDER BY print_sheet"
        ).df()


def list_print_sheets(db_path: Path | str) -> list[str]:
    df = load_sheet_filters(db_path)
    if df.empty:
        return []
    return df["print_sheet"].tolist()


def set_sheet_filter(
    db_path: Path | str,
    print_sheet: str,
    include: bool,
    notes: str | None = None,
) -> None:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    sheet = _normalize_sheet(print_sheet)
    with _connect(path) as con:
        _ensure_sheet_filters(con, [sheet])
        assignments = ["include = ?"]
        params: list[object] = [bool(include)]
        if notes is not None:
            assignments.append("notes = ?")
            params.append(notes)
        assignments.append("updated_at = now()")
        params.append(sheet)
        sql = "UPDATE issue_sheet_filter SET " + ", ".join(assignments) + " WHERE print_sheet = ?"
        con.execute(sql, params)


def create_print_sheet(db_path: Path | str, print_sheet: str, *, include: bool = True) -> None:
    sheet = _normalize_sheet(print_sheet)
    path = _as_path(db_path)
    ensure_issue_schema(path)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    with _connect(path) as con:
        con.execute(
            "INSERT INTO issue_sheet_filter (print_sheet, include, notes, updated_at) VALUES (?, ?, NULL, ?) ON CONFLICT(print_sheet) DO UPDATE SET include = excluded.include, updated_at = excluded.updated_at",
            [sheet, bool(include), now],
        )


def delete_print_sheet(db_path: Path | str, print_sheet: str) -> None:
    sheet = _normalize_sheet(print_sheet)
    if sheet == DEFAULT_SHEET:
        return  # default sheet is reserved
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        con.execute("DELETE FROM issue_sheet_membership WHERE print_sheet = ?", [sheet])
        con.execute("DELETE FROM issue_sheet_filter WHERE print_sheet = ?", [sheet])


def load_sheet_membership(db_path: Path | str) -> pd.DataFrame:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        roster = _fetch_table(con, "roster_all")
        membership = _fetch_table(con, "issue_sheet_membership")
        items: list[pd.DataFrame] = []
        if not roster.empty:
            if "print_sheet" not in roster.columns:
                roster["print_sheet"] = DEFAULT_SHEET
            roster_base = (
                roster[["license_key", "person_key", "print_sheet"]]
                .dropna(subset=["license_key"])
                .copy()
            )
            roster_base["print_sheet"] = roster_base["print_sheet"].map(_normalize_sheet)
            items.append(roster_base)
        if not membership.empty:
            membership = membership[
                ["license_key", "person_key", "print_sheet", "include", "notes"]
            ].copy()
            membership["print_sheet"] = membership["print_sheet"].map(_normalize_sheet)
            items.append(membership)
        if not items:
            return pd.DataFrame(
                columns=["license_key", "person_key", "print_sheet", "include", "notes"]
            )
        merged = pd.concat(items, ignore_index=True, sort=False)
        merged["print_sheet"] = merged["print_sheet"].map(_normalize_sheet)
        merged = merged.drop_duplicates(subset=["license_key", "print_sheet"], keep="last")
        if not roster.empty:
            roster_lookup = roster[["license_key", "person_key", "print_sheet"]].copy()
            roster_lookup["print_sheet"] = roster_lookup["print_sheet"].map(_normalize_sheet)
            merged = merged.merge(
                roster_lookup,
                on=["license_key", "print_sheet"],
                how="left",
                suffixes=("", "_roster"),
            )
            merged["person_key"] = (
                merged["person_key"]
                .replace("", pd.NA)
                .fillna(merged.get("person_key_roster"))
                .fillna("")
            )
            merged = merged.drop(columns=[c for c in merged.columns if c.endswith("_roster")])
        if "include" in merged.columns:
            include_series = merged["include"].astype("boolean", errors="ignore")
            if include_series.dtype == "boolean":
                merged["include"] = include_series.fillna(True)
            else:
                merged["include"] = merged["include"].where(merged["include"].notna(), True)
        else:
            merged["include"] = True
        if "notes" not in merged.columns:
            merged["notes"] = None
        return merged


def set_sheet_membership(
    db_path: Path | str,
    license_key: str,
    print_sheet: str,
    include: bool,
    *,
    person_key: str | None = None,
    notes: str | None = None,
) -> None:
    sheet = _normalize_sheet(print_sheet)
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        con.execute(
            """
            INSERT INTO issue_sheet_membership (license_key, person_key, print_sheet, include, notes, updated_at)
            VALUES (?, ?, ?, ?, ?, now())
            ON CONFLICT (license_key, print_sheet) DO UPDATE SET
                include = excluded.include,
                notes = excluded.notes,
                updated_at = now()
            """,
            [license_key, person_key or "", sheet, bool(include), notes],
        )




def _load_person_override_df(con) -> pd.DataFrame:
    if not _table_exists(con, 'roster_person_override'):
        return pd.DataFrame(columns=['person_key', 'display_name', 'employee_id'])
    return con.execute("SELECT person_key, display_name, employee_id FROM roster_person_override").df()


def load_person_overrides(db_path: Path | str) -> pd.DataFrame:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        df = _load_person_override_df(con)
    if df.empty:
        return pd.DataFrame(columns=['person_key', 'display_name', 'employee_id'])
    return df


def set_person_override(
    db_path: Path | str, person_key: str, *, display_name: str | None = None, employee_id: str | None = None
) -> None:
    if not person_key:
        raise ValueError('person_key is required')
    name_clean = (display_name or '').strip()
    emp_clean = (employee_id or '').strip()
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        if not name_clean and not emp_clean:
            con.execute('DELETE FROM roster_person_override WHERE person_key = ?', [person_key])
        else:
            con.execute(
                """
                INSERT INTO roster_person_override (person_key, display_name, employee_id, updated_at)
                VALUES (?, ?, ?, now())
                ON CONFLICT(person_key) DO UPDATE SET
                    display_name = excluded.display_name,
                    employee_id = excluded.employee_id,
                    updated_at = now()
                """,
                [person_key, name_clean or None, emp_clean or None],
            )


def _apply_person_overrides(df: pd.DataFrame, overrides: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    result = df.copy()
    if 'employee_id' not in result.columns:
        result['employee_id'] = pd.Series([''] * len(result), dtype='string')
    if overrides is None or overrides.empty:
        return result
    over = overrides.copy()
    over['display_name'] = over['display_name'].fillna('').astype(str)
    over['employee_id'] = over['employee_id'].fillna('').astype(str)
    if 'person_key' not in result.columns:
        return result
    merged = result.merge(over, on='person_key', how='left', suffixes=('', '_override'))
    if 'display_name_override' in merged.columns:
        override = merged['display_name_override'].fillna('').astype(str)
        mask = override.str.strip() != ''
        if mask.any():
            merged.loc[mask, 'name'] = override[mask]
        merged = merged.drop(columns=['display_name_override'])
    if 'employee_id_override' in merged.columns:
        override = merged['employee_id_override'].fillna('').astype(str)
        mask = override.str.strip() != ''
        if mask.any():
            merged.loc[mask, 'employee_id'] = override[mask]
        merged = merged.drop(columns=['employee_id_override'])
    return merged


def reapply_due_filters(db_path: Path | str) -> pd.DataFrame:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        if not _table_exists(con, "due_raw"):
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
    needs_keys = "person_key" not in data.columns or "license_key" not in data.columns
    payload = attach_identity_columns(data) if needs_keys else data.copy()
    payload = _normalize_due(payload)
    with _connect(path) as con:
        row_count = len(payload)
        con.execute(
            "INSERT INTO issue_runs (run_id, created_at, created_by, comment, row_count, due_version, filters_version) VALUES (?, now(), ?, ?, ?, NULL, NULL)",
            [run_id, creator, comment, row_count],
        )
        if row_count:
            records: list[dict[str, object]] = []
            for row_index, snap in enumerate(payload.to_dict(orient="records")):
                records.append(
                    {
                        "run_id": run_id,
                        "row_index": row_index,
                        "person_key": str(snap.get("person_key") or ""),
                        "license_key": str(snap.get("license_key") or ""),
                        "payload": json.dumps(snap, ensure_ascii=False, default=str),
                    }
                )
            df_items = pd.DataFrame.from_records(records)
            con.register("_issue_items", df_items)
            try:
                con.execute(
                    "INSERT INTO issue_run_items SELECT run_id, row_index, person_key, license_key, CAST(payload AS JSON) FROM _issue_items"
                )
            finally:
                con.unregister("_issue_items")
    return run_id


def load_issue_runs(db_path: Path | str) -> pd.DataFrame:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        if not _table_exists(con, "issue_runs"):
            return pd.DataFrame()
        return con.execute(
            "SELECT run_id, created_at, created_by, comment, row_count FROM issue_runs ORDER BY created_at DESC"
        ).df()


def load_issue_run_items(db_path: Path | str, run_id: str) -> pd.DataFrame:
    path = _as_path(db_path)
    ensure_issue_schema(path)
    with _connect(path) as con:
        if not _table_exists(con, "issue_run_items"):
            return pd.DataFrame()
        df = con.execute(
            "SELECT row_index, person_key, license_key, payload FROM issue_run_items WHERE run_id = ? ORDER BY row_index",
            [run_id],
        ).df()
    if df.empty:
        return df
    payloads = []
    for value in df["payload"]:
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
    base = df[["row_index", "person_key", "license_key"]].reset_index(drop=True)
    if detail.empty:
        return base
    detail = detail.fillna("")
    return pd.concat([base, detail.reset_index(drop=True)], axis=1)


__all__ = [
    "attach_identity_columns",
    "create_print_sheet",
    "delete_print_sheet",
    "ensure_issue_schema",
    "list_print_sheets",
    "load_issue_run_items",
    "load_issue_runs",
    "load_sheet_filters",
    "load_sheet_membership",
    "load_person_overrides",
    "materialize_roster_all",
    "list_qualifications",
    "add_manual_qualification",
    "update_manual_qualification",
    "delete_manual_qualification",
    "add_report_definition",
    "delete_report_definition",
    "list_report_definitions",
    "add_report_entry",
    "remove_report_entry",
    "list_report_entries",
    "record_issue_run",
    "reapply_due_filters",
    "set_license_filter",
    "set_person_filter",
    "set_person_override",
    "set_sheet_filter",
    "set_sheet_membership",
    "write_due_tables",
    "DEFAULT_SHEET",
    "ALL_SHEETS_LABEL",
]


