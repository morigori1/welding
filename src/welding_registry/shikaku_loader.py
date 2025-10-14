from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

import duckdb
import pandas as pd
from pandas import DataFrame

CANONICAL_COLUMN_MAP = {
    "No.": "row_no",
    "証明番号": "license_no",
    "資格": "qualification",
    "資格種類": "category",
    "登録年月日": "registration_date",
    "継続": "continuation_count",
    "有効期限": "expiry_date",
    "氏名": "name",
    "生年月日": "birth_date",
    "自宅住所": "address",
    "勤務先": "affiliation",
    "受験申請した溶接協会": "issuing_body",
    "次回区分": "next_stage_label",
    "次回\uff7b\uff70\uff8d\uff9e\uff72\uff97\uff9d\uff7d/\n再評価受験期間": "next_exam_window",
    "次回手続き状況": "next_procedure_status",
    "WEB申込番号": "web_publish_no",
}

REQUIRED_COLUMNS = tuple(CANONICAL_COLUMN_MAP.keys())
MANDATORY_CANONICAL = {
    "row_no",
    "license_no",
    "qualification",
    "category",
    "registration_date",
    "continuation_count",
    "expiry_date",
    "name",
    "birth_date",
    "address",
    "affiliation",
    "issuing_body",
    "next_stage_label",
    "next_exam_window",
    "web_publish_no",
}
DATE_COLUMNS = ("registration_date", "expiry_date", "birth_date")
WINDOW_PATTERN = re.compile(r"(\d{4})[./](\d{2})[./](\d{2})")
RANGE_SEPARATOR_PATTERN = re.compile(r"[〜～\-ー－~─〜ー]")

ALT_COLUMN_MAP = {
    "認証番号": "license_no",
    "資格種別": "category",
    "住所": "address",
    "所属": "affiliation",
    "試験実施機関": "issuing_body",
    "次回受験手続き区分": "next_stage_label",
    "次回受験予定期間": "next_exam_window",
    "次回／再評価受験期間": "next_exam_window",
    "備考": "notes",
    "WEB公開番号": "web_publish_no",
    "�ؖ��ԍ�": "license_no",
    "���i": "qualification",
    "���i���": "category",
    "�o�^�N����": "registration_date",
    "�p��": "continuation_count",
    "�L������": "expiry_date",
    "����": "name",
    "���N����": "birth_date",
    "����Z��": "address",
    "�Ζ���": "affiliation",
    "�󌱐\\�������n�ڋ���": "issuing_body",
    "����敪": "next_stage_label",
    "�����޲�ݽ/\n�ĕ]���󌱊���": "next_exam_window",
    "����葱����": "notes",
    "WEB�\\���ԍ�": "web_publish_no",
}


@dataclass(slots=True, frozen=True)
class LoadSummary:
    source_file: Path
    row_count: int
    duckdb_path: Optional[Path]
    out_dir: Optional[Path]


def detect_shikaku_workbook(path: Path, *, max_rows: int = 1) -> bool:
    """Return True if the workbook appears to be the 資格一覧形式."""

    try:
        import openpyxl  # type: ignore
    except ImportError as exc:  # pragma: no cover - should exist, but guard regardless
        raise RuntimeError("openpyxl is required to inspect 資格一覧.xlsx") from exc

    path = Path(path)
    if not path.exists():
        return False

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        sheet = wb.active
        rows = []
        for _ in range(max_rows):
            try:
                rows.append(next(sheet.iter_rows(values_only=True)))
            except StopIteration:
                break
        if not rows:
            return False
        header = [str(value).strip() if value is not None else "" for value in rows[0]]
        return all(col in header for col in CANONICAL_COLUMN_MAP.keys())
    finally:
        wb.close()


def load_shikaku_workbook(
    excel_path: Path | str,
    *,
    duckdb_path: Path | str | None = None,
    out_dir: Path | str | None = None,
) -> LoadSummary:
    """Load 資格一覧.xlsx into DuckDB dim/fact tables and optional outputs."""

    excel_path = Path(excel_path)
    if not excel_path.exists():
        raise FileNotFoundError(excel_path)

    df_raw = pd.read_excel(
        excel_path,
        sheet_name=0,
        dtype="object",
        engine="openpyxl",
    )
    rename_map: dict[str, str] = {}
    for col in df_raw.columns:
        if col in CANONICAL_COLUMN_MAP:
            rename_map[col] = CANONICAL_COLUMN_MAP[col]
        elif col in ALT_COLUMN_MAP:
            rename_map[col] = ALT_COLUMN_MAP[col]

    canonical_present = {target for target in rename_map.values()}
    missing_required = sorted(MANDATORY_CANONICAL - canonical_present)
    if missing_required:
        raise ValueError(f"Workbook is missing required columns: {', '.join(missing_required)}")

    df = df_raw.rename(columns=rename_map)
    for canonical_name in CANONICAL_COLUMN_MAP.values():
        if canonical_name not in df.columns:
            df[canonical_name] = None
    ordered_cols = [CANONICAL_COLUMN_MAP[col] for col in REQUIRED_COLUMNS]
    df = df.loc[:, ordered_cols].copy()
    df = _clean_dataframe(df, source_file=excel_path.name)
    row_count = len(df)

    con = None
    duckdb_path_opt: Optional[Path] = Path(duckdb_path) if duckdb_path else None
    if duckdb_path_opt is not None:
        duckdb_path_opt.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(duckdb_path_opt))
        _write_to_duckdb(con, df, excel_path)

    if out_dir:
        _write_side_outputs(Path(out_dir), df)

    if con is not None:
        con.close()

    return LoadSummary(
        source_file=excel_path,
        row_count=row_count,
        duckdb_path=duckdb_path_opt,
        out_dir=Path(out_dir) if out_dir else None,
    )


def _clean_dataframe(df: DataFrame, *, source_file: str) -> DataFrame:
    cleaned = df.copy()
    cleaned = cleaned.apply(lambda col: col.map(_clean_cell))

    cleaned["row_no"] = pd.to_numeric(cleaned["row_no"], errors="coerce").astype("Int64")
    cleaned["continuation_count"] = (
        pd.to_numeric(cleaned["continuation_count"], errors="coerce").astype("Int64")
    )

    for col in DATE_COLUMNS:
        cleaned[col] = pd.to_datetime(cleaned[col], errors="coerce").dt.date

    cleaned["next_exam_window"] = cleaned["next_exam_window"].replace("", None)
    next_windows = cleaned["next_exam_window"].astype("object").fillna("")
    windows_parsed = next_windows.map(_parse_window)
    cleaned["next_exam_start"] = windows_parsed.map(lambda tup: tup[0])
    cleaned["next_exam_end"] = windows_parsed.map(lambda tup: tup[1])
    if "next_procedure_status" not in cleaned.columns:
        cleaned["next_procedure_status"] = None

    load_ts = datetime.utcnow().replace(microsecond=0)
    cleaned["load_timestamp"] = load_ts
    cleaned["source_file"] = source_file

    cleaned["name"] = cleaned["name"].fillna("").astype(str).str.strip()
    cleaned["address"] = cleaned["address"].fillna("").astype(str).str.strip()
    cleaned["affiliation"] = cleaned["affiliation"].fillna("").astype(str).str.strip()

    cleaned["birth_date"] = cleaned["birth_date"]

    cleaned["person_id"] = cleaned.apply(_generate_person_id, axis=1)
    cleaned["license_id"] = cleaned.apply(_generate_license_id, axis=1)
    if "notes" not in cleaned.columns:
        cleaned["notes"] = None

    return cleaned


def _clean_cell(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    return value


def _parse_window(text: object) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    if not text or not isinstance(text, str):
        return (None, None)
    cleaned = RANGE_SEPARATOR_PATTERN.sub(" ", text)
    matches = WINDOW_PATTERN.findall(cleaned)
    if not matches:
        return (None, None)

    def _to_timestamp(parts: Iterable[str]) -> Optional[pd.Timestamp]:
        try:
            return pd.Timestamp(year=int(parts[0]), month=int(parts[1]), day=int(parts[2]))
        except Exception:
            return None

    start = _to_timestamp(matches[0])
    end = _to_timestamp(matches[1]) if len(matches) > 1 else None
    return (start, end)


def _hash_string(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def _generate_person_id(row: pd.Series) -> str:
    name = (row.get("name") or "").strip().lower()
    birth = row.get("birth_date")
    birth_str = ""
    if pd.notna(birth):
        birth_str = pd.Timestamp(birth).strftime("%Y-%m-%d")
    base = f"{name}|{birth_str}"
    if not base.strip("|"):
        fallback = str(row.get("license_no") or row.get("row_no") or "")
        base = f"fallback|{fallback}"
    return _hash_string(base)


def _generate_license_id(row: pd.Series) -> str:
    license_no = str(row.get("license_no") or "").strip().lower()
    if not license_no:
        fallback = str(row.get("row_no") or row.name or "")
        license_no = f"fallback|{fallback}"
    return _hash_string(license_no)


def _write_to_duckdb(con: duckdb.DuckDBPyConnection, df: DataFrame, excel_path: Path) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS stg_shikaku_raw (
            row_no BIGINT,
            license_no TEXT,
            qualification TEXT,
            category TEXT,
            registration_date DATE,
            continuation_count INTEGER,
            expiry_date DATE,
            name TEXT,
            birth_date DATE,
            address TEXT,
            affiliation TEXT,
            issuing_body TEXT,
            next_stage_label TEXT,
            next_exam_window TEXT,
            next_exam_start TIMESTAMP,
            next_exam_end TIMESTAMP,
            next_procedure_status TEXT,
            notes TEXT,
            web_publish_no TEXT,
            source_file TEXT,
            person_id TEXT,
            license_id TEXT,
            load_timestamp TIMESTAMP
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_person (
            person_id TEXT PRIMARY KEY,
            display_name TEXT,
            birth_date DATE,
            address TEXT,
            affiliation TEXT
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_license (
            license_id TEXT PRIMARY KEY,
            license_no TEXT,
            web_publish_no TEXT
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_qualification (
            license_id TEXT,
            person_id TEXT,
            qualification TEXT,
            category TEXT,
            registration_date DATE,
            continuation_count INTEGER,
            expiry_date DATE,
            issuing_body TEXT,
            next_stage_label TEXT,
            next_exam_window TEXT,
            next_exam_start TIMESTAMP,
            next_exam_end TIMESTAMP,
            next_procedure_status TEXT,
            notes TEXT,
            source_file TEXT,
            row_no BIGINT,
            load_timestamp TIMESTAMP
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_run_history (
            run_id UUID DEFAULT uuid(),
            source_file TEXT,
            row_count BIGINT,
            load_started TIMESTAMP,
            load_completed TIMESTAMP,
            source_hash TEXT
        )
        """
    )

    con.execute("DELETE FROM stg_shikaku_raw")
    con.execute("DELETE FROM fact_qualification")
    con.execute("DELETE FROM dim_person")
    con.execute("DELETE FROM dim_license")

    con.register("df_stage", df)
    con.execute(
        """
        INSERT INTO stg_shikaku_raw
        SELECT
            row_no,
            license_no,
            qualification,
            category,
            registration_date,
            continuation_count,
            expiry_date,
            name,
            birth_date,
            address,
            affiliation,
            issuing_body,
            next_stage_label,
            next_exam_window,
            next_exam_start,
            next_exam_end,
            next_procedure_status,
            notes,
            web_publish_no,
            source_file,
            person_id,
            license_id,
            load_timestamp
        FROM df_stage
        """
    )
    con.unregister("df_stage")

    df_person = (
        df[["person_id", "name", "birth_date", "address", "affiliation"]]
        .drop_duplicates("person_id")
        .rename(columns={"name": "display_name"})
    )
    con.register("df_person", df_person)
    con.execute(
        """
        INSERT INTO dim_person
        SELECT person_id, display_name, birth_date, address, affiliation
        FROM df_person
        """
    )
    con.unregister("df_person")

    df_license = (
        df[["license_id", "license_no", "web_publish_no"]]
        .drop_duplicates("license_id")
    )
    con.register("df_license", df_license)
    con.execute(
        """
        INSERT INTO dim_license
        SELECT license_id, license_no, web_publish_no
        FROM df_license
        """
    )
    con.unregister("df_license")

    df_fact = df[
        [
            "license_id",
            "person_id",
            "qualification",
            "category",
            "registration_date",
            "continuation_count",
            "expiry_date",
            "issuing_body",
            "next_stage_label",
            "next_exam_window",
            "next_exam_start",
            "next_exam_end",
            "next_procedure_status",
            "notes",
            "source_file",
            "row_no",
            "load_timestamp",
        ]
    ]

    con.register("df_fact", df_fact)
    con.execute(
        """
        INSERT INTO fact_qualification
        SELECT
            license_id,
            person_id,
            qualification,
            category,
            registration_date,
            continuation_count,
            expiry_date,
            issuing_body,
            next_stage_label,
            next_exam_window,
            next_exam_start,
            next_exam_end,
            next_procedure_status,
            notes,
            source_file,
            row_no,
            load_timestamp
        FROM df_fact
        """
    )
    con.unregister("df_fact")

    con.execute("DROP VIEW IF EXISTS vw_due_schedule")
    con.execute(
        """
        CREATE VIEW vw_due_schedule AS
        SELECT
            f.license_id,
            l.license_no,
            f.person_id,
            p.display_name,
            p.birth_date,
            p.address,
            p.affiliation,
            f.qualification,
            f.category,
            f.registration_date,
            f.continuation_count,
            f.expiry_date,
            f.issuing_body,
            f.next_stage_label,
            f.next_exam_window,
            f.next_exam_start,
            f.next_exam_end,
            f.next_procedure_status,
            f.notes,
            f.source_file,
            f.row_no,
            f.load_timestamp,
            DATE_DIFF('day', CURRENT_DATE, f.expiry_date) AS days_to_expiry
        FROM fact_qualification f
        JOIN dim_person p USING(person_id)
        JOIN dim_license l USING(license_id)
        """
    )

    source_hash = hashlib.sha1(excel_path.read_bytes()).hexdigest()
    now = datetime.utcnow().replace(microsecond=0)
    con.execute(
        """
        INSERT INTO etl_run_history (source_file, row_count, load_started, load_completed, source_hash)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            excel_path.name,
            len(df),
            now,
            now,
            source_hash,
        ],
    )


def _write_side_outputs(out_dir: Path, df: DataFrame) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    canonical_path = out_dir / "shikaku_canonical.csv"
    df.to_csv(canonical_path, index=False, encoding="utf-8")


__all__ = [
    "detect_shikaku_workbook",
    "load_shikaku_workbook",
    "LoadSummary",
]
