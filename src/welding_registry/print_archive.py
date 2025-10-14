from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from .paths import resolve_duckdb_path, resolve_warehouse_path


PRINT_ARCHIVE_DDL = """
CREATE TABLE IF NOT EXISTS issue_print_runs (
  print_id BIGINT PRIMARY KEY,
  created_at TIMESTAMP NOT NULL,
  printed_at TIMESTAMP,
  generated_at TIMESTAMP,
  sheet TEXT,
  sheet_label TEXT,
  orientation TEXT,
  rows_per_page INT,
  record_count INT,
  page_total INT,
  columns TEXT,
  content_hash TEXT,
  csv_path TEXT,
  payload_path TEXT
);

CREATE SEQUENCE IF NOT EXISTS issue_print_runs_seq;
"""


@dataclass(frozen=True)
class PrintArchiveResult:
    print_id: int
    csv_path: Path
    payload_path: Path
    content_hash: str
    recorded_at: datetime


def _slugify(value: str) -> str:
    text = value.strip()
    if not text:
        return "all"
    text = text.replace("\\", "-").replace("/", "-")
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^0-9A-Za-zぁ-んァ-ヶ一-龠ー_\\-]", "_", text)
    return text[:48] or "all"


def _parse_generated_at(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _parse_printed_at(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        return datetime.now(timezone.utc).replace(tzinfo=None)


def _compute_content_hash(df: pd.DataFrame, columns: Sequence[str]) -> str:
    ordered = [col for col in columns if col in df.columns]
    temp = df.copy()
    temp = temp.reindex(columns=ordered, fill_value="")
    temp = temp.fillna("")
    blob = temp.to_csv(index=False).encode("utf-8")
    import hashlib

    return hashlib.sha256(blob).hexdigest()


def _ensure_tables(con: Any) -> None:
    con.execute(PRINT_ARCHIVE_DDL)


def archive_print_run(
    *,
    duckdb_path: Path | str,
    payload: dict[str, Any],
    df: pd.DataFrame,
    columns: Sequence[str],
    sheet: str,
    sheet_label: str,
    orientation: str,
    rows_per_page: int,
    page_total: int,
    record_count: int,
    generated_at: str | None,
    printed_at: str | None,
    content_hash: str,
) -> PrintArchiveResult:
    """Persist an issuance print payload to DuckDB and warehouse files."""

    resolved_duckdb = resolve_duckdb_path(duckdb_path)
    warehouse_root = resolve_warehouse_path()
    archive_dir = warehouse_root / "issue_prints"
    archive_dir.mkdir(parents=True, exist_ok=True)

    sheet_display = sheet_label or sheet or "ALL"
    slug = _slugify(sheet_display)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    timestamp = now.strftime("%Y%m%d-%H%M%S")

    df_clean = df.copy()
    if not columns:
        columns = list(df_clean.columns)
    df_clean = df_clean.reindex(columns=columns, fill_value="")
    df_clean = df_clean.fillna("")
    combined_hash = _compute_content_hash(df_clean, columns)
    if not content_hash:
        content_hash = combined_hash

    base_name = f"issue_{timestamp}_{slug}_{content_hash[:8]}"
    csv_path = archive_dir / f"{base_name}.csv"
    payload_path = archive_dir / f"{base_name}.json"

    df_clean.to_csv(csv_path, index=False, encoding="utf-8-sig")
    with payload_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)

    rel_csv = csv_path.relative_to(warehouse_root)
    rel_payload = payload_path.relative_to(warehouse_root)

    generated_dt = _parse_generated_at(generated_at)
    printed_dt = _parse_printed_at(printed_at)

    import duckdb  # type: ignore

    with duckdb.connect(str(resolved_duckdb)) as con:
        _ensure_tables(con)
        print_id = con.execute("SELECT nextval('issue_print_runs_seq')").fetchone()[0]
        con.execute(
            """
            INSERT INTO issue_print_runs (
                print_id,
                created_at,
                printed_at,
                generated_at,
                sheet,
                sheet_label,
                orientation,
                rows_per_page,
                record_count,
                page_total,
                columns,
                content_hash,
                csv_path,
                payload_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                int(print_id),
                now,
                printed_dt,
                generated_dt,
                sheet or "",
                sheet_display,
                orientation,
                int(rows_per_page),
                int(record_count),
                int(page_total),
                json.dumps(list(columns), ensure_ascii=False),
                content_hash or combined_hash,
                str(rel_csv),
                str(rel_payload),
            ],
        )

    return PrintArchiveResult(
        print_id=int(print_id),
        csv_path=csv_path,
        payload_path=payload_path,
        content_hash=content_hash or combined_hash,
        recorded_at=now,
    )

