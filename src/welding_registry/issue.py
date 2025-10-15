from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Optional, Sequence

import math

import pandas as pd

from .reminders import DueConfig, annotate_due
from .warehouse import DEFAULT_SHEET, reapply_due_filters, write_due_tables

__all__ = [
    "COLUMN_LABELS",
    "COLUMN_WIDTHS",
    "DEFAULT_ISSUE_COLUMNS",
    "ISSUE_WINDOW_DAYS",
    "IssuePage",
    "build_issue_dataframe",
    "ensure_due_dataframe",
    "paginate_issue",
]

COLUMN_LABELS: dict[str, str] = {
    "print_sheet": "シート",
    "name": "氏名",
    "license_no": "免許番号",
    "qualification": "資格",
    "continuation_status": "継続",
    "qualification_category": "資格種別",
    "category": "区分",
    "first_issue_date": "初回発行日",
    "issue_date": "登録年月日",
    "expiry_date": "有効期限",
    "birth_date": "生年月日",
    "birth_year_west": "生年",
    "registration_date": "登録年月日",
    "employee_id": "社員番号",
    "days_to_expiry": "残日数",
    "notice_stage": "通知区分",
    "next_stage_label": "次回区分",
    "next_notice_date": "次回通知日",
    "next_surveillance_window": "次回受検期間",
    "next_exam_period": "次回受験期間/再試験猶予",
    "retest_window": "再試験猶予",
    "next_procedure_status": "次回手続状況",
}

COLUMN_WIDTHS: dict[str, int] = {
    "print_sheet": 120,
    "name": 220,
    "license_no": 150,
    "qualification": 160,
    "continuation_status": 120,
    "qualification_category": 140,
    "category": 120,
    "first_issue_date": 120,
    "issue_date": 120,
    "expiry_date": 120,
    "birth_date": 120,
    "birth_year_west": 100,
    "registration_date": 120,
    "employee_id": 120,
    "days_to_expiry": 90,
    "notice_stage": 120,
    "next_stage_label": 160,
    "next_notice_date": 140,
    "next_surveillance_window": 200,
    "next_exam_period": 200,
    "retest_window": 200,
    "next_procedure_status": 160,
}

DEFAULT_ISSUE_COLUMNS: list[str] = [
    "print_sheet",
    "name",
    "license_no",
    "qualification",
    "continuation_status",
    "qualification_category",
    "first_issue_date",
    "registration_date",
    "expiry_date",
    "birth_date",
    "next_stage_label",
    "next_surveillance_window",
    "retest_window",
]

ISSUE_WINDOW_DAYS = 90


@dataclass(frozen=True)
class IssuePage:
    number: int
    sheet: str
    sheet_page: int
    sheet_total: int
    rows: list[dict[str, str]]


def _log(log: Optional[Callable[[str], None]], message: str) -> None:
    if log is not None:
        log(message)


def _normalize_sheet_column(df: pd.DataFrame) -> pd.DataFrame:
    if "print_sheet" not in df.columns:
        df = df.copy()
        df["print_sheet"] = DEFAULT_SHEET
        return df
    result = df.copy()
    result["print_sheet"] = (
        result["print_sheet"].astype("string").fillna(DEFAULT_SHEET)
    )
    return result


def build_issue_dataframe(
    duckdb_path: Path | str,
    *,
    log: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    """Construct the base issuance dataframe from DuckDB roster tables."""

    path = Path(duckdb_path)
    _log(log, f"[issue] build_issue_dataframe start path={path}")
    try:
        import duckdb  # type: ignore
    except Exception as exc:  # pragma: no cover - environment issue
        _log(log, f"[issue] DuckDB import failed: {exc}")
        return pd.DataFrame()

    membership = pd.DataFrame()
    try:
        with duckdb.connect(str(path)) as con:
            has_roster_all = bool(
                con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name='roster_all'"
                ).fetchone()
            )
            if has_roster_all:
                base = con.execute("SELECT * FROM roster_all").df()
            else:
                has_roster = bool(
                    con.execute(
                        "SELECT 1 FROM information_schema.tables WHERE table_name='roster'"
                    ).fetchone()
                )
                if not has_roster:
                    _log(log, "[issue] roster tables not found")
                    return pd.DataFrame()
                base = con.execute("SELECT * FROM roster").df()
            membership = con.execute(
                "SELECT license_key, person_key, print_sheet, include FROM issue_sheet_membership"
            ).df()
    except Exception as exc:
        _log(log, f"[issue] failed to read roster data: {exc}")
        return pd.DataFrame()

    if base.empty:
        _log(log, "[issue] roster base dataframe empty")
        return base

    df = base.copy()
    _log(log, f"[issue] loaded base rows={len(df)} membership={len(membership)}")

    rename_map: dict[str, str] = {}
    if "次回区分" in df.columns:
        rename_map["次回区分"] = "next_stage_label"
    if "次回手続状況" in df.columns:
        rename_map["次回手続状況"] = "next_procedure_status"
    df = df.rename(columns=rename_map)

    combined_col = next(
        (col for col in df.columns if "次回受検期間" in col and "再試験猶予" in col),
        None,
    )
    if combined_col:
        window_series = df[combined_col].astype("string").fillna("")
        df["next_surveillance_window"] = window_series
        df["retest_window"] = window_series
        df = df.drop(columns=[combined_col])

    if "qualification_category" in df.columns:
        df["qualification_category"] = df["qualification_category"].astype("string")
    else:
        df["qualification_category"] = pd.Series([""] * len(df), dtype="string")

    mapped: pd.Series | None = None
    if "継続" in df.columns:
        mapped = (
            df["継続"]
            .astype("Int64")
            .map({0: "新規", 1: "継続", 2: "再試験"})
            .fillna("")
            .astype("string")
        )
    elif "continuation_status" in df.columns:
        mapped = df["continuation_status"].astype("string").fillna("")
    if mapped is not None:
        mask = df["qualification_category"].isna() | (df["qualification_category"].str.strip() == "")
        if mask.any():
            df.loc[mask, "qualification_category"] = mapped.loc[mask]

    df = _normalize_sheet_column(df)

    if membership is not None and not membership.empty:
        membership = membership.copy()
        membership["license_key"] = membership["license_key"].astype("string")
        membership["print_sheet"] = (
            membership["print_sheet"].astype("string").fillna("").str.strip()
        )
        membership["print_sheet"] = membership["print_sheet"].replace("", DEFAULT_SHEET)
        if "include" in membership.columns:
            membership = membership[membership["include"].fillna(True)]
        overrides = membership[["license_key", "print_sheet"]].dropna(subset=["license_key"])
        overrides = overrides.rename(columns={"print_sheet": "print_sheet_override"})
        if not overrides.empty:
            df = df.merge(overrides, on="license_key", how="left")
            mask = df["print_sheet_override"].notna()
            df.loc[mask, "print_sheet"] = df.loc[mask, "print_sheet_override"].astype("string")
            df = df.drop(columns=["print_sheet_override"])

    annotated = annotate_due(df, cfg=DueConfig(window_days=ISSUE_WINDOW_DAYS))
    annotated = annotated.drop(columns=["due_within_window"], errors="ignore")
    annotated = annotated.drop(columns=["継続"], errors="ignore")

    for col in [
        "license_no",
        "qualification",
        "qualification_category",
        "first_issue_date",
        "expiry_date",
        "birth_date",
        "next_stage_label",
        "next_notice_date",
        "next_surveillance_window",
        "retest_window",
        "next_procedure_status",
    ]:
        if col in annotated.columns:
            annotated[col] = annotated[col].astype("string").fillna("")

    _log(log, f"[issue] build_issue_dataframe result rows={len(annotated)}")
    return annotated.reset_index(drop=True)


def ensure_due_dataframe(
    duckdb_path: Path | str,
    *,
    log: Optional[Callable[[str], None]] = None,
) -> tuple[pd.DataFrame, bool]:
    """Return filtered due dataframe, regenerating if necessary.

    Returns tuple of (dataframe, regenerated_flag).
    """

    path = Path(duckdb_path)
    regenerated = False
    df = reapply_due_filters(path)
    if df is None or df.empty:
        fallback = build_issue_dataframe(path, log=log)
        if fallback is not None and not fallback.empty:
            try:
                df = write_due_tables(path, fallback)
                regenerated = True
                _log(log, "[issue] due table regenerated from roster")
            except Exception as exc:
                _log(log, f"[issue] write_due_tables failed, using fallback: {exc}")
                df = fallback
        else:
            df = pd.DataFrame()
    df = df.drop(columns=["due_within_window"], errors="ignore")
    return df, regenerated


def paginate_issue(
    df: pd.DataFrame,
    *,
    columns: Sequence[str],
    rows_per_page: int,
    max_pages: int | None = None,
) -> tuple[list[IssuePage], int]:
    if rows_per_page <= 0:
        rows_per_page = 40

    if df is None or df.empty:
        return [], 0

    normalized = _normalize_sheet_column(df)
    normalized = normalized.reset_index(drop=True)

    def _format(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, pd.Timestamp):
            if pd.isna(value):
                return ""
            return value.date().isoformat()
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        return str(value)

    pages: list[IssuePage] = []
    page_counter = 0
    total_pages = 0
    for sheet, chunk in normalized.groupby("print_sheet", sort=True):
        if chunk.empty:
            continue
        records = chunk.to_dict("records")
        sheet_total = max(1, math.ceil(len(records) / rows_per_page))
        total_pages += sheet_total
        for idx, offset in enumerate(range(0, len(records), rows_per_page), start=1):
            if max_pages is not None and page_counter >= max_pages:
                break
            subset = records[offset : offset + rows_per_page]
            formatted: list[dict[str, str]] = []
            for row in subset:
                formatted.append({col: _format(row.get(col)) for col in columns})
            page_counter += 1
            pages.append(
                IssuePage(
                    number=page_counter,
                    sheet=str(sheet or DEFAULT_SHEET),
                    sheet_page=idx,
                    sheet_total=sheet_total,
                    rows=formatted,
                )
            )
        if max_pages is not None and page_counter >= max_pages:
            continue
    return pages, total_pages


def list_issue_columns(df: pd.DataFrame) -> list[str]:
    """Return ordered list of columns usable for issuance display."""

    if df is None or df.empty:
        return list(DEFAULT_ISSUE_COLUMNS)
    ordered = [col for col in DEFAULT_ISSUE_COLUMNS if col in df.columns]
    extras = [col for col in df.columns if col not in ordered]
    return ordered + sorted(extras)
