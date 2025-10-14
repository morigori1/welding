from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from flask import Blueprint, current_app, render_template, request, url_for

from ..issue import (
    COLUMN_LABELS,
    COLUMN_WIDTHS,
    DEFAULT_ISSUE_COLUMNS,
    ensure_due_dataframe,
    list_issue_columns,
    paginate_issue,
)
from ..warehouse import (
    DEFAULT_SHEET,
    list_report_definitions,
)

SHEET_ALL_TOKEN = "__ALL__"
SHEET_ALL_LABEL = "全て"
ALLOWED_ORIENTATIONS = {"portrait", "landscape"}
PREVIEW_MAX_PAGES = 12

issue_bp = Blueprint(
    "issue",
    __name__,
    template_folder="templates",
)


def _logger() -> Any:
    logger = getattr(current_app, "logger", None)
    if logger is None:
        return None

    def _log(message: str) -> None:
        try:
            logger.info(message)
        except Exception:
            pass

    return _log


def _rows_per_page(arg_value: str | None, default: int) -> int:
    if not arg_value:
        return default
    try:
        value = int(arg_value)
    except (TypeError, ValueError):
        return default
    value = max(1, min(value, 500))
    return value


def _sheet_counts(df: pd.DataFrame) -> Dict[str, int]:
    if df is None or df.empty or "print_sheet" not in df.columns:
        return {}
    series = df["print_sheet"].astype("string").fillna(DEFAULT_SHEET)
    counts = series.value_counts(sort=False)
    return {str(idx): int(counts.loc[idx]) for idx in counts.index}


def _filter_by_sheet(df: pd.DataFrame, sheet: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    if sheet == SHEET_ALL_TOKEN:
        return df
    series = df["print_sheet"].astype("string").fillna(DEFAULT_SHEET)
    mask = series == sheet
    return df.loc[mask].copy()


def _build_issue_context(*, max_pages: int | None = None) -> Dict[str, Any]:
    config = current_app.config
    duckdb_path = Path(config["WELDING_DUCKDB_PATH"])
    log = _logger()
    df, regenerated = ensure_due_dataframe(duckdb_path, log=log)

    available_columns = list_issue_columns(df)
    default_columns = [col for col in DEFAULT_ISSUE_COLUMNS if col in available_columns]
    if not default_columns:
        default_columns = available_columns

    requested_columns = request.args.getlist("columns")
    selected_columns = [col for col in requested_columns if col in available_columns]
    if not selected_columns:
        selected_columns = default_columns

    unused_columns = [col for col in available_columns if col not in selected_columns]

    rows_default = int(config.get("WELDING_ROWS_PER_PAGE", 40))
    rows_arg = request.args.get("rows_per_page") or request.args.get("rows")
    rows_per_page = _rows_per_page(rows_arg, rows_default)

    orientation = request.args.get("orientation", "portrait")
    if orientation not in ALLOWED_ORIENTATIONS:
        orientation = "portrait"

    sheet_counts = _sheet_counts(df)
    total_rows = int(len(df)) if df is not None else 0
    sheet_options: List[Dict[str, Any]] = [
        {"value": SHEET_ALL_TOKEN, "label": SHEET_ALL_LABEL, "count": total_rows}
    ]
    for name in sorted(sheet_counts.keys()):
        label = name or DEFAULT_SHEET
        sheet_options.append({"value": name, "label": label, "count": sheet_counts[name]})

    selected_sheet = request.args.get("sheet", SHEET_ALL_TOKEN)
    valid_sheet_values = {opt["value"] for opt in sheet_options}
    if selected_sheet not in valid_sheet_values:
        selected_sheet = SHEET_ALL_TOKEN

    filtered_df = _filter_by_sheet(df, selected_sheet)
    filtered_count = int(len(filtered_df)) if filtered_df is not None else 0
    pages, page_total = paginate_issue(
        filtered_df,
        columns=selected_columns,
        rows_per_page=rows_per_page,
        max_pages=max_pages,
    )

    report_counts: Dict[str, int] = {}
    if filtered_df is not None and not filtered_df.empty and "report_ids" in filtered_df.columns:
        for value in filtered_df["report_ids"]:
            if isinstance(value, list):
                for rid in value:
                    rid_str = str(rid).strip()
                    if rid_str:
                        report_counts[rid_str] = report_counts.get(rid_str, 0) + 1
            elif pd.notna(value):
                rid_str = str(value).strip()
                if rid_str:
                    report_counts[rid_str] = report_counts.get(rid_str, 0) + 1

    report_defs_df = list_report_definitions(duckdb_path)
    report_definitions: List[Dict[str, Any]] = []
    definition_lookup: Dict[str, Dict[str, Any]] = {}
    if report_defs_df is not None and not report_defs_df.empty:
        for entry in report_defs_df.to_dict(orient="records"):
            report_id_value = str(entry.get("report_id") or "").strip()
            if not report_id_value:
                continue
            label_value = str(entry.get("label") or report_id_value)
            description_value = str(entry.get("description") or "")
            record = {"id": report_id_value, "label": label_value, "description": description_value, "count": report_counts.get(report_id_value, 0)}
            report_definitions.append(record)
            definition_lookup[report_id_value] = record

    for report_id_value, count in report_counts.items():
        entry = definition_lookup.get(report_id_value)
        if entry is None:
            entry = {"id": report_id_value, "label": report_id_value, "description": "", "count": count}
            report_definitions.append(entry)
            definition_lookup[report_id_value] = entry
        else:
            entry["count"] = count

    report_definitions.sort(key=lambda item: item["id"])
    report_options = report_definitions

    summary_reports: Dict[str, int] = {}
    for report_id_value, count in report_counts.items():
        if not count:
            continue
        entry = definition_lookup.get(report_id_value)
        label_value = entry.get("label") if entry else report_id_value
        summary_reports[label_value] = count

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    print_url = url_for(
        "issue.print_view",
        sheet=selected_sheet,
        rows_per_page=rows_per_page,
        orientation=orientation,
        columns=selected_columns,
    )
    preview_url = url_for(
        "issue.index",
        sheet=selected_sheet,
        rows_per_page=rows_per_page,
        orientation=orientation,
        columns=selected_columns,
    )

    page_total_value = page_total
    preview_limit = max_pages if max_pages is not None else 0
    preview_limited = max_pages is not None and page_total_value > len(pages)

    return {
        "pages": pages,
        "available_columns": available_columns,
        "selected_columns": selected_columns,
        "unused_columns": unused_columns,
        "column_labels": COLUMN_LABELS,
        "column_widths": COLUMN_WIDTHS,
        "sheet_options": sheet_options,
        "selected_sheet": selected_sheet,
        "sheet_all_token": SHEET_ALL_TOKEN,
        "rows_per_page": rows_per_page,
        "orientation": orientation,
        "generated_at": generated_at,
        "total_rows": total_rows,
        "filtered_rows": filtered_count,
        "regenerated": regenerated,
        "print_url": print_url,
        "preview_url": preview_url,
        "report_definitions": report_definitions,
        "report_options": report_options,
        "summary_reports": summary_reports,
        "page_total": page_total_value,
        "preview_limit": preview_limit,
        "preview_limited": preview_limited,
    }


@issue_bp.route("/", methods=["GET"])
def index() -> Any:
    context = _build_issue_context(max_pages=PREVIEW_MAX_PAGES)
    context.setdefault("title", "資格発行プレビュー")
    return render_template("issue/index.html", **context)


@issue_bp.route("/print", methods=["GET"])
def print_view() -> Any:
    context = _build_issue_context(max_pages=None)
    context.setdefault("title", "資格発行 印刷")
    context["is_print_view"] = True
    return render_template("issue/print.html", **context)
