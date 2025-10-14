from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from flask import Blueprint, current_app, jsonify, render_template, request, url_for

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
from ..print_archive import archive_print_run

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


def _serialize_pages(pages: List[Any], columns: List[str]) -> List[Dict[str, Any]]:
    serialized: List[Dict[str, Any]] = []
    for page in pages:
        rows = []
        for row in getattr(page, "rows", []):
            rows.append({col: row.get(col, "") for col in columns})
        serialized.append(
            {
                "sheet": getattr(page, "sheet", ""),
                "sheet_page": getattr(page, "sheet_page", 1),
                "sheet_total": getattr(page, "sheet_total", 1),
                "rows": rows,
            }
        )
    return serialized


def _content_digest(columns: List[str], pages: List[Dict[str, Any]]) -> str:
    bundle = {"columns": columns, "pages": pages}
    blob = json.dumps(bundle, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


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
    selected_sheet_label = next(
        (opt["label"] for opt in sheet_options if opt["value"] == selected_sheet),
        SHEET_ALL_LABEL if selected_sheet == SHEET_ALL_TOKEN else selected_sheet,
    )

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

    serialized_pages = _serialize_pages(pages, selected_columns)
    digest = _content_digest(selected_columns, serialized_pages)
    archive_payload = {
        "sheet": selected_sheet,
        "sheet_label": selected_sheet_label,
        "columns": selected_columns,
        "orientation": orientation,
        "rows_per_page": rows_per_page,
        "generated_at": generated_at,
        "page_total": page_total_value,
        "record_count": filtered_count,
        "pages": serialized_pages,
        "content_hash": digest,
    }

    return {
        "pages": pages,
        "pages_serialized": serialized_pages,
        "available_columns": available_columns,
        "selected_columns": selected_columns,
        "unused_columns": unused_columns,
        "column_labels": COLUMN_LABELS,
        "column_widths": COLUMN_WIDTHS,
        "sheet_options": sheet_options,
        "selected_sheet": selected_sheet,
        "selected_sheet_label": selected_sheet_label,
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
        "archive_payload": archive_payload,
        "archive_url": url_for("issue.archive_print"),
        "content_hash": digest,
        "filtered_df": filtered_df,
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


@issue_bp.route("/archive", methods=["POST"])
def archive_print() -> Any:
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"error": "Invalid JSON payload"}), 400

    columns = payload.get("columns")
    if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
        return jsonify({"error": "列情報(columns)が不正です"}), 400

    pages = payload.get("pages", [])
    if not isinstance(pages, list):
        return jsonify({"error": "ページ情報(pages)が不正です"}), 400

    rows: List[Dict[str, Any]] = []
    for page in pages:
        if not isinstance(page, dict):
            continue
        page_rows = page.get("rows", [])
        if not isinstance(page_rows, list):
            continue
        for row in page_rows:
            if isinstance(row, dict):
                rows.append({col: str(row.get(col, "")) for col in columns})

    df = pd.DataFrame(rows, columns=columns)
    duckdb_path = Path(current_app.config["WELDING_DUCKDB_PATH"])
    rows_per_page = payload.get("rows_per_page") or current_app.config.get("WELDING_ROWS_PER_PAGE", 40)
    orientation = payload.get("orientation", "portrait")
    page_total = payload.get("page_total", len(pages))
    record_count = payload.get("record_count", len(rows))
    sheet_value = str(payload.get("sheet") or "")
    sheet_label = str(payload.get("sheet_label") or sheet_value or SHEET_ALL_LABEL)
    generated_at = payload.get("generated_at")
    printed_at = payload.get("printed_at")
    content_hash = payload.get("content_hash", "")

    try:
        result = archive_print_run(
            duckdb_path=duckdb_path,
            payload=payload,
            df=df,
            columns=columns,
            sheet=sheet_value,
            sheet_label=sheet_label,
            orientation=str(orientation),
            rows_per_page=int(rows_per_page),
            page_total=int(page_total),
            record_count=int(record_count),
            generated_at=str(generated_at) if generated_at else None,
            printed_at=str(printed_at) if printed_at else None,
            content_hash=str(content_hash),
        )
    except Exception as exc:  # pragma: no cover - defensive
        return jsonify({"error": f"印刷履歴の保存でエラーが発生しました: {exc}"}), 500

    return (
        jsonify(
            {
                "status": "ok",
                "print_id": result.print_id,
                "content_hash": result.content_hash,
                "csv_path": str(result.csv_path),
                "payload_path": str(result.payload_path),
            }
        ),
        201,
    )
