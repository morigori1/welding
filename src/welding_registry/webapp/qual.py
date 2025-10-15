from __future__ import annotations

from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import duckdb
import pandas as pd
from flask import (
    Blueprint,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from ..db import to_duckdb
from ..io_excel import list_sheets, read_sheet, to_canonical
from ..normalize import add_positions_columns, normalize
from ..warehouse import (
    DEFAULT_SHEET,
    add_manual_qualification,
    add_report_definition,
    add_report_entry,
    delete_manual_qualification,
    delete_report_definition,
    list_qualifications,
    list_print_sheets,
    list_report_definitions,
    materialize_roster_all,
    remove_report_entry,
    update_manual_qualification,
)


SHEET_ALL_TOKEN = "__ALL__"

QUAL_COLUMN_LABELS = {
    "name": "氏名",
    "license_no": "免許番号",
    "qualification": "資格",
    "category": "資格種別",
    "continuation_status": "継続状況",
    "registration_date": "登録日",
    "first_issue_date": "初回交付",
    "issue_date": "交付日",
    "expiry_date": "有効期限",
    "birth_date": "生年月日",
    "source_sheet": "入力元シート",
    "print_sheet": "印刷シート",
    "source": "データ種別",
    "next_stage_label": "次回ステージ",
    "next_surveillance_window": "次回ｻｰﾍﾞｲﾗﾝｽ/再評価受験期間",
    "next_exam_period": "次回受験期間/再試験猶予",
    "next_procedure_status": "次回手続",
    "employee_id": "社員ID",
    "display_name": "表示名",
    "birth_year_west": "生年(西暦)",
    "last_updated": "最終更新",
    "report_ids": "レポート",
    "address": "自宅住所",
    "web_publish_no": "WEB申込番号",
    "sheet_source": "シート割当元",
}
QUAL_DEFAULT_COLUMNS = [
    "name",
    "license_no",
    "qualification",
    "category",
    "continuation_status",
    "registration_date",
    "first_issue_date",
    "issue_date",
    "expiry_date",
    "source_sheet",
    "print_sheet",
    "source",
    "next_stage_label",
    "next_surveillance_window",
    "next_procedure_status",
    "birth_date",
    "employee_id",
    "display_name",
    "birth_year_west",
    "address",
    "web_publish_no",
    "report_ids",
]
QUAL_SORTABLE_COLUMNS = [
    "name",
    "license_no",
    "qualification",
    "category",
    "continuation_status",
    "registration_date",
    "first_issue_date",
    "issue_date",
    "expiry_date",
    "source_sheet",
    "print_sheet",
    "next_stage_label",
    "next_exam_period",
    "next_surveillance_window",
    "next_procedure_status",
    "birth_date",
    "employee_id",
    "birth_year_west",
    "address",
    "web_publish_no",
    "sheet_source",
    "last_updated",
]
QUAL_DATE_COLUMNS = {"registration_date", "first_issue_date", "issue_date", "expiry_date", "birth_date", "last_updated"}
QUAL_SORT_LEVELS = 3



qual_bp = Blueprint(
    "qual",
    __name__,
    template_folder="templates",
)


def _duckdb_path() -> Path:
    config = current_app.config
    return Path(config["WELDING_DUCKDB_PATH"])


def _payload_from_request() -> Dict[str, Any]:
    if request.is_json:
        data = request.get_json(silent=True) or {}
    else:
        data = request.form.to_dict()
    return {k: v for k, v in data.items() if v is not None}


def _wants_json() -> bool:
    if request.is_json:
        return True
    accept = request.accept_mimetypes
    if accept:
        best = accept.best
        if best == "application/json" and accept[best] > accept["text/html"]:
            return True
    return False


def _redirect_with_next(fallback: str, *, error: str | None = None):
    next_url = request.values.get("next")
    if next_url and next_url.startswith("/"):
        if error:
            parsed = urlparse(next_url)
            params = parse_qs(parsed.query, keep_blank_values=True)
            params["error"] = [error]
            query = urlencode([(k, value) for k, values in params.items() for value in values])
            target = urlunparse(parsed._replace(query=query))
        else:
            target = next_url
        return redirect(target, code=303)
    if error:
        return redirect(url_for(fallback, error=error), code=303)
    return redirect(url_for(fallback), code=303)


def _qual_success(status: int = 200):
    if _wants_json():
        return jsonify({"status": "ok"}), status
    return _redirect_with_next("qual.qual_index")


def _qual_error(message: str, status: int = 400):
    if _wants_json():
        return jsonify({"status": "error", "message": message}), status
    return _redirect_with_next("qual.qual_index", error=message)


def _ingest_excel_to_roster(
    source_path: Path,
    *,
    duckdb_path: Path,
    sheet: str | int | None = None,
) -> int:
    if sheet is None:
        sheets = list_sheets(source_path)
        if not sheets:
            raise ValueError("シートが見つかりません。")
        sheet_name: str | int = sheets[0]
    else:
        sheet_name = sheet

    df_raw, _ = read_sheet(source_path, sheet_name)
    if df_raw.empty:
        raise ValueError("取り込むデータが見つかりません。")

    df = to_canonical(df_raw)

    if df.columns.duplicated().any():
        dup_names = [name for name, count in df.columns.value_counts().items() if count > 1]
        for name in dup_names:
            cols = [col for col in df.columns if col == name]
            merged = df.loc[:, cols].bfill(axis=1).iloc[:, 0]
            df[name] = merged
        df = df.loc[:, ~df.columns.duplicated()]

    if "license_no" not in df.columns:
        raise ValueError("資格一覧に登録番号の列が見つかりません。")

    df["source_sheet"] = str(sheet_name)

    try:
        df = add_positions_columns(df, source_col="qualification")
    except Exception:
        pass

    df = normalize(df)
    df["license_no"] = df["license_no"].astype("string").str.strip()
    df = df[df["license_no"] != ""].reset_index(drop=True)

    required_columns = [
        "registration_date",
        "first_issue_date",
        "issue_date",
        "expiry_date",
        "category",
        "continuation_status",
        "next_stage_label",
        "next_exam_period",
        "next_procedure_status",
        "print_sheet",
        "source_sheet",
    ]
    for column in required_columns:
        if column not in df.columns:
            df[column] = pd.Series([None] * len(df), dtype="object")

    fallback_df = pd.DataFrame()
    if duckdb_path.exists():
        try:
            with duckdb.connect(str(duckdb_path)) as con:
                try:
                    fallback_df = con.execute("SELECT * FROM roster_all").df()
                except duckdb.Error:
                    frames: List[pd.DataFrame] = []
                    for table in ("roster_manual", "roster"):
                        try:
                            frames.append(con.execute(f"SELECT * FROM {table}").df())
                        except duckdb.Error:
                            continue
                    if frames:
                        fallback_df = pd.concat(frames, ignore_index=True, sort=False)
        except Exception:
            fallback_df = pd.DataFrame()
    if not fallback_df.empty and "license_no" in fallback_df.columns:
        fallback_df = fallback_df.copy()
        fallback_df["license_no"] = fallback_df["license_no"].astype("string").str.strip()
        fallback_df = fallback_df[fallback_df["license_no"] != ""]
        fallback_df = fallback_df.drop_duplicates(subset=["license_no"], keep="first")
        fallback_df = fallback_df.set_index("license_no")

        def _is_missing(value: Any, column: str) -> bool:
            if value is None:
                return True
            if isinstance(value, float) and pd.isna(value):
                return True
            if isinstance(value, str):
                trimmed = value.strip()
                if not trimmed:
                    return True
                if column == "print_sheet" and trimmed.lower() == DEFAULT_SHEET.lower():
                    return True
            if isinstance(value, pd.Timestamp):
                return pd.isna(value)
            if isinstance(value, (list, tuple, set, dict)):
                return len(value) == 0
            try:
                return bool(pd.isna(value))
            except Exception:
                return False

        fallback_columns = [
            "registration_date",
            "first_issue_date",
            "issue_date",
            "expiry_date",
            "category",
            "continuation_status",
            "next_stage_label",
            "next_exam_period",
            "next_procedure_status",
            "print_sheet",
            "source_sheet",
        ]

        df_indexed = df.set_index("license_no")
        for column in df_indexed.columns:
            if column not in fallback_df.columns:
                fallback_df[column] = pd.NA
        fallback_df = fallback_df.reindex(columns=df_indexed.columns)

        for column in fallback_columns:
            if column not in df_indexed.columns:
                continue
            prev_series = fallback_df[column]
            if prev_series is None:
                continue
            mask = df_indexed[column].map(lambda value: _is_missing(value, column))
            if mask.any():
                replacement = prev_series.where(
                    ~prev_series.map(lambda value: _is_missing(value, column))
                )
                df_indexed.loc[mask, column] = replacement.loc[mask]

        df = df_indexed.reset_index()

    if "print_sheet" not in df.columns:
        df["print_sheet"] = DEFAULT_SHEET
    df["print_sheet"] = (
        df["print_sheet"]
        .astype("string")
        .fillna(DEFAULT_SHEET)
        .replace("", DEFAULT_SHEET)
    )
    df["source_sheet"] = df["source_sheet"].astype("string").fillna("")

    to_duckdb(df, duckdb_path)
    materialize_roster_all(duckdb_path)
    return int(len(df))


def _normalize_report_ids(df: pd.DataFrame) -> pd.DataFrame:
    if "report_ids" not in df.columns:
        df["report_ids"] = [[] for _ in range(len(df))]
        return df

    def _ensure_list(value: Any) -> List[str]:
        if isinstance(value, list):
            return [str(v) for v in value if v is not None and str(v).strip()]
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return []
        return [str(value)]

    df = df.copy()
    df["report_ids"] = df["report_ids"].apply(_ensure_list)
    return df


def _sheet_options(df: pd.DataFrame, field: str | None) -> List[Dict[str, Any]]:
    if df.empty or not field or field not in df.columns:
        return []
    series = df[field].astype("string").fillna("")
    counts = series.value_counts(sort=True)
    options: List[Dict[str, Any]] = []
    for key in counts.index:
        label = str(key) if str(key) else "未指定"
        options.append({"value": str(key), "label": label, "count": int(counts.loc[key])})
    return options


def _filter_by_sheet(df: pd.DataFrame, sheet: str, field: str | None) -> pd.DataFrame:
    if df is None or df.empty or not field or field not in df.columns:
        return df if df is not None else pd.DataFrame()
    if sheet == SHEET_ALL_TOKEN or not sheet:
        return df
    series = df[field].astype("string").fillna("")
    mask = series == sheet
    return df.loc[mask].copy()


def _serialize_row(raw: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in raw.items():
        if key == "report_ids":
            result[key] = value if isinstance(value, list) else []
            continue
        if isinstance(value, pd.Timestamp):
            if pd.isna(value):
                result[key] = ""
            else:
                result[key] = value.date().isoformat()
            continue
        if isinstance(value, datetime):
            if pd.isna(value):
                result[key] = ""
            else:
                result[key] = value.strftime("%Y-%m-%d")
            continue
        if value is None or (isinstance(value, float) and pd.isna(value)):
            result[key] = ""
            continue
        result[key] = str(value)
    if "report_ids" not in result:
        result["report_ids"] = []
    sheet_override = result.get("sheet_source") == "manual"
    result["is_manual"] = sheet_override or (result.get("source") == "manual")
    return result


@qual_bp.route("/", methods=["GET"])



def qual_index() -> Any:
    duck_path = _duckdb_path()

    raw_sort_columns = request.args.getlist("sort")
    raw_sort_orders = request.args.getlist("order")
    sort_columns_active: List[str] = []
    sort_orders_active: List[str] = []
    for idx, value in enumerate(raw_sort_columns):
        column_name = (value or "").strip()
        if not column_name or column_name not in QUAL_SORTABLE_COLUMNS:
            continue
        sort_columns_active.append(column_name)
        order_value = (raw_sort_orders[idx] if idx < len(raw_sort_orders) else "asc") or "asc"
        order_value = order_value.lower()
        if order_value not in {"asc", "desc"}:
            order_value = "asc"
        sort_orders_active.append(order_value)

    if sort_columns_active and len(sort_orders_active) < len(sort_columns_active):
        sort_orders_active.extend(["asc"] * (len(sort_columns_active) - len(sort_orders_active)))

    ascending_flags = [order != "desc" for order in sort_orders_active] if sort_columns_active else []

    df_all = list_qualifications(
        duck_path,
        sort_by=sort_columns_active or None,
        ascending=ascending_flags or True,
    )

    df_all = _normalize_report_ids(df_all)

    selected_sort_columns = [
        sort_columns_active[idx] if idx < len(sort_columns_active) else ""
        for idx in range(QUAL_SORT_LEVELS)
    ]
    selected_sort_orders = [
        sort_orders_active[idx] if idx < len(sort_orders_active) else "asc"
        for idx in range(QUAL_SORT_LEVELS)
    ]
    sort_indices = list(range(QUAL_SORT_LEVELS))

    available_columns = [col for col in QUAL_COLUMN_LABELS if col in df_all.columns or col == "report_ids"]
    requested_columns = request.args.getlist("columns")
    default_columns = [col for col in QUAL_DEFAULT_COLUMNS if col in available_columns]
    selected_columns = [col for col in (requested_columns or default_columns) if col in available_columns]
    if not selected_columns:
        selected_columns = available_columns

    column_options = [
        {"value": col, "label": QUAL_COLUMN_LABELS[col], "checked": col in selected_columns}
        for col in available_columns
    ]
    sort_options = [
        {"value": col, "label": QUAL_COLUMN_LABELS[col]}
        for col in QUAL_SORTABLE_COLUMNS
        if col in df_all.columns
    ]

    if "print_sheet" in df_all.columns:
        sheet_field = "print_sheet"
    elif "source_sheet" in df_all.columns:
        sheet_field = "source_sheet"
    else:
        sheet_field = None
    sheet_options = _sheet_options(df_all, sheet_field)
    if sheet_field == "print_sheet":
        known_print_sheets = list_print_sheets(duck_path)
        existing_values = {opt["value"] for opt in sheet_options}
        for name in known_print_sheets:
            value = str(name)
            if value not in existing_values:
                label = value if value else "���w��"
                sheet_options.append({"value": value, "label": label, "count": 0})
                existing_values.add(value)
        sheet_options.sort(key=lambda option: option["label"])

    query = (request.args.get("q") or "").strip()
    selected_sheet = (request.args.get("sheet") or "").strip()
    selected_license = (request.args.get("selected") or "").strip()
    error_message = request.args.get("error")

    df_filtered = df_all.copy()
    if selected_sheet:
      df_filtered = _filter_by_sheet(df_filtered, selected_sheet, sheet_field)

    if query:
        lowered = query.lower()
        masks: list[pd.Series] = []
        for col in ("name", "license_no", "qualification"):
            if col in df_filtered.columns:
                series = df_filtered[col].astype("string").str.lower()
                masks.append(series.str.contains(lowered, na=False))
        if masks:
            mask = masks[0]
            for series in masks[1:]:
                mask = mask | series
            df_filtered = df_filtered.loc[mask]

    rows = [_serialize_row(row) for row in df_filtered.to_dict(orient="records")]

    prefill_serialized = None
    if selected_license:
        matches = df_all[df_all["license_no"].astype("string") == selected_license]
        if not matches.empty:
            prefill_serialized = _serialize_row(matches.iloc[0].to_dict())

    def _prefill(key: str) -> str:
        if prefill_serialized is None:
            return ""
        value = prefill_serialized.get(key)
        if value is None:
            return ""
        if isinstance(value, list):
            return value[0] if value else ""
        return value

    form_initial = {
        "name": _prefill("name"),
        "license_no": _prefill("license_no"),
        "qualification": _prefill("qualification"),
        "category": _prefill("category"),
        "continuation_status": _prefill("continuation_status"),
        "registration_date": _prefill("registration_date"),
        "first_issue_date": _prefill("first_issue_date"),
        "issue_date": _prefill("issue_date"),
        "expiry_date": _prefill("expiry_date"),
        "next_stage_label": _prefill("next_stage_label"),
        "next_exam_period": _prefill("next_exam_period"),
        "next_procedure_status": _prefill("next_procedure_status"),
        "employee_id": _prefill("employee_id"),
        "birth_year_west": _prefill("birth_year_west"),
        "print_sheet": _prefill("print_sheet"),
        "source_sheet": _prefill("source_sheet"),
        "mode": "update" if prefill_serialized and prefill_serialized.get("source") == "manual" else "add",
    }

    report_initial = {
        "license_no": _prefill("license_no"),
        "report_id": "",
        "note": "",
    }

    total_rows = len(df_filtered)
    manual_count = None
    if not df_filtered.empty and "source" in df_filtered.columns:
        manual_count = int((df_filtered["source"].astype("string") == "manual").sum())

    report_counts: Dict[str, int] = {}
    for row in rows:
        for rid in row.get("report_ids", []):
            rid_str = str(rid)
            report_counts[rid_str] = report_counts.get(rid_str, 0) + 1

    report_defs_df = list_report_definitions(duck_path)
    report_definitions: List[Dict[str, Any]] = []
    definition_lookup: Dict[str, Dict[str, Any]] = {}

    if report_defs_df is not None and not report_defs_df.empty:
        for entry in report_defs_df.to_dict(orient="records"):
            report_id_value = str(entry.get("report_id") or "")
            if not report_id_value:
                continue
            label_value = str(entry.get("label") or report_id_value)
            description_value = str(entry.get("description") or "")
            record = {"id": report_id_value, "label": label_value, "description": description_value, "count": 0}
            report_definitions.append(record)
            definition_lookup[report_id_value] = record

    for report_id_value in sorted(report_counts.keys()):
        entry = definition_lookup.get(report_id_value)
        if entry is None:
            entry = {"id": report_id_value, "label": report_id_value, "description": "", "count": 0}
            report_definitions.append(entry)
            definition_lookup[report_id_value] = entry
        entry["count"] = report_counts[report_id_value]

    report_definitions.sort(key=lambda item: item["id"])
    report_options = report_definitions

    summary_reports: Dict[str, int] = {}
    for report_id_value in sorted(report_counts.keys()):
        count = report_counts[report_id_value]
        if not count:
            continue
        entry = definition_lookup.get(report_id_value)
        label_value = entry.get("label") if entry else report_id_value
        summary_reports[label_value] = count

    summary = {
        "total": total_rows,
        "manual": manual_count,
        "reports": summary_reports,
    }

    return render_template(
        "qual/index.html",
        title="資格一覧",
        qualifications=rows,
        summary=summary,
        sheets=sheet_options,
        sheet_field=sheet_field,
        selected_sheet=selected_sheet,
        selected_license=selected_license,
        search=query,
        report_options=report_options,
        report_definitions=report_definitions,
        form_initial=form_initial,
        report_initial=report_initial,
        error_message=error_message,
        column_options=column_options,
        selected_columns=selected_columns,
        column_labels=QUAL_COLUMN_LABELS,
        date_columns=QUAL_DATE_COLUMNS,
        sort_options=sort_options,
        sort_levels=QUAL_SORT_LEVELS,
        sort_indices=sort_indices,
        selected_sort_columns=selected_sort_columns,
        selected_sort_orders=selected_sort_orders,
        sort_columns_active=sort_columns_active,
        sort_orders_active=sort_orders_active,
    )


@qual_bp.route("/import", methods=["POST"])
def qual_import() -> Any:
    file_obj = request.files.get("excel") or request.files.get("file")
    if file_obj is None or not file_obj.filename:
        return _qual_error("エクセルファイルを選択してください。")

    suffix = Path(file_obj.filename).suffix.lower()
    if suffix not in {".xls", ".xlsx"}:
        return _qual_error("Excelファイル（.xls / .xlsx）のみ取り込めます。")

    sheet_hint = (request.form.get("sheet") or "").strip()
    sheet_value: str | int | None = None
    if sheet_hint:
        try:
            sheet_value = int(sheet_hint)
        except ValueError:
            sheet_value = sheet_hint

    duck_path = _duckdb_path()
    temp_path: Path | None = None
    try:
        with NamedTemporaryFile(delete=False, suffix=suffix or ".xlsx") as tmp:
            temp_path = Path(tmp.name)
        file_obj.save(str(temp_path))
        rows = _ingest_excel_to_roster(
            temp_path,
            duckdb_path=duck_path,
            sheet=sheet_value,
        )
    except ValueError as exc:
        return _qual_error(str(exc))
    except Exception as exc:
        logger = getattr(current_app, "logger", None)
        if logger is not None:
            try:
                logger.exception("Excel import failed", exc_info=exc)
            except Exception:
                pass
        return _qual_error("エクセルの取り込みに失敗しました。")
    finally:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except Exception:
                pass

    if _wants_json():
        return jsonify({"status": "ok", "imported_rows": rows})
    return _redirect_with_next("qual.qual_index")


@qual_bp.route("/manual", methods=["POST"])
def manual_add() -> Any:
    payload = _payload_from_request()
    name = payload.get("name")
    license_no = payload.get("license_no")
    if not name or not license_no:
        return _qual_error("name and license_no are required")
    action = str(payload.get("mode") or payload.get("action") or "add").lower()
    first_issue = payload.get("first_issue_date") or payload.get("first_issue")
    print_sheet = payload.get("print_sheet")
    source_sheet = payload.get("source_sheet") or payload.get("source")

    def _sanitize(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            trimmed = value.strip()
            return trimmed if trimmed else None
        return value

    manual_kwargs = {
        "qualification": payload.get("qualification"),
        "registration_date": payload.get("registration_date"),
        "first_issue_date": first_issue,
        "issue_date": payload.get("issue_date"),
        "expiry_date": payload.get("expiry_date"),
        "category": payload.get("category"),
        "continuation_status": payload.get("continuation_status"),
        "next_stage_label": payload.get("next_stage_label"),
        "next_exam_period": payload.get("next_exam_period"),
        "next_procedure_status": payload.get("next_procedure_status"),
        "print_sheet": print_sheet,
        "source_sheet": source_sheet,
        "employee_id": payload.get("employee_id"),
        "birth_year_west": payload.get("birth_year_west"),
        "birth_date": payload.get("birth_date"),
        "address": payload.get("address"),
        "web_publish_no": payload.get("web_publish_no"),
    }
    manual_kwargs = {key: _sanitize(value) for key, value in manual_kwargs.items()}

    fallback_columns = [
        "registration_date",
        "first_issue_date",
        "issue_date",
        "expiry_date",
        "category",
        "continuation_status",
        "next_stage_label",
        "next_exam_period",
        "next_procedure_status",
        "print_sheet",
        "source_sheet",
        "birth_date",
        "address",
        "web_publish_no",
    ]
    missing_columns = [col for col in fallback_columns if manual_kwargs.get(col) is None]
    if missing_columns:
        duck_path = _duckdb_path()
        fallback_df = pd.DataFrame()
        try:
            with duckdb.connect(str(duck_path)) as con:
                try:
                    fallback_df = con.execute(
                        """
                        SELECT *
                        FROM roster_all
                        WHERE license_no = ?
                        ORDER BY
                            CASE WHEN source = 'manual' THEN 0 ELSE 1 END,
                            last_updated DESC
                        LIMIT 1
                        """,
                        [license_no],
                    ).df()
                except duckdb.Error:
                    fallback_df = pd.DataFrame()
                if fallback_df.empty:
                    try:
                        fallback_df = con.execute(
                            """
                            SELECT *
                            FROM roster
                            WHERE license_no = ?
                            LIMIT 1
                            """,
                            [license_no],
                        ).df()
                    except duckdb.Error:
                        fallback_df = pd.DataFrame()
        except Exception:
            fallback_df = pd.DataFrame()

        if not fallback_df.empty:
            fallback_row = fallback_df.iloc[0].to_dict()

            def _has_value(value: Any, column: str) -> bool:
                if value is None:
                    return False
                if isinstance(value, float) and pd.isna(value):
                    return False
                if isinstance(value, str):
                    trimmed = value.strip()
                    if not trimmed:
                        return False
                    if column == "print_sheet" and trimmed.lower() == DEFAULT_SHEET.lower():
                        return False
                return True

            for column in missing_columns:
                fallback_value = fallback_row.get(column)
                if _has_value(fallback_value, column):
                    manual_kwargs[column] = fallback_value

    try:
        if action == "update":
            try:
                update_manual_qualification(
                    _duckdb_path(),
                    name=name,
                    license_no=license_no,
                    **manual_kwargs,
                )
            except ValueError:
                add_manual_qualification(
                    _duckdb_path(),
                    name=name,
                    license_no=license_no,
                    **manual_kwargs,
                )
        else:
            add_manual_qualification(
                _duckdb_path(),
                name=name,
                license_no=license_no,
                **manual_kwargs,
            )
    except ValueError as exc:
        return _qual_error(str(exc))
    return _qual_success()



@qual_bp.route("/manual/<license_no>/delete", methods=["POST"])
def manual_delete(license_no: str) -> Any:
    payload = _payload_from_request()
    name = payload.get("name")
    if not name:
        return _qual_error("name is required to delete")
    try:
        delete_manual_qualification(
            _duckdb_path(),
            name=name,
            license_no=license_no,
        )
    except ValueError as exc:
        return _qual_error(str(exc))
    return _qual_success()


@qual_bp.route("/report", methods=["POST"])
def report_add() -> Any:
    payload = _payload_from_request()
    report_id = payload.get("report_id") or payload.get("report")
    license_no = payload.get("license_no")
    note = payload.get("note")
    if not report_id or not license_no:
        return _qual_error("report_id and license_no are required")
    try:
        add_report_entry(
            _duckdb_path(),
            report_id=report_id,
            license_no=license_no,
            note=note,
        )
    except ValueError as exc:
        return _qual_error(str(exc))
    return _qual_success()


@qual_bp.route("/report/<report_id>/<license_no>/delete", methods=["POST"])
def report_delete(report_id: str, license_no: str) -> Any:
    try:
        remove_report_entry(
            _duckdb_path(),
            report_id=report_id,
            license_no=license_no,
        )
    except ValueError as exc:
        return _qual_error(str(exc))
    return _qual_success()


@qual_bp.route("/report/definitions", methods=["POST"])
def report_definition_add_route() -> Any:
    payload = _payload_from_request()
    report_id = payload.get("report_id") or payload.get("id")
    label = payload.get("label")
    description = payload.get("description")
    if not report_id:
        return _qual_error("report_id is required")
    try:
        add_report_definition(
            _duckdb_path(),
            report_id=report_id,
            label=label,
            description=description,
        )
    except ValueError as exc:
        return _qual_error(str(exc))
    return _qual_success()


@qual_bp.route("/report/definitions/<report_id>/delete", methods=["POST"])
def report_definition_delete_route(report_id: str) -> Any:
    try:
        delete_report_definition(
            _duckdb_path(),
            report_id=report_id,
        )
    except ValueError as exc:
        return _qual_error(str(exc))
    return _qual_success()
