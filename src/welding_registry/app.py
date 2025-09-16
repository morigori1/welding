from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import duckdb  # type: ignore
from flask import Flask, render_template, request, redirect, url_for
from datetime import datetime

from .normalize import name_key
from .review import ReviewStore
from .versioned import asof_dataframe, ingest_snapshot, ingest_snapshot_df
from .csvdb import (
    ensure_dirs as csv_ensure,
    read_asof_csv,
    write_asof_csv,
    get_person_list as csv_persons,
    get_qualification_list as csv_quals,
    log_display_selection,
)
from .io_excel import to_canonical
from .paths import resolve_duckdb_path, resolve_review_db_path
from .warehouse import materialize_roster_all
import pandas as _pd
import uuid
from werkzeug.utils import secure_filename


def create_app(warehouse: Optional[Path] = None, review_db: Optional[Path] = None) -> Flask:
    wh = resolve_duckdb_path(warehouse)
    rv = resolve_review_db_path(review_db)
    app = Flask(__name__)
    store = ReviewStore(rv)

    def _con():
        return duckdb.connect(str(wh))

    def _workers_dept_map() -> dict[str, str]:
        """Return name -> department mapping if workers table exists.
        Tries common column names for department/所属.
        """
        candidates = [
            "department",
            "dept",
            "所属",
            "部署",
            "部門",
            "課",
            "グループ",
        ]
        out: dict[str, str] = {}
        with _con() as con:
            try:
                has = bool(
                    con.execute(
                        "SELECT 1 FROM information_schema.tables WHERE table_name='workers'"
                    ).fetchone()
                )
                if not has:
                    return {}
                cols = [
                    r[0]
                    for r in con.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_name='workers'"
                    ).fetchall()
                ]
                target = None
                for c in candidates:
                    if c in cols:
                        target = c
                        break
                if target is None:
                    # sometimes department-like info is in 'department_name'
                    for c in cols:
                        if any(tok in c.lower() for tok in ("dept", "department")):
                            target = c
                            break
                if target is None:
                    return {}
                dfw = con.execute(
                    f"SELECT name, {target} as dept FROM workers WHERE name IS NOT NULL"
                ).df()
                for _, r in dfw.dropna(subset=["name"]).iterrows():
                    nm = str(r["name"]).strip()
                    dp = str(r.get("dept") or "").strip()
                    if nm and dp and nm not in out:
                        out[nm] = dp
            except Exception:
                return {}
        return out

    @app.route("/")
    def index():
        q = request.args.get("q", "").strip()
        only_active = request.args.get("active") == "1"
        persons = []
        with _con() as con:
            # roster may or may not have 'name' column; guard clauses
            has_roster = bool(
                con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name='roster'"
                ).fetchone()
            )
            cols = []
            if has_roster:
                cols = [
                    r[0]
                    for r in con.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_name='roster'"
                    ).fetchall()
                ]
            if "name" in cols:
                sql = "SELECT name, COUNT(*) as n FROM roster GROUP BY name ORDER BY name"
                df = con.execute(sql).df()
                persons = [(r["name"], r["n"]) for _, r in df.iterrows()]
            if q:
                persons = [(n, c) for (n, c) in persons if q in str(n)]
            # If active filter and workers table exists, intersect by names
            if (
                only_active
                and con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name='workers'"
                ).fetchone()
            ):
                w = (
                    con.execute("SELECT DISTINCT name FROM workers WHERE name IS NOT NULL")
                    .df()["name"]
                    .dropna()
                    .astype(str)
                    .tolist()
                )
                wset = set(w)
                persons = [(n, c) for (n, c) in persons if str(n) in wset]
        return render_template("index.html", persons=persons, q=q, only_active=only_active)

    # ---- Versioned views ----
    @app.get("/ver")
    def ver_index():
        # List snapshots and show upload form (Excel/CSV)
        snaps = []
        with _con() as con:
            try:
                df = con.execute(
                    "SELECT snapshot_id, snapshot_date, row_count, source_path FROM ver_snapshots ORDER BY snapshot_date DESC"
                ).df()
                snaps = df.to_dict("records") if not df.empty else []
            except Exception:
                snaps = []
        return render_template("ver_index.html", snapshots=snaps)

    # Simple as-of list view (for direct links)
    @app.get("/ver/asof/<date>")
    def ver_asof(date: str):
        df = read_asof_csv(date)
        if df is None:
            try:
                ddf = asof_dataframe(duckdb_path=wh, date=date)
                write_asof_csv(ddf, date=date)
                df = read_asof_csv(date)
            except Exception:
                df = None
        rows = (
            []
            if df is None
            else df.sort_values(["name", "qualification", "license_no"], kind="stable").to_dict(
                "records"
            )
        )
        return render_template(
            "ver_print.html",
            date=date,
            mode="person",
            operator="",
            persons=[],
            quals=[],
            rows=rows,
            session_id="",
        )

    @app.post("/ver/snapshot")
    def ver_snapshot_post():
        # Accept file upload (xlsx/xls/csv) + optional date
        f = request.files.get("file")
        date = request.form.get("date") or None
        if not f:
            return redirect(url_for("ver_index"))
        name = secure_filename(f.filename)
        ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
        tmpdir = Path("out/tmp_uploads")
        tmpdir.mkdir(parents=True, exist_ok=True)
        tmppath = tmpdir / f"{uuid.uuid4().hex}_{name}"
        f.save(str(tmppath))
        try:
            if ext in ("xlsx", "xlsm", "xls"):
                meta = ingest_snapshot(tmppath, duckdb_path=wh, snapshot_date=date)
                try:
                    ddf = asof_dataframe(duckdb_path=wh, date=meta.snapshot_date)
                    write_asof_csv(ddf, date=str(meta.snapshot_date.date()))
                except Exception:
                    pass
            elif ext == "csv":
                df = _csv_to_norm_df(tmppath)
                meta = ingest_snapshot_df(
                    df, duckdb_path=wh, snapshot_date=date, source_path=tmppath
                )
                try:
                    ddf = asof_dataframe(duckdb_path=wh, date=meta.snapshot_date)
                    write_asof_csv(ddf, date=str(meta.snapshot_date.date()))
                except Exception:
                    pass
            else:
                return redirect(url_for("ver_index"))
        finally:
            try:
                tmppath.unlink(missing_ok=True)
            except Exception:
                pass
        return redirect(url_for("ver_index"))

    @app.get("/ver/person")
    def ver_person():
        # Show as-of details for one person
        name = request.args.get("name", "").strip()
        date = request.args.get("date", datetime.now().strftime("%Y-%m-%d"))
        rows = []
        if name:
            try:
                df = asof_dataframe(duckdb_path=wh, date=date)
                df = df[df["name"].astype(str) == name]
                rows = df.sort_values(["expiry_date"], ascending=[False]).to_dict("records")
            except Exception:
                rows = []
        return render_template("ver_person.html", name=name, date=date, rows=rows)

    @app.get("/ver/csv")
    def ver_csv_input():
        # simple CSV upload form -> preview diff -> commit
        return render_template("ver_csv_input.html")

    def _csv_to_norm_df(path: Path) -> _pd.DataFrame:
        from .csvdb import read_csv_robust

        df = read_csv_robust(path)
        try:
            df = to_canonical(df)
        except Exception:
            pass
        return df

    def _diff_against_open(df_norm: _pd.DataFrame):
        """Compare incoming normalized rows vs current open assignments.
        Returns (added_keys, removed_keys, changed_summaries).
        """
        from .versioned import _record_key

        df = df_norm.copy()
        df["_rec_key"] = df.apply(_record_key, axis=1)
        new_keys = set(df["_rec_key"].astype(str))
        new_map: dict[str, _pd.Series] = {str(r["_rec_key"]): r for _, r in df.iterrows()}
        open_now = set()
        old_map: dict[str, dict[str, Any]] = {}
        with _con() as con:
            try:
                base = con.execute(
                    "SELECT rec_key, license_no, qualification, category, first_issue_date, issue_date, expiry_date FROM ver_assignments WHERE valid_to IS NULL"
                ).df()
                if not base.empty:
                    open_now = set(base["rec_key"].astype(str))
                    old_map = {str(r["rec_key"]): r.to_dict() for _, r in base.iterrows()}
            except Exception:
                open_now = set()
                old_map = {}
        added = sorted(new_keys - open_now)
        removed = sorted(open_now - new_keys)
        changed: list[str] = []
        fields = ["category", "first_issue_date", "issue_date", "expiry_date"]

        def _stringify(val: object) -> str:
            iso = getattr(val, "isoformat", None)
            if callable(iso):
                try:
                    return str(iso())
                except Exception:
                    return str(val)
            return str(val)

        for key in sorted(new_keys & open_now):
            new_row = new_map.get(key)
            if new_row is None:
                continue
            old_row = old_map.get(key, {})
            diffs: list[str] = []
            for field in fields:
                nv = new_row.get(field, None)
                ov = old_row.get(field)
                nv_fmt = _stringify(nv)
                ov_fmt = _stringify(ov)
                if nv_fmt != ov_fmt:
                    diffs.append(f"{field}: {ov_fmt} -> {nv_fmt}")
            if diffs:
                changed.append(f"{key} | " + "; ".join(diffs))
        return added, removed, changed

    @app.post("/ver/csv/preview")
    def ver_csv_preview():
        f = request.files.get("file")
        date = request.form.get("date") or None
        if not f:
            return redirect(url_for("ver_csv_input"))
        name = secure_filename(f.filename or f"upload_{uuid.uuid4().hex}.csv")
        tmpdir = Path("out/tmp_uploads")
        tmpdir.mkdir(parents=True, exist_ok=True)
        tmppath = tmpdir / f"{uuid.uuid4().hex}_{name}"
        f.save(str(tmppath))
        # If user accidentally uploaded Excel here, handle gracefully
        try:
            ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
        except Exception:
            ext = ""
        if ext in ("xlsx", "xls"):
            from .versioned import read_snapshot_xls

            df_norm, _ = read_snapshot_xls(tmppath, None)
            added, removed, changed = _diff_against_open(df_norm)
            token = tmppath.name
            return render_template(
                "ver_xlsx_preview.html",
                date=date,
                sheet=None,
                token=token,
                added=added,
                removed=removed,
                changed=changed,
                filename=name,
            )

        # Build preview diff vs current open assignments (CSV path)
        from .versioned import _normalize_snapshot_df

        df = _csv_to_norm_df(tmppath)
        df = _normalize_snapshot_df(df)
        added, removed, changed = _diff_against_open(df)
        token = tmppath.name
        return render_template(
            "ver_csv_preview.html",
            date=date,
            token=token,
            added=added,
            removed=removed,
            changed=changed,
            filename=name,
        )

    @app.post("/ver/csv/commit")
    def ver_csv_commit():
        token = request.form.get("token")
        date = request.form.get("date") or None
        if not token:
            return redirect(url_for("ver_csv_input"))
        tmppath = Path("out/tmp_uploads") / token
        if not tmppath.exists():
            return redirect(url_for("ver_csv_input"))
        try:
            df = _csv_to_norm_df(tmppath)
            ingest_snapshot_df(df, duckdb_path=wh, snapshot_date=date, source_path=tmppath)
        finally:
            try:
                tmppath.unlink(missing_ok=True)
            except Exception:
                pass
        return redirect(url_for("ver_index"))

    # ----- Excel (XLSX/XLS) diff preview + commit -----
    @app.get("/ver/xlsx")
    def ver_xlsx_input():
        return render_template("ver_xlsx_input.html")

    @app.post("/ver/xlsx/preview")
    def ver_xlsx_preview():
        f = request.files.get("file")
        token = request.form.get("token")
        date = request.form.get("date") or None
        sheet = request.form.get("sheet") or None
        tmpdir = Path("out/tmp_uploads")
        tmpdir.mkdir(parents=True, exist_ok=True)
        if f:
            name = secure_filename(f.filename or f"upload_{uuid.uuid4().hex}.xlsx")
            tmppath = tmpdir / f"{uuid.uuid4().hex}_{name}"
            f.save(str(tmppath))
        elif token:
            tmppath = tmpdir / token
            name = tmppath.name
            if not tmppath.exists():
                return redirect(url_for("ver_xlsx_input"))
        else:
            return redirect(url_for("ver_xlsx_input"))
        # If sheet is not specified and multiple sheets exist, ask to choose
        try:
            import pandas as _p

            with _p.ExcelFile(tmppath) as xf:
                names = list(map(str, xf.sheet_names))
        except Exception:
            names = []
        if (not sheet) and names and len(names) > 1:
            return render_template(
                "ver_xlsx_select.html", token=tmppath.name, date=date, sheets=names, filename=name
            )
        # Build preview diff vs current open assignments
        from .versioned import read_snapshot_xls

        df_norm, _ = read_snapshot_xls(tmppath, sheet)
        added, removed, changed = _diff_against_open(df_norm)
        token = tmppath.name
        return render_template(
            "ver_xlsx_preview.html",
            date=date,
            sheet=sheet,
            token=token,
            added=added,
            removed=removed,
            changed=changed,
            filename=name,
        )

    @app.post("/ver/xlsx/commit")
    def ver_xlsx_commit():
        token = request.form.get("token")
        date = request.form.get("date") or None
        sheet = request.form.get("sheet") or None
        if not token:
            return redirect(url_for("ver_xlsx_input"))
        tmppath = Path("out/tmp_uploads") / token
        if not tmppath.exists():
            return redirect(url_for("ver_xlsx_input"))
        try:
            meta = ingest_snapshot(tmppath, duckdb_path=wh, snapshot_date=date, sheet=sheet)
            try:
                ddf = asof_dataframe(duckdb_path=wh, date=meta.snapshot_date)
                write_asof_csv(ddf, date=str(meta.snapshot_date.date()))
            except Exception:
                pass
        finally:
            try:
                tmppath.unlink(missing_ok=True)
            except Exception:
                pass
        return redirect(url_for("ver_index"))

    # ---------------- Editor (exclusive select and print) ----------------
    @app.get("/ver/editor")
    def ver_editor():
        # mode: person or qualification (exclusive)
        date = request.args.get("date") or datetime.now().strftime("%Y-%m-%d")
        mode = request.args.get("mode") or "person"
        qual_filter = request.args.get("qual_filter", "").strip()
        # Ensure CSV exists for the date; create from DuckDB if missing
        df_csv = read_asof_csv(date)
        if df_csv is None:
            try:
                df = asof_dataframe(duckdb_path=wh, date=date)
                write_asof_csv(df, date=date)
                df_csv = df
            except Exception:
                df_csv = None
        # Build persons with department labels
        persons = []
        quals = []
        persons_aug = []
        if df_csv is not None:
            persons = csv_persons(date)
            quals = csv_quals(date)
            dept_map = _workers_dept_map()
            for name, cnt in persons:
                persons_aug.append(
                    {"name": name, "count": cnt, "dept": dept_map.get(str(name), "")}
                )
        return render_template(
            "ver_editor.html",
            date=date,
            mode=mode,
            persons=persons_aug,
            quals=quals,
            qual_filter=qual_filter,
        )

    @app.post("/ver/editor/preview")
    def ver_editor_preview():
        date = request.form.get("date") or datetime.now().strftime("%Y-%m-%d")
        mode = request.form.get("mode") or "person"
        operator = request.form.get("operator") or ""
        qual_filter = request.form.get("qual_filter", "").strip()
        rows_per_page = int(request.form.get("rows_per_page", "40") or 40)
        selected_persons = request.form.getlist("persons")
        selected_quals = request.form.getlist("quals")
        selected_keys = set(request.form.getlist("rowsel"))  # optional prior selection
        # Auto-switch mode if only the other selection is present
        if selected_quals and not selected_persons:
            mode = "qualification"
        elif selected_persons and not selected_quals:
            mode = "person"
        # Load CSV and filter
        df = read_asof_csv(date)
        if df is None:
            # Try to generate then reload
            ddf = asof_dataframe(duckdb_path=wh, date=date)
            write_asof_csv(ddf, date=date)
            df = read_asof_csv(date)
        if df is None:
            rows = []
        else:
            if mode == "person":
                mask = (
                    df["name"].astype(str).isin(set(selected_persons))
                    if selected_persons
                    else df["name"].astype(bool)
                )
            else:
                mask = (
                    df["qualification"].astype(str).isin(set(selected_quals))
                    if selected_quals
                    else df["qualification"].astype(bool)
                )
            if qual_filter:
                mask = mask & df["qualification"].astype(str).str.contains(qual_filter)
            df2 = df[mask].copy()
            # attach department
            dept_map = _workers_dept_map()
            if "name" in df2.columns and dept_map:
                df2["dept"] = df2["name"].map(lambda x: dept_map.get(str(x), ""))
            # add stable key for selection (rec_key)
            from .versioned import _record_key

            df2["rec_key"] = df2.apply(_record_key, axis=1)
            # apply previous selection if any
            if selected_keys:
                df2 = df2[df2["rec_key"].astype(str).isin(selected_keys)]
            # final rows
            df2 = df2.sort_values(["name", "qualification", "license_no"], kind="stable")
            rows = df2.to_dict("records")
        # Generate a transient session id for save
        sess = uuid.uuid4().hex
        # Chunk pages for print
        pages = []
        for i in range(0, len(rows), rows_per_page):
            pages.append({"no": (i // rows_per_page) + 1, "rows": rows[i : i + rows_per_page]})
        return render_template(
            "ver_print.html",
            date=date,
            mode=mode,
            operator=operator,
            persons=selected_persons,
            quals=selected_quals,
            pages=pages,
            rows_per_page=rows_per_page,
            session_id=sess,
            qual_filter=qual_filter,
        )

    # Tolerate accidental GET on preview/save
    @app.get("/ver/editor/preview")
    def ver_editor_preview_get():
        return redirect(url_for("ver_editor"))

    @app.get("/ver/editor/save")
    def ver_editor_save_get():
        return redirect(url_for("ver_editor"))

    @app.post("/ver/editor/save")
    def ver_editor_save():
        date = request.form.get("date") or datetime.now().strftime("%Y-%m-%d")
        mode = request.form.get("mode") or "person"
        operator = request.form.get("operator") or ""
        selected_persons = request.form.getlist("persons")
        selected_quals = request.form.getlist("quals")
        session_id = request.form.get("session_id") or uuid.uuid4().hex
        csv_ensure()
        log_display_selection(
            date=date,
            mode=mode,
            persons=selected_persons,
            qualifications=selected_quals,
            operator=operator,
            session_id=session_id,
        )
        # Redirect back to editor (or show a simple confirmation)
        return redirect(url_for("ver_editor") + f"?date={date}&mode={mode}")

    @app.route("/report")
    def report():
        # Prefer 'due' table if present; else compute a quick due from roster (90日)
        rows = []
        counts = {}
        with _con() as con:
            has_due = bool(
                con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name='due'"
                ).fetchone()
            )
            df = None
            if has_due:
                # Load whatever columns exist, normalize later
                tmp = con.execute("SELECT * FROM due").df()
                if "expiry_date" in tmp.columns:
                    # Ensure required logical columns exist; compute if missing
                    for c in ("name", "license_no", "qualification"):
                        if c not in tmp.columns:
                            tmp[c] = None
                    if ("days_to_expiry" not in tmp.columns) or ("notice_stage" not in tmp.columns):
                        from .reminders import compute_due, DueConfig

                        base = tmp[["name", "license_no", "qualification", "expiry_date"]].copy()
                        try:
                            tmp = compute_due(base, cfg=DueConfig(window_days=90))
                        except Exception:
                            tmp = None
                    df = tmp
            if df is None:
                # Fallback: compute from roster if expiry_date exists
                has_roster = bool(
                    con.execute(
                        "SELECT 1 FROM information_schema.tables WHERE table_name='roster'"
                    ).fetchone()
                )
                if has_roster:
                    cols = [
                        r[0]
                        for r in con.execute(
                            "SELECT column_name FROM information_schema.columns WHERE table_name='roster'"
                        ).fetchall()
                    ]
                    if "expiry_date" in cols:
                        from .reminders import compute_due, DueConfig

                        r = con.execute(
                            "SELECT name, license_no, qualification, expiry_date FROM roster"
                        ).df()
                        try:
                            df = compute_due(r, cfg=DueConfig(window_days=90))
                        except Exception:
                            df = None
            # Attach birth year if roster_enriched has it
            try:
                if (
                    df is not None
                    and not df.empty
                    and "name" in df.columns
                    and bool(
                        con.execute(
                            "SELECT 1 FROM information_schema.tables WHERE table_name='roster_enriched'"
                        ).fetchone()
                    )
                ):
                    b = con.execute(
                        "SELECT name, birth_year_west FROM roster_enriched WHERE name IS NOT NULL"
                    ).df()
                    if not b.empty:
                        b = b.dropna(subset=["name"]).drop_duplicates(subset=["name"], keep="first")
                        df = df.merge(b, on="name", how="left")
            except Exception:
                pass
            if df is not None and not df.empty:
                # Coerce display columns to strings to avoid None/NaT rendering
                for c in (
                    "name",
                    "license_no",
                    "qualification",
                    "expiry_date",
                    "days_to_expiry",
                    "notice_stage",
                    "birth_year_west",
                ):
                    if c in df.columns:
                        df[c] = df[c].astype("string").fillna("")
                rows = df.to_dict("records")
                counts = (
                    df["notice_stage"].value_counts().to_dict()
                    if "notice_stage" in df.columns
                    else {}
                )
        return render_template("report.html", rows=rows, counts=counts)

    @app.route("/report/print")
    def report_print():
        # Query params
        rows_per_page = int(request.args.get("rows", "35") or 35)
        orientation = request.args.get("ori", "portrait")  # portrait|landscape
        only_active = request.args.get("active") == "1"
        q = request.args.get("q", "").strip()
        title = request.args.get("title", "資格期限一覧")
        # Resolve dataset similar to /report (prefer due table)
        recs = []
        with _con() as con:
            has_due = bool(
                con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name='due'"
                ).fetchone()
            )
            df = None
            if has_due:
                tmp = con.execute("SELECT * FROM due").df()
                if "expiry_date" in tmp.columns:
                    for c in ("name", "license_no", "qualification"):
                        if c not in tmp.columns:
                            tmp[c] = None
                    if ("days_to_expiry" not in tmp.columns) or ("notice_stage" not in tmp.columns):
                        from .reminders import compute_due, DueConfig

                        base = tmp[["name", "license_no", "qualification", "expiry_date"]].copy()
                        try:
                            tmp = compute_due(base, cfg=DueConfig(window_days=90))
                        except Exception:
                            tmp = None
                    df = tmp
            if df is None:
                has_roster = bool(
                    con.execute(
                        "SELECT 1 FROM information_schema.tables WHERE table_name='roster'"
                    ).fetchone()
                )
                if has_roster:
                    from .reminders import compute_due, DueConfig

                    r = con.execute(
                        "SELECT name, qualification, license_no, expiry_date FROM roster WHERE expiry_date IS NOT NULL"
                    ).df()
                    try:
                        df = compute_due(r, cfg=DueConfig(window_days=90))
                    except Exception:
                        df = None
            # Attach birth year if roster_enriched available
            try:
                if (
                    df is not None
                    and not df.empty
                    and "name" in df.columns
                    and bool(
                        con.execute(
                            "SELECT 1 FROM information_schema.tables WHERE table_name='roster_enriched'"
                        ).fetchone()
                    )
                ):
                    b = con.execute(
                        "SELECT name, birth_year_west FROM roster_enriched WHERE name IS NOT NULL"
                    ).df()
                    if not b.empty:
                        b = b.dropna(subset=["name"]).drop_duplicates(subset=["name"], keep="first")
                        df = df.merge(b, on="name", how="left")
            except Exception:
                pass
            if df is not None and not df.empty:
                # Filter active by workers if requested
                if (
                    only_active
                    and con.execute(
                        "SELECT 1 FROM information_schema.tables WHERE table_name='workers'"
                    ).fetchone()
                ):
                    w = (
                        con.execute("SELECT DISTINCT name FROM workers WHERE name IS NOT NULL")
                        .df()["name"]
                        .dropna()
                        .astype(str)
                        .tolist()
                    )
                    df = df[df["name"].astype(str).isin(set(w))]
                if q:
                    df = df[df["name"].astype(str).str.contains(q)]
                df = df.sort_values(["expiry_date", "name"], kind="stable")
                for c in (
                    "name",
                    "license_no",
                    "qualification",
                    "expiry_date",
                    "days_to_expiry",
                    "notice_stage",
                    "birth_year_west",
                ):
                    if c in df.columns:
                        df[c] = df[c].astype("string").fillna("")
                recs = df.to_dict("records")
        # Chunk into pages
        pages = []
        if recs:
            for i in range(0, len(recs), rows_per_page):
                pages.append(
                    {
                        "no": (i // rows_per_page) + 1,
                        "rows": recs[i : i + rows_per_page],
                    }
                )
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        return render_template(
            "print.html",
            pages=pages,
            total=len(pages),
            orientation=orientation,
            title=title,
            rows_per_page=rows_per_page,
            q=q,
            only_active=only_active,
            now=now_str,
        )

    @app.route("/person")
    def person():
        name = request.args.get("name", "")
        nk = name_key(name)
        rows = []
        with _con() as con:
            has_roster = bool(
                con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name='roster'"
                ).fetchone()
            )
            has_manual = bool(
                con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name='roster_manual'"
                ).fetchone()
            )
            df = None
            if has_roster:
                cols = [
                    r[0]
                    for r in con.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_name='roster'"
                    ).fetchall()
                ]
                if "name" in cols:
                    base = con.execute(
                        "SELECT name, license_no, qualification, first_issue_date, issue_date, expiry_date FROM roster WHERE name = ?",
                        [name],
                    ).df()
                    df = base
            if has_manual:
                add = con.execute(
                    "SELECT name, license_no, qualification, first_issue_date, issue_date, expiry_date FROM roster_manual WHERE name = ?",
                    [name],
                ).df()
                if df is None:
                    df = add
                else:
                    import pandas as _pd

                    df = _pd.concat([df, add], ignore_index=True)
            if df is not None and not df.empty:
                df = df.sort_values(by=["expiry_date"], ascending=[False])
                rows = df.to_dict("records")
        decisions = store.get(nk)
        return render_template(
            "person.html", name=name, name_key=nk, rows=rows, decisions=decisions
        )

    # Accept /person/<name> for convenience (GET)
    @app.get("/person/<path:name>")
    def person_path(name: str):
        return redirect(url_for("person") + f"?name={name}")

    @app.post("/decision")
    def decision():
        name = request.form.get("name", "")
        license_no = request.form.get("license_no") or None
        status = request.form.get("status", "ok")
        notes = request.form.get("notes") or None
        store.set(name_key(name), license_no, status, notes)
        return redirect(url_for("person", name=name))

    @app.get("/input")
    def input_form():
        return render_template("input.html")

    @app.post("/input")
    def input_submit():
        name = request.form.get("name", "").strip()
        license_no = request.form.get("license_no", "").strip() or None
        qualification = request.form.get("qualification", "").strip() or None
        first_issue_date = request.form.get("first_issue_date", "").strip() or None
        issue_date = request.form.get("issue_date", "").strip() or None
        expiry_date = request.form.get("expiry_date", "").strip() or None
        if not name or not expiry_date:
            return redirect(url_for("input_form") + "?err=1")
        with _con() as con:
            con.execute(
                "CREATE TABLE IF NOT EXISTS roster_manual (name VARCHAR, license_no VARCHAR, qualification VARCHAR, first_issue_date DATE, issue_date DATE, expiry_date DATE, created TIMESTAMP DEFAULT now())"
            )
            con.execute(
                "INSERT INTO roster_manual (name, license_no, qualification, first_issue_date, issue_date, expiry_date) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    name,
                    license_no,
                    qualification,
                    first_issue_date or None,
                    issue_date or None,
                    expiry_date,
                ],
            )
        materialize_roster_all(wh)
        return redirect(url_for("person") + f"?name={name}")

    _register_error_handlers(app)
    return app


def _register_error_handlers(app: Flask) -> None:
    @app.errorhandler(404)
    def _nf(e):
        return render_template("layout.html", title="404 - Not Found"), 404

    @app.errorhandler(500)
    def _err(e):
        return render_template("layout.html", title="500 - Server Error"), 500


def run(
    host: str = "127.0.0.1",
    port: int = 8765,
    warehouse: Optional[Path] = None,
    review_db: Optional[Path] = None,
) -> None:
    app = create_app(warehouse=warehouse, review_db=review_db)
    app.run(host=host, port=port, debug=False)
