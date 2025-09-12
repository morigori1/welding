from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

import pandas as pd

from .io_excel import (
    list_sheets,
    read_sheet,
    summarize,
    to_canonical,
    write_csv,
    write_xlsx,
    read_vertical_blocks,
    detect_vertical_layout,
)
from .normalize import normalize, license_key, name_key, add_positions_columns
from .reminders import compute_due, DueConfig, write_ics
from .dates_jp import parse_jp_date
from .licenses import (
    scan_dir as scan_license_dir,
    scan_pdf as scan_license_pdf,
    scan_pdf_dates,
    scan_pdf_labeled_dates,
    scan_image_dates,
    scan_image_labeled_dates,
    audit_pdf as audit_license_pdf,
)
from .xdw import batch_convert, find_dwviewer
from .db import to_sqlite, to_duckdb, read_sqlserver_table
from .review import ReviewStore

def cmd_enrich(args: argparse.Namespace) -> int:
    import pandas as _pd
    from .normalize import name_key
    from .dates_jp import parse_jp_date
    from .db import to_duckdb as _to_duckdb

    def _coerce_birth_date(s: _pd.Series) -> _pd.Series:
        dt = _pd.to_datetime(s, errors="coerce")
        base = dt if dt.notna().any() else _pd.Series([_pd.NaT] * len(s))
        if base.isna().any():
            alt = s.map(lambda v: _pd.to_datetime(parse_jp_date(v), errors="coerce") if _pd.notna(v) else _pd.NaT)
            base = base.combine_first(alt)
        return base.dt.date

    con = _duckdb_con_from_args(args)
    try:
        if not con.execute("SELECT 1 FROM information_schema.tables WHERE table_name='roster'").fetchone():
            print("No 'roster' table in DuckDB.", file=sys.stderr)
            return 2
        roster = con.execute("SELECT * FROM roster").df()
        # Workers source
        if getattr(args, "workers_csv", None):
            wdf = _load_workers_csv(Path(args.workers_csv))
        elif con.execute("SELECT 1 FROM information_schema.tables WHERE table_name='workers'").fetchone():
            wdf = con.execute("SELECT * FROM workers").df()
        else:
            print("Workers not provided; use --workers-csv or populate DuckDB table 'workers'.", file=sys.stderr)
            return 2

        ren = {}
        if "氏名" in wdf.columns and "name" not in wdf.columns:
            ren["氏名"] = "name"
        for cand in ("生年月日", "birth_date", "birth", "dob"):
            if cand in wdf.columns:
                ren[cand] = "birth_date"
                break
        if ren:
            wdf = wdf.rename(columns=ren)
        if "birth_date" in wdf.columns:
            wdf["birth_date"] = _coerce_birth_date(wdf["birth_date"]).astype("string")
            wdf["birth_year_west"] = wdf["birth_date"].map(lambda x: str(x)[:4] if isinstance(x, str) and len(x)>=4 else "")

        # Join keys
        if "name" in roster.columns:
            roster["_name_key"] = roster["name"].map(name_key)
        if "name" in wdf.columns:
            wdf["_name_key"] = wdf["name"].map(name_key)

        if "employee_id" in roster.columns and "employee_id" in wdf.columns:
            on = ["employee_id"]
        elif "_name_key" in roster.columns and "_name_key" in wdf.columns:
            on = ["_name_key"]
        else:
            print("No join key (employee_id/name) found to match roster with workers.", file=sys.stderr)
            return 2

        cols = [c for c in ["name", "employee_id", "birth_date", "birth_year_west"] if c in wdf.columns]
        merged = roster.merge(wdf[on + cols], on=on, how="left", suffixes=("", "_w"))
        outp = Path(getattr(args, "duckdb", None) or os.getenv("DUCKDB_DB_PATH") or "warehouse/local.duckdb")
        _to_duckdb(merged, outp, table="roster_enriched")
        print(f"Wrote 'roster_enriched' ({len(merged)} rows) to {outp}")
        return 0
    finally:
        con.close()


def cmd_inspect(args: argparse.Namespace) -> int:
    xls = Path(args.xls)
    if not xls.exists():
        print(f"File not found: {xls}", file=sys.stderr)
        return 2
    items = summarize(xls)
    for s in items:
        print(f"Sheet: {s.name} | rows={s.n_rows} cols={s.n_cols}")
        print("  Headers:", ", ".join(map(str, s.headers)))
    # Also preview canonicalized headers for the first sheet
    try:
        first = items[0].name if items else None
        if first is not None:
            df_raw, _ = read_sheet(xls, first)
            df_prev = to_canonical(df_raw)
            print("  Canonical:", ", ".join(map(str, df_prev.columns)))
    except Exception:
        pass
    return 0


def cmd_ingest(args: argparse.Namespace) -> int:
    xls = Path(args.xls)
    outdir = Path(args.out)
    sheet = args.sheet

    if not xls.exists():
        print(f"File not found: {xls}", file=sys.stderr)
        return 2

    # Choose a sheet: user-specified or first one
    if sheet is None:
        sheets = list_sheets(xls)
        if not sheets:
            print("No sheets found.", file=sys.stderr)
            return 2
        sheet = sheets[0]

    header_override = args.header_row if hasattr(args, "header_row") else None
    df_raw, header_row = read_sheet(xls, sheet, header_row_override=header_override)
    df = to_canonical(df_raw)
    # Active/Retired by print area (rows)
    if getattr(args, "active_by_print", False) or getattr(args, "only_active_print", False):
        from .io_excel import get_print_areas
        areas = get_print_areas(xls, sheet)
        if areas:
            rowset = set()
            for r0, r1, c0, c1 in areas:
                # df row 0 corresponds to original (header_row + 1)
                for rr in range(max(r0, header_row + 1), r1):
                    rowset.add(rr)
            mask = []
            for i in range(len(df)):
                orig = (header_row + 1) + i
                mask.append(orig in rowset)
            df["status"] = ["active" if m else "retired" for m in mask]
            if getattr(args, "only_active_print", False):
                df = df.loc[[m for m in mask]].reset_index(drop=True)
    # Derive normalized weld positions from qualification text if present
    try:
        df = add_positions_columns(df, source_col="qualification")
    except Exception:
        pass
    # Collapse duplicate-named columns by coalescing left-to-right
    if df.columns.duplicated().any():
        dup_names = [n for n, c in df.columns.value_counts().items() if c > 1]
        for name in dup_names:
            cols = [c for c in df.columns if c == name]
            base = df.loc[:, cols].bfill(axis=1).iloc[:, 0]
            df[name] = base
        df = df.loc[:, ~df.columns.duplicated()]

    def _coalesce(df, name: str):
        idx = [i for i, c in enumerate(df.columns) if c == name]
        if not idx:
            return None
        if len(idx) == 1:
            return df.iloc[:, idx[0]]
        return df.iloc[:, idx].bfill(axis=1).iloc[:, 0]

    # If 'expiry_date' missing, optionally compute from another date column
    if "expiry_date" not in df.columns and args.expiry_from != "expiry_date":
        src = args.expiry_from
        if src not in df.columns:
            # tolerate duplicated headers mapped to e.g. 'issue_date.1'
            candidates = [c for c in df.columns if str(c).startswith(src)]
            if candidates:
                src = candidates[0]
        if src not in df.columns:
            print(f"Column '{args.expiry_from}' not present; cannot compute expiry.", file=sys.stderr)
            return 2
        dt = pd.to_datetime(df[src], errors="coerce")
        df["expiry_date"] = (dt + pd.DateOffset(years=int(args.valid_years))).dt.date
    df = normalize(df)

    outdir.mkdir(parents=True, exist_ok=True)
    # CSV
    write_csv(df, outdir / "roster.csv")
    # XLSX (normalized)
    write_xlsx(df, outdir / "roster.xlsx")
    # SQLite / DuckDB
    if args.sqlite:
        to_sqlite(df, Path(args.sqlite))
    duckdb_path = getattr(args, "duckdb", None) or os.getenv("DUCKDB_DB_PATH")
    if duckdb_path:
        to_duckdb(df, Path(duckdb_path))

    print(f"Ingested sheet '{sheet}' (header row {header_row}); wrote {outdir}")
    if args.sqlite:
        print(f"SQLite DB: {args.sqlite}")
    if duckdb_path:
        print(f"DuckDB: {duckdb_path}")
    return 0


def cmd_ingest_vertical(args: argparse.Namespace) -> int:
    xls = Path(args.xls)
    outdir = Path(args.out)
    sel_sheet = args.sheet
    if not xls.exists():
        print(f"File not found: {xls}", file=sys.stderr)
        return 2
    # Determine target sheets
    sheets_all = list_sheets(xls)
    target_sheets: list[str] = []
    if getattr(args, "all_sheets", False):
        import re as _re
        # Prefer P1/P2/... style sheets; fallback to all if none matches
        cand = [s for s in sheets_all if _re.match(r"^[Pp]\d+$", str(s))]
        target_sheets = cand if cand else sheets_all
    else:
        if sel_sheet is None:
            if not sheets_all:
                print("No sheets found.", file=sys.stderr)
                return 2
            target_sheets = [sheets_all[0]]
        else:
            target_sheets = [sel_sheet]

    # Build blocks from args (support multiple --block LABEL=RANGE)
    blocks = []
    blk_multi = getattr(args, "block", None) or []
    for b in blk_multi:
        if not b:
            continue
        if "=" in b:
            lab, rng = b.split("=", 1)
            blocks.append((lab.strip() or "BLOCK", rng.strip()))
    if args.jis:
        blocks.append(("JIS", args.jis))
    if args.boiler:
        blocks.append(("BOILER", args.boiler))
    # Auto if requested and no explicit blocks provided
    auto_blocks = getattr(args, "auto_blocks", False) or getattr(args, "auto", False)
    auto_cols = getattr(args, "auto_person", False) or getattr(args, "auto_regno", False) or getattr(args, "auto", False)

    # Auto-detect columns/blocks if requested
    auto_detected_cache: dict[str, tuple[str, str, list[tuple[str, str]]]] = {}
    # Read each target sheet and concatenate
    frames = []
    for sheet in target_sheets:
        person = args.person
        regno = args.regno
        blks = blocks
        if auto_cols or auto_blocks or str(person).upper() == "AUTO" or str(regno).upper() == "AUTO" or (not blks):
            # perform detection per sheet (cache by name)
            if str(sheet) not in auto_detected_cache:
                p_idx, r_idx, detected = detect_vertical_layout(xls, sheet, max_probe_rows=10)
                # convert to letters/ranges
                from .io_excel import _index_to_col_letter as _itoc
                p_letter = _itoc(p_idx)
                r_letter = _itoc(r_idx)
                blks2 = [(lab, f"{_itoc(a)}:{_itoc(b-1)}") for lab, (a, b) in detected]
                auto_detected_cache[str(sheet)] = (p_letter, r_letter, blks2)
            p_letter, r_letter, blks2 = auto_detected_cache[str(sheet)]
            if auto_cols or str(person).upper() == "AUTO":
                person = p_letter
            if auto_cols or str(regno).upper() == "AUTO":
                regno = r_letter
            if auto_blocks or not blks:
                blks = blks2
        df_i = read_vertical_blocks(xls_path=xls, sheet=sheet, person_col=person, regno_col=regno, blocks=blks)
        # Mark status by print area rows if requested
        if (getattr(args, "active_by_print", False) or getattr(args, "only_active_print", False)) and not df_i.empty:
            try:
                from .io_excel import get_print_areas
                areas = get_print_areas(xls, sheet)
                if areas and "row_index" in df_i.columns:
                    # row_index in df_i is based on 0.. index of df_aux; we need original row numbers.
                    # read_vertical_blocks sets row_index using df_aux after dropna/reset; we cannot
                    # recover original row without support -> adjust read_vertical_blocks to include orig_row if present.
                    # Backward-compatible path: treat row_index as proxy by shifting with minimal header guessing (best-effort).
                    # Use heuristic: original row ~= row_index (since we drop only fully empty rows above headers, most keep order)
                    rowset = set()
                    for r0, r1, c0, c1 in areas:
                        for rr in range(r0, r1):
                            rowset.add(rr)
                    # Try a safe combination: if 'orig_row' present, prefer it; else use row_index
                    col = "orig_row" if "orig_row" in df_i.columns else "row_index"
                    df_i["status"] = df_i[col].map(lambda r: "active" if (isinstance(r, (int, float)) and int(r) in rowset) else "retired")
                    # right-of print area => history flag
                    # take the max right edge among areas as print boundary (inclusive-exclusive c1)
                    pa_right = max(c1 for _, _, _, c1 in areas)
                    if "used_col_min" in df_i.columns:
                        df_i["record_type"] = df_i["used_col_min"].map(lambda c: "history" if (isinstance(c, (int, float)) and int(c) >= pa_right) else "current")
                    else:
                        df_i["record_type"] = "current"
                    if getattr(args, "only_active_print", False):
                        # filter: active rows and records within print-area columns
                        incol = df_i["record_type"] == "current"
                        df_i = df_i[(df_i["status"] == "active") & incol].reset_index(drop=True)
            except Exception:
                pass
        if not df_i.empty:
            frames.append(df_i)

    if not frames:
        print("No data extracted from selected sheets.", file=sys.stderr)
        return 2
    df = pd.concat(frames, ignore_index=True)

    # Prepare normalized roster-like output
    outdir.mkdir(parents=True, exist_ok=True)
    # Derive positions and flags
    try:
        df = add_positions_columns(df, source_col="qualification")
    except Exception:
        pass
    roster_cols = [
        "name",
        "license_no",
        "qualification",
        "positions",
        "positions_jp",
        "positions_en",
        "positions_code",
        "pos_flat",
        "pos_horizontal",
        "pos_vertical",
        "pos_overhead",
        "category",
        "status",
        "record_type",
        "row_index",
        "orig_row",
        "issue_year",
        "first_issue_date",
        "issue_date",
        "expiry_date",
    ]
    df_roster = df[[c for c in roster_cols if c in df.columns]].copy()
    # Write CSV and XLSX
    write_csv(df_roster, outdir / "roster.csv")
    write_xlsx(df_roster, outdir / "roster.xlsx")
    # Optionally write raw (with values map) for troubleshooting
    if getattr(args, "with_raw", False):
        # Convert dict column to JSON-like strings for CSV
        df2 = df.copy()
        try:
            import json as _json
            df2["values"] = df2["values"].map(lambda d: _json.dumps(d, ensure_ascii=False))
        except Exception:
            pass
        write_csv(df2, outdir / "roster_raw.csv")

    # Warehouse
    duckdb_path = getattr(args, "duckdb", None) or os.getenv("DUCKDB_DB_PATH")
    if duckdb_path:
        to_duckdb(df_roster, Path(duckdb_path))
        print(f"DuckDB: {duckdb_path} (table 'roster')")

    sheets_info = ",".join(map(str, target_sheets))
    print(f"Ingested vertical blocks from sheets [{sheets_info}]. Rows: {len(df_roster)}. Wrote {outdir}")
    return 0


def cmd_dates(args: argparse.Namespace) -> int:
    path = Path(args.pdf)
    if not path.exists():
        print(f"File not found: {path}", file=sys.stderr)
        return 2
    if path.suffix.lower() == ".pdf":
        items = scan_pdf_dates(path)
    else:
        items = scan_image_dates(path)
    if not items:
        print("No date-like tokens found")
        return 0
    for raw, norm in items:
        if norm:
            print(f"{raw} -> {norm}")
        else:
            print(raw)
    return 0


def cmd_pdfdates(args: argparse.Namespace) -> int:
    path = Path(args.pdf)
    if not path.exists():
        print(f"File not found: {path}", file=sys.stderr)
        return 2
    if path.suffix.lower() == ".pdf":
        df = scan_pdf_labeled_dates(path)
    else:
        df = scan_image_labeled_dates(path)
    if df.empty:
        print("No labeled dates found")
        return 0
    for _, r in df.iterrows():
        page = r.get("page")
        fi = r.get("first_issue_date")
        isd = r.get("issue_date")
        exp = r.get("expiry_date")
        def _fmt(x):
            try:
                return x.date().isoformat() if hasattr(x, 'date') else (str(x) if x is not None else '')
            except Exception:
                return str(x) if x is not None else ''
        print(f"page={page} first_issue={_fmt(fi)} issue={_fmt(isd)} expiry={_fmt(exp)}")
    return 0


def cmd_app(args: argparse.Namespace) -> int:
    from .app import run as run_app
    wh = Path(args.duckdb) if getattr(args, "duckdb", None) else None
    rv = Path(args.review_db) if getattr(args, "review_db", None) else None
    run_app(host=args.host, port=int(args.port), warehouse=wh, review_db=rv)
    return 0


def cmd_gui(args: argparse.Namespace) -> int:
    # Lazy import to avoid requiring Tk on environments that use only CLI/web
    from . import gui as _gui  # local import intentional to avoid Tk dependency at import time

    duck = Path(args.duckdb) if getattr(args, "duckdb", None) else None
    return _gui.run(warehouse=duck)


def _load_workers_csv(path: Path) -> pd.DataFrame:
    import pandas as _pd

    dfw = _pd.read_csv(path)
    # Canonicalize headers we rely on
    if "name" not in dfw.columns:
        # Common Japanese header
        for cand in ("氏名", "なまえ", "名前"):
            if cand in dfw.columns:
                dfw = dfw.rename(columns={cand: "name"})
                break
    return dfw


def cmd_workers(args: argparse.Namespace) -> int:
    # Minimal PII-safe logging: only counts, not values
    missing = [k for k in ("host", "db", "user", "password") if not getattr(args, k)]
    if missing:
        print("Missing connection options: " + ", ".join(missing) + ". You can also set DB_HOST, DB_NAME, DB_USER, DB_PASSWORD.", file=sys.stderr)
        return 2
    # Default WHERE: treat rows as active if both name and name_code are present
    default_where = "(name IS NOT NULL AND LTRIM(RTRIM(name)) <> '' AND name_code IS NOT NULL AND LTRIM(RTRIM(name_code)) <> '')"
    df = read_sqlserver_table(
        host=args.host,
        database=args.db,
        schema=args.schema,
        table=args.table,
        user=args.user,
        password=args.password,
        driver=args.driver,
        encrypt=args.encrypt,
        trust_server_certificate=args.trust,
        limit=args.limit,
        where=(args.where or default_where),
    )
    # Heuristic rename for common name/employee id columns
    ren = {}
    for cand in ("氏名", "名前", "name"):
        if cand in df.columns:
            ren[cand] = "name"
            break
    for cand in ("社員番号", "従業員番号", "employee_id", "emp_id", "name_code"):
        if cand in df.columns:
            ren[cand] = "employee_id"
            break
    if ren:
        df = df.rename(columns=ren)
    out_csv = Path(args.out)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    duckdb_path = getattr(args, "duckdb", None) or os.getenv("DUCKDB_DB_PATH")
    if duckdb_path:
        to_duckdb(df, Path(duckdb_path), table="workers")
    print(f"Fetched workers: {len(df)} rows. Wrote {out_csv}")
    if duckdb_path:
        print(f"DuckDB: {duckdb_path} (table 'workers')")
    return 0


def _duckdb_con_from_args(args: argparse.Namespace):
    import duckdb  # type: ignore
    dbp = getattr(args, "duckdb", None) or os.getenv("DUCKDB_DB_PATH")
    if not dbp:
        print("DuckDB path not provided (use --duckdb or set DUCKDB_DB_PATH)", file=sys.stderr)
        raise SystemExit(2)
    return duckdb.connect(str(dbp))


def cmd_review_persons(args: argparse.Namespace) -> int:
    con = _duckdb_con_from_args(args)
    try:
        if not con.execute("SELECT 1 FROM information_schema.tables WHERE table_name='roster'").fetchone():
            print("No 'roster' table in DuckDB.", file=sys.stderr)
            return 2
        df = con.execute("SELECT name, COUNT(*) AS n FROM roster WHERE name IS NOT NULL GROUP BY name ORDER BY name").df()
        if getattr(args, "active", False) and con.execute("SELECT 1 FROM information_schema.tables WHERE table_name='workers'").fetchone():
            w = con.execute("SELECT DISTINCT name FROM workers WHERE name IS NOT NULL").df()["name"].dropna().astype(str).tolist()
            df = df[df["name"].astype(str).isin(set(w))]
        q = getattr(args, "q", None)
        if q:
            df = df[df["name"].astype(str).str.contains(q)]
        for _, r in df.iterrows():
            print(f"{r['name']}\t{int(r['n'])}")
        return 0
    finally:
        con.close()


def cmd_review_show(args: argparse.Namespace) -> int:
    con = _duckdb_con_from_args(args)
    try:
        name = args.name
        # Include positions column if present
        has_pos = bool(con.execute("SELECT 1 FROM information_schema.columns WHERE table_name='roster' AND column_name='positions'").fetchone())
        cols = "name, license_no, qualification, " + ("positions, " if has_pos else "") + "first_issue_date, issue_date, expiry_date"
        sql = f"SELECT {cols} FROM roster WHERE name = ? ORDER BY expiry_date DESC NULLS LAST, issue_date DESC NULLS LAST"
        df = con.execute(sql, [name]).df()
        if df.empty:
            print("No rows.")
            return 0
        print(df.to_string(index=False))
        if getattr(args, "with_decisions", False):
            store = ReviewStore(Path("warehouse/review.sqlite"))
            decs = store.get(name_key(name))
            if decs:
                print("\n[decisions]")
                for d in decs:
                    print(f"license={d.license_no or ''}\tstatus={d.status}\tnotes={d.notes or ''}")
        return 0
    finally:
        con.close()


def cmd_review_mark(args: argparse.Namespace) -> int:
    store = ReviewStore(Path(args.review_db))
    store.set(name_key(args.name), getattr(args, "license_no", None), args.status, getattr(args, "notes", None))
    print("recorded")
    return 0


def cmd_review_export(args: argparse.Namespace) -> int:
    import pandas as pd
    con = _duckdb_con_from_args(args)
    try:
        df = con.execute("SELECT name, license_no, qualification, first_issue_date, issue_date, expiry_date FROM roster").df()
    finally:
        con.close()
    store = ReviewStore(Path(args.review_db))
    decs = list(store.all())
    if decs:
        ddf = pd.DataFrame([{
            "name_key": d.name_key,
            "license_no": d.license_no,
            "status": d.status,
            "notes": d.notes,
            "ts": d.ts,
        } for d in decs])
        df["name_key"] = df["name"].map(name_key)
        out = df.merge(ddf, how="left", on=["name_key", "license_no"])\
                 .drop(columns=["name_key"])\
                 .sort_values(["name", "qualification", "license_no"])  # type: ignore
    else:
        out = df
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"wrote {out_path}")
    return 0


def cmd_due(args: argparse.Namespace) -> int:
    xls = Path(args.xls)
    sheet = args.sheet
    if not xls.exists():
        print(f"File not found: {xls}", file=sys.stderr)
        return 2

    # Decide sheet
    if sheet is None:
        sheets = list_sheets(xls)
        if not sheets:
            print("No sheets found.", file=sys.stderr)
            return 2
        sheet = sheets[0]

    header_override = args.header_row if hasattr(args, "header_row") else None
    df_raw, _ = read_sheet(xls, sheet, header_row_override=header_override)
    df = to_canonical(df_raw)
    # Collapse duplicate-named columns by coalescing left-to-right (due path)
    if df.columns.duplicated().any():
        dup_names = [n for n, c in df.columns.value_counts().items() if c > 1]
        for name in dup_names:
            cols = [c for c in df.columns if c == name]
            base = df.loc[:, cols].bfill(axis=1).iloc[:, 0]
            df[name] = base
        df = df.loc[:, ~df.columns.duplicated()]
    def _coalesce(df, name: str):
        idx = [i for i, c in enumerate(df.columns) if c == name]
        if not idx:
            return None
        if len(idx) == 1:
            return df.iloc[:, idx[0]]
        return df.iloc[:, idx].bfill(axis=1).iloc[:, 0]

    src = None
    # If 'expiry_date' missing, compute from a source date if possible
    if "expiry_date" not in df.columns:
        pref = getattr(args, "expiry_from", None)
        src = pref if pref and pref != "expiry_date" else None
        if not src:
            for cand in ("issue_date", "test_date", "first_issue_date"):
                if cand in df.columns:
                    src = cand
                    break
        if not src:
            # Heuristic: choose the date-like column with the latest median as expiry
            date_candidates: list[tuple[str, pd.Series]] = []
            for c in df.columns:
                try:
                    s = df[c]
                    # First try pandas parser
                    series = pd.to_datetime(s, errors="coerce")
                    valid = series.notna().sum()
                    if valid < max(3, int(0.2 * len(df))):
                        # Try JP date parser row-wise
                        parsed = s.map(lambda v: parse_jp_date(v) if pd.notna(v) else None)
                        series = pd.to_datetime(parsed, errors="coerce")
                        valid = series.notna().sum()
                    if valid >= max(3, int(0.2 * len(df))):
                        date_candidates.append((str(c), series))
                except Exception:
                    continue
            if date_candidates:
                # sort by median date
                date_candidates.sort(key=lambda kv: kv[1].median(skipna=True))
                # assign earliest as issue, latest as expiry
                earliest = date_candidates[0][1]
                latest = date_candidates[-1][1]
                if "first_issue_date" not in df.columns:
                    df["first_issue_date"] = earliest.dt.date
                if "issue_date" not in df.columns:
                    df["issue_date"] = latest.dt.date
                if "expiry_date" not in df.columns:
                    df["expiry_date"] = latest.dt.date
                src = None
            else:
                print("expiry_date not present and no source date column found to compute from.", file=sys.stderr)
                src = None

    # Apply domain validity rules when expiry is still missing or partial
    s_exp = _coalesce(df, "expiry_date")
    exp_missing = (s_exp is None) or (s_exp.isna().all())
    if exp_missing:
        base_col = (
            "issue_date" if "issue_date" in df.columns else (
            "test_date" if "test_date" in df.columns else (
            "first_issue_date" if "first_issue_date" in df.columns else None))
        )
        if base_col and "qualification" in df.columns:
            q = df["qualification"].astype(str).str.lower()
            years = pd.Series([None] * len(df))
            mask_jis = q.str.contains("jis", na=False) | q.str.contains("ｊｉｓ", na=False)
            mask_boiler = q.str.contains("ﾎﾞｲﾗ", na=False) | q.str.contains("ボイラ", na=False) | q.str.contains("boiler", na=False)
            years = years.mask(mask_jis, int(getattr(args, "validity_jis_years", 1)))
            years = years.mask(mask_boiler, int(getattr(args, "validity_boiler_years", 2)))
            # default fallback
            years = years.fillna(int(getattr(args, "valid_years", 3)))
            base_dt = pd.to_datetime(df[base_col], errors="coerce")
            exp_series = base_dt + years.astype(int).map(lambda y: pd.DateOffset(years=y))
            right = df["expiry_date"] if "expiry_date" in df.columns else pd.Series([None] * len(df))
            df["expiry_date"] = right.combine_first(exp_series.dt.date if hasattr(exp_series, 'dt') else exp_series)
        if src and src not in df.columns:
            candidates = [c for c in df.columns if str(c).startswith(src)]
            if candidates:
                src = candidates[0]
        if src and src in df.columns:
            dt = pd.to_datetime(df[src], errors="coerce")
            df["expiry_date"] = (dt + pd.DateOffset(years=int(getattr(args, "valid_years", 3)))).dt.date

    as_of = None
    if args.as_of:
        try:
            as_of = pd.to_datetime(args.as_of).date()
        except Exception:
            print("--as-of must be YYYY-MM-DD", file=sys.stderr)
            return 2

    # Prefer expiry dates from scanned licenses, if provided. Accept directory or single PDF file.
    lic_df = pd.DataFrame()
    lic_sources = getattr(args, "licenses_dir", None)
    if lic_sources:
        frames = []
        dump_dir = Path(args.dump_ocr) if getattr(args, "dump_ocr", None) else None
        for src in lic_sources:
            lic_path = Path(src)
            if lic_path.is_file():
                f = scan_license_pdf(lic_path)
            else:
                f = scan_license_dir(lic_path, debug=bool(getattr(args, "debug_ocr", False)), dump_dir=dump_dir)
            if f is not None and not f.empty:
                frames.append(f)
        if frames:
            lic_df = pd.concat(frames, ignore_index=True)
            # Deduplicate by license_no + expiry_date if available
            if "license_no" in lic_df.columns:
                cols = [c for c in ["license_no", "expiry_date"] if c in lic_df.columns]
                if cols:
                    lic_df = lic_df.drop_duplicates(subset=cols, keep="first")
        if not lic_df.empty:
            # Build normalized keys
            if "license_no" in df.columns:
                df["_lic_key"] = df["license_no"].map(license_key)
            if "name" in df.columns:
                df["_name_key"] = df["name"].map(name_key)
            if "license_no" in lic_df.columns:
                lic_df["_lic_key"] = lic_df["license_no"].map(license_key)
            if "name" in lic_df.columns:
                lic_df["_name_key"] = lic_df["name"].map(name_key)

            merged = df.copy()
            did_merge = False
            if "_lic_key" in merged.columns and "_lic_key" in lic_df.columns:
                merged = merged.merge(
                    lic_df[["_lic_key", "expiry_date", "issue_date"]].rename(columns={
                        "expiry_date": "expiry_date_lic",
                        "issue_date": "issue_date_lic",
                    }),
                    on="_lic_key",
                    how="left",
                )
                did_merge = True
            if not did_merge and "_name_key" in merged.columns and "_name_key" in lic_df.columns:
                merged = merged.merge(
                    lic_df[["_name_key", "expiry_date", "issue_date"]].rename(columns={
                        "expiry_date": "expiry_date_lic",
                        "issue_date": "issue_date_lic",
                    }),
                    on="_name_key",
                    how="left",
                )
                did_merge = True
            if did_merge:
                if "expiry_date_lic" in merged.columns:
                    right = merged["expiry_date"] if "expiry_date" in merged.columns else pd.Series([None] * len(merged))
                    merged["expiry_date"] = merged["expiry_date_lic"].combine_first(right)
                if "issue_date_lic" in merged.columns and "issue_date" in merged.columns:
                    merged["issue_date"] = merged["issue_date_lic"].combine_first(merged["issue_date"])
                df = merged

    cfg = DueConfig(window_days=int(args.window))
    # Optional: filter to active workers
    if getattr(args, "workers_csv", None):
        wpath = Path(args.workers_csv)
        if wpath.exists():
            wdf = _load_workers_csv(wpath)
            # Build keys
            if "employee_id" in wdf.columns and "employee_id" in df.columns:
                # Clean simple string dtype
                left = df["employee_id"].astype(str)
                right = wdf["employee_id"].astype(str)
                df = df[left.isin(set(right))]
            else:
                # Fallback to name-key match
                if "name" in df.columns:
                    df["_name_key"] = df["name"].map(name_key)
                if "name" in wdf.columns:
                    wdf["_name_key"] = wdf["name"].map(name_key)
                if "_name_key" in df.columns and "_name_key" in wdf.columns:
                    keys = set(wdf["_name_key"].dropna().unique())
                    df = df[df["_name_key"].isin(keys)]
        else:
            print(f"--workers-csv file not found: {wpath}", file=sys.stderr)
    try:
        due = compute_due(df, as_of=as_of, cfg=cfg)
    except ValueError as e:
        # Fallback: if OCR license data exists, compute due from it directly
        if not lic_df.empty and "expiry_date" in lic_df.columns:
            due = compute_due(lic_df, as_of=as_of, cfg=cfg)
        else:
            print(str(e), file=sys.stderr)
            due = pd.DataFrame(columns=["name", "license_no", "qualification", "expiry_date", "days_to_expiry", "next_notice_date", "notice_stage"])  # type: ignore
    out_csv = Path(args.out)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    due.to_csv(out_csv, index=False, encoding="utf-8-sig")
    duckdb_path = getattr(args, "duckdb", None) or os.getenv("DUCKDB_DB_PATH")
    if duckdb_path:
        to_duckdb(due, Path(duckdb_path), table="due")
    print(f"Wrote due list CSV: {out_csv} ({len(due)} rows)")

    if args.ics:
        if "name" in due.columns:
            summary_tpl = "資格有効期限: {name}"
        elif "license_no" in due.columns:
            summary_tpl = "資格有効期限: {license_no}"
        else:
            summary_tpl = "資格有効期限: {expiry_date}"
        write_ics(due, Path(args.ics), summary_tpl=summary_tpl)
        print(f"Wrote ICS calendar: {args.ics}")
    return 0

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="welding_registry", description="Welding roster utilities")
    sub = p.add_subparsers(dest="cmd", required=True)

    pi = sub.add_parser("inspect", help="List sheets and headers of an XLS")
    pi.add_argument("xls", help="Path to XLS file")
    pi.set_defaults(func=cmd_inspect)

    pg = sub.add_parser("ingest", help="Ingest an XLS sheet and export CSV/XLSX/SQLite/DuckDB")
    pg.add_argument("xls", help="Path to XLS file")
    pg.add_argument("--sheet", help="Sheet name (default: first sheet)")
    pg.add_argument("--header-row", dest="header_row", type=int, help="Override header row index (0-based)")
    pg.add_argument("--out", default="out", help="Output directory (default: out)")
    pg.add_argument("--sqlite", help="Optional path to SQLite DB to write")
    pg.add_argument("--duckdb", help="Optional path to DuckDB file to write")
    pg.add_argument(
        "--expiry-from",
        choices=["expiry_date", "issue_date", "test_date"],
        default="expiry_date",
        help="Which column to base expiry on if 'expiry_date' is missing",
    )
    pg.add_argument(
        "--valid-years",
        type=int,
        default=3,
        help="Validity in years when computing expiry (default: 3)",
    )
    pg.add_argument("--active-by-print-area", dest="active_by_print", action="store_true", help="Mark rows within print area as status=active; outside as retired")
    pg.add_argument("--only-active-print", action="store_true", help="Filter output to rows within print area only")
    pg.set_defaults(func=cmd_ingest)

    # ingest-vertical for name-spanning vertical blocks layout
    pv = sub.add_parser("ingest-vertical", help="Ingest vertical-block sheets; supports auto-detection of columns/blocks")
    pv.add_argument("xls", help="Path to XLS/XLSX file")
    pv.add_argument("--sheet", help="Sheet name (default: first sheet)")
    pv.add_argument("--all-sheets", action="store_true", help="Process all P1/P2/... sheets (or all if none match)")
    pv.add_argument("--person", default="A", help="Column letter for name (default: A; use 'AUTO' to detect)")
    pv.add_argument("--regno", default="B", help="Column letter for registration number (default: B; use 'AUTO' to detect)")
    pv.add_argument("--jis", help="Column range for JIS block, e.g., C:H")
    pv.add_argument("--boiler", help="Column range for Boiler block, e.g., I:K")
    pv.add_argument("--block", action="append", help="Additional block as LABEL=RANGE (repeatable), e.g., --block JIS=C:H --block BOILER=I:K")
    pv.add_argument("--auto", action="store_true", help="Auto-detect person/regno columns and block ranges")
    pv.add_argument("--auto-person", action="store_true", help="Auto-detect person column only")
    pv.add_argument("--auto-regno", action="store_true", help="Auto-detect regno column only")
    pv.add_argument("--auto-blocks", action="store_true", help="Auto-detect block ranges (JIS/Boiler)")
    pv.add_argument("--out", default="out", help="Output directory (default: out)")
    pv.add_argument("--duckdb", help="Optional DuckDB path to write table 'roster' (default: env DUCKDB_DB_PATH)")
    pv.add_argument("--with-raw", action="store_true", help="Also export roster_raw.csv with raw block cell values")
    pv.add_argument("--active-by-print-area", dest="active_by_print", action="store_true", help="Mark rows within print area as status=active; outside as retired")
    pv.add_argument("--only-active-print", action="store_true", help="Filter output to rows within print area only")
    pv.set_defaults(func=cmd_ingest_vertical)

    pdue = sub.add_parser("due", help="List licenses expiring soon and generate notices")
    pdue.add_argument("xls", help="Path to XLS file")
    pdue.add_argument("--sheet", help="Sheet name (default: first sheet)")
    pdue.add_argument("--window", type=int, default=90, help="Days ahead to include (default: 90)")
    pdue.add_argument("--as-of", dest="as_of", help="Reference date YYYY-MM-DD (default: today)")
    pdue.add_argument("--out", default="out/due.csv", help="Output CSV path")
    pdue.add_argument("--duckdb", help="Optional path to DuckDB file to write result table 'due' (default: env DUCKDB_DB_PATH)")
    pdue.add_argument("--ics", dest="ics", help="Optional ICS calendar path to write")
    pdue.add_argument("--header-row", dest="header_row", type=int, help="Override header row index (0-based)")
    pdue.add_argument("--expiry-from", choices=["expiry_date", "issue_date", "test_date"], default="expiry_date", help="Which column to base expiry on if 'expiry_date' is missing")
    pdue.add_argument("--valid-years", type=int, default=3, help="Validity in years when computing expiry (default: 3)")
    pdue.add_argument(
        "--licenses-scan",
        dest="licenses_dir",
        action="append",
        help="Directory or file to scan for license PDFs. Repeat to combine multiple sources.",
    )
    pdue.add_argument("--dump-ocr", dest="dump_ocr", help="Optional directory to write sanitized OCR snippets for debugging patterns")
    pdue.add_argument("--workers-csv", dest="workers_csv", help="Optional CSV of active workers to filter by (name/employee_id match)")
    pdue.add_argument("--debug-ocr", action="store_true", help="Print minimal OCR debug (no PII): match flags and text length")
    pdue.set_defaults(func=cmd_due)

    px = sub.add_parser("xdw2pdf", help="Batch convert XDW/XBD by printing to a PDF printer")
    px.add_argument("input", help="Input directory containing .xdw/.xbd")
    px.add_argument("--printer", default="DocuWorks PDF", help="Target printer (default: DocuWorks PDF)")
    px.add_argument("--no-recurse", action="store_true", help="Do not recurse into subdirectories")
    px.add_argument("--auto-enter", action="store_true", help="Windows only: auto-press Enter on Save dialogs during print")
    px.add_argument("--save-window-titles", default="名前を付けて保存;Save Print Output As;DocuWorks;CubePDF", help="Semicolon-separated window title hints to target when auto-pressing Enter")
    px.add_argument("--viewer", help="Optional full path to dwviewer.exe/dwdesk.exe")
    px.set_defaults(func=cmd_xdw2pdf)

    pdts = sub.add_parser("dates", help="List date-like tokens from a PDF and their normalized forms")
    pdts.add_argument("pdf", help="Path to a PDF file")
    pdts.set_defaults(func=cmd_dates)

    pa = sub.add_parser("app", help="Run review web app (per-person licenses)")
    pa.add_argument("--host", default="127.0.0.1")
    pa.add_argument("--port", type=int, default=8765)
    pa.add_argument("--duckdb", help="Path to DuckDB warehouse (default: env DUCKDB_DB_PATH)")
    pa.add_argument("--review-db", help="Path to review decisions SQLite (default: warehouse/review.sqlite)")
    pa.set_defaults(func=cmd_app)

    # Enrich: join roster with workers to add birth_date/birth_year
    pe = sub.add_parser("enrich", help="Enrich roster with workers info (e.g., birth_date)")
    pe.add_argument("--duckdb", help="DuckDB path (default: env DUCKDB_DB_PATH)")
    pe.add_argument("--workers-csv", dest="workers_csv", help="Optional workers CSV (fallback to DuckDB table 'workers')")
    pe.set_defaults(func=cmd_enrich)
    # Extract labeled dates from a PDF
    pld = sub.add_parser("pdfdates", help="Extract labeled dates (登録/交付/有効/継続) from a PDF")
    pld.add_argument("pdf", help="Path to a PDF file")
    pld.set_defaults(func=cmd_pdfdates)

    # Audit license number extraction with reasons
    paud = sub.add_parser("licenses-audit", help="Audit license-no extraction with reasons")
    paud.add_argument("input", help="PDF file or directory")
    paud.add_argument("--window", type=int, default=1, help="Context window for labels (+/- lines)")
    paud.add_argument("--include-rejected", action="store_true", help="Also list rejected candidates")
    paud.add_argument("--out", help="Optional CSV to write")
    def _cmd_audit(args: argparse.Namespace) -> int:
        root = Path(args.input)
        frames = []
        if root.is_file():
            df = audit_license_pdf(root, window=int(args.window), include_rejected=bool(args.include_rejected))
            if not df.empty:
                df.insert(0, "source", str(root))
                frames.append(df)
        else:
            for pth in sorted(root.rglob("*.pdf")):
                df = audit_license_pdf(pth, window=int(args.window), include_rejected=bool(args.include_rejected))
                if not df.empty:
                    df.insert(0, "source", str(pth))
                    frames.append(df)
        if not frames:
            print("No candidates found.")
            return 0
        out = pd.concat(frames, ignore_index=True)
        if getattr(args, "out", None):
            Path(args.out).parent.mkdir(parents=True, exist_ok=True)
            out.to_csv(args.out, index=False, encoding="utf-8-sig")
            print(f"Wrote {args.out} ({len(out)} rows)")
        else:
            print(out.to_string(index=False))
        return 0
    paud.set_defaults(func=_cmd_audit)

    pw = sub.add_parser("workers", help="Fetch worker list from SQL Server and export")
    pw.add_argument("--host", dest="host", default=os.getenv("DB_HOST"), help="SQL Server host (e.g., 192.9.200.253\\OBCINSTANCE4)")
    pw.add_argument("--db", dest="db", default=os.getenv("DB_NAME"), help="Database name")
    pw.add_argument("--schema", dest="schema", default=os.getenv("DB_SCHEMA", "dbo"), help="Schema name (default: dbo)")
    pw.add_argument("--table", dest="table", default=os.getenv("DB_WORKER_TABLE", "T_TM_Worker_T"), help="Table name (default: T_TM_Worker_T)")
    pw.add_argument("--user", dest="user", default=os.getenv("DB_USER"), help="DB username")
    pw.add_argument("--password", dest="password", default=os.getenv("DB_PASSWORD"), help="DB password")
    pw.add_argument("--driver", dest="driver", default=os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server"), help="ODBC driver name")
    pw.add_argument("--encrypt", dest="encrypt", default=os.getenv("DB_ENCRYPT"), help="Encrypt=yes|no")
    pw.add_argument("--trust-server-certificate", dest="trust", default=os.getenv("DB_TRUST_SERVER_CERTIFICATE"), help="TrustServerCertificate=yes|no")
    pw.add_argument("--where", dest="where", help="Optional WHERE clause to filter active employees")
    pw.add_argument("--limit", dest="limit", type=int, help="Optional TOP limit for sampling")
    pw.add_argument("--out", dest="out", default="out/workers.csv", help="Output CSV path")
    pw.add_argument("--duckdb", help="Optional DuckDB path to write table 'workers' (default: env DUCKDB_DB_PATH)")
    pw.set_defaults(func=cmd_workers)

    # review CLI (non-web)
    prv = sub.add_parser("review", help="Review per-person licenses and record decisions")
    prv_sub = prv.add_subparsers(dest="review_cmd", required=True)

    pvp = prv_sub.add_parser("persons", help="List persons with counts from DuckDB roster")
    pvp.add_argument("--duckdb", help="Path to DuckDB DB (default: env DUCKDB_DB_PATH)")
    pvp.add_argument("--q", help="Substring filter for name")
    pvp.add_argument("--active", action="store_true", help="Limit to names present in workers table")
    pvp.set_defaults(func=cmd_review_persons)

    pvs = prv_sub.add_parser("show", help="Show licenses for one person")
    pvs.add_argument("name", help="Exact name to show")
    pvs.add_argument("--duckdb", help="Path to DuckDB DB (default: env DUCKDB_DB_PATH)")
    pvs.add_argument("--with-decisions", action="store_true", help="Include recorded decisions")
    pvs.set_defaults(func=cmd_review_show)

    pvm = prv_sub.add_parser("mark", help="Record a decision for a person/license")
    pvm.add_argument("name", help="Person name")
    pvm.add_argument("--license-no", help="License number (optional)")
    pvm.add_argument("--status", choices=["ok", "needs_update"], required=True)
    pvm.add_argument("--notes", help="Optional notes")
    pvm.add_argument("--review-db", default="warehouse/review.sqlite")
    pvm.set_defaults(func=cmd_review_mark)

    pve = prv_sub.add_parser("export", help="Export roster with decisions merged")
    pve.add_argument("--duckdb", help="Path to DuckDB DB (default: env DUCKDB_DB_PATH)")
    pve.add_argument("--review-db", default="warehouse/review.sqlite")
    pve.add_argument("--out", default="out/review_export.csv")
    pve.set_defaults(func=cmd_review_export)

    # local GUI
    pg = sub.add_parser("gui", help="Launch local GUI (Tkinter)")
    pg.add_argument("--duckdb", help="Path to DuckDB DB (default: env DUCKDB_DB_PATH or warehouse/local.duckdb)")
    pg.set_defaults(func=cmd_gui)

    return p


def _spawn_auto_enter(titles: list[str]):
    if os.name != "nt":
        return None
    # PowerShell loop that focuses known dialog titles and presses Enter
    ps = r'''
$ErrorActionPreference = 'SilentlyContinue'
$titles = @(%s)
$wshell = New-Object -ComObject WScript.Shell
while ($true) {
  Start-Sleep -Milliseconds 200
  foreach ($t in $titles) {
    if ($wshell.AppActivate($t)) { Start-Sleep -Milliseconds 120; $wshell.SendKeys('{ENTER}') }
  }
}
''' % (", ".join([f"'{t.strip()}'" for t in titles if t.strip()]))
    try:
        proc = __import__("subprocess").Popen([
            "powershell", "-NoProfile", "-WindowStyle", "Hidden", "-Command", ps
        ])
        return proc
    except Exception:
        return None


def cmd_xdw2pdf(args: argparse.Namespace) -> int:
    if not find_dwviewer():
        print("DocuWorks Viewer not found (dwviewer.exe). Install DocuWorks Viewer/Desk and retry.", file=sys.stderr)
        return 2
    root = Path(args.input)
    if not root.exists():
        print(f"Directory not found: {root}", file=sys.stderr)
        return 2
    helper = None
    if getattr(args, "auto_enter", False):
        titles = [t for t in str(getattr(args, "save_window_titles", "")).split(";") if t]
        helper = _spawn_auto_enter(titles)
        if helper is None:
            print("--auto-enter requested but helper could not start (non-Windows or PowerShell unavailable). Continuing without it.")
    vpath = getattr(args, "viewer", None)
    results = batch_convert(root, recurse=not args.no_recurse, printer=args.printer, viewer=vpath)
    failed = [str(p) for p, rc in results if rc != 0]
    print(f"Printed {len(results)} files to '{args.printer}'. Failures: {len(failed)}")
    if failed:
        for f in failed[:10]:
            print("  -", f)
    if helper is not None:
        try:
            helper.terminate()
        except Exception:
            pass
    return 0 if not failed else 1


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())


