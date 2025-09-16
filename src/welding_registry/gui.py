from __future__ import annotations

import os
import errno
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd

try:  # stdlib on CPython (Tk may be missing on minimal Linux)
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
except Exception as e:  # pragma: no cover - GUI import errors are user environment issues
    raise RuntimeError(
        "Tkinter is not available in this Python. Install Tk support or use the web app."
    ) from e

from .io_excel import list_sheets, read_sheet, to_canonical, write_csv, write_xlsx
from .normalize import normalize
from .db import to_duckdb
from .reminders import compute_due, DueConfig, write_ics


@dataclass
class IngestConfig:
    excel_path: Optional[Path] = None
    sheet: Optional[str] = None
    header_row: Optional[int] = None
    vertical_mode: bool = False
    person_col: str = "A"
    regno_col: str = "B"
    jis_block: str = "C:H"
    boiler_block: str = "I:K"


class LocalApp(ttk.Frame):
    def __init__(self, master: tk.Tk, *, duckdb_path: Path) -> None:
        super().__init__(master)
        self.master = master
        self.duckdb_path = duckdb_path
        self.pack(fill=tk.BOTH, expand=True)
        self._build_ui()

        # Working state
        self._ingest_cfg = IngestConfig()
        self._due_df: Optional[pd.DataFrame] = None

    # UI
    def _build_ui(self) -> None:
        nb = ttk.Notebook(self)
        nb.pack(fill=tk.BOTH, expand=True)

        self.tab_home = ttk.Frame(nb)
        self.tab_report = ttk.Frame(nb)
        nb.add(self.tab_home, text="ホーム")
        nb.add(self.tab_report, text="レポート")

        # Home tab
        self._build_home(self.tab_home)
        # Report tab
        self._build_report(self.tab_report)

    def _build_home(self, root: ttk.Frame) -> None:
        # DuckDB path
        frm_db = ttk.Labelframe(root, text="データベース (DuckDB)")
        frm_db.pack(fill=tk.X, padx=8, pady=6)
        self.var_duck = tk.StringVar(value=str(self.duckdb_path))
        ttk.Label(frm_db, text="ファイル:").grid(row=0, column=0, sticky=tk.W, padx=6, pady=6)
        ent_db = ttk.Entry(frm_db, textvariable=self.var_duck, width=60)
        ent_db.grid(row=0, column=1, sticky=tk.W, padx=4, pady=6)
        ttk.Button(frm_db, text="参照", command=self._choose_duckdb).grid(row=0, column=2, padx=4)

        # Ingest
        frm_ing = ttk.Labelframe(root, text="ロスター取込 (Excel)")
        frm_ing.pack(fill=tk.X, padx=8, pady=6)

        self.var_excel = tk.StringVar()
        self.var_sheet = tk.StringVar()
        self.var_hdr = tk.StringVar()
        self.var_vertical = tk.BooleanVar(value=False)
        self.var_person_col = tk.StringVar(value="A")
        self.var_regno_col = tk.StringVar(value="B")
        self.var_jis = tk.StringVar(value="C:H")
        self.var_boiler = tk.StringVar(value="I:K")

        # Row 0: file
        ttk.Label(frm_ing, text="Excel:").grid(row=0, column=0, sticky=tk.W, padx=6, pady=6)
        ent_x = ttk.Entry(frm_ing, textvariable=self.var_excel, width=60)
        ent_x.grid(row=0, column=1, sticky=tk.W, padx=4)
        ttk.Button(frm_ing, text="参照", command=self._choose_excel).grid(row=0, column=2, padx=4)

        # Row 1: sheet + header
        ttk.Label(frm_ing, text="シート:").grid(row=1, column=0, sticky=tk.W, padx=6)
        self.cb_sheet = ttk.Combobox(
            frm_ing, textvariable=self.var_sheet, width=40, state="readonly"
        )
        self.cb_sheet.grid(row=1, column=1, sticky=tk.W, padx=4)
        ttk.Label(frm_ing, text="ヘッダー行(任意):").grid(row=1, column=2, sticky=tk.E, padx=4)
        ttk.Entry(frm_ing, textvariable=self.var_hdr, width=6).grid(
            row=1, column=3, sticky=tk.W, padx=4
        )

        # Row 2: vertical mode options
        ttk.Checkbutton(
            frm_ing,
            text="縦レイアウト (A列=氏名)",
            variable=self.var_vertical,
            command=self._toggle_vertical,
        ).grid(row=2, column=0, sticky=tk.W, padx=6, pady=2)
        ttk.Label(frm_ing, text="氏名列:").grid(row=2, column=1, sticky=tk.E)
        self.ent_person = ttk.Entry(frm_ing, textvariable=self.var_person_col, width=4)
        self.ent_person.grid(row=2, column=2, sticky=tk.W)
        ttk.Label(frm_ing, text="番号列:").grid(row=2, column=3, sticky=tk.E)
        self.ent_regno = ttk.Entry(frm_ing, textvariable=self.var_regno_col, width=4)
        self.ent_regno.grid(row=2, column=4, sticky=tk.W)
        ttk.Label(frm_ing, text="JISブロック C:H 等:").grid(row=3, column=1, sticky=tk.E, padx=2)
        self.ent_jis = ttk.Entry(frm_ing, textvariable=self.var_jis, width=10)
        self.ent_jis.grid(row=3, column=2, sticky=tk.W)
        ttk.Label(frm_ing, text="ボイラ I:K 等:").grid(row=3, column=3, sticky=tk.E)
        self.ent_boiler = ttk.Entry(frm_ing, textvariable=self.var_boiler, width=10)
        self.ent_boiler.grid(row=3, column=4, sticky=tk.W)

        # Row 4: actions
        ttk.Button(frm_ing, text="取込→DuckDB", command=self._run_ingest).grid(
            row=4, column=0, padx=6, pady=8, sticky=tk.W
        )
        ttk.Button(frm_ing, text="期限を計算", command=self._run_due).grid(
            row=4, column=1, padx=6, pady=8, sticky=tk.W
        )
        ttk.Button(frm_ing, text="CSV出力", command=self._export_roster_csv).grid(
            row=4, column=2, padx=6, pady=8, sticky=tk.W
        )

        for i in range(5):
            frm_ing.grid_columnconfigure(i, weight=0)
        frm_ing.grid_columnconfigure(1, weight=1)

        # Status
        frm_status = ttk.Labelframe(root, text="状態")
        frm_status.pack(fill=tk.BOTH, expand=True, padx=8, pady=6)
        self.txt_status = tk.Text(frm_status, height=6)
        self.txt_status.pack(fill=tk.BOTH, expand=True)

        self._toggle_vertical()  # set initial disabled state

    def _build_report(self, root: ttk.Frame) -> None:
        # Controls
        ctl = ttk.Frame(root)
        ctl.pack(fill=tk.X, padx=8, pady=6)
        ttk.Button(ctl, text="再読込", command=self._reload_due).pack(side=tk.LEFT)
        ttk.Label(ctl, text="表示対象: 90日以内 + 期限切れ").pack(side=tk.LEFT, padx=10)
        ttk.Button(ctl, text="CSV", command=self._export_due_csv).pack(side=tk.RIGHT)
        ttk.Button(ctl, text="ICS", command=self._export_due_ics).pack(side=tk.RIGHT, padx=4)
        ttk.Button(ctl, text="印刷プレビュー", command=self._print_preview).pack(side=tk.RIGHT)

        # Table
        cols = (
            "name",
            "birth_year_west",
            "qualification",
            "license_no",
            "expiry_date",
            "days_to_expiry",
            "notice_stage",
        )
        self.tree = ttk.Treeview(root, columns=cols, show="headings", height=18)
        for c, text in zip(
            cols, ("氏名", "生年", "資格", "登録番号", "有効期限", "残日数", "通知")
        ):
            self.tree.heading(c, text=text)
            self.tree.column(c, width=120 if c != "name" else 220, anchor=tk.W)
        self.tree.pack(fill=tk.BOTH, expand=True, padx=8, pady=6)

    # Helpers
    def _log(self, msg: str) -> None:
        self.txt_status.insert(tk.END, f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n")
        self.txt_status.see(tk.END)

    def _choose_duckdb(self) -> None:
        path = filedialog.asksaveasfilename(
            title="DuckDBファイルを選択/作成",
            initialfile=str(self.var_duck.get()),
            defaultextension=".duckdb",
            filetypes=[("DuckDB", "*.duckdb"), ("All", "*.*")],
        )
        if path:
            self.var_duck.set(path)
            self.duckdb_path = Path(path)
            self._log(f"DuckDB: {path}")

    def _choose_excel(self) -> None:
        path = filedialog.askopenfilename(
            title="Excelファイルを選択", filetypes=[("Excel", "*.xlsx;*.xls"), ("All", "*.*")]
        )
        if not path:
            return
        self.var_excel.set(path)
        try:
            sheets = list_sheets(Path(path))
            self.cb_sheet.configure(values=sheets)
            if sheets:
                self.var_sheet.set(sheets[0])
        except Exception as e:
            messagebox.showerror("エラー", f"シート取得に失敗しました: {e}")

    def _toggle_vertical(self) -> None:
        state = tk.NORMAL if self.var_vertical.get() else tk.DISABLED
        for w in (self.ent_person, self.ent_regno, self.ent_jis, self.ent_boiler):
            w.configure(state=state)

    # Actions
    def _run_ingest(self) -> None:
        try:
            duck = Path(self.var_duck.get()).resolve()
            xls = Path(self.var_excel.get()).resolve()
            if not xls.exists():
                messagebox.showwarning("入力不足", "Excelファイルを選択してください。")
                return
            sheet = self.var_sheet.get() or None
            header_row = int(self.var_hdr.get()) if self.var_hdr.get().strip() else None

            if self.var_vertical.get():
                from .io_excel import read_vertical_blocks

                blocks: list[tuple[str, str]] = []
                if self.var_jis.get().strip():
                    blocks.append(("JIS", self.var_jis.get().strip()))
                if self.var_boiler.get().strip():
                    blocks.append(("BOILER", self.var_boiler.get().strip()))
                if not blocks:
                    blocks = [("JIS", "C:H"), ("BOILER", "I:K")]

                df = read_vertical_blocks(
                    xls_path=xls,
                    sheet=sheet or list_sheets(xls)[0],
                    person_col=self.var_person_col.get().strip() or "A",
                    regno_col=self.var_regno_col.get().strip() or "B",
                    blocks=blocks,
                )
                roster = df[
                    [
                        "name",
                        "license_no",
                        "qualification",
                        "category",
                        "first_issue_date",
                        "issue_date",
                        "expiry_date",
                    ]
                ].copy()
            else:
                df_raw, _ = read_sheet(
                    xls, sheet or list_sheets(xls)[0], header_row_override=header_row
                )
                df = to_canonical(df_raw)
                # collapse duplicate columns
                if df.columns.duplicated().any():
                    dup_names = [n for n, c in df.columns.value_counts().items() if c > 1]
                    for name in dup_names:
                        cols = [c for c in df.columns if c == name]
                        base = df.loc[:, cols].bfill(axis=1).iloc[:, 0]
                        df[name] = base
                    df = df.loc[:, ~df.columns.duplicated()]
                roster = normalize(df)

            # Write outputs
            outdir = Path("out")
            outdir.mkdir(parents=True, exist_ok=True)
            write_csv(roster, outdir / "roster.csv")
            write_xlsx(roster, outdir / "roster.xlsx")
            to_duckdb(roster, duck, table="roster")
            self._log(f"ロスター取込完了: {len(roster)} 行。DuckDBに保存 ({duck}).")
            messagebox.showinfo(
                "完了", "ロスターを取り込みました。『レポート』タブで集計してください。"
            )
        except PermissionError as e:
            target = getattr(e, 'filename', None) or getattr(e, 'filename2', None)
            if not target:
                target = str(duck if 'duck' in locals() else Path.cwd())
            message = (
                '出力先に書き込めないため処理を中断しました。\n'
                f'対象パス: {target}\n'
                'ZIP を展開したフォルダや書き込み可能な場所で実行するか、DuckDB/出力先パスを変更してください。'
            )
            messagebox.showerror('取込エラー', message)
            self._log(f"[ERROR] {message}")
            return
        except OSError as e:
            if getattr(e, 'errno', None) in (errno.EACCES, errno.EPERM):
                target = getattr(e, 'filename', None) or getattr(e, 'filename2', None) or str(Path.cwd())
                message = (
                    'ファイルにアクセスできないため処理を中断しました。\n'
                    f'対象パス: {target}\n'
                    '権限を確認するか、別のフォルダに保存してください。'
                )
                messagebox.showerror('取込エラー', message)
                self._log(f"[ERROR] {message}")
                return
            raise
        except Exception as e:
            messagebox.showerror('取込エラー', str(e))
            self._log(f"[ERROR] {e}")

    def _run_due(self) -> None:
        try:
            duck = Path(self.var_duck.get()).resolve()
            import duckdb as _dd  # type: ignore

            con = _dd.connect(str(duck))
            try:
                if not con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name='roster'"
                ).fetchone():
                    messagebox.showwarning(
                        "データなし",
                        "DuckDBに roster テーブルがありません。先に取込を実行してください。",
                    )
                    return
                # Minimal set of columns
                base = con.execute(
                    "SELECT name, license_no, qualification, expiry_date FROM roster"
                ).df()
            finally:
                con.close()

            due = compute_due(base, cfg=DueConfig(window_days=90))
            # Attach birth year if available
            try:
                con = _dd.connect(str(duck))
                if bool(
                    con.execute(
                        "SELECT 1 FROM information_schema.tables WHERE table_name='roster_enriched'"
                    ).fetchone()
                ):
                    b = con.execute(
                        "SELECT name, birth_year_west FROM roster_enriched WHERE name IS NOT NULL"
                    ).df()
                    if not b.empty:
                        b = b.dropna(subset=["name"]).drop_duplicates(subset=["name"], keep="first")
                        due = due.merge(b, on="name", how="left")
            except Exception:
                pass
            finally:
                try:
                    con.close()
                except Exception:
                    pass

            # Persist and show
            to_duckdb(due, duck, table="due")
            self._due_df = due
            self._populate_due_table(due)
            self._log(f"期限レポートを作成: {len(due)} 行。DuckDBに保存 ({duck}).")
            messagebox.showinfo(
                "完了", "期限レポートを作成しました。『レポート』タブを参照してください。"
            )
        except Exception as e:
            messagebox.showerror("集計エラー", str(e))
            self._log(f"[ERROR] {e}")

    def _populate_due_table(self, df: pd.DataFrame) -> None:
        for i in self.tree.get_children():
            self.tree.delete(i)
        if df is None or df.empty:
            return
        df2 = df.copy()
        # Order columns for display
        order: Sequence[str] = [
            "name",
            "birth_year_west",
            "qualification",
            "license_no",
            "expiry_date",
            "days_to_expiry",
            "notice_stage",
        ]
        for c in order:
            if c not in df2.columns:
                df2[c] = ""
        df2 = df2.loc[:, list(order)]
        # Render as strings
        for c in df2.columns:
            df2[c] = df2[c].astype("string").fillna("")
        for _, r in df2.iterrows():
            self.tree.insert("", tk.END, values=tuple(r[c] for c in df2.columns))

    def _reload_due(self) -> None:
        try:
            import duckdb as _dd  # type: ignore

            con = _dd.connect(str(Path(self.var_duck.get())))
            try:
                if not con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name='due'"
                ).fetchone():
                    self._log(
                        "'due' テーブルが見つかりません。先に『期限を計算』を実行してください。"
                    )
                    return
                df = con.execute("SELECT * FROM due").df()
            finally:
                con.close()
            self._due_df = df
            self._populate_due_table(df)
            self._log(f"期限レポートを再読込: {len(df)} 行。")
        except Exception as e:
            messagebox.showerror("読込エラー", str(e))

    def _export_roster_csv(self) -> None:
        # Save last ingested roster (from DuckDB)
        try:
            import duckdb as _dd

            con = _dd.connect(str(Path(self.var_duck.get())))
            try:
                if not con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name='roster'"
                ).fetchone():
                    messagebox.showwarning(
                        "データなし",
                        "DuckDBに roster テーブルがありません。先に取込を実行してください。",
                    )
                    return
                df = con.execute("SELECT * FROM roster").df()
            finally:
                con.close()
            path = filedialog.asksaveasfilename(
                title="CSVに保存",
                defaultextension=".csv",
                initialfile="roster.csv",
                filetypes=[("CSV", "*.csv")],
            )
            if not path:
                return
            df.to_csv(Path(path), index=False, encoding="utf-8-sig")
            self._log(f"CSV出力: {path}")
        except Exception as e:
            messagebox.showerror("書き出しエラー", str(e))

    def _export_due_csv(self) -> None:
        try:
            df = self._current_due()
            if df is None or df.empty:
                messagebox.showwarning(
                    "データなし", "表示できる行がありません。先に『期限を計算』を実行してください。"
                )
                return
            path = filedialog.asksaveasfilename(
                title="CSVに保存",
                defaultextension=".csv",
                initialfile="due.csv",
                filetypes=[("CSV", "*.csv")],
            )
            if not path:
                return
            df.to_csv(Path(path), index=False, encoding="utf-8-sig")
            self._log(f"CSV出力: {path}")
        except Exception as e:
            messagebox.showerror("書き出しエラー", str(e))

    def _export_due_ics(self) -> None:
        try:
            df = self._current_due()
            if df is None or df.empty:
                messagebox.showwarning(
                    "データなし", "表示できる行がありません。先に『期限を計算』を実行してください。"
                )
                return
            path = filedialog.asksaveasfilename(
                title="ICSに保存",
                defaultextension=".ics",
                initialfile="due.ics",
                filetypes=[("iCalendar", "*.ics")],
            )
            if not path:
                return
            write_ics(df, Path(path))
            self._log(f"ICS出力: {path}")
        except Exception as e:
            messagebox.showerror("書き出しエラー", str(e))

    def _current_due(self) -> Optional[pd.DataFrame]:
        if self._due_df is not None:
            return self._due_df
        try:
            import duckdb as _dd

            con = _dd.connect(str(Path(self.var_duck.get())))
            try:
                if not con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name='due'"
                ).fetchone():
                    return None
                return con.execute("SELECT * FROM due").df()
            finally:
                con.close()
        except Exception:
            return None

    def _print_preview(self) -> None:
        try:
            df = self._current_due()
            if df is None or df.empty:
                messagebox.showwarning(
                    "データなし", "表示できる行がありません。先に『期限を計算』を実行してください。"
                )
                return
            # Render Jinja2 template directly (no server)
            from jinja2 import Environment, FileSystemLoader  # type: ignore

            tpl_dir = Path(__file__).parent / "templates"
            env = Environment(loader=FileSystemLoader(str(tpl_dir)))
            tpl = env.get_template("print.html")

            # Chunk rows
            rows = []
            df2 = df.copy()
            for c in (
                "name",
                "license_no",
                "qualification",
                "expiry_date",
                "days_to_expiry",
                "notice_stage",
                "birth_year_west",
            ):
                if c in df2.columns:
                    df2[c] = df2[c].astype("string").fillna("")
            for _, r in df2.iterrows():
                rows.append(dict(r))

            rows_per_page = 40
            pages = []
            for i in range(0, len(rows), rows_per_page):
                pages.append({"no": (i // rows_per_page) + 1, "rows": rows[i : i + rows_per_page]})

            html = tpl.render(
                title="資格更新 期限レポート",
                orientation="portrait",
                rows_per_page=rows_per_page,
                pages=pages,
                total=len(pages),
                q="",
                only_active=False,
                now=datetime.now().strftime("%Y-%m-%d %H:%M"),
            )
            out_dir = Path("out")
            out_dir.mkdir(parents=True, exist_ok=True)
            out_html = out_dir / "print_report.html"
            out_html.write_text(html, encoding="utf-8")
            import webbrowser

            webbrowser.open(out_html.resolve().as_uri())
            self._log(f"印刷プレビューを生成: {out_html}")
        except Exception as e:
            messagebox.showerror("プレビューエラー", str(e))


def run(warehouse: Optional[Path] = None) -> int:
    # Decide DuckDB path
    duck = warehouse or Path(os.getenv("DUCKDB_DB_PATH") or "warehouse/local.duckdb")
    root = tk.Tk()
    root.title("Welding Registry / ローカルアプリ")
    root.geometry("980x720")
    LocalApp(root, duckdb_path=duck)
    root.mainloop()
    return 0
