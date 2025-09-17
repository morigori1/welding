from __future__ import annotations

import os
import errno
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Sequence

import pandas as pd

try:  # stdlib on CPython (Tk may be missing on minimal Linux)
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox, simpledialog
except Exception as e:  # pragma: no cover - GUI import errors are user environment issues
    raise RuntimeError(
        "Tkinter is not available in this Python. Install Tk support or use the web app."
    ) from e

from .io_excel import list_sheets, read_sheet, to_canonical, write_csv, write_xlsx
from .normalize import normalize
from .db import to_duckdb
from .warehouse import (
    ensure_issue_schema,
    load_issue_run_items,
    load_issue_runs,
    materialize_roster_all,
    reapply_due_filters,
    record_issue_run,
    set_license_filter,
    set_person_filter,
    write_due_tables,
)
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
        ensure_issue_schema(self.duckdb_path)
        self.pack(fill=tk.BOTH, expand=True)
        self._build_ui()

        # Working state
        self._ingest_cfg = IngestConfig()
        self._due_current_df: Optional[pd.DataFrame] = None
        self._due_df: Optional[pd.DataFrame] = None
        self._roster_df: pd.DataFrame = pd.DataFrame()
        self._people_df: pd.DataFrame = pd.DataFrame()
        self._license_df: pd.DataFrame = pd.DataFrame()
        self._selected_person_key: Optional[str] = None
        self._selected_run_id: Optional[str] = None
        self._selected_run_meta: Optional[dict[str, Any]] = None

    # UI
    def _build_ui(self) -> None:
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill=tk.BOTH, expand=True)

        self.tab_register = ttk.Frame(self.nb)
        self.tab_people = ttk.Frame(self.nb)
        self.tab_issue = ttk.Frame(self.nb)
        self.nb.add(self.tab_register, text="登録")
        self.nb.add(self.tab_people, text="全体リスト")
        self.nb.add(self.tab_issue, text="発行")

        self._build_register_tab(self.tab_register)
        self._build_people_tab(self.tab_people)
        self._build_issue_tab(self.tab_issue)

        self.nb.bind("<<NotebookTabChanged>>", self._on_tab_changed)

    @staticmethod
    def _disp(value: Any) -> str:
        if value is None:
            return ""
        try:
            import pandas as _pd
            if _pd.isna(value):
                return ""
        except Exception:
            pass
        return str(value)

    def _build_register_tab(self, root: ttk.Frame) -> None:
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

    def _build_issue_tab(self, root: ttk.Frame) -> None:
        control = ttk.Frame(root)
        control.pack(fill=tk.X, padx=8, pady=6)
        ttk.Button(control, text="再読込", command=self._reload_due).pack(side=tk.LEFT)
        ttk.Button(control, text="発行", command=self._issue_current_run).pack(side=tk.LEFT, padx=4)
        ttk.Button(control, text="帳票プレビュー", command=self._print_preview).pack(side=tk.RIGHT)
        ttk.Button(control, text="ICS", command=self._export_due_ics).pack(side=tk.RIGHT, padx=4)
        ttk.Button(control, text="CSV", command=self._export_due_csv).pack(side=tk.RIGHT)

        caption = ttk.Frame(root)
        caption.pack(fill=tk.X, padx=8)
        self.var_due_caption = tk.StringVar(value="現在の期限一覧")
        ttk.Label(caption, textvariable=self.var_due_caption).pack(anchor=tk.W)

        split = ttk.Panedwindow(root, orient=tk.HORIZONTAL)
        split.pack(fill=tk.BOTH, expand=True, padx=8, pady=6)

        due_frame = ttk.Frame(split)
        due_frame.columnconfigure(0, weight=1)
        due_frame.rowconfigure(0, weight=1)
        split.add(due_frame, weight=3)

        cols = (
            "name",
            "birth_year_west",
            "qualification",
            "license_no",
            "expiry_date",
            "days_to_expiry",
            "notice_stage",
        )
        self.tree_due = ttk.Treeview(due_frame, columns=cols, show="headings", height=18)
        headings = (
            ("name", "氏名", 220),
            ("birth_year_west", "西暦生年", 90),
            ("qualification", "資格", 180),
            ("license_no", "登録番号", 120),
            ("expiry_date", "期限", 110),
            ("days_to_expiry", "残日数", 80),
            ("notice_stage", "通知", 80),
        )
        for col, title, width in headings:
            self.tree_due.heading(col, text=title)
            anchor = tk.W if col in {"name", "qualification"} else tk.CENTER
            self.tree_due.column(col, width=width, anchor=anchor)
        self.tree_due.grid(row=0, column=0, sticky="nsew")
        self.tree_due.bind("<Double-1>", lambda _evt: None)
        due_scroll = ttk.Scrollbar(due_frame, orient=tk.VERTICAL, command=self.tree_due.yview)
        self.tree_due.configure(yscrollcommand=due_scroll.set)
        due_scroll.grid(row=0, column=1, sticky="ns")

        history = ttk.Frame(split, width=260)
        history.columnconfigure(0, weight=1)
        history.rowconfigure(1, weight=1)
        split.add(history, weight=1)

        ttk.Label(history, text="発行履歴").grid(row=0, column=0, sticky="w")
        run_cols = ("run_id", "created_at", "created_by", "comment", "row_count")
        self.history_tree = ttk.Treeview(history, columns=run_cols, show="headings", height=12, selectmode="browse")
        self.history_tree.heading("run_id", text="ID")
        self.history_tree.heading("created_at", text="日時")
        self.history_tree.heading("created_by", text="担当")
        self.history_tree.heading("comment", text="コメント")
        self.history_tree.heading("row_count", text="件数")
        self.history_tree.column("run_id", width=0, stretch=False)
        self.history_tree.column("created_at", width=140, anchor=tk.W)
        self.history_tree.column("created_by", width=80, anchor=tk.W)
        self.history_tree.column("comment", width=160, anchor=tk.W)
        self.history_tree.column("row_count", width=60, anchor=tk.E)
        self.history_tree.grid(row=1, column=0, sticky="nsew")
        self.history_tree.bind("<<TreeviewSelect>>", self._on_history_select)
        hist_scroll = ttk.Scrollbar(history, orient=tk.VERTICAL, command=self.history_tree.yview)
        self.history_tree.configure(yscrollcommand=hist_scroll.set)
        hist_scroll.grid(row=1, column=1, sticky="ns")

        ttk.Button(history, text="履歴選択を解除", command=self._clear_history_selection).grid(
            row=2, column=0, sticky="ew", pady=(6, 0)
        )
    def _on_tab_changed(self, event: tk.Event) -> None:
        if not hasattr(self, "nb"):
            return
        tab_text = event.widget.tab(event.widget.select(), "text")
        if tab_text == "全体リスト":
            self._refresh_people_data()
        elif tab_text == "発行":
            self._refresh_issue_history()
            if self._due_current_df is None:
                self._reload_due(quiet=True)

    def _refresh_people_data(self) -> None:
        try:
            roster = materialize_roster_all(self.duckdb_path)
        except Exception as exc:
            self._log(f"[ERROR] 名簿再構築に失敗: {exc}")
            roster = pd.DataFrame()
        self._roster_df = roster
        self._people_df = self._build_people_dataframe(roster)
        self._populate_people_tree(self._people_df)
        if getattr(self, 'var_people_search', None) and self.var_people_search.get().strip():
            self._filter_people_list()
        if self._selected_person_key and self._selected_person_key in self._people_df.index:
            self.people_tree.selection_set(self._selected_person_key)
            self.people_tree.see(self._selected_person_key)
            self._refresh_license_view(self._selected_person_key)
        else:
            self._selected_person_key = None
            self.var_person_label.set("(未選択)")
            self._populate_license_tree(pd.DataFrame())
            self._populate_manual_history(None)

    def _build_people_dataframe(self, roster: pd.DataFrame) -> pd.DataFrame:
        if roster is None or roster.empty:
            return pd.DataFrame(columns=["display_name", "license_count", "include"]).set_index(pd.Index([], name="person_key"))
        names = (
            roster.groupby("person_key")["name"]
            .apply(lambda s: next((str(v).strip() for v in s if isinstance(v, str) and str(v).strip()), ""))
            .rename("display_name")
        )
        counts = roster.groupby("person_key")["license_key"].nunique().rename("license_count")
        people = pd.concat([names, counts], axis=1).fillna({"display_name": "", "license_count": 0})
        filters = self._load_person_filters()
        if not filters.empty:
            filters = filters.set_index("person_key")
            people = people.join(filters["include"], how="left")
        people["include"] = people["include"].fillna(True)
        people["license_count"] = people["license_count"].astype(int)
        people = people.sort_values(by=["include", "display_name"], ascending=[False, True])
        people.index.name = "person_key"
        return people

    def _populate_people_tree(self, df: pd.DataFrame) -> None:
        for item in self.people_tree.get_children():
            self.people_tree.delete(item)
        if df is None or df.empty:
            return
        for person_key, row in df.iterrows():
            include_label = "載せる" if bool(row.get("include", True)) else "外す"
            count = int(row.get("license_count", 0))
            display = self._disp(row.get("display_name")) or str(person_key)
            self.people_tree.insert("", tk.END, iid=str(person_key), values=(display, include_label, count))

    def _filter_people_list(self) -> None:
        if self._people_df is None or self._people_df.empty:
            for item in self.people_tree.get_children():
                self.people_tree.delete(item)
            return
        query = self.var_people_search.get().strip().lower()
        if not query:
            subset = self._people_df
        else:
            subset = self._people_df[
                self._people_df["display_name"].str.lower().str.contains(query, na=False)
                | self._people_df.index.str.lower().str.contains(query, na=False)
            ]
        self._populate_people_tree(subset)

    def _clear_people_filter(self) -> None:
        if hasattr(self, "var_people_search"):
            self.var_people_search.set("")
        self._filter_people_list()

    def _on_person_selected(self, _event: tk.Event | None = None) -> None:
        selection = self.people_tree.selection()
        if not selection:
            self._selected_person_key = None
            self.var_person_label.set("(未選択)")
            self._populate_license_tree(pd.DataFrame())
            self._populate_manual_history(None)
            return
        key = selection[0]
        self._selected_person_key = key
        display = ""
        if key in self._people_df.index:
            display = self._people_df.loc[key, "display_name"]
        self.var_person_label.set(self._disp(display) or key)
        self._refresh_license_view(key)

    def _toggle_selected_person(self, _event: tk.Event | None = None) -> None:
        if not self._selected_person_key:
            return
        current = bool(self._people_df.loc[self._selected_person_key, "include"]) if self._selected_person_key in self._people_df.index else True
        self._set_selected_person_include(not current)

    def _set_selected_person_include(self, include: bool) -> None:
        key = self._selected_person_key or (self.people_tree.selection()[0] if self.people_tree.selection() else None)
        if not key:
            messagebox.showinfo("選択なし", "対象者を選択してください。", parent=self)
            return
        try:
            set_person_filter(self.duckdb_path, key, include)
        except Exception as exc:
            messagebox.showerror("更新エラー", str(exc), parent=self)
            return
        self._log(f"個人フィルタ更新: {key} -> {'載せる' if include else '外す'}")
        self._refresh_people_data()
        reapply_due_filters(self.duckdb_path)
        self._reload_due(quiet=True)

    def _set_all_people(self, include: bool) -> None:
        if self._people_df is None or self._people_df.empty:
            return
        action = "載せる" if include else "外す"
        if not messagebox.askyesno("確認", f"全員を「{action}」に更新します。よろしいですか？", parent=self):
            return
        try:
            for key in self._people_df.index.tolist():
                set_person_filter(self.duckdb_path, key, include)
        except Exception as exc:
            messagebox.showerror("更新エラー", str(exc), parent=self)
            return
        self._log(f"全員を「{action}」に設定しました。")
        self._refresh_people_data()
        reapply_due_filters(self.duckdb_path)
        self._reload_due(quiet=True)

    def _refresh_license_view(self, person_key: str) -> None:
        if self._roster_df is None or self._roster_df.empty:
            self._populate_license_tree(pd.DataFrame())
            self._populate_manual_history(None)
            return
        df = self._roster_df[self._roster_df["person_key"] == person_key].copy()
        if df.empty:
            self._populate_license_tree(pd.DataFrame())
            self._populate_manual_history(None)
            return
        filters = self._load_license_filters()
        if not filters.empty:
            df = df.merge(filters, on="license_key", how="left", suffixes=("", "_flt"))
            df["include"] = df["include"].fillna(df["include_flt"]).fillna(True)
            df = df.drop(columns=[c for c in df.columns if c.endswith("_flt")])
        else:
            df["include"] = True
        df = df.sort_values(by=["include", "qualification", "license_no"], ascending=[False, True, True])
        self._license_df = df.set_index("license_key", drop=False)
        self._populate_license_tree(self._license_df)
        self._populate_manual_history(person_key, df)

    def _populate_license_tree(self, df: pd.DataFrame) -> None:
        for item in self.license_tree.get_children():
            self.license_tree.delete(item)
        self._selected_license_key = None
        if df is None or df.empty:
            return
        for _, row in df.iterrows():
            key = str(row.get("license_key"))
            include_label = "載せる" if bool(row.get("include", True)) else "外す"
            values = (
                include_label,
                self._disp(row.get("qualification")),
                self._disp(row.get("license_no")),
                self._disp(row.get("expiry_date")),
                self._disp(row.get("source")),
            )
            self.license_tree.insert("", tk.END, iid=key, values=values)

    def _on_license_selected(self, _event: tk.Event | None = None) -> None:
        selection = self.license_tree.selection()
        self._selected_license_key = selection[0] if selection else None

    def _toggle_selected_license(self, _event: tk.Event | None = None) -> None:
        key = self._selected_license_key or (self.license_tree.selection()[0] if self.license_tree.selection() else None)
        if not key or key not in self._license_df.index:
            return
        current = bool(self._license_df.loc[key, "include"])
        self._set_selected_license_include(not current)

    def _set_selected_license_include(self, include: bool) -> None:
        key = self._selected_license_key or (self.license_tree.selection()[0] if self.license_tree.selection() else None)
        if not key or key not in self._license_df.index:
            messagebox.showinfo("選択なし", "資格を選択してください。", parent=self)
            return
        row = self._license_df.loc[key]
        try:
            set_license_filter(self.duckdb_path, key, include, person_key=row.get("person_key"))
        except Exception as exc:
            messagebox.showerror("更新エラー", str(exc), parent=self)
            return
        self._log(f"資格フィルタ更新: {row.get('qualification', '')} -> {'載せる' if include else '外す'}")
        self._refresh_license_view(str(row.get("person_key")))
        reapply_due_filters(self.duckdb_path)
        self._reload_due(quiet=True)

    def _populate_manual_history(self, person_key: str | None, df_person: pd.DataFrame | None = None) -> None:
        for item in self.manual_history_tree.get_children():
            self.manual_history_tree.delete(item)
        if person_key is None:
            return
        try:
            import duckdb as _dd  # type: ignore
            with _dd.connect(str(self.duckdb_path)) as con:
                if not con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name='roster_manual'"
                ).fetchone():
                    return
                names: list[str] = []
                if df_person is not None and "name" in df_person.columns:
                    names = [str(n).strip() for n in df_person["name"].dropna().unique().tolist() if str(n).strip()]
                if not names and self._people_df is not None and person_key in self._people_df.index:
                    candidate = str(self._people_df.loc[person_key, "display_name"]).strip()
                    if candidate:
                        names = [candidate]
                if not names:
                    return
                placeholders = ",".join("?" for _ in names)
                query = f"SELECT name, qualification, license_no, expiry_date, created FROM roster_manual WHERE name IN ({placeholders}) ORDER BY created DESC LIMIT 20"
                hist = con.execute(query, names).df()
        except Exception:
            return
        for _, row in hist.iterrows():
            created = row.get("created")
            if isinstance(created, datetime):
                created_str = created.strftime("%Y-%m-%d %H:%M")
            else:
                created_str = str(created) if created is not None else ""
            self.manual_history_tree.insert(
                "", tk.END, values=(
                    created_str,
                    self._disp(row.get("qualification")),
                    self._disp(row.get("license_no")),
                    self._disp(row.get("expiry_date")),
                )
            )

    def _open_manual_editor(self) -> None:
        key = self._selected_license_key or (self.license_tree.selection()[0] if self.license_tree.selection() else None)
        if not key or key not in self._license_df.index:
            messagebox.showinfo("選択なし", "手修正する資格を選択してください。", parent=self)
            return
        row = self._license_df.loc[key]
        win = tk.Toplevel(self)
        win.title("手修正の登録")
        win.grab_set()
        fields = [
            ("氏名", "name"),
            ("資格", "qualification"),
            ("番号", "license_no"),
            ("初回交付日", "first_issue_date"),
            ("交付日", "issue_date"),
            ("期限*", "expiry_date"),
        ]
        vars_map: dict[str, tk.StringVar] = {}
        for idx, (label, col) in enumerate(fields):
            ttk.Label(win, text=label).grid(row=idx, column=0, sticky=tk.W, padx=8, pady=4)
            value = row.get(col, "")
            vars_map[col] = tk.StringVar(value="" if pd.isna(value) else str(value))
            ttk.Entry(win, textvariable=vars_map[col], width=32).grid(row=idx, column=1, sticky=tk.W, padx=8, pady=4)
        ttk.Label(win, text="* 必須項目").grid(row=len(fields), column=1, sticky=tk.W, padx=8)
        btns = ttk.Frame(win)
        btns.grid(row=len(fields) + 1, column=0, columnspan=2, pady=8)
        ttk.Button(btns, text="保存", command=lambda: self._save_manual_edit(win, vars_map, row)).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="キャンセル", command=win.destroy).pack(side=tk.LEFT, padx=4)

    def _save_manual_edit(self, win: tk.Toplevel, values: dict[str, tk.StringVar], source_row: pd.Series) -> None:
        name = values["name"].get().strip()
        expiry = values["expiry_date"].get().strip()
        if not name or not expiry:
            messagebox.showwarning("未入力", "氏名と期限を入力してください。", parent=win)
            return
        payload = [
            name,
            values["license_no"].get().strip() or None,
            values["qualification"].get().strip() or None,
            values["first_issue_date"].get().strip() or None,
            values["issue_date"].get().strip() or None,
            expiry,
        ]
        try:
            import duckdb as _dd  # type: ignore
            with _dd.connect(str(self.duckdb_path)) as con:
                con.execute(
                    "CREATE TABLE IF NOT EXISTS roster_manual (name VARCHAR, license_no VARCHAR, qualification VARCHAR, first_issue_date DATE, issue_date DATE, expiry_date DATE, created TIMESTAMP DEFAULT now())"
                )
                con.execute(
                    "INSERT INTO roster_manual (name, license_no, qualification, first_issue_date, issue_date, expiry_date) VALUES (?, ?, ?, ?, ?, ?)",
                    payload,
                )
        except Exception as exc:
            messagebox.showerror("保存エラー", str(exc), parent=win)
            return
        materialize_roster_all(self.duckdb_path)
        self._log(f"手修正を登録: {name} / {values['qualification'].get().strip()}")
        win.destroy()
        self._refresh_people_data()
        if self._selected_person_key:
            self._refresh_license_view(self._selected_person_key)
        reapply_due_filters(self.duckdb_path)
        self._reload_due(quiet=True)

    def _load_person_filters(self) -> pd.DataFrame:
        try:
            import duckdb as _dd  # type: ignore
            with _dd.connect(str(self.duckdb_path)) as con:
                if not con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name='issue_person_filter'"
                ).fetchone():
                    return pd.DataFrame(columns=["person_key", "include", "notes"])
                return con.execute("SELECT person_key, include, notes FROM issue_person_filter").df()
        except Exception:
            return pd.DataFrame(columns=["person_key", "include", "notes"])

    def _load_license_filters(self) -> pd.DataFrame:
        try:
            import duckdb as _dd  # type: ignore
            with _dd.connect(str(self.duckdb_path)) as con:
                if not con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name='issue_license_filter'"
                ).fetchone():
                    return pd.DataFrame(columns=["license_key", "person_key", "include", "notes"])
                return con.execute("SELECT license_key, person_key, include, notes FROM issue_license_filter").df()
        except Exception:
            return pd.DataFrame(columns=["license_key", "person_key", "include", "notes"])

    def _refresh_issue_history(self, select_run_id: str | None = None) -> None:
        try:
            runs = load_issue_runs(self.duckdb_path)
        except Exception as exc:
            self._log(f"[ERROR] 発行履歴の取得に失敗: {exc}")
            runs = pd.DataFrame()
        for item in self.history_tree.get_children():
            self.history_tree.delete(item)
        if runs.empty:
            return
        for _, row in runs.iterrows():
            run_id = str(row.get("run_id", ""))
            values = (
                run_id,
                self._fmt_timestamp(row.get("created_at")),
                row.get("created_by", "") or "",
                row.get("comment", "") or "",
                int(row.get("row_count", 0) or 0),
            )
            self.history_tree.insert("", tk.END, iid=run_id, values=values)
        if select_run_id and select_run_id in self.history_tree.get_children(""):
            self.history_tree.selection_set(select_run_id)
            self.history_tree.see(select_run_id)

    def _fmt_timestamp(self, value: Any) -> str:
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M")
        if isinstance(value, str):
            return value
        return ""

    def _on_history_select(self, _event: tk.Event | None = None) -> None:
        selection = self.history_tree.selection()
        if not selection:
            self._clear_history_selection()
            return
        run_id = selection[0]
        try:
            df = load_issue_run_items(self.duckdb_path, run_id)
        except Exception as exc:
            messagebox.showerror("履歴エラー", str(exc), parent=self)
            return
        if df is None:
            df = pd.DataFrame()
        self._selected_run_id = run_id
        item = self.history_tree.item(run_id)
        values = item.get("values", [])
        self._selected_run_meta = {
            "run_id": run_id,
            "created_at": values[1] if len(values) > 1 else "",
            "comment": values[3] if len(values) > 3 else "",
        }
        self._due_df = df
        self.var_due_caption.set(f"発行履歴 (ID: {run_id})")
        self._populate_due_table(df)

    def _clear_history_selection(self) -> None:
        self.history_tree.selection_remove(self.history_tree.selection())
        self._selected_run_id = None
        self._selected_run_meta = None
        if self._due_current_df is not None:
            self._due_df = self._due_current_df
            self.var_due_caption.set("現在の期限一覧")
            self._populate_due_table(self._due_current_df)

    def _issue_current_run(self) -> None:
        df = self._current_due()
        if df is None or df.empty:
            messagebox.showwarning("データなし", "発行対象がありません。", parent=self)
            return
        comment = simpledialog.askstring("発行", "コメント (任意)", parent=self)
        try:
            run_id = record_issue_run(self.duckdb_path, df, comment=comment)
        except Exception as exc:
            messagebox.showerror("発行エラー", str(exc), parent=self)
            return
        self._log(f"発行を保存: run_id={run_id} 行数={len(df)}")
        self._refresh_issue_history(select_run_id=run_id)
        messagebox.showinfo("発行完了", "発行履歴に追加しました。", parent=self)

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
            ensure_issue_schema(self.duckdb_path)
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

    def _build_people_tab(self, root: ttk.Frame) -> None:
        container = ttk.Frame(root)
        container.pack(fill=tk.BOTH, expand=True, padx=8, pady=6)
        container.columnconfigure(0, weight=0)
        container.columnconfigure(1, weight=1)
        container.rowconfigure(0, weight=1)

        left = ttk.Frame(container)
        left.grid(row=0, column=0, sticky="ns")

        search_row = ttk.Frame(left)
        search_row.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(search_row, text="検索:").pack(side=tk.LEFT)
        self.var_people_search = tk.StringVar()
        ent_search = ttk.Entry(search_row, textvariable=self.var_people_search, width=24)
        ent_search.pack(side=tk.LEFT, fill=tk.X, expand=True)
        ent_search.bind("<KeyRelease>", lambda _evt: self._filter_people_list())
        ttk.Button(search_row, text="クリア", command=self._clear_people_filter).pack(side=tk.LEFT, padx=4)

        cols = ("name", "include", "count")
        self.people_tree = ttk.Treeview(
            left, columns=cols, show="headings", height=18, selectmode="browse"
        )
        self.people_tree.heading("name", text="氏名")
        self.people_tree.heading("include", text="状態")
        self.people_tree.heading("count", text="資格数")
        self.people_tree.column("name", width=220, anchor=tk.W)
        self.people_tree.column("include", width=80, anchor=tk.CENTER)
        self.people_tree.column("count", width=70, anchor=tk.CENTER)
        self.people_tree.pack(fill=tk.BOTH, expand=True)
        self.people_tree.bind("<<TreeviewSelect>>", self._on_person_selected)
        self.people_tree.bind("<Double-1>", self._toggle_selected_person)

        btn_frame = ttk.Frame(left)
        btn_frame.pack(fill=tk.X, pady=6)
        ttk.Button(btn_frame, text="選択を載せる", command=lambda: self._set_selected_person_include(True)).pack(fill=tk.X)
        ttk.Button(btn_frame, text="選択を外す", command=lambda: self._set_selected_person_include(False)).pack(fill=tk.X, pady=2)
        ttk.Separator(btn_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=4)
        ttk.Button(btn_frame, text="全員載せる", command=lambda: self._set_all_people(True)).pack(fill=tk.X)
        ttk.Button(btn_frame, text="全員外す", command=lambda: self._set_all_people(False)).pack(fill=tk.X, pady=2)

        right = ttk.Frame(container)
        right.grid(row=0, column=1, sticky="nsew", padx=(10, 0))
        right.columnconfigure(0, weight=1)
        right.rowconfigure(1, weight=1)
        right.rowconfigure(3, weight=1)

        header = ttk.Frame(right)
        header.grid(row=0, column=0, sticky="ew")
        ttk.Label(header, text="選択中:").pack(side=tk.LEFT)
        self.var_person_label = tk.StringVar(value="(未選択)")
        ttk.Label(header, textvariable=self.var_person_label).pack(side=tk.LEFT, padx=4)

        license_box = ttk.Labelframe(right, text="資格一覧")
        license_box.grid(row=1, column=0, sticky="nsew")
        license_box.columnconfigure(0, weight=1)
        license_box.rowconfigure(0, weight=1)

        lic_cols = ("include", "qualification", "license_no", "expiry", "source")
        self.license_tree = ttk.Treeview(
            license_box, columns=lic_cols, show="headings", selectmode="browse"
        )
        headings = {
            "include": "状態",
            "qualification": "資格",
            "license_no": "番号",
            "expiry": "期限",
            "source": "ソース",
        }
        widths = {
            "include": 70,
            "qualification": 200,
            "license_no": 120,
            "expiry": 120,
            "source": 80,
        }
        for col, title in headings.items():
            self.license_tree.heading(col, text=title)
            self.license_tree.column(col, width=widths[col], anchor=tk.W if col in ("qualification",) else tk.CENTER)
        self.license_tree.grid(row=0, column=0, sticky="nsew")
        self.license_tree.bind("<<TreeviewSelect>>", self._on_license_selected)
        self.license_tree.bind("<Double-1>", self._toggle_selected_license)
        scroll = ttk.Scrollbar(license_box, orient=tk.VERTICAL, command=self.license_tree.yview)
        self.license_tree.configure(yscrollcommand=scroll.set)
        scroll.grid(row=0, column=1, sticky="ns")

        lic_btns = ttk.Frame(right)
        lic_btns.grid(row=2, column=0, sticky="ew", pady=6)
        ttk.Button(lic_btns, text="選択を載せる", command=lambda: self._set_selected_license_include(True)).pack(side=tk.LEFT)
        ttk.Button(lic_btns, text="選択を外す", command=lambda: self._set_selected_license_include(False)).pack(side=tk.LEFT, padx=4)
        ttk.Button(lic_btns, text="手修正", command=self._open_manual_editor).pack(side=tk.RIGHT)

        hist_box = ttk.Labelframe(right, text="手修正履歴 (最新20件)")
        hist_box.grid(row=3, column=0, sticky="nsew")
        hist_box.columnconfigure(0, weight=1)
        hist_box.rowconfigure(0, weight=1)

        hist_cols = ("created", "qualification", "license_no", "expiry")
        self.manual_history_tree = ttk.Treeview(hist_box, columns=hist_cols, show="headings", height=6)
        labels = {
            "created": "登録日時",
            "qualification": "資格",
            "license_no": "番号",
            "expiry": "期限",
        }
        widths_hist = {
            "created": 140,
            "qualification": 180,
            "license_no": 110,
            "expiry": 110,
        }
        for col, title in labels.items():
            self.manual_history_tree.heading(col, text=title)
            self.manual_history_tree.column(col, width=widths_hist[col], anchor=tk.W if col == "qualification" else tk.CENTER)
        self.manual_history_tree.grid(row=0, column=0, sticky="nsew")
        hist_scroll = ttk.Scrollbar(hist_box, orient=tk.VERTICAL, command=self.manual_history_tree.yview)
        self.manual_history_tree.configure(yscrollcommand=hist_scroll.set)
        hist_scroll.grid(row=0, column=1, sticky="ns")

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
            materialize_roster_all(duck)
            self._refresh_people_data()
            self._refresh_issue_history()
            self._reload_due(quiet=True)
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
            due = write_due_tables(duck, due)
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
        for i in self.tree_due.get_children():
            self.tree_due.delete(i)
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
            self.tree_due.insert("", tk.END, values=tuple(r[c] for c in df2.columns))

    def _reload_due(self, quiet: bool = False) -> None:
        try:
            df = reapply_due_filters(self.duckdb_path)
        except Exception as exc:
            messagebox.showerror("再読込エラー", str(exc), parent=self)
            self._log(f"[ERROR] 期限一覧の再読込に失敗: {exc}")
            return
        if df is None:
            df = pd.DataFrame()
        self._due_current_df = df
        self._due_df = df
        self._selected_run_id = None
        self._selected_run_meta = None
        self.var_due_caption.set("現在の期限一覧")
        self._populate_due_table(df)
        self._refresh_issue_history()
        if not quiet:
            self._log(f"期限一覧を再読込: {len(df)} 件。")

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
        self._reload_due(quiet=True)
        return self._due_df

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





