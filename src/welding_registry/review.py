from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Iterable

import sqlite3
from contextlib import closing
import time


@dataclass
class Decision:
    name_key: str
    license_no: Optional[str]
    status: str  # 'ok' | 'needs_update'
    notes: Optional[str]
    ts: float


class ReviewStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._ensure()

    def _ensure(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.db_path)) as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS decisions (
                    name_key TEXT NOT NULL,
                    license_no TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    notes TEXT,
                    ts REAL NOT NULL,
                    PRIMARY KEY (name_key, license_no)
                )
                """
            )

    def set(
        self, name_key: str, license_no: Optional[str], status: str, notes: Optional[str] = None
    ) -> None:
        ts = time.time()
        if license_no is None:
            license_no = ""
        with closing(sqlite3.connect(self.db_path)) as con:
            con.execute(
                """
                INSERT INTO decisions(name_key, license_no, status, notes, ts)
                VALUES (?,?,?,?,?)
                ON CONFLICT(name_key, license_no) DO UPDATE SET
                    status=excluded.status,
                    notes=excluded.notes,
                    ts=excluded.ts
                """,
                (name_key, license_no, status, notes, ts),
            )

    def get(self, name_key: str) -> list[Decision]:
        with closing(sqlite3.connect(self.db_path)) as con:
            cur = con.execute(
                "SELECT name_key, license_no, status, notes, ts FROM decisions WHERE name_key=?",
                (name_key,),
            )
            rows = cur.fetchall()
        return [Decision(*r) for r in rows]

    def all(self) -> Iterable[Decision]:
        with closing(sqlite3.connect(self.db_path)) as con:
            rows = list(
                con.execute("SELECT name_key, license_no, status, notes, ts FROM decisions")
            )
        for r in rows:
            yield Decision(*r)
