from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path
from typing import Iterable

WAREHOUSE_ROOT_ENV = "WELDING_WAREHOUSE_ROOT"
_DEFAULT_DUCKDB_NAME = "local.duckdb"
_DEFAULT_REVIEW_DB_NAME = "review.sqlite"


def _user_data_base() -> Path:
    if os.name == "nt":
        base = os.getenv("LOCALAPPDATA")
        if base:
            return Path(base) / "welding-registry"
        return Path.home() / "AppData" / "Local" / "welding-registry"
    data_home = os.getenv("XDG_DATA_HOME")
    if data_home:
        return Path(data_home) / "welding-registry"
    return Path.home() / ".local" / "share" / "welding-registry"


def _uniquify(paths: Iterable[Path]) -> list[Path]:
    seen: set[Path] = set()
    ordered: list[Path] = []
    for p in paths:
        if p in seen:
            continue
        seen.add(p)
        ordered.append(p)
    return ordered


def _candidate_warehouse_dirs() -> list[Path]:
    candidates: list[Path] = []

    exe = Path(sys.executable)
    try:
        exe_dir = exe.resolve().parent
    except OSError:
        exe_dir = exe.parent
    candidates.extend([exe_dir.parent / "warehouse", exe_dir / "warehouse"])

    try:
        repo_root = Path(__file__).resolve().parents[2]
    except IndexError:
        repo_root = Path(__file__).resolve().parent
    candidates.extend(
        [
            Path.cwd() / "warehouse",
            repo_root / "warehouse",
        ]
    )

    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        mp = Path(meipass)
        candidates.extend([mp / "warehouse", mp.parent / "warehouse"])

    user_dir = _user_data_base() / "warehouse"
    candidates.append(user_dir)

    return _uniquify([c for c in candidates])


def _dir_is_writable(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError:
        return False
    token = f".permcheck-{uuid.uuid4().hex}"
    probe = path / token
    try:
        probe.write_bytes(b"")
        probe.unlink()
        return True
    except OSError:
        return False


def resolve_warehouse_path(
    explicit: Path | str | None = None, *, ensure_exists: bool = True
) -> Path:
    if explicit:
        resolved = Path(explicit).expanduser()
        if ensure_exists:
            resolved.mkdir(parents=True, exist_ok=True)
        return resolved

    env_root = os.getenv(WAREHOUSE_ROOT_ENV)
    if env_root:
        resolved = Path(env_root).expanduser()
        if ensure_exists:
            resolved.mkdir(parents=True, exist_ok=True)
        return resolved

    candidates = _candidate_warehouse_dirs()
    for candidate in candidates:
        if candidate.exists() and _dir_is_writable(candidate):
            return candidate

    for candidate in candidates:
        if _dir_is_writable(candidate):
            return candidate

    fallback = _user_data_base() / "warehouse"
    if ensure_exists:
        fallback.mkdir(parents=True, exist_ok=True)
    return fallback


def resolve_duckdb_path(explicit: Path | str | None = None) -> Path:
    if explicit:
        return Path(explicit).expanduser()
    env_path = os.getenv("DUCKDB_DB_PATH")
    if env_path:
        return Path(env_path).expanduser()
    return resolve_warehouse_path() / _DEFAULT_DUCKDB_NAME


def _duckdb_base_dir() -> Path:
    duck = resolve_duckdb_path()
    if duck.is_dir():
        return duck
    if duck.suffix:
        return duck.parent
    if duck.exists():
        return duck
    # Path does not exist yet; treat suffix-less path as directory
    return duck


def resolve_review_db_path(explicit: Path | str | None = None) -> Path:
    if explicit:
        path = Path(explicit).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    base = _duckdb_base_dir()
    try:
        base.mkdir(parents=True, exist_ok=True)
    except OSError:
        base = resolve_warehouse_path()
    review = base / _DEFAULT_REVIEW_DB_NAME
    review.parent.mkdir(parents=True, exist_ok=True)
    return review


def resolve_log_path(filename: str = "app.log") -> Path:
    log_dir = resolve_warehouse_path() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / filename


def resolve_csv_base(explicit: Path | str | None = None) -> Path:
    if explicit:
        base = Path(explicit).expanduser()
    else:
        base_candidate = _duckdb_base_dir()
        try:
            base_candidate.mkdir(parents=True, exist_ok=True)
            base = base_candidate
        except OSError:
            base = resolve_warehouse_path()
    csv_dir = base / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    return csv_dir


__all__ = [
    "resolve_warehouse_path",
    "resolve_duckdb_path",
    "resolve_review_db_path",
    "resolve_csv_base",
    "resolve_log_path",
    "WAREHOUSE_ROOT_ENV",
]
