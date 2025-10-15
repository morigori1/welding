from __future__ import annotations

from pathlib import Path
from typing import Any
import os
import subprocess
from importlib.metadata import PackageNotFoundError, version as pkg_version

from flask import Flask, redirect, url_for

from ..paths import resolve_duckdb_path
from .routes import issue_bp
from .qual import qual_bp

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8766


def _resolve_build_label(project_root: Path) -> str:
    """Return a human-readable build label (package version + short commit)."""

    env_label = os.getenv("WELDING_BUILD_LABEL")
    if env_label:
        return env_label

    try:
        pkg_ver = pkg_version("welding-registry")
    except PackageNotFoundError:
        pkg_ver = "0.0.0"

    commit = os.getenv("WELDING_BUILD_COMMIT")
    if not commit:
        try:
            commit = (
                subprocess.check_output(
                    ["git", "-C", str(project_root), "rev-parse", "--short", "HEAD"],
                    text=True,
                    stderr=subprocess.DEVNULL,
                )
                .strip()
            )
        except Exception:
            commit = None

    if commit:
        return f"{pkg_ver} ({commit})"
    return pkg_ver


def create_app(*, warehouse: Path | str | None = None, rows_per_page: int = 40) -> Flask:
    """Flask application factory for the web issuance interface."""

    app = Flask(
        __name__,
        template_folder="templates",
        static_folder="static",
    )
    duck_path = resolve_duckdb_path(warehouse)
    app.config["WELDING_DUCKDB_PATH"] = str(duck_path)
    app.config["WELDING_ROWS_PER_PAGE"] = int(rows_per_page)
    app.config["JSON_AS_ASCII"] = False
    project_root = Path(__file__).resolve().parents[2]
    build_label = _resolve_build_label(project_root)
    app.config["WELDING_BUILD_LABEL"] = build_label

    @app.context_processor
    def _inject_version() -> dict[str, str]:
        return {"welding_version": build_label}

    app.register_blueprint(issue_bp, url_prefix="/issue")
    app.register_blueprint(qual_bp, url_prefix="/qualifications")

    @app.route("/")
    def root_redirect() -> Any:
        return redirect(url_for("qual.qual_index"))

    return app


def run(
    *,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    warehouse: Path | str | None = None,
    rows_per_page: int = 40,
) -> None:
    """Convenience helper to run the issuance web UI."""

    app = create_app(warehouse=warehouse, rows_per_page=rows_per_page)
    app.run(host=host, port=port, debug=False)
