from __future__ import annotations

from pathlib import Path
from typing import Any

from flask import Flask, redirect, url_for

from ..paths import resolve_duckdb_path
from .routes import issue_bp
from .qual import qual_bp

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8766


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
