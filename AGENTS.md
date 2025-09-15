# Repository Guidelines

This repository manages welding qualification rosters (溶接従事者名簿), centered on the Excel ledger at `data/溶接資格250901現在試験結果含む生年・西暦.xls`. Contributions should improve ingestion, validation, normalization, and export of rosters while protecting personal data.

## Project Structure & Module Organization
- `data/` Input and archival datasets (XLS/XLSX/PDF). Example: `data/溶接資格250901現在試験結果含む生年・西暦.xls`.
- `src/welding_registry/` Core ETL, domain models, and I/O adapters (Excel/PDF/DB).
- `scripts/` Small CLIs for one‑off tasks (e.g., `convert_xls_to_parquet.py`).
- `tests/` Pytest suites; redacted fixtures in `tests/fixtures/`.
- `docs/` Architecture notes and field mappings (JP header → canonical keys).

## Build, Test, and Development Commands
- Create venv (PowerShell): `py -3.11 -m venv .venv; .\.venv\Scripts\Activate.ps1`
- Install (editable + dev): `pip install -e .[dev]`  (fallback: `pip install -r requirements-dev.txt`).
- Run tests: `pytest -q`  | Coverage: `pytest --cov=src/welding_registry`.
- Lint/format: `ruff check .` and `ruff format .` (or `black .` if configured).
- Type check: `mypy src`.
- Example local run: `python -m welding_registry ingest data/対象.xls --out out/対象.parquet`.

## Coding Style & Naming Conventions
- Python 3.11, PEP 8, 4‑space indent, UTF‑8 everywhere (handle CP932 on Windows at boundaries).
- Modules/functions `snake_case`; classes `PascalCase`; constants `UPPER_SNAKE`.
- Canonical field names are English `snake_case`; map Japanese headers via a centralized dictionary.

## Testing Guidelines
- Framework: `pytest` with `hypothesis` optional for parsers.
- Name tests `test_*.py`; mirror package paths.
- Include fixtures: tiny, redacted rows in `tests/fixtures/` (no PII).
- Target ≥85% coverage for core parsing/validation modules.

## Commit & Pull Request Guidelines
- Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`.
- PRs include: purpose, sample input/output (redacted), test coverage notes, and linked issue.
- For bug fixes, add a failing test first; update docs if fields or CLI change.

## Security & Data Handling
- Do not commit full rosters or secrets. Use redacted samples; prefer Git LFS for large binaries.
- Validate inputs; strip PII from logs; .env files are ignored.

## Agent‑Specific Instructions
- Keep changes minimal and focused; do not reorganize folders without rationale.
- Preserve Windows‑friendly paths and Japanese filenames; add tests for path/encoding edge cases.
- Follow this file’s guidance for any code you touch under the repo root.

