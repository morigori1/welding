# Vertical-Block Auto Detection (2025-09-12)

This extends `ingest-vertical` to auto-detect columns and block ranges in header-less sheets where a person's name/registration spans rows and license blocks (e.g., JIS / BOILER) are arranged in column ranges.

Usage (PowerShell examples):
- Auto-detect everything (person/regno/blocks):
  - `python -m welding_registry ingest-vertical data/対象.xls --sheet P1 --auto --out out/対象 --duckdb warehouse/local.duckdb`
- Detect only person/regno columns:
  - `python -m welding_registry ingest-vertical data/対象.xls --sheet P1 --auto-person --auto-regno --jis C:H --boiler I:K --out out/対象`
- Add/override blocks explicitly (repeatable):
  - `python -m welding_registry ingest-vertical data/対象.xls --sheet P1 --auto --block JIS=C:H --block BOILER=I:K --out out/対象`

Notes
- Detection looks for:
  - `regno` column: high ratio of registration-number-like tokens (e.g., `SE2500123`, `36709`, `12-3456`).
  - `person` column: left of `regno` with name-like tokens (short text, not date/headers).
  - Block ranges: contiguous column clusters to the right with sufficient non-null density; labels inferred from header text (e.g., contains `JIS`, `ボイラ` → `BOILER`).
- You can still specify `--person A` / `--regno B` and `--jis/--boiler` explicitly. Use `--person AUTO` / `--regno AUTO` to auto-detect columns individually.
- Output columns are the same as before: `name, license_no, qualification, positions* , category, first_issue_date, issue_date, expiry_date`.
