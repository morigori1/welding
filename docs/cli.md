# CLI Overview (2025-09-11)

Core commands (examples assume PowerShell on Windows):

- Inspect Excel headers/sheets
  - `python -m welding_registry inspect data/対象.xls`

- Ingest a sheet to normalized CSV/XLSX and optional DuckDB
  - `python -m welding_registry ingest data/対象.xls --sheet P1 --out out/対象 --duckdb warehouse/local.duckdb`

- Ingest vertical-block layout (names span rows; A=氏名/B=番号、JIS/Boilerが縦ブロック)
  - `python -m welding_registry ingest-vertical data/対象.xls --sheet P1 --person A --regno B --jis C:H --boiler I:K --out out/対象 --duckdb warehouse/local.duckdb`
  - A列に「氏名」ではなく「（生年・西暦）」等が入っていても、最初の日本語名行を氏名として採用（ローマ字/年・注記行は無視）。

- Enrich roster with WorkerT (生年月日の突合付与)
  - `python -m welding_registry enrich --duckdb warehouse/local.duckdb --workers-csv out/workers.csv`
  - DuckDB に `workers` テーブルがある場合は `--workers-csv` 省略可。`roster_enriched` を出力。

- Expiry due list with OCR license preference（複数フォルダ併用可）
  - `python -m welding_registry due data/台帳.xls --sheet P1 --window 120 --licenses-scan out/jis --licenses-scan out/boiler --out out/due.csv --ics out/expiry.ics`

- DocuWorks conversion helper (see docs/docuworks_automation.md)
  - `python -m welding_registry xdw2pdf out/xdw --printer "DocuWorks PDF"`

- Extract labeled dates from a PDF (per-page)
  - `python -m welding_registry pdfdates data/license.pdf`

- Audit license-number extraction with reasons
  - `python -m welding_registry licenses-audit data/licenses --window 1 --include-rejected --out out/licenses_audit.csv`

Notes:
- CSV is UTF-8 with BOM (`utf-8-sig`).
- `DUCKDB_DB_PATH` env var can be used instead of `--duckdb`.
- For OCR, set `AZURE_OCR_ENDPOINT` / `AZURE_OCR_KEY` or install Tesseract. Multiple `--licenses-scan` can be provided to combine sources.

Web App
- Run: `python -m welding_registry web --duckdb warehouse/local.duckdb`
- Routes:
  - `/` 人別一覧（名前リンク→個別画面）
  - `/report` 期限レポート（列: 氏名・生年・資格・登録番号・有効期限・残日数・通知）
- `/report/print` 印刷ビュー（A4固定幅、`?rows=35&ori=portrait` 等で調整）
- `/person?name=...` 個別確認＋レビュー記録（最新/要更新・メモ）
- `/input` ロスターへの手入力（`roster_manual` に追記）
- 印刷ビューの「PDF保存」はブラウザの印刷ダイアログで PDF 保存を開き、完了後に DuckDB (`issue_print_runs`) と `warehouse/issue_prints/` に出力内容をスナップショットとして記録します。

Legacy Commands
- 旧コマンド `python -m welding_registry app` は互換目的で残っていますが、新規利用は `python -m welding_registry web` を推奨します。
- Tk ベースの `python -m welding_registry gui` は 2025-10 をもって廃止されました。
