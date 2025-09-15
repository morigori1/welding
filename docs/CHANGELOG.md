Changelog
=========

2025-09-15
----------

- fix(csvdb): Coerce roster keys to strings and sort qualification sets so CLI snapshots stay deterministic.
- fix(io_excel): Make vertical layout detection deterministic and cover it with regression tests.
- fix(app): Stringify snapshot record keys and header offsets to keep diffs and date math stable.
- fix(review): Wrap sqlite connections with contextlib.closing to eliminate ResourceWarning.
- fix(licenses): Guard TESSERACT_CMD before building a Path so env misconfigurations fail gracefully.

2025-09-11
----------

- feat(app): Add report (/report), print view (/report/print), input (/input) screens
- feat(cli): New `enrich` command to join roster with WorkerT (adds birth_date / birth_year_west)
- feat(cli): `ingest-vertical` documented for A列=氏名の縦レイアウト（JIS/Boilerブロック）
- feat(ocr): Azure DI API version fallback; Tesseract WSL→Windows bridge via `TESSERACT_CMD`
- fix(app): DuckDB metadata access uses information_schema (no PRAGMA dependency); robust due fallback and column normalization
- ui(print): Columns = 氏名・生年・資格・登録番号・有効期限・残日数・通知（日本語表示）

2025-09-10
----------

- feat(licenses): Robust license-no extraction
  - Label-aware (証明書番号/登録番号/認定番号/番号/No.) + context window (±N lines)
  - Normalization (NFKC, hyphen unification), date-like rejection
  - Table header synonyms mapped to `license_no` with value normalization
- feat(cli): Add `licenses-audit` to inspect candidates with acceptance/confidence/reason
- fix(io_excel): Header fallback hardened; date-like header cells demoted to `Unnamed:*`
- chore(deps): Add `Flask`; dev: `types-PyYAML`, `pandas-stubs`, `types-requests`
 - docs: Update OCR guide and add CLI overview
