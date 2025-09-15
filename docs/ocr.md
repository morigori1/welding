# License OCR & Extraction

This module extracts license/certificate data from PDFs (text or image). When a PDF
is image-only, it falls back to OCR.

How it works:
- `scan_pdf` first tries to read tables/text via `pdfplumber`.
- If no usable text is found, it rasterizes and calls OCR (Azure if configured, else Tesseract).
- All extracted text is parsed by robust rules to find license numbers and labeled dates.

Extraction logic (robust by default):
- Labels recognized: 証明書番号 / 証書番号 / 登録番号 / 認定番号 / 資格番号 / 番号 / No./NO
- Normalization: NFKC (fullwidth→halfwidth), hyphen unification, uppercase.
- Date-like strings are excluded from license numbers.
- Context window (±N lines) is used to boost confidence if labels are adjacent.

CLI auditing (reasoned output):
- `licenses-audit` prints candidates with acceptance, confidence, and reasons.
  - Single file: `python -m welding_registry licenses-audit path/to/file.pdf --window 1 --include-rejected --out out/audit.csv`
  - Directory: `python -m welding_registry licenses-audit data/licenses --window 1 --include-rejected --out out/audit.csv`
  - Columns: `source, page, line_no, candidate, accepted, confidence, reason, line`.

OCR providers:

- Azure OCR (recommended)
  - `.env` supported; set:
    - `AZURE_OCR_ENDPOINT`, `AZURE_OCR_KEY`
  - Tries Document Intelligence prebuilt-read with version fallback (2024-07-31 → 2023-07-31 → preview), then Vision Read v3.2.
- Tesseract (fallback)
  - Install Tesseract + Japanese data; `pip install pytesseract`.
  - Ensure `tesseract` is on `PATH`. For WSL bridging to Windows: set `TESSERACT_CMD="/mnt/c/Program Files/Tesseract-OCR/tesseract.exe"`.
  - Languages tried: `jpn_vert+jpn+eng`, then `jpn+eng`, then `eng`. 300DPI raster for JP text.

Behavior notes:
- If Azure env is present, it is used first; otherwise Tesseract; otherwise OCR is skipped.
- With `--dump-ocr` in `due`, sanitized snippets (PII-minimized) are written for debugging.
- WSL tips: you can use Windows Tesseract without installing on WSL by setting `TESSERACT_CMD`; path conversion is handled automatically.
