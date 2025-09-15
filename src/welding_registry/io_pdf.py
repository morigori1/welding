from __future__ import annotations

from pathlib import Path
from typing import List

import pdfplumber
import pandas as pd


def extract_tables(pdf_path: Path) -> List[pd.DataFrame]:
    """Best-effort table extraction for roster PDFs using pdfplumber.
    Returns a list of DataFrames (one per detected table).
    Note: Table structure varies; downstream normalization is required.
    """
    frames: List[pd.DataFrame] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for t in tables or []:
                if not t or not any(t):
                    continue
                # First non-empty row as header
                header = next((r for r in t if any(cell for cell in r)), None)
                if not header:
                    continue
                rows = [r for r in t if r is not header]
                df = pd.DataFrame(rows, columns=[str(c or "").strip() for c in header])
                frames.append(df)
    return frames
