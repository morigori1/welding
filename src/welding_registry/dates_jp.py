from __future__ import annotations

import re
from datetime import date
from typing import Optional

import pandas as pd

_ERA_BASE = {
    "R": 2019,  # Reiwa 1 = 2019
    "H": 1989,  # Heisei 1 = 1989
    "S": 1926,  # Showa 1 = 1926
    "T": 1912,  # Taisho 1 = 1912
    "M": 1868,  # Meiji 1 = 1868
}


def parse_jp_date(text: str) -> Optional[date]:
    if not text:
        return None
    s = str(text).strip()

    # Common formats
    # 1) YYYY年MM月DD日
    m = re.search(r"(\d{4})[./年](\d{1,2})[./月](\d{1,2})日?", s)
    if m:
        y1, mo1, d1 = map(int, m.groups())
        return date(y1, mo1, d1)

    # 2) Japanese era: R6.09.01 / 令和6年9月1日 / S49. 8.22 など（空白許容）
    m = re.search(
        r"([RrHhSsTtMm令和平成昭和大正明治])\s*(\d{1,2})\s*[./年]\s*(\d{1,2})\s*[./月]\s*(\d{1,2})日?",
        s,
    )
    if m:
        era, nen, mo_s, d_s = m.groups()
        # Normalize kanji to initial
        era_initial = {
            "令": "R",
            "平": "H",
            "昭": "S",
            "大": "T",
            "明": "M",
        }.get(era[0], era[0].upper())
        base = _ERA_BASE.get(era_initial)
        if base:
            y = base + int(nen) - 1
            return date(y, int(mo_s), int(d_s))

    # 3) YY.MM.DD or YY/MM/DD (assume 2000+ if < 70 else 1900+)
    m = re.search(r"(\d{2})[./](\d{1,2})[./](\d{1,2})", s)
    if m:
        yy, mo3, d3 = map(int, m.groups())
        y = 2000 + yy if yy < 70 else 1900 + yy
        return date(y, mo3, d3)

    # Fallback via pandas
    try:
        ts = pd.to_datetime(s, errors="coerce")
        return ts.date() if pd.notna(ts) else None
    except Exception:
        return None
