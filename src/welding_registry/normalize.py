from __future__ import annotations

import re
from typing import Optional
import unicodedata as _ud

import pandas as pd

from .field_map import DATE_COLUMNS


def strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    def _clean(v):
        if isinstance(v, str):
            return re.sub(r"\s+", " ", v.strip())
        return v

    # pandas 2.2 deprecates DataFrame.applymap in favor of DataFrame.map
    return df.map(_clean)


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = strip_whitespace(df)
    # Ensure date columns are ISO strings for CSV portability
    for c in DATE_COLUMNS:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce").dt.date.astype("string")
    return out


def license_key(s: Optional[str | int]) -> str:
    if s is None:
        return ""
    t = _ud.normalize("NFKC", str(s)).upper()
    # remove hyphens, spaces
    return re.sub(r"[\s\-]", "", t)


def name_key(s: Optional[str]) -> str:
    if s is None:
        return ""
    t = _ud.normalize("NFKC", str(s))
    # collapse whitespace
    t = re.sub(r"\s+", "", t)
    return t


# --- Position classification from qualification text ---

_POS_LABELS_JP = {
    "flat": "下向",
    "horizontal": "横向",
    "vertical": "立向",
    "overhead": "上向",
}


def _detect_positions_set(text: Optional[str]) -> set[str]:
    """Heuristically detect weld positions from a free-form qualification string.
    Returns a set among {flat, horizontal, vertical, overhead}.
    Recognizes:
      - Japanese tokens: 下向/横向/立向/縦向/上向/全姿勢
      - English tokens: FLAT/HORIZONTAL/VERTICAL/OVERHEAD (case-insensitive)
      - Compact codes: 1G/2G/3G/4G and 1F/2F/3F/4F
      - Abbreviations within token lists: F,V,H,OH (as separate tokens, e.g., "CN-F,V,H", "..., OH")
    """
    out: set[str] = set()
    if not text:
        return out
    s = str(text)
    s_norm = _ud.normalize("NFKC", s)
    s_low = s_norm.lower()

    # Japanese explicit words
    if "全姿勢" in s_norm:
        return {"flat", "horizontal", "vertical", "overhead"}
    if "下向" in s_norm:
        out.add("flat")
    if ("横向" in s_norm) or ("水平" in s_norm):
        out.add("horizontal")
    if ("立向" in s_norm) or ("縦向" in s_norm) or ("縦" in s_norm and "向" in s_norm):
        out.add("vertical")
    if "上向" in s_norm:
        out.add("overhead")

    # English words
    if "flat" in s_low:
        out.add("flat")
    if "horizontal" in s_low:
        out.add("horizontal")
    if "vertical" in s_low:
        out.add("vertical")
    if "overhead" in s_low:
        out.add("overhead")

    import re as _re

    # Positional codes: 1/2/3/4 (G or F)
    for m in _re.finditer(r"\b([1-4])\s*([FG])\b", s_norm, flags=_re.IGNORECASE):
        num = int(m.group(1))
        # 1: flat, 2: horizontal, 3: vertical, 4: overhead
        if num == 1:
            out.add("flat")
        elif num == 2:
            out.add("horizontal")
        elif num == 3:
            out.add("vertical")
        elif num == 4:
            out.add("overhead")

    # Abbrev tokens within comma/space/slash separated lists: F,V,H,OH
    # Guard so that a lone 'F' means Flat only when delimited, not substrings like 'SC-3F' (handled above) or 'FUTSU'.
    tokens = [t.strip() for t in _re.split(r"[\s,／/、]+", s_norm) if t.strip()]
    for t in tokens:
        tu = t.upper()
        if tu in ("OH", "O/H"):
            out.add("overhead")
        elif tu == "V":
            out.add("vertical")
        elif tu == "H":
            out.add("horizontal")
        elif tu == "F":
            # If already captured by 1F/2F/3F/4F it's fine; otherwise treat as Flat position token
            out.add("flat")

    return out


def positions_jp_label(qualification: Optional[str]) -> str:
    """Return a compact JP label like '下向/横向/立向/上向' or '全姿勢'. Empty string if none.
    Intended for display/export. Uses deterministic order flat,horizontal,vertical,overhead.
    """
    pos = _detect_positions_set(qualification)
    if not pos:
        return ""
    if pos == {"flat", "horizontal", "vertical", "overhead"}:
        return "全姿勢"
    order = ["flat", "horizontal", "vertical", "overhead"]
    items = [_POS_LABELS_JP[k] for k in order if k in pos]
    return "/".join(items)


def positions_en_label(qualification: Optional[str]) -> str:
    pos = _detect_positions_set(qualification)
    if not pos:
        return ""
    if pos == {"flat", "horizontal", "vertical", "overhead"}:
        return "all"
    order = ["flat", "horizontal", "vertical", "overhead"]
    return "/".join([k for k in order if k in pos])


def positions_codes(qualification: Optional[str]) -> str:
    """Return a compact code string like 'F/H/V/OH' (order: F,H,V,OH)."""
    pos = _detect_positions_set(qualification)
    if not pos:
        return ""
    order = [("flat", "F"), ("horizontal", "H"), ("vertical", "V"), ("overhead", "OH")]
    return "/".join([code for key, code in order if key in pos])


def add_positions_columns(df: pd.DataFrame, source_col: str = "qualification") -> pd.DataFrame:
    """Add normalized position columns to DataFrame if source_col exists:
    - positions: JP label (下向/横向/立向/上向 or 全姿勢)
    - positions_jp, positions_en, positions_code
    - pos_flat, pos_horizontal, pos_vertical, pos_overhead (0/1 integers)
    Returns the same DataFrame with columns added (in-place safe in pandas semantics when assigning new columns).
    """
    if source_col not in df.columns:
        return df
    s = df[source_col].astype("string")
    try:
        df["positions"] = s.map(positions_jp_label)
    except Exception:
        df["positions"] = ""
    try:
        df["positions_jp"] = df["positions"]
    except Exception:
        pass
    try:
        df["positions_en"] = s.map(positions_en_label)
    except Exception:
        df["positions_en"] = ""
    try:
        df["positions_code"] = s.map(positions_codes)
    except Exception:
        df["positions_code"] = ""

    # Flags
    def _flag_map(txt: Optional[str]) -> tuple[int, int, int, int]:
        st = _detect_positions_set(txt)
        return (
            1 if "flat" in st else 0,
            1 if "horizontal" in st else 0,
            1 if "vertical" in st else 0,
            1 if "overhead" in st else 0,
        )

    flags = s.map(_flag_map)
    try:
        df["pos_flat"] = flags.map(lambda t: t[0])
        df["pos_horizontal"] = flags.map(lambda t: t[1])
        df["pos_vertical"] = flags.map(lambda t: t[2])
        df["pos_overhead"] = flags.map(lambda t: t[3])
    except Exception:
        pass
    return df
