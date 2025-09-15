from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Any, cast

import pdfplumber
import pandas as pd
import unicodedata as _ud
import os as _os
import subprocess as _sp
import tempfile as _tmp

from .dates_jp import parse_jp_date


@dataclass
class LicenseRecord:
    source: str
    name: Optional[str]
    license_no: Optional[str]
    qualification: Optional[str]
    issue_date: Optional[pd.Timestamp]
    expiry_date: Optional[pd.Timestamp]


def _norm_label(s: Optional[str]) -> str:
    return _ud.normalize("NFKC", str(s or "").strip())


def _normalize_hyphens(s: str) -> str:
    return re.sub(r"[‐‑‒–—−ー－]", "-", s)


def _extract_license_no(text: str) -> Optional[str]:
    s = _ud.normalize("NFKC", str(text or ""))
    label = r"(?:証明書番号|証書番号|登録番号|認定番号|資格番号|番号|No\.?|NO\.?|Ｎｏ\.?)"
    val = r"([A-Za-zＡ-Ｚa-z０-９0-9][A-Za-zＡ-Ｚa-z０-９0-9\-‐‑‒–—−ー－]{2,})"
    m = re.search(label + r"[：:\-\s]*" + val, s)
    if m:
        cand = _normalize_hyphens(_ud.normalize("NFKC", m.group(1))).upper()
        if not re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", cand):
            return cand
    m = re.search(r"\b([A-Z]{1,4}-?\d{3,})\b", s, re.IGNORECASE)
    if m:
        return _normalize_hyphens(_ud.normalize("NFKC", m.group(1))).upper()
    for m in re.finditer(r"\b(\d{6,})\b", s):
        num = m.group(1)
        if not re.match(r"\d{8}", num):
            return num
    return None


_LABEL_TOKENS = [
    "証明書番号",
    "証書番号",
    "登録番号",
    "認定番号",
    "資格番号",
    "免許番号",
    "免許証番号",
    "許可番号",
    "登録No",
    "登録№",
    "証番号",
    "登録第",
    "証第",
    "番号",
    "No",
    "No.",
]
_DATE_TOKENS = ["有効期限", "有効期間", "発行", "交付", "更新", "年月日", "年", "月", "日"]


def _looks_dateish(s: str) -> bool:
    if re.search(r"\b\d{4}[./-]\d{1,2}[./-]\d{1,2}\b", s):
        return True
    if re.search(r"\b\d{2}[./-]\d{1,2}[./-]\d{1,2}\b", s):
        return True
    if parse_jp_date(s):
        return True
    return False


def extract_license_candidates(
    text: str, window: int = 1, include_rejected: bool = False
) -> pd.DataFrame:
    """Return candidate license numbers with acceptance decision and reason.
    - Splits into lines; detects candidates; checks labels within +/- window lines.
    - Rejects obvious date-like tokens and very short strings.
    Columns: line_no, candidate, accepted, confidence, reason, line.
    """
    s = _ud.normalize("NFKC", str(text or ""))
    lines = s.splitlines()
    recs: list[dict] = []
    n = len(lines)
    for idx, line in enumerate(lines):
        # collect context lines within +/- window using 0-based indices
        ctx = lines[max(0, idx - window) : min(n, idx + window + 1)]
        has_label_here = any(tok in line for tok in _LABEL_TOKENS)
        has_label_near = has_label_here or any(
            any(tok in ln for tok in _LABEL_TOKENS) for ln in ctx
        )
        has_date_near = any(_looks_dateish(ln) or any(t in ln for t in _DATE_TOKENS) for ln in ctx)
        # Find candidates in this line
        cands: list[str] = []
        # Label + value pattern
        m = re.search(
            r"(?:"
            + "|".join(map(re.escape, _LABEL_TOKENS))
            + r")[：:\-\s]*([A-Za-z0-9Ａ-Ｚ０-９\-‐‑‒–—−ー－]{3,})",
            line,
        )
        if m:
            cands.append(m.group(1))
        # Generic pattern
        for m in re.finditer(r"\b([A-Za-z]{1,4}-?\d{3,}|\d{6,})\b", line):
            cands.append(m.group(1))
        # Dedup
        seen = set()
        cands2 = []
        for c in cands:
            c = _normalize_hyphens(_ud.normalize("NFKC", c)).upper()
            if c not in seen:
                seen.add(c)
                cands2.append(c)
        for c in cands2:
            val = _extract_license_no(c) or c
            is_date_like = bool(re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", val)) or _looks_dateish(val)
            if is_date_like:
                decision = False
                reason = "reject:date_like"
                conf = "low"
            elif has_label_near:
                decision = True
                reason = "accept:labeled" if has_label_here else "accept:adjacent_label"
                conf = "high" if has_label_here else "medium"
            elif re.match(r"^[A-Z]{1,4}-?\d{3,}$", val):
                decision = True
                reason = "accept:pattern"
                conf = "medium"
            elif re.match(r"^\d{6,}$", val):
                decision = True
                reason = "accept:numeric_long"
                conf = "low" if has_date_near else "medium"
            else:
                decision = False
                reason = "reject:weak_pattern"
                conf = "low"
            if decision or include_rejected:
                recs.append(
                    {
                        "line_no": idx + 1,
                        "candidate": val,
                        "accepted": decision,
                        "confidence": conf,
                        "reason": reason,
                        "line": line.strip(),
                    }
                )
    if not recs:
        return pd.DataFrame(
            columns=["line_no", "candidate", "accepted", "confidence", "reason", "line"]
        )
    return pd.DataFrame(recs)


HEADER_MAP = {
    "氏名": "name",
    "名前": "name",
    "ﾌﾘｶﾞﾅ": "kana",
    "フリガナ": "kana",
    "登録番号": "license_no",
    "登録No": "license_no",
    "登録№": "license_no",
    "免許番号": "license_no",
    "免許証番号": "license_no",
    "許可番号": "license_no",
    "証番号": "license_no",
    "資格番号": "license_no",
    "資格": "qualification",
    "資格種別": "qualification",
    "交付日": "issue_date",
    "発行日": "issue_date",
    "交付年月日": "issue_date",
    "発行年月日": "issue_date",
    "有効期限": "expiry_date",
    "有効期限日": "expiry_date",
    "有効期間": "expiry_date",
    "有効期間満了日": "expiry_date",
    "満了日": "expiry_date",
}


def _parse_date_cell(v) -> Optional[pd.Timestamp]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (pd.Timestamp,)):
        return v
    dt = parse_jp_date(str(v))
    return pd.Timestamp(dt) if dt else None


def _from_table(df: pd.DataFrame, source: Path) -> pd.DataFrame:
    cols = {}
    for c in df.columns:
        label = _norm_label(c)
        key = HEADER_MAP.get(label)
        if not key:
            for k, v in HEADER_MAP.items():
                if k and k in label:
                    key = v
                    break
        if key:
            cols[c] = key
    if not cols:
        return pd.DataFrame()
    out = df.rename(columns=cols)
    # Fallback: map common synonyms to license_no if not already present
    if "license_no" not in out.columns:
        for cc in df.columns:
            lab = _norm_label(cc)
            if any(
                tok in lab
                for tok in ("証明書番号", "証書番号", "登録番号", "認定番号", "番号", "No", "No.")
            ):
                out = out.assign(license_no=df[cc])
                break
    # Normalize license number cell values
    if "license_no" in out.columns:
        out["license_no"] = out["license_no"].astype(str).map(lambda s: _extract_license_no(s) or s)
    for c in ("issue_date", "expiry_date"):
        if c in out.columns:
            out[c] = out[c].map(_parse_date_cell)
    out["source"] = str(source)
    keep = [
        c
        for c in ["source", "name", "license_no", "qualification", "issue_date", "expiry_date"]
        if c in out.columns
    ]
    return out[keep]


def _from_text(text: str, source: Path) -> Optional[dict]:
    name = None
    lic = None
    qual = None
    issue = None
    exp = None

    # License no
    m = re.search(
        r"(登録番号|免許番号|資格番号|登録No\.?|登録№|No\.?|許可番号|証番号)[：:：]?\s*([A-Za-z0-9\-]+)",
        text,
    )
    if m:
        lic = m.group(2)
    if not lic:
        m = re.search(r"登録第\s*([A-Za-z0-9\-]+)\s*号", text)
        if m:
            lic = m.group(1)
    if not lic:
        lic = _extract_license_no(text)

    # Qualification
    m = re.search(r"(資格|資格種別|資格名称|免許の種類)[：:：]?\s*([^\n\r]{1,80})", text)
    if m:
        qual = m.group(2).strip()

    # Dates
    # Date range like 2024/04/01〜2027/03/31 or 有効期間: ...
    m = re.search(r"(有効期間|有効)[:：]?\s*([^\n\r]{4,60})", text)
    if m:
        rng = m.group(2)
        m2 = re.search(
            r"(\d{2,4}[^\d\n]{0,2}\d{1,2}[^\d\n]{0,2}\d{1,2}).{0,6}[〜~\-－—–].{0,6}(\d{2,4}[^\d\n]{0,2}\d{1,2}[^\d\n]{0,2}\d{1,2})",
            rng,
        )
        if m2:
            i1, i2 = m2.group(1), m2.group(2)
            issue = issue or _parse_date_cell(i1)
            exp = exp or _parse_date_cell(i2)

    m = re.search(
        r"(交付日|発行日|交付年月日|発行年月日|試験日|受験日|実施日|発給日|発効日)[：:：]?\s*([\S ]{4,}?)\s",
        text,
    )
    if m:
        issue = _parse_date_cell(m.group(2))
    m = re.search(
        r"(有効期限|有効期限日|有効期間満了日|満了日|満了予定日|有効期間)[：:：]?\s*([\S ]{4,}?)\s",
        text,
    )
    if m:
        exp = _parse_date_cell(m.group(2))

    if lic or exp or issue:
        return {
            "source": str(source),
            "name": name,
            "license_no": lic,
            "qualification": qual,
            "issue_date": issue,
            "expiry_date": exp,
        }
    return None


def _extract_names(text: str) -> List[str]:
    """Extract likely Japanese personal names from patterns seen in JIS roster scans.
    - Handles lines like "... #61松岡正" or "#114内田浩"
    - Collapses simple whitespace noise
    Returns a de-duplicated list of names.
    """
    s = _norm_label(text)
    names: List[str] = []
    # Pattern: '#' + digits + name (Japanese Kanji/Kana, 2-8 chars)
    pat = re.compile(r"#\s*\d+\s*([\u4E00-\u9FFF\u3400-\u4DBF\u3040-\u30FF]{2,8})")
    for m in pat.finditer(s):
        nm = m.group(1).strip()
        if nm and nm not in names:
            names.append(nm)
    # Fallback: lines like "氏名: 〇〇" if present
    for m in re.finditer(r"氏名[：:：]?\s*([\u4E00-\u9FFF\u3040-\u30FF]{2,8})", s):
        nm = m.group(1).strip()
        if nm and nm not in names:
            names.append(nm)
    return names


def extract_date_candidates(text: str) -> List[str]:
    """Return raw date-like substrings found in text (not normalized).
    Covers:
    - YYYY/MM/DD, YYYY-MM-DD, YYYY.MM.DD
    - YY/MM/DD, YY.MM.DD (2-digit year)
    - Japanese era like R6.09.01, 令和6年9月1日, H23.2.5, S49. 8.22
    - Ranges like 2024/04/01〜2027/03/31
    - Forms like 26.07.31(23) -> capture 26.07.31
    """
    s = _norm_label(text)
    pats = [
        r"\b\d{4}[./-]\d{1,2}[./-]\d{1,2}\b",  # 2025/09/10
        r"\b\d{2}[./-]\d{1,2}[./-]\d{1,2}\b",  # 25.09.10
        r"[RrHhSsTtMm令平昭大明]\s*\d{1,2}[./年]\s*\d{1,2}[./月]\s*\d{1,2}日?",  # R6.9.1, 令和6年9月1日
        r"\b\d{1,2}[./]\d{1,2}[./]\d{1,2}\s*\(\d{1,2}\)",  # 26.07.31(23)
    ]
    out: List[str] = []
    import re as _re

    for p in pats:
        for m in _re.finditer(p, s):
            tok = m.group(0).strip()
            if tok not in out:
                out.append(tok)
    # Extract from ranges like A〜B
    for m in _re.finditer(
        r"(\d{4}[./-]\d{1,2}[./-]\d{1,2}).{0,6}[〜~\-－—–至から]{1,}.{0,6}(\d{4}[./-]\d{1,2}[./-]\d{1,2})",
        s,
    ):
        a, b = m.group(1), m.group(2)
        for tok in (a, b):
            if tok not in out:
                out.append(tok)
    return out


def scan_pdf_dates(path: Path) -> List[tuple[str, str]]:
    """Extract raw date-like tokens and a normalized ISO date if parseable.
    Returns list of tuples: (raw_token, normalized_YYYY_MM_DD_or_empty)
    """
    texts: List[str] = []
    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                t = page.extract_text() or ""
                if t.strip():
                    texts.append(t)
    except Exception:
        pass
    if not texts:
        # try OCR providers
        t2 = _azure_ocr_pdf(path) or ""
        if t2:
            texts.append(t2)
    raw: List[str] = []
    for t in texts:
        raw.extend(extract_date_candidates(t))
    # de-duplicate, preserve order
    seen = set()
    uniq = []
    for r in raw:
        if r not in seen:
            seen.add(r)
            uniq.append(r)
    out: List[tuple[str, str]] = []
    for tok in uniq:
        # strip trailing parenthetical note like (23)
        norm_try = tok.split("(")[0].strip()
        dt = parse_jp_date(norm_try)
        out.append((tok, dt.isoformat() if dt else ""))
    return out


def _from_text_v2(text: str, source: Path) -> Optional[dict]:
    """More robust text parser that tolerates label variations and falls back to
    min/max date inference and generic license patterns.
    """
    name = None
    lic = None
    qual = None
    issue = None
    exp = None

    m = re.search(
        r"(登録番号|免許番号|資格番号|登録No\.?|登録№|No\.?|許可番号|証番号)[：:：]?\s*([A-Za-z0-9\-]+)",
        text,
    )
    if m:
        lic = m.group(2)
    if not lic:
        m = re.search(r"(証第|登録第)\s*([A-Za-z0-9\-]+)\s*(号|號)", text)
        if m:
            lic = m.group(2)
    if not lic:
        m = re.search(r"\b(?:No\.?|NO\.?|記号)\s*([A-Za-z0-9\-]{4,})\b", text)
        if m:
            lic = m.group(1)
    if not lic:
        lic = _extract_license_no(text)

    m = re.search(r"(資格|資格種別|資格名称|免許の種類)[：:：]?\s*([^\n\r]{1,120})", text)
    if m:
        qual = m.group(2).strip()

    m = re.search(r"(有効期間|有効)[:：]?\s*([^\n\r]{4,80})", text)
    if m:
        rng = m.group(2)
        m2 = re.search(
            r"(\d{2,4}[^\d\n]{0,2}\d{1,2}[^\d\n]{0,2}\d{1,2}).{0,8}[〜~\-－—–至から]{1,}.{0,8}(\d{2,4}[^\d\n]{0,2}\d{1,2}[^\d\n]{0,2}\d{1,2})",
            rng,
        )
        if m2:
            issue = _parse_date_cell(m2.group(1)) or issue
            exp = _parse_date_cell(m2.group(2)) or exp

    m = re.search(
        r"(交付日|発行日|交付年月日|発行年月日|試験日|受験日|実施日|発給日|発効日)[：:：]?\s*([\S ]{4,}?)\s",
        text,
    )
    if m:
        issue = issue or _parse_date_cell(m.group(2))
    m = re.search(
        r"(有効期限|有効期限日|有効期間満了日|満了日|満了予定日|有効期間)[：:：]?\s*([\S ]{4,}?)\s",
        text,
    )
    if m:
        exp = exp or _parse_date_cell(m.group(2))

    if not exp:
        cand = re.findall(r"\b\d{4}[./-]\d{1,2}[./-]\d{1,2}\b", text)
        dates = []
        for s in cand:
            dt = _parse_date_cell(s)
            if dt:
                dates.append(dt)
        if dates:
            dates.sort()
            issue = issue or dates[0]
            exp = dates[-1]
    if not lic:
        m = re.search(r"\b[A-Z0-9]{2,3}-?\d{4,}\b", text, re.IGNORECASE)
        if m:
            lic = m.group(0)

    if lic or exp or issue:
        return {
            "source": str(source),
            "name": name,
            "license_no": lic,
            "qualification": qual,
            "issue_date": issue,
            "expiry_date": exp,
        }
    # If no dates or license but names are present (JIS roster pages), return first name-only record
    names = _extract_names(text)
    if names:
        return {
            "source": str(source),
            "name": names[0],
            "license_no": None,
            "qualification": qual
            or (
                "JIS 溶接士"
                if "JIS" in _norm_label(text) or "ＪＩＳ" in _norm_label(text)
                else None
            ),
            "issue_date": None,
            "expiry_date": None,
        }
    return None


def scan_pdf(path: Path, debug: bool = False, dump_dir: Optional[Path] = None) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    try:
        with pdfplumber.open(path) as pdf:
            has_table = False
            for page in pdf.pages:
                tables = page.extract_tables() or []
                for t in tables:
                    if not t:
                        continue
                    header = next((r for r in t if any(cell for cell in r)), None)
                    if not header:
                        continue
                    rows = [r for r in t if r is not header]
                    df = pd.DataFrame(rows, columns=[str(c or "").strip() for c in header])
                    f = _from_table(df, path)
                    if not f.empty:
                        frames.append(f)
                        has_table = True
            if not has_table:
                # Try per-page text parsing first to capture multiple roster entries
                any_hit = False
                for page in pdf.pages:
                    txt = page.extract_text() or ""
                    t = cast(Any, txt)  # keep compatibility with downstream uses
                    if not txt.strip():
                        continue
                    # Name-only extraction (e.g., "#61松岡正") may yield multiple per page
                    names = _extract_names(txt)
                    for nm in names:
                        frames.append(
                            pd.DataFrame(
                                [
                                    {
                                        "source": str(path),
                                        "name": nm,
                                        "license_no": None,
                                        "qualification": "JIS 溶接士"
                                        if ("JIS" in _norm_label(t) or "ＪＩＳ" in _norm_label(t))
                                        else None,
                                        "issue_date": None,
                                        "expiry_date": None,
                                    }
                                ]
                            )
                        )
                        any_hit = True
                    if not names:
                        rec = _from_text_v2(txt, path)
                        if rec:
                            frames.append(pd.DataFrame([rec]))
                            any_hit = True
                if not any_hit:
                    text = "\n".join(page.extract_text() or "" for page in pdf.pages)
                    if debug and text and len(text.strip()) >= 1:
                        print(f"[OCR] {path.name}: text_len={len(text)} no-match")
                    # Try Azure OCR by file path, then Tesseract OCR fallback
                    text2 = _azure_ocr_pdf(Path(path)) or _ocr_pdf(pdf)
                    if text2:
                        # Attempt multi-name extraction as well
                        names = _extract_names(text2)
                        for nm in names:
                            frames.append(
                                pd.DataFrame(
                                    [
                                        {
                                            "source": str(path),
                                            "name": nm,
                                            "license_no": None,
                                            "qualification": "JIS 溶接士"
                                            if (
                                                "JIS" in _norm_label(text2)
                                                or "ＪＩＳ" in _norm_label(text2)
                                            )
                                            else None,
                                            "issue_date": None,
                                            "expiry_date": None,
                                        }
                                    ]
                                )
                            )
                        if not names:
                            rec = _from_text_v2(text2, path)
                            if rec:
                                frames.append(pd.DataFrame([rec]))
                        if dump_dir:
                            try:
                                dump_dir.mkdir(parents=True, exist_ok=True)
                                sanitized = re.sub(r"[0-9]", "0", text2)
                                sanitized = re.sub(r"[A-Za-z]", "X", sanitized)
                                keep = []
                                for line in sanitized.splitlines():
                                    if any(
                                        tok in line
                                        for tok in (
                                            "番号",
                                            "有効",
                                            "満了",
                                            "交付",
                                            "発行",
                                            "期間",
                                            "#",
                                        )
                                    ):
                                        keep.append(line)
                                out = "\n".join(keep) or sanitized[:2000]
                                (dump_dir / f"{path.stem}.txt").write_text(out, encoding="utf-8")
                            except Exception:
                                pass
                    elif debug:
                        print(f"[OCR] {path.name}: no text, no ocr provider")
    except Exception:
        pass
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame(
        columns=["source", "name", "license_no", "qualification", "issue_date", "expiry_date"]
    )


def audit_pdf(path: Path, window: int = 1, include_rejected: bool = False) -> pd.DataFrame:
    """Extract candidate license numbers from a PDF with context-based reasoning.
    Columns: page, line_no, candidate, accepted, confidence, reason, line
    """
    rows: list[pd.DataFrame] = []
    try:
        with pdfplumber.open(path) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                txt = page.extract_text() or ""
                if txt.strip():
                    df = extract_license_candidates(
                        txt, window=window, include_rejected=include_rejected
                    )
                    if not df.empty:
                        df.insert(0, "page", i)
                        rows.append(df)
    except Exception:
        pass
    if not rows:
        # try OCR as fallback (Azure first, then local Tesseract)
        t2 = _azure_ocr_pdf(path) or ""
        if not t2:
            try:
                with pdfplumber.open(path) as _pdf:
                    t2 = _ocr_pdf(_pdf)
            except Exception:
                t2 = ""
        if t2:
            df = extract_license_candidates(t2, window=window, include_rejected=include_rejected)
            if not df.empty:
                df.insert(0, "page", None)
                rows.append(df)
    if not rows:
        return pd.DataFrame(
            columns=["page", "line_no", "candidate", "accepted", "confidence", "reason", "line"]
        )
    return pd.concat(rows, ignore_index=True)


def scan_dir(root: Path, debug: bool = False, dump_dir: Optional[Path] = None) -> pd.DataFrame:
    pdfs = list(Path(root).rglob("*.pdf"))
    frames = [scan_pdf(p, debug=debug, dump_dir=dump_dir) for p in pdfs]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(
            columns=["source", "name", "license_no", "qualification", "issue_date", "expiry_date"]
        )
    out = pd.concat(frames, ignore_index=True)
    if "license_no" in out.columns:
        out = out.sort_values(by=["license_no", "issue_date"], ascending=[True, False])
        out = out.drop_duplicates(subset=["license_no", "expiry_date"], keep="first")
    return out


def _extract_labeled_dates_from_text(text: str) -> dict:
    """Extract labeled dates from free text.
    Supports JIS-style labels: 登録年月日(=first_issue_date), 継続年月日/交付年月日(=issue_date), 有効年月日/有効期限(=expiry_date), 有効期間(A〜B)。
    Returns dict of possible fields (pd.Timestamp or None).
    """
    s = _norm_label(text)
    out: dict = {"first_issue_date": None, "issue_date": None, "expiry_date": None}
    import re as _re

    def _find_after(label: str) -> Optional[pd.Timestamp]:
        m = _re.search(_re.escape(label) + r"[：:：]?\s*([\S ].{0,20})", s)
        if not m:
            return None
        tail = m.group(1)
        # Try direct date in the same segment
        toks = extract_date_candidates(tail)
        for t in toks:
            dt = parse_jp_date(t.split("(")[0].strip())
            if dt:
                return pd.Timestamp(dt)
        return None

    # first_issue_date
    out["first_issue_date"] = _find_after("登録年月日") or out["first_issue_date"]
    out["issue_date"] = (
        _find_after("継続年月日")
        or _find_after("交付年月日")
        or _find_after("交付日")
        or out["issue_date"]
    )
    out["expiry_date"] = _find_after("有効年月日") or _find_after("有効期限") or out["expiry_date"]
    # 有効期間: prefer end date as expiry
    m = _re.search(r"有効期間[：:：]?\s*([^\n\r]{4,80})", s)
    if m and not out.get("expiry_date"):
        rng = m.group(1)
        m2 = _re.search(
            r"(\d{2,4}[^\d\n]{0,2}\d{1,2}[^\d\n]{0,2}\d{1,2}).{0,8}[〜~\-－—–至から]{1,}.{0,8}(\d{2,4}[^\d\n]{0,2}\d{1,2}[^\d\n]{0,2}\d{1,2})",
            rng,
        )
        if m2:
            end = parse_jp_date(m2.group(2))
            if end:
                out["expiry_date"] = pd.Timestamp(end)
            start = parse_jp_date(m2.group(1))
            if start and not out.get("issue_date"):
                out["issue_date"] = pd.Timestamp(start)
    return out


def scan_pdf_labeled_dates(path: Path) -> pd.DataFrame:
    """Scan a PDF and extract labeled dates per page.
    Returns columns: page, first_issue_date, issue_date, expiry_date.
    """
    recs = []
    try:
        with pdfplumber.open(path) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                t = page.extract_text() or ""
                if not t.strip():
                    continue
                d = _extract_labeled_dates_from_text(t)
                if any(d.values()):
                    rec = {"page": i, **d}
                    recs.append(rec)
    except Exception:
        pass
    if not recs:
        # Try Azure OCR full-text
        t2 = _azure_ocr_pdf(path)
        if t2:
            d = _extract_labeled_dates_from_text(t2)
            if any(d.values()):
                recs.append({"page": None, **d})
    if not recs:
        return pd.DataFrame(columns=["page", "first_issue_date", "issue_date", "expiry_date"])
    return pd.DataFrame(recs)


def _ocr_pdf(pdf) -> str:
    """Try OCR across all pages; returns concatenated text or empty string.
    Uses pdfplumber to rasterize and pytesseract if available. Safe fallback when missing.
    """

    def _has_windows_tess() -> bool:
        cmd = _os.environ.get("TESSERACT_CMD", "")
        return cmd.lower().endswith(".exe") and Path(cmd).exists()

    def _wsl_to_win_path(p: str) -> str:
        try:
            cp = _sp.run(["wslpath", "-w", p], capture_output=True, text=True, check=True)
            out = (cp.stdout or "").strip()
            return out or p
        except Exception:
            return p

    def _ocr_with_windows_exe(img) -> str:
        cmd = _os.environ.get("TESSERACT_CMD")
        if not cmd:
            return ""
        langs = ["jpn_vert+jpn+eng", "jpn+eng", "eng"]
        with _tmp.TemporaryDirectory() as td:
            # Save image to temp PNG path
            png = str(Path(td) / "page.png")
            try:
                img.save(png)
            except Exception:
                return ""
            png_win = _wsl_to_win_path(png)
            for lg in langs:
                try:
                    cp = _sp.run([cmd, png_win, "stdout", "-l", lg], capture_output=True)
                    if cp.returncode == 0:
                        txt = cp.stdout.decode("utf-8", errors="ignore")
                        if txt and len(txt.strip()) >= 8:
                            return txt
                except Exception:
                    continue
        return ""

    text_chunks: list[str] = []
    langs_to_try = ["jpn_vert+jpn+eng", "jpn+eng", "eng"]

    # Prefer Windows Tesseract bridge if TESSERACT_CMD points to .exe (WSL環境向け)
    use_windows_bridge = _has_windows_tess()

    for page in pdf.pages:
        try:
            img = page.to_image(resolution=300).original
            chunk = ""
            if use_windows_bridge:
                chunk = _ocr_with_windows_exe(img)
            else:
                try:
                    import pytesseract  # type: ignore

                    _ = pytesseract.get_tesseract_version()
                    for lg in langs_to_try:
                        try:
                            tmp = pytesseract.image_to_string(img, lang=lg)
                            if tmp and len(tmp.strip()) >= 8:
                                chunk = tmp
                                break
                        except Exception:
                            continue
                    if not chunk:
                        try:
                            chunk = pytesseract.image_to_string(img)
                        except Exception:
                            chunk = ""
                except Exception:
                    chunk = ""
            if not chunk and not use_windows_bridge:
                # As a final fallback, try Windows bridge if available
                if _has_windows_tess():
                    chunk = _ocr_with_windows_exe(img)
            if chunk:
                text_chunks.append(chunk)
        except Exception:
            continue
    return "\n".join(text_chunks)


def _azure_ocr_pdf(file_path: Path) -> str:
    """Use Azure OCR if AZURE_OCR_ENDPOINT and AZURE_OCR_KEY are set.
    Tries Document Intelligence prebuilt-read first, then Vision Read v3.2.
    Returns concatenated text or empty string on failure.
    """
    import os

    def _read_env() -> None:
        # First, try python-dotenv
        try:
            from dotenv import load_dotenv  # type: ignore

            load_dotenv()
        except Exception:
            pass
        # Next, try plain .env at CWD and project root
        for p in (Path.cwd() / ".env", Path(__file__).resolve().parents[2] / ".env"):
            try:
                if p.exists():
                    for line in p.read_text(encoding="utf-8").splitlines():
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if "=" in line:
                            k, v = line.split("=", 1)
                            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
            except Exception:
                continue

    _read_env()

    endpoint = (
        os.getenv("AZURE_OCR_ENDPOINT")
        or os.getenv("AZURE_VISION_ENDPOINT")
        or os.getenv("AZURE_DOCUMENT_ENDPOINT")
        or os.getenv("AZURE_FORMRECOGNIZER_ENDPOINT")
        or os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT")
        or os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT_FREE")
        or os.getenv("FORM_RECOGNIZER_ENDPOINT")
    )
    key = (
        os.getenv("AZURE_OCR_KEY")
        or os.getenv("AZURE_VISION_KEY")
        or os.getenv("AZURE_DOCUMENT_KEY")
        or os.getenv("AZURE_FORMRECOGNIZER_KEY")
        or os.getenv("AZURE_DOCUMENT_INTELLIGENCE_KEY")
        or os.getenv("AZURE_DOCUMENT_INTELLIGENCE_KEY_FREE")
        or os.getenv("FORM_RECOGNIZER_KEY")
        or os.getenv("COGNITIVE_SERVICE_KEY")
    )
    if not endpoint or not key:
        return ""
    endpoint = endpoint.rstrip("/")

    import time
    import requests  # type: ignore

    # 1) Try Document Intelligence prebuilt-read (prefer stable API versions)
    try:
        data = file_path.read_bytes()
        api_versions = [
            "2024-07-31",  # GA newer
            "2023-07-31",  # GA widely available
            "2024-02-29-preview",  # preview fallback
        ]
        for api in api_versions:
            try:
                url = f"{endpoint}/formrecognizer/documentModels/prebuilt-read:analyze?api-version={api}"
                headers = {"Ocp-Apim-Subscription-Key": key, "Content-Type": "application/pdf"}
                r = requests.post(url, headers=headers, data=data, timeout=60)
                if r.status_code not in (200, 202):
                    continue
                op = r.headers.get("operation-location") or r.headers.get("Operation-Location")
                if not op:
                    continue
                for _ in range(30):
                    rr = requests.get(op, headers={"Ocp-Apim-Subscription-Key": key}, timeout=30)
                    js = rr.json()
                    status = (js.get("status") or js.get("statusCode") or "").lower()
                    if status in ("succeeded", "success"):
                        pages = js.get("analyzeResult", {}).get("pages", []) or js.get(
                            "documents", []
                        )
                        lines = []
                        for p in pages:
                            for line in p.get("lines", []):
                                txt = line.get("content") or line.get("text")
                                if txt:
                                    lines.append(txt)
                        if lines:
                            return "\n".join(lines)
                        paras = js.get("analyzeResult", {}).get("paragraphs", [])
                        if paras:
                            return "\n".join(
                                p.get("content", "") for p in paras if p.get("content")
                            )
                        break
                    if status in ("failed", "error"):
                        break
                    time.sleep(1.0)
            except Exception:
                continue
    except Exception:
        pass

    # 2) Try Vision Read v3.2
    try:
        url = f"{endpoint}/vision/v3.2/read/analyze"
        headers = {"Ocp-Apim-Subscription-Key": key, "Content-Type": "application/pdf"}
        data = file_path.read_bytes()
        r = requests.post(url, headers=headers, data=data, timeout=60)
        if r.status_code in (200, 202):
            op = r.headers.get("operation-location") or r.headers.get("Operation-Location")
            if op:
                for _ in range(30):
                    rr = requests.get(op, headers={"Ocp-Apim-Subscription-Key": key}, timeout=30)
                    js = rr.json()
                    status = js.get("status", "").lower()
                    if status == "succeeded":
                        results = js.get("analyzeResult", {}).get("readResults", [])
                        lines = []
                        for p in results:
                            for line in p.get("lines", []):
                                if line.get("text"):
                                    lines.append(line["text"])
                        if lines:
                            return "\n".join(lines)
                        break
                    if status in ("failed", "error"):
                        break
                    time.sleep(1.0)
    except Exception:
        pass

    return ""


def _azure_ocr_image(file_path: Path) -> str:
    """Azure OCR for images (PNG/JPG). Returns concatenated text or empty string."""
    import os

    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv()
    except Exception:
        pass
    endpoint = (
        os.getenv("AZURE_OCR_ENDPOINT")
        or os.getenv("AZURE_VISION_ENDPOINT")
        or os.getenv("AZURE_DOCUMENT_ENDPOINT")
        or os.getenv("AZURE_FORMRECOGNIZER_ENDPOINT")
        or os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT")
        or os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT_FREE")
        or os.getenv("FORM_RECOGNIZER_ENDPOINT")
    )
    key = (
        os.getenv("AZURE_OCR_KEY")
        or os.getenv("AZURE_VISION_KEY")
        or os.getenv("AZURE_DOCUMENT_KEY")
        or os.getenv("AZURE_FORMRECOGNIZER_KEY")
        or os.getenv("AZURE_DOCUMENT_INTELLIGENCE_KEY")
        or os.getenv("AZURE_DOCUMENT_INTELLIGENCE_KEY_FREE")
        or os.getenv("FORM_RECOGNIZER_KEY")
        or os.getenv("COGNITIVE_SERVICE_KEY")
    )
    if not endpoint or not key:
        return ""
    endpoint = endpoint.rstrip("/")
    import requests  # type: ignore
    import time

    data = file_path.read_bytes()
    content_type = "image/png" if file_path.suffix.lower() == ".png" else "image/jpeg"
    # Try Document Intelligence prebuilt-read first (often better for JP), fallback API versions
    try:
        for api in ["2024-07-31", "2023-07-31", "2024-02-29-preview"]:
            try:
                url = f"{endpoint}/formrecognizer/documentModels/prebuilt-read:analyze?api-version={api}"
                headers = {"Ocp-Apim-Subscription-Key": key, "Content-Type": content_type}
                r = requests.post(url, headers=headers, data=data, timeout=60)
                if r.status_code not in (200, 202):
                    continue
                op = r.headers.get("operation-location") or r.headers.get("Operation-Location")
                if not op:
                    continue
                for _ in range(30):
                    rr = requests.get(op, headers={"Ocp-Apim-Subscription-Key": key}, timeout=30)
                    js = rr.json()
                    status = (js.get("status") or js.get("statusCode") or "").lower()
                    if status in ("succeeded", "success"):
                        pages = js.get("analyzeResult", {}).get("pages", []) or js.get(
                            "documents", []
                        )
                        lines = []
                        for p in pages:
                            for line in p.get("lines", []):
                                txt = line.get("content") or line.get("text")
                                if txt:
                                    lines.append(txt)
                        if lines:
                            return "\n".join(lines)
                        paras = js.get("analyzeResult", {}).get("paragraphs", [])
                        if paras:
                            return "\n".join(
                                p.get("content", "") for p in paras if p.get("content")
                            )
                        break
                    if status in ("failed", "error"):
                        break
                    time.sleep(1.0)
            except Exception:
                continue
    except Exception:
        pass
    # Vision Read v3.2 as fallback
    try:
        url = f"{endpoint}/vision/v3.2/read/analyze"
        headers = {"Ocp-Apim-Subscription-Key": key, "Content-Type": content_type}
        r = requests.post(url, headers=headers, data=data, timeout=60)
        if r.status_code in (200, 202):
            op = r.headers.get("operation-location") or r.headers.get("Operation-Location")
            if op:
                for _ in range(30):
                    rr = requests.get(op, headers={"Ocp-Apim-Subscription-Key": key}, timeout=30)
                    js = rr.json()
                    status = js.get("status", "").lower()
                    if status == "succeeded":
                        results = js.get("analyzeResult", {}).get("readResults", [])
                        lines = []
                        for p in results:
                            for line in p.get("lines", []):
                                if line.get("text"):
                                    lines.append(line["text"])
                        if lines:
                            return "\n".join(lines)
                        break
                    if status in ("failed", "error"):
                        break
                    time.sleep(1.0)
    except Exception:
        pass
    return ""


def _ocr_image(path: Path) -> str:
    """Local Tesseract OCR for an image file."""

    # Prefer Windows Tesseract bridge when available
    def _wsl_to_win_path(p: str) -> str:
        try:
            cp = _sp.run(["wslpath", "-w", p], capture_output=True, text=True, check=True)
            return (cp.stdout or "").strip() or p
        except Exception:
            return p

    tess_cmd = _os.environ.get("TESSERACT_CMD")
    if tess_cmd and tess_cmd.lower().endswith(".exe") and Path(tess_cmd).exists():
        try:
            cmd = tess_cmd
            png_win = _wsl_to_win_path(str(path))
            for lg in ("jpn_vert+jpn+eng", "jpn+eng", "eng"):
                try:
                    cp = _sp.run([cmd, png_win, "stdout", "-l", lg], capture_output=True)
                    if cp.returncode == 0:
                        txt = cp.stdout.decode("utf-8", errors="ignore")
                        if txt and len(txt.strip()) >= 8:
                            return txt
                except Exception:
                    continue
        except Exception:
            pass
    # Fallback to pytesseract (WSL/Ubuntuにtesseractが入っている場合)
    try:
        import pytesseract  # type: ignore
        from PIL import Image  # type: ignore

        _ = pytesseract.get_tesseract_version()
        img = Image.open(path)
        try:
            return pytesseract.image_to_string(img, lang="jpn+eng")
        except Exception:
            return pytesseract.image_to_string(img)
    except Exception:
        return ""


def scan_image_dates(path: Path) -> List[tuple[str, str]]:
    text = _azure_ocr_image(path) or _ocr_image(path)
    if not text:
        return []
    raw = extract_date_candidates(text)
    out = []
    for tok in raw:
        dt = parse_jp_date(tok.split("(")[0].strip())
        out.append((tok, dt.isoformat() if dt else ""))
    return out


def scan_image_labeled_dates(path: Path) -> pd.DataFrame:
    text = _azure_ocr_image(path) or _ocr_image(path)
    if not text:
        return pd.DataFrame(columns=["page", "first_issue_date", "issue_date", "expiry_date"])
    d = _extract_labeled_dates_from_text(text)
    if any(d.values()):
        return pd.DataFrame([{**{"page": None}, **d}])
    return pd.DataFrame(columns=["page", "first_issue_date", "issue_date", "expiry_date"])
