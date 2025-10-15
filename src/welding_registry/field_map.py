from __future__ import annotations

from pathlib import Path
from functools import lru_cache
from typing import Dict, Iterable
import unicodedata as _ud

import yaml

# Canonical date-like columns used across the project
DATE_COLUMNS = {"birth_date", "test_date", "first_issue_date", "issue_date", "expiry_date", "registration_date"}


def _norm_token(s: str) -> str:
    if s is None:
        return ""
    t = _ud.normalize("NFKC", str(s)).strip()
    if t.lower().startswith("unnamed:"):
        return t.lower()
    for l, r in [("(", ")"), ("（", "）"), ("[", "]"), ("{", "}")]:
        while l in t and r in t and t.index(l) < t.index(r):
            li, ri = t.index(l), t.index(r)
            t = (t[:li] + t[ri + 1 :]).strip()
    return t


def _project_root(start: Path) -> Path:
    cur = start
    for _ in range(6):
        if (cur / "pyproject.toml").exists() or (cur / ".git").exists():
            return cur
        cur = cur.parent
    return start


def _ensure_str_list(x) -> Iterable[str]:
    if isinstance(x, (list, tuple, set)):
        return [str(i) if i is not None else "" for i in x]
    return [str(x) if x is not None else ""]


@lru_cache(maxsize=1)
def get_header_map() -> Dict[str, str]:
    """Load docs/field_map.yaml if present and build a reverse map of
    Japanese headers -> canonical keys, including normalized variants.
    """
    base: Dict[str, Iterable[str]] = {
        "name": ["氏名"],
        "kana": ["ﾌﾘｶﾞﾅ", "フリガナ"],
        "birth_date": ["生年月日"],
        "birth_year_west": ["生年・西暦", "（生年・西暦）", "生年", "生年(西暦)", "生年（西暦）"],
        "license_no": ["登録番号", "免許番号", "資格番号"],

        "category": ["区分", "資格種類"],
        "registration_date": ["登録年月日", "登録日"],
        "continuation_status": ["継続"],
        "next_stage_label": ["次回区分"],
        "next_exam_period": [
            "次回サーベイランス/再評価受験期間",
            "次回ｻｰﾍﾞｲﾗﾝｽ/再評価受験期間",
            "次回ｻｰﾍﾞｲﾗﾝｽ/\n再評価受験期間",
            "次回ｻｰﾍﾞｲﾗﾝｽ/\r\n再評価受験期間",
            "次回／再評価受験期間",
            "次回受験期間",
        ],
        "next_procedure_status": ["次回手続き状況"],
        "qualification": ["資格"],
        "process": ["溶接方法"],
        "material": ["材質", "材料"],
        "thickness": ["板厚", "径"],
        "test_date": ["試験日"],
        "issue_date": ["交付日", "発行日"],
        "expiry_date": ["有効期限"],
        "result": ["試験結果", "結果"],
        "notes": ["備考"],
        "address": ["自宅住所", "住所", "住所（自宅）"],
        "web_publish_no": ["WEB申込番号", "WEB公開番号", "WEB申込No.", "WEB申込No"],
    }

    try:
        here = Path(__file__).resolve()
        root = _project_root(here.parent.parent)
        yml = root / "docs" / "field_map.yaml"
        if yml.exists():
            data = yaml.safe_load(yml.read_text(encoding="utf-8")) or {}
            for canon, tokens in data.items():
                if not isinstance(tokens, list):
                    continue
                base.setdefault(canon, [])
                base[canon] = list({*_ensure_str_list(base[canon]), *_ensure_str_list(tokens)})
    except Exception:
        # fall back silently
        pass

    rev: Dict[str, str] = {}
    for canon, tokens in base.items():
        # Augment synonyms programmatically to avoid YAML encoding pitfalls
        if canon == "license_no":
            extra = ["証明書番号", "証書番号", "登録番号", "認定番号", "番号", "No", "No.", "証明番号"]
            tokens = list(_ensure_str_list(tokens)) + extra
        for tok in _ensure_str_list(tokens):
            if not tok:
                continue
            rev[tok] = canon
            rev[_norm_token(tok)] = canon
    return rev


__all__ = ["get_header_map", "DATE_COLUMNS"]

