from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from hashlib import sha1
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclass
class DueConfig:
    window_days: int = 90  # include items expiring within N days
    include_overdue: bool = True
    first_notice_days: int = 90
    second_notice_days: int = 60
    final_notice_days: int = 30


def _to_date(v) -> Optional[date]:
    if pd.isna(v):
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    try:
        return pd.to_datetime(v, errors="coerce").date()
    except Exception:
        return None


def compute_due(
    df: pd.DataFrame, as_of: date | None = None, cfg: DueConfig | None = None
) -> pd.DataFrame:
    """Return rows with an expiry within window or already overdue, annotated with days_to_expiry
    and suggested next_notice_date/stage.
    """
    as_of = as_of or date.today()
    cfg = cfg or DueConfig()

    if "expiry_date" not in df.columns:
        raise ValueError("DataFrame must contain 'expiry_date'")

    # Work on a copy to avoid mutating caller's frame
    out = df.copy()
    out["_expiry_date_obj"] = out["expiry_date"].map(_to_date)
    out = out[~out["_expiry_date_obj"].isna()].copy()

    out["days_to_expiry"] = out["_expiry_date_obj"].map(lambda d: (d - as_of).days)

    if cfg.include_overdue:
        mask = out["days_to_expiry"] <= cfg.window_days
    else:
        mask = (out["days_to_expiry"] >= 0) & (out["days_to_expiry"] <= cfg.window_days)
    out = out[mask].copy()

    def _next_notice(exp: date):
        milestones = [
            ("first", exp - timedelta(days=cfg.first_notice_days)),
            ("second", exp - timedelta(days=cfg.second_notice_days)),
            ("final", exp - timedelta(days=cfg.final_notice_days)),
        ]
        # Find the first milestone not yet passed relative to as_of
        for stage, dt in milestones:
            if dt >= as_of:
                return dt, stage
        # If all have passed but not yet expired, suggest today
        if exp >= as_of:
            return as_of, "same-day"
        return None, "expired"

    next_dates, stages = [], []
    for exp in out["_expiry_date_obj"].tolist():
        nd, st = _next_notice(exp)
        next_dates.append(nd)
        stages.append(st)
    out["next_notice_date"] = next_dates
    out["notice_stage"] = stages

    # Sort by expiry ascending, then name if present
    by = ["_expiry_date_obj"] + (["name"] if "name" in out.columns else [])
    out = out.sort_values(by=by, kind="stable")
    # Present-friendly date strings
    out["expiry_date"] = out["_expiry_date_obj"].astype("string")
    out["next_notice_date"] = out["next_notice_date"].astype("string")
    out = out.drop(columns=["_expiry_date_obj"])  # internal helper
    return out


def _ics_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,").replace("\n", "\\n")


def write_ics(df: pd.DataFrame, out_path: Path, summary_tpl: str = "資格有効期限: {name}") -> None:
    """Write a minimal ICS calendar with one all-day event per expiry_date.
    summary_tpl can reference columns like {name}, {qualification}.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//welding-registry//JP",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
    ]

    for _, row in df.iterrows():
        exp = _to_date(row.get("expiry_date"))
        if not exp:
            continue
        ymd = exp.strftime("%Y%m%d")
        # build summary
        summary = summary_tpl.format(**{k: (str(row[k]) if k in row else "") for k in df.columns})
        summary = _ics_escape(summary)
        uid_src = f"{row.get('name', '')}-{ymd}-{row.get('license_no', '')}".encode(
            "utf-8", "ignore"
        )
        uid = sha1(uid_src).hexdigest() + "@welding-registry"
        lines += [
            "BEGIN:VEVENT",
            f"DTSTART;VALUE=DATE:{ymd}",
            f"DTEND;VALUE=DATE:{ymd}",
            f"SUMMARY:{summary}",
            f"UID:{uid}",
            "TRANSP:TRANSPARENT",
            "END:VEVENT",
        ]

    lines.append("END:VCALENDAR")
    out_path.write_text("\r\n".join(lines), encoding="utf-8")
