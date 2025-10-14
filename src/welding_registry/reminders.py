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


def _project_due(exp: Optional[date], as_of: date, cfg: DueConfig) -> tuple[Optional[int], bool, str, str]:
    if exp is None:
        return None, False, "", ""

    days = (exp - as_of).days
    include = False
    stage = ""
    next_date_str = ""

    if days <= cfg.window_days:
        if days < 0:
            if cfg.include_overdue:
                include = True
                stage = "expired"
        else:
            include = True
            milestones = [
                ("first", exp - timedelta(days=cfg.first_notice_days)),
                ("second", exp - timedelta(days=cfg.second_notice_days)),
                ("final", exp - timedelta(days=cfg.final_notice_days)),
            ]
            for label, dt in milestones:
                if dt >= as_of:
                    stage = label
                    next_date_str = dt.isoformat()
                    break
            else:
                if exp >= as_of:
                    stage = "same-day"
                    next_date_str = as_of.isoformat()
    else:
        if days < 0 and cfg.include_overdue:
            stage = "expired"

    return days, include, stage, next_date_str


def annotate_due(
    df: pd.DataFrame, as_of: date | None = None, cfg: DueConfig | None = None
) -> pd.DataFrame:
    """Annotate every row with due metadata without filtering the window."""
    as_of = as_of or date.today()
    cfg = cfg or DueConfig()

    if "expiry_date" not in df.columns:
        raise ValueError("DataFrame must contain 'expiry_date'")

    result = df.copy()
    expiry_series = result["expiry_date"].map(_to_date)

    days_list: list[Optional[int]] = []
    include_flags: list[bool] = []
    stages: list[str] = []
    next_dates: list[str] = []

    for exp in expiry_series.tolist():
        days_to_expiry, include, stage, next_date = _project_due(exp, as_of, cfg)
        days_list.append(days_to_expiry)
        include_flags.append(include)
        stages.append(stage)
        next_dates.append(next_date)

    result["days_to_expiry"] = pd.Series(days_list, dtype="Int64")
    result["notice_stage"] = pd.Series(stages, dtype="string")
    result["next_notice_date"] = pd.Series(next_dates, dtype="string")
    result["due_within_window"] = pd.Series(include_flags, dtype="boolean")
    return result


def compute_due(
    df: pd.DataFrame, as_of: date | None = None, cfg: DueConfig | None = None
) -> pd.DataFrame:
    """Return rows with an expiry within window or already overdue."""
    annotated = annotate_due(df, as_of=as_of, cfg=cfg)
    mask = annotated["due_within_window"].fillna(False)
    out = annotated.loc[mask].copy()
    out = out.drop(columns=["due_within_window"])

    sort_cols: list[str] = []
    if "days_to_expiry" in out.columns:
        sort_cols.append("days_to_expiry")
    if "name" in out.columns:
        sort_cols.append("name")
    if sort_cols:
        out = out.sort_values(by=sort_cols, kind="stable")
    return out


def _ics_escape(text: str) -> str:
    return (
        text.replace("\\", "\\\\")
        .replace(";", "\\;")
        .replace(",", "\\,")
        .replace("\n", "\\n")
    )


def write_ics(df: pd.DataFrame, out_path: Path, summary_tpl: str = "溶接資格: {name}") -> None:
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
