from datetime import date, timedelta

import pandas as pd

from welding_registry.reminders import annotate_due, compute_due, DueConfig


def test_annotate_due_marks_rows_without_filtering() -> None:
    today = date(2025, 1, 1)
    frame = pd.DataFrame(
        {
            "name": ["expired", "soon", "later"],
            "expiry_date": [
                today - timedelta(days=10),
                today + timedelta(days=10),
                today + timedelta(days=90),
            ],
        }
    )

    annotated = annotate_due(frame, as_of=today, cfg=DueConfig(window_days=30))
    assert list(annotated["name"]) == ["expired", "soon", "later"]
    assert annotated["due_within_window"].tolist() == [True, True, False]
    assert annotated["days_to_expiry"].tolist() == [-10, 10, 90]
    assert annotated["notice_stage"].tolist() == ["expired", "same-day", ""]
    assert annotated["next_notice_date"].tolist()[0] == ""
    assert annotated["next_notice_date"].tolist()[1] == today.isoformat()

    due_only = compute_due(frame, as_of=today, cfg=DueConfig(window_days=30))
    assert due_only["days_to_expiry"].tolist() == [-10, 10]
    assert "due_within_window" not in due_only.columns
    assert due_only["name"].tolist() == ["expired", "soon"]
