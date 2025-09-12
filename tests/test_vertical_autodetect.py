from __future__ import annotations

import pandas as pd

from welding_registry.io_excel import detect_vertical_layout_df


def test_detect_vertical_layout_basic():
    # Synthetic header-less frame roughly matching vertical layout
    # Columns: A (name), B (regno), C..H (JIS), I..K (BOILER)
    rows = []
    # row0: pseudo headers
    rows.append([
        "氏名", "登録番号", "JIS", None, None, None, None, None, "BOILER", None, None,
    ])
    # row1: person 1
    rows.append([
        "YAMADA TARO", "12345", "SC-3F", "2019-05-31", "2022-05-31", None, None, None, "A-3F", "2020-12-01", "2023-12-01",
    ])
    # row2: continuation (empty name/regno)
    rows.append([
        None, None, None, None, None, None, None, None, None, None, None,
    ])
    # row3: person 2
    rows.append([
        "SUZUKI ICHIRO", "67890", "SC-3V", "2021-01-01", "2024-01-01", None, None, None, "A-3V", "2022-01-15", "2025-01-15",
    ])

    df = pd.DataFrame(rows)

    p_idx, r_idx, blocks = detect_vertical_layout_df(df, max_probe_rows=4)

    # person and regno columns detected
    assert p_idx == 0
    assert r_idx == 1
    # Two blocks detected, labeled and non-empty ranges
    labs = [lab for lab, _ in blocks]
    assert "JIS" in labs
    assert "BOILER" in labs
    for _, (a, b) in blocks:
        assert b > a >= 2
