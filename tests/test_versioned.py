from __future__ import annotations

from pathlib import Path
import pandas as pd

import welding_registry.versioned as ver


def _df(rows):
    return pd.DataFrame(
        rows,
        columns=[
            "name",
            "license_no",
            "qualification",
            "category",
            "first_issue_date",
            "issue_date",
            "expiry_date",
        ],
    )


def test_versioned_snapshot_asof(tmp_path: Path, monkeypatch):
    db = tmp_path / "ver.duckdb"
    dummy_xls = tmp_path / "dummy.xlsx"
    dummy_xls.write_bytes(b"")

    # Snapshot 1: only Yamada
    df1 = _df(
        [
            ["YAMADA TARO", "AB-123", "SC-3F", "JIS", None, "2024-09-01", "2028-09-01"],
        ]
    )
    monkeypatch.setattr(ver, "read_snapshot_xls", lambda *args, **kwargs: (df1, None))
    ver.ingest_snapshot(dummy_xls, duckdb_path=db, snapshot_date="2025-09-01")

    out1 = ver.asof_dataframe(duckdb_path=db, date="2025-09-15")
    assert len(out1) == 1
    assert out1.loc[0, "license_no"] == "AB-123"

    # Snapshot 2: Yamada persists, Suzuki added
    df2 = _df(
        [
            ["YAMADA TARO", "AB-123", "SC-3F", "JIS", None, "2024-09-01", "2028-09-01"],
            ["SUZUKI ICHIRO", "ZX-999", "A-3V", "BOILER", None, "2025-10-01", "2028-10-01"],
        ]
    )
    monkeypatch.setattr(ver, "read_snapshot_xls", lambda *args, **kwargs: (df2, None))
    ver.ingest_snapshot(dummy_xls, duckdb_path=db, snapshot_date="2025-10-01")

    out2 = ver.asof_dataframe(duckdb_path=db, date="2025-10-15")
    assert len(out2) == 2
    assert sorted(out2["license_no"]) == ["AB-123", "ZX-999"]

    # Snapshot 3: Yamada removed, Suzuki stays
    df3 = _df(
        [
            ["SUZUKI ICHIRO", "ZX-999", "A-3V", "BOILER", None, "2025-10-01", "2028-10-01"],
        ]
    )
    monkeypatch.setattr(ver, "read_snapshot_xls", lambda *args, **kwargs: (df3, None))
    ver.ingest_snapshot(dummy_xls, duckdb_path=db, snapshot_date="2025-11-01")

    out3 = ver.asof_dataframe(duckdb_path=db, date="2025-11-15")
    assert len(out3) == 1
    assert out3.loc[0, "license_no"] == "ZX-999"

    # As-of right before removal still has Yamada
    out_prev = ver.asof_dataframe(duckdb_path=db, date="2025-10-31")
    assert set(out_prev["license_no"]) == {"AB-123", "ZX-999"}
