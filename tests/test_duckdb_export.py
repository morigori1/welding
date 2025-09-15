from pathlib import Path
import pandas as pd

from welding_registry.db import to_duckdb


def test_to_duckdb_roundtrip(tmp_path: Path):
    df = pd.DataFrame(
        {"name": ["山田太郎"], "license_no": ["AB-123"], "expiry_date": ["2028-09-01"]}
    )
    db = tmp_path / "t.duckdb"
    to_duckdb(df, db, table="roster")

    import duckdb

    con = duckdb.connect(str(db))
    try:
        out = con.execute("SELECT * FROM roster").df()
    finally:
        con.close()
    assert len(out) == 1
    assert out.loc[0, "license_no"] == "AB-123"
