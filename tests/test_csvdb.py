from pathlib import Path
import pandas as pd

from welding_registry.csvdb import (
    write_asof_csv,
    read_asof_csv,
    get_person_list,
    get_qualification_list,
    log_display_selection,
    asof_csv_path,
    read_csv_robust,
)


def test_csvdb_roundtrip(tmp_path: Path, monkeypatch):
    # Redirect BASE_DIR at runtime by monkeypatching module-level constants
    import welding_registry.csvdb as csvdb

    monkeypatch.setattr(csvdb, "BASE_DIR", tmp_path / "warehouse" / "csv", raising=False)
    monkeypatch.setattr(csvdb, "ASOF_DIR", csvdb.BASE_DIR / "asof", raising=False)
    monkeypatch.setattr(csvdb, "LOG_FILE", csvdb.BASE_DIR / "display_log.csv", raising=False)

    df = pd.DataFrame(
        {
            "name": ["YAMADA TARO", "SUZUKI ICHIRO"],
            "license_no": ["A1", "B2"],
            "qualification": ["SC-3F", "A-3V"],
            "category": ["JIS", "BOILER"],
            "expiry_date": ["2028-09-01", "2027-03-31"],
        }
    )
    date = "2025-09-12"
    path = write_asof_csv(df, date=date)
    assert path == asof_csv_path(date)
    assert path.exists()

    df2 = read_asof_csv(date)
    assert df2 is not None
    assert len(df2) == 2

    persons = dict(get_person_list(date))
    assert persons.get("YAMADA TARO") == 1

    quals = get_qualification_list(date)
    assert "SC-3F" in quals and "A-3V" in quals

    log = log_display_selection(
        date=date,
        mode="person",
        persons=["YAMADA TARO"],
        qualifications=None,
        operator="tester",
        session_id="s1",
    )
    assert log.exists()

    # CP932 CSV read robustness
    cp = tmp_path / "sjis.csv"
    content = "氏名,登録番号\n山田太郎,123\n".encode("cp932")
    cp.write_bytes(content)
    df3 = read_csv_robust(cp)
    assert list(df3.columns)[0].startswith("氏")
