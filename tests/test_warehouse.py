import duckdb  # type: ignore
import pandas as pd

from welding_registry.warehouse import (
    attach_identity_columns,
    load_issue_run_items,
    load_issue_runs,
    materialize_roster_all,
    reapply_due_filters,
    record_issue_run,
    set_license_filter,
    set_person_filter,
    set_sheet_membership,
    write_due_tables,
    add_report_definition,
    delete_report_definition,
    list_report_definitions,
    add_report_entry,
    list_report_entries,
)


def test_attach_identity_columns_generates_keys() -> None:
    df = pd.DataFrame(
        {
            "name": ["田中", "佐藤"],
            "license_no": ["A-001", None],
            "qualification": ["基本", "上級"],
            "expiry_date": ["2025-03-01", "2025-05-01"],
        }
    )
    out = attach_identity_columns(df)
    assert "person_key" in out.columns
    assert "license_key" in out.columns
    assert out["person_key"].nunique() == len(out)
    assert out["license_key"].nunique() == len(out)


def test_materialize_roster_all_merges_manual(tmp_path) -> None:
    db_path = tmp_path / "warehouse.duckdb"
    roster = pd.DataFrame(
        {
            "name": ["田中", "佐藤"],
            "license_no": ["A-001", "A-002"],
            "qualification": ["基本", "上級"],
            "expiry_date": ["2025-03-01", "2025-05-01"],
        }
    )
    with duckdb.connect(str(db_path)) as con:
        con.register("roster_src", roster)
        con.execute("CREATE TABLE roster AS SELECT * FROM roster_src")
        con.unregister("roster_src")
        con.execute(
            """
            CREATE TABLE roster_manual AS
            SELECT
                '田中' AS name,
                'A-001' AS license_no,
                '手動' AS qualification,
                NULL AS first_issue_date,
                NULL AS issue_date,
                '2025-06-01' AS expiry_date,
                'P1' AS print_sheet,
                'SheetX' AS source_sheet,
                TIMESTAMP '2025-06-01 00:00:00' AS created
            """
        )
    df = materialize_roster_all(db_path)
    assert not df.empty
    assert set(["person_key", "license_key", "source", "sheet_source"]).issubset(df.columns)
    row = df.loc[df["license_no"] == "A-001"].iloc[0]
    assert row["source"] == "ingest"
    assert row["qualification"] == "基本"
    assert row["print_sheet"] == "P1"
    assert row["sheet_source"] == "manual"
    assert row["source_sheet"] == "SheetX"
    with duckdb.connect(str(db_path)) as con:
        pf = con.execute("SELECT person_key, include FROM issue_person_filter").fetchall()
        assert len(pf) == df["person_key"].nunique()

def test_materialize_roster_all_manual_keeps_registration_date(tmp_path) -> None:
    db_path = tmp_path / "registration.duckdb"
    roster = pd.DataFrame(
        {
            "name": ["田中"],
            "license_no": ["A-001"],
            "qualification": ["TN-F"],
            "registration_date": ["2024-04-01"],
            "expiry_date": ["2025-06-30"],
        }
    )
    with duckdb.connect(str(db_path)) as con:
        con.register("roster_src", roster)
        con.execute("CREATE TABLE roster AS SELECT * FROM roster_src")
        con.unregister("roster_src")
        con.execute(
            """
            CREATE TABLE roster_manual AS
            SELECT
                '田中' AS name,
                'A-001' AS license_no,
                '手動' AS qualification,
                NULL AS first_issue_date,
                NULL AS issue_date,
                '2025-06-30' AS expiry_date,
                'P1' AS print_sheet,
                'Sheet1' AS source_sheet,
                TIMESTAMP '2025-06-30 00:00:00' AS created
            """
        )
    df = materialize_roster_all(db_path)
    row = df.loc[df["license_no"] == "A-001"].iloc[0]
    assert row["registration_date"] == "2024-04-01"
    assert row["expiry_date"] == "2025-06-30"
    assert row["print_sheet"] == "P1"
    assert row["sheet_source"] == "manual"


def test_materialize_roster_all_prefers_latest_by_license(tmp_path) -> None:
    db_path = tmp_path / "latest.duckdb"
    roster = pd.DataFrame(
        {
            "name": ["田中", "田中"],
            "license_no": ["A-001", "A-001"],
            "qualification": ["旧資格", "新資格"],
            "registration_date": ["2023-01-01", "2024-12-01"],
            "issue_date": ["2023-02-01", "2025-01-15"],
            "expiry_date": ["2024-02-01", "2026-01-14"],
        }
    )
    with duckdb.connect(str(db_path)) as con:
        con.register("roster_src", roster)
        con.execute("CREATE TABLE roster AS SELECT * FROM roster_src")
        con.unregister("roster_src")
    df = materialize_roster_all(db_path)
    row = df.loc[df["license_no"] == "A-001"].iloc[0]
    assert row["qualification"] == "新資格"
    assert row["registration_date"] == "2024-12-01"
    assert row["issue_date"] == "2025-01-15"



def test_report_definition_roundtrip(tmp_path) -> None:
    db_path = tmp_path / "report_defs.duckdb"
    add_report_definition(db_path, report_id="inspection", label="\u5b9a\u671f\u691c\u67fb", description="\u5e74\u6b21\u4e88\u5b9a")
    defs = list_report_definitions(db_path)
    assert not defs.empty
    row = defs.iloc[0]
    assert row["report_id"] == "inspection"
    assert row["label"] == "\u5b9a\u671f\u691c\u67fb"

    add_report_definition(db_path, report_id="inspection", label="\u518d\u691c\u8a3a", description=None)
    defs = list_report_definitions(db_path)
    row = defs.iloc[0]
    assert row["label"] == "\u518d\u691c\u8a3a"

    delete_report_definition(db_path, report_id="inspection")
    defs = list_report_definitions(db_path)
    assert defs.empty



def test_add_report_entry_requires_definition(tmp_path) -> None:
    db_path = tmp_path / "report_requires.duckdb"
    roster = pd.DataFrame({"name": ["\u7530\u4e2d"], "license_no": ["A-001"], "qualification": ["TN-F"], "expiry_date": ["2026-03-01"]})
    with duckdb.connect(str(db_path)) as con:
        con.register("roster_src", roster)
        con.execute("CREATE TABLE roster AS SELECT * FROM roster_src")
        con.unregister("roster_src")
    materialize_roster_all(db_path)
    try:
        add_report_entry(db_path, report_id="inspection", license_no="A-001")
    except ValueError as exc:
        assert "not defined" in str(exc)
    else:
        raise AssertionError("expected ValueError when report definition missing")
    add_report_definition(db_path, report_id="inspection", label="\u5b9a\u671f\u691c\u67fb")
    add_report_entry(db_path, report_id="inspection", license_no="A-001")
    defs = list_report_entries(db_path)
    assert not defs.empty



def test_write_due_tables_respects_filters(tmp_path) -> None:
    db_path = tmp_path / "due.duckdb"
    due = pd.DataFrame(
        {
            "name": ["田中", "佐藤"],
            "license_no": ["A-001", "A-002"],
            "qualification": ["基本", "上級"],
            "expiry_date": ["2025-03-01", "2025-05-01"],
        }
    )
    df = write_due_tables(db_path, due)
    assert len(df) == 2
    assert all(key.startswith("name:") for key in df["person_key"].tolist())
    with duckdb.connect(str(db_path)) as con:
        info = con.execute("PRAGMA table_info('issue_sheet_membership')").fetchall()
        columns = [row[1] for row in info]
        assert 'updated_at' in columns
        row = con.execute("SELECT include, updated_at FROM issue_sheet_membership LIMIT 1").fetchone()
    assert row is not None
    assert row[0] in (True, 1)
    assert row[1] is not None
    first_person = df.iloc[0]["person_key"]
    first_license = df.iloc[0]["license_key"]
    set_person_filter(db_path, first_person, include=False)
    set_license_filter(db_path, first_license, include=False, person_key=first_person)
    filtered = reapply_due_filters(db_path)
    assert len(filtered) == 1
    assert filtered.iloc[0]["person_key"] != first_person


def test_record_issue_run_roundtrip(tmp_path) -> None:
    db_path = tmp_path / "runs.duckdb"
    due = pd.DataFrame(
        {
            "name": ["田中"],
            "license_no": ["A-001"],
            "qualification": ["基本"],
            "expiry_date": ["2025-03-01"],
        }
    )
    write_due_tables(db_path, due)
    run_id = record_issue_run(db_path, due, comment="テスト")
    runs = load_issue_runs(db_path)
    assert run_id in runs["run_id"].tolist()
    items = load_issue_run_items(db_path, run_id)
    assert not items.empty
    assert items.iloc[0]["name"] == "田中"


def test_write_due_tables_applies_sheet_membership(tmp_path) -> None:
    db_path = tmp_path / "due_sheet.duckdb"
    due = pd.DataFrame(
        {
            "name": ["Tanaka Taro"],
            "license_no": ["LIC-001"],
            "qualification": ["JIS"],
            "expiry_date": ["2023-01-01"],
        }
    )
    df = write_due_tables(db_path, due)
    assert not df.empty
    license_key = str(df.iloc[0]["license_key"])
    person_key_value = df.iloc[0]["person_key"]
    person_key = None if pd.isna(person_key_value) else str(person_key_value)
    set_sheet_membership(db_path, license_key, "P1", True, person_key=person_key)
    refreshed = reapply_due_filters(db_path)
    sheets = set(refreshed["print_sheet"].astype(str))
    assert "P1" in sheets
    p1_rows = refreshed[refreshed["print_sheet"] == "P1"]
    assert not p1_rows.empty
    display = p1_rows["display_name"].astype(str).str.strip()
    assert (display != "").all()


def test_materialize_roster_coerces_license_to_string(tmp_path) -> None:
    db_path = tmp_path / "warehouse_string.duckdb"
    roster = pd.DataFrame(
        {
            "name": ["田中"],
            "license_no": [123456],
            "qualification": ["A-3F"],
            "expiry_date": ["2025-12-31"],
        }
    )
    with duckdb.connect(str(db_path)) as con:
        con.register("roster_src", roster)
        con.execute("CREATE TABLE roster AS SELECT * FROM roster_src")
        con.unregister("roster_src")
    df = materialize_roster_all(db_path)
    assert not df.empty
    value = df.iloc[0]["license_no"]
    assert isinstance(value, str)
    assert value == "123456"
