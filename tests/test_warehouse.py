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
    write_due_tables,
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
            "CREATE TABLE roster_manual AS SELECT '田中' AS name, 'A-001' AS license_no, '手動' AS qualification, NULL AS first_issue_date, NULL AS issue_date, '2025-06-01' AS expiry_date, TIMESTAMP '2025-06-01 00:00:00' AS created"
        )
    df = materialize_roster_all(db_path)
    assert not df.empty
    assert set(["person_key", "license_key", "source"]).issubset(df.columns)
    manual_row = df.loc[df["source"] == "manual"].iloc[0]
    assert manual_row["qualification"] == "手動"
    with duckdb.connect(str(db_path)) as con:
        pf = con.execute("SELECT person_key, include FROM issue_person_filter").fetchall()
        assert len(pf) == df["person_key"].nunique()


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

