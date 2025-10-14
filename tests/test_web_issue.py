from pathlib import Path

import pandas as pd
import pytest

from welding_registry.webapp import create_app
from welding_registry.warehouse import write_due_tables


@pytest.fixture
def sample_duckdb(tmp_path: Path) -> Path:
    db_path = tmp_path / "warehouse.duckdb"
    df = pd.DataFrame(
        [
            {
                "name": "山田太郎",
                "license_no": "A-001",
                "qualification": "溶接士",
                "continuation_status": "継続",
                "print_sheet": "A",
                "expiry_date": pd.Timestamp("2026-03-01"),
                "birth_year_west": "1980",
            },
            {
                "name": "佐藤花子",
                "license_no": "B-002",
                "qualification": "溶接士",
                "continuation_status": "継続",
                "print_sheet": "B",
                "expiry_date": pd.Timestamp("2026-05-15"),
            },
            {
                "name": "田中一郎",
                "license_no": "B-003",
                "qualification": "溶接士",
                "continuation_status": "継続",
                "print_sheet": "B",
                "expiry_date": pd.Timestamp("2026-06-20"),
            },
        ]
    )
    write_due_tables(db_path, df)
    return db_path


@pytest.fixture
def empty_duckdb(tmp_path: Path) -> Path:
    db_path = tmp_path / "empty.duckdb"
    df = pd.DataFrame({
        "name": pd.Series(dtype="string"),
        "license_no": pd.Series(dtype="string"),
        "qualification": pd.Series(dtype="string"),
        "continuation_status": pd.Series(dtype="string"),
        "print_sheet": pd.Series(dtype="string"),
        "expiry_date": pd.Series(dtype="datetime64[ns]"),
    })
    write_due_tables(db_path, df)
    return db_path


def _make_client(db_path: Path):
    app = create_app(warehouse=db_path)
    app.testing = True
    return app.test_client()


def test_issue_index_preview_renders(sample_duckdb: Path):
    client = _make_client(sample_duckdb)
    resp = client.get("/issue/")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8")
    assert "山田太郎" in html
    assert "印刷ビューを開く" in html
    assert html.count('name="columns"') >= 2


def test_issue_index_sheet_filter_and_columns(sample_duckdb: Path):
    client = _make_client(sample_duckdb)
    resp = client.get(
        "/issue/",
        query_string={
            "sheet": "B",
            "columns": ["name", "expiry_date"],
            "rows_per_page": "1",
        },
    )
    assert resp.status_code == 200
    html = resp.data.decode("utf-8")
    assert "シート: B / 1 - 2" in html
    header = html.split("<thead>", 1)[1].split("</thead>", 1)[0]
    assert "氏名" in header
    assert "有効期限" in header


def test_issue_print_landscape(sample_duckdb: Path):
    client = _make_client(sample_duckdb)
    resp = client.get(
        "/issue/print",
        query_string={
            "orientation": "landscape",
            "columns": ["name", "license_no"],
            "rows_per_page": 2,
        },
    )
    assert resp.status_code == 200
    html = resp.data.decode("utf-8")
    assert "A4 landscape" in html
    assert "山田太郎" in html


def test_issue_print_empty(empty_duckdb: Path):
    client = _make_client(empty_duckdb)
    resp = client.get("/issue/print")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8")
    assert "印刷対象がありません" in html
