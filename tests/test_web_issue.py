import json
from pathlib import Path

import duckdb
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
                "継続": 1,
                "next_surveillance_window": "2026/01/01〜2026/06/30",
                "web_publish_no": "WEB-001",
                "address": "東京都港区1-1-1",
            },
            {
                "name": "佐藤花子",
                "license_no": "B-002",
                "qualification": "溶接士",
                "continuation_status": "継続",
                "print_sheet": "B",
                "expiry_date": pd.Timestamp("2026-05-15"),
                "継続": 1,
                "next_surveillance_window": "2026/04/01〜2026/09/30",
                "web_publish_no": "WEB-002",
                "address": "東京都新宿区2-2-2",
            },
            {
                "name": "田中一郎",
                "license_no": "B-003",
                "qualification": "溶接士",
                "continuation_status": "継続",
                "print_sheet": "B",
                "expiry_date": pd.Timestamp("2026-06-20"),
                "継続": 1,
                "next_surveillance_window": "2026/05/01〜2026/10/31",
                "web_publish_no": "WEB-003",
                "address": "東京都世田谷区3-3-3",
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


def _extract_archive_payload(html: str) -> dict:
    marker = "const archiveBasePayload = "
    idx = html.find(marker)
    if idx == -1:
        raise AssertionError("archive payload marker not found in HTML")
    start = idx + len(marker)
    end = html.find(";", start)
    if end == -1:
        raise AssertionError("archive payload terminator not found")
    snippet = html[start:end].strip()
    return json.loads(snippet)


def test_issue_index_preview_renders(sample_duckdb: Path):
    client = _make_client(sample_duckdb)
    resp = client.get("/issue/")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8")
    assert "山田太郎" in html
    assert "印刷ビューを開く" in html
    assert html.count('name="columns"') >= 2
    assert "資格種別" in html
    assert "継続" in html
    assert "次回ｻｰﾍﾞｲﾗﾝｽ/再評価受験期間" in html
    assert "WEB申込番号" in html
    assert "生年(西暦)" in html


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


def test_issue_archive_endpoint_records(sample_duckdb: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    warehouse_root = tmp_path / "warehouse"
    monkeypatch.setenv("WELDING_WAREHOUSE_ROOT", str(warehouse_root))
    client = _make_client(sample_duckdb)
    resp = client.get("/issue/print", query_string={"sheet": "A"})
    assert resp.status_code == 200
    html = resp.data.decode("utf-8")
    payload = _extract_archive_payload(html)
    payload["printed_at"] = "2025-10-14T09:15:00Z"
    archive_resp = client.post("/issue/archive", json=payload)
    assert archive_resp.status_code == 201
    body = archive_resp.get_json()
    assert body["status"] == "ok"
    assert body["print_id"] == 1

    csv_files = list((warehouse_root / "issue_prints").glob("*.csv"))
    json_files = list((warehouse_root / "issue_prints").glob("*.json"))
    assert len(csv_files) == 1
    assert len(json_files) == 1

    with duckdb.connect(str(sample_duckdb)) as con:
        rows = con.execute("SELECT sheet_label, record_count FROM issue_print_runs").fetchall()
    assert rows == [("A", 1)]


def test_issue_button_records_and_history(sample_duckdb: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    warehouse_root = tmp_path / "warehouse"
    monkeypatch.setenv("WELDING_WAREHOUSE_ROOT", str(warehouse_root))
    client = _make_client(sample_duckdb)

    resp = client.get("/issue/print", query_string={"sheet": "A"})
    payload = _extract_archive_payload(resp.data.decode("utf-8"))

    issue_resp = client.post("/issue/issue", json=payload)
    assert issue_resp.status_code == 201
    issue_body = issue_resp.get_json()
    assert issue_body["status"] == "ok"
    print_id = issue_body["print_id"]
    assert print_id == 1
    assert "print_view_url" in issue_body

    history_resp = client.get("/issue/runs")
    assert history_resp.status_code == 200
    history_html = history_resp.data.decode("utf-8")
    assert f">{print_id}<" in history_html
    assert "発行履歴" in history_html

    run_resp = client.get(f"/issue/runs/{print_id}")
    assert run_resp.status_code == 200
    run_html = run_resp.data.decode("utf-8")
    assert f"発行記録 #{print_id}" in run_html
    assert "PDF保存" not in run_html  # archived view should not show new archive button
