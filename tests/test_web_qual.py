from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pytest
import pandas as pd

from welding_registry.webapp import create_app
from welding_registry.db import to_duckdb
from welding_registry.warehouse import (
    add_manual_qualification,
    add_report_definition,
    list_qualifications,
    materialize_roster_all,
)


@pytest.fixture()
def sample_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "qual.duckdb"
    add_manual_qualification(
        db_path,
        name="\u7530\u4e2d \u592a\u90ce",
        license_no="A-001",
        qualification="TN-F",
        registration_date="2025-01-10",
        first_issue_date="2020-04-01",
        issue_date="2023-05-15",
        expiry_date="2026-03-01",
        category="\u624b\u6eb6\u63a5",
        continuation_status="\u7d99\u7d9a",
        next_stage_label="\u66f4\u65b0\u6e96\u5099",
        next_exam_period="2026/01/01〜2026/06/30",
        next_procedure_status="未着手",
        print_sheet="P1",
        source_sheet="Sheet1",
        employee_id="E-001",
        birth_year_west="1985",
        birth_date="1985-03-10",
        address="\u6771\u4eac\u90fd\u6e2f\u533a1-1-1",
        web_publish_no="WEB-100",
    )
    add_manual_qualification(
        db_path,
        name="\u4f50\u85e4 \u82b1\u5b50",
        license_no="A-002",
        qualification="MN-F",
        registration_date="2024-12-20",
        first_issue_date="2021-02-14",
        issue_date="2024-01-20",
        expiry_date="2026-06-20",
        category="\u30b9\u30c6\u30f3\u30ec\u30b9",
        continuation_status="\u8981\u78ba\u8a8d",
        next_stage_label="\u66f4\u65b0\u70b9\u691c",
        next_exam_period="2026/07/01〜2026/12/31",
        next_procedure_status="\u901a\u77e5\u6e08",
        print_sheet="P2",
        source_sheet="Sheet2",
        employee_id="E-002",
        birth_year_west="1990",
        birth_date="1990-07-15",
        address="\u6771\u4eac\u90fd\u65b0\u5bbf\u533a2-2-2",
        web_publish_no="WEB-200",
    )
    add_report_definition(
        db_path,
        report_id="inspection",
        label="\u5b9a\u671f\u691c\u67fb",
        description="\u5e74\u6b21\u70b9\u691c",
    )
    materialize_roster_all(db_path)
    return db_path


def test_qualifications_index_html(sample_db: Path) -> None:
    app = create_app(warehouse=sample_db)
    client = app.test_client()

    resp = client.get("/qualifications/")
    assert resp.status_code == 200
    text = resp.data.decode("utf-8")
    assert "A-001" in text
    assert "TN-F" in text
    assert "\u8cc7\u683c" in text
    assert "\u521d\u56de\u4ea4\u4ed8" in text
    assert "\u5165\u529b\u5143\u30b7\u30fc\u30c8" in text
    assert "\u793e\u54e1ID" in text
    assert "\u5b9a\u671f\u691c\u67fb" in text
    assert "option value=\"inspection\"" in text
    assert "\u30ec\u30dd\u30fc\u30c8\u5b9a\u7fa9\u767b\u9332" in text
    assert "WEB-100" in text
    assert "次回ｻｰﾍﾞｲﾗﾝｽ/再評価受験期間" in text
    assert "生年(西暦)" in text
    assert "生年月日" in text
    assert "東京都港区1-1-1" in text
    assert "header-meta" in text


def test_manual_add_update_delete(sample_db: Path) -> None:
    app = create_app(warehouse=sample_db)
    client = app.test_client()

    payload = {
        "name": "\u9ad8\u6a4b \u4e09\u90ce",
        "license_no": "A-003",
        "qualification": "SC-3F",
        "category": "\u7279\u5225",
        "continuation_status": "\u7d99\u7d9a\u6e08",
        "registration_date": "2026-01-15",
        "first_issue_date": "2022-02-01",
        "issue_date": "2024-04-01",
        "expiry_date": "2026-09-30",
        "next_stage_label": "\u518d\u8a55\u4fa1",
        "next_exam_period": "2026/10/01〜2027/03/31",
        "next_procedure_status": "\u6848\u5185\u4e2d",
        "print_sheet": "P3",
        "source_sheet": "Manual",
        "employee_id": "E-777",
        "birth_year_west": "1978",
        "birth_date": "1978-04-04",
        "address": "\u5927\u962a\u5e9c\u5927\u962a\u5e021-2-3",
        "web_publish_no": "WEB-777",
    }
    resp = client.post("/qualifications/manual", json=payload)
    assert resp.status_code == 200
    df = list_qualifications(sample_db)
    assert "A-003" in df["license_no"].tolist()
    row = df[df["license_no"] == "A-003"].iloc[0]
    assert row.get("source_sheet") == "Manual"
    assert row.get("category") == "\u7279\u5225"
    assert row.get("continuation_status") == "\u7d99\u7d9a\u6e08"
    assert row.get("registration_date") == "2026-01-15"
    assert row.get("next_stage_label") == "\u518d\u8a55\u4fa1"
    assert row.get("next_exam_period") == "2026/10/01〜2027/03/31"
    assert row.get("next_procedure_status") == "\u6848\u5185\u4e2d"
    assert row.get("employee_id") == "E-777"
    assert row.get("birth_year_west") == "1978"
    assert str(row.get("birth_date")) == "1978-04-04"
    assert row.get("address") == "大阪府大阪市1-2-3"
    assert row.get("web_publish_no") == "WEB-777"

    resp = client.post(
        "/qualifications/manual",
        json={
            "mode": "update",
            "name": "\u9ad8\u6a4b \u4e09\u90ce",
            "license_no": "A-003",
            "qualification": "SC-3V",
            "category": "\u66f4\u65b0\u5f8c\u30ab\u30c6\u30b4\u30ea",
            "continuation_status": "\u505c\u6b62",
            "registration_date": "2026-02-01",
            "next_stage_label": "\u7279\u5225\u5bfe\u5fdc",
        "next_exam_period": "2027/01/01〜2027/04/30",
        "next_procedure_status": "\u5b8c\u4e86",
        "employee_id": "E-888",
        "birth_year_west": "1979",
        "birth_date": "1979-05-05",
        "source_sheet": "Manual",
        "address": "大阪府堺市2-3-4",
        "web_publish_no": "WEB-888",
        },
    )
    assert resp.status_code == 200
    df = list_qualifications(sample_db)
    row = df[df["license_no"] == "A-003"].iloc[0]
    assert row["qualification"] == "SC-3V"
    assert row.get("category") == "\u66f4\u65b0\u5f8c\u30ab\u30c6\u30b4\u30ea"
    assert row.get("continuation_status") == "\u505c\u6b62"
    assert row.get("registration_date") == "2026-02-01"
    assert row.get("next_stage_label") == "\u7279\u5225\u5bfe\u5fdc"
    assert row.get("next_exam_period") == "2027/01/01〜2027/04/30"
    assert row.get("next_procedure_status") == "\u5b8c\u4e86"
    assert row.get("employee_id") == "E-888"
    assert row.get("birth_year_west") == "1979"
    assert str(row.get("birth_date")) == "1979-05-05"
    assert row.get("address") == "大阪府堺市2-3-4"
    assert row.get("web_publish_no") == "WEB-888"

    resp = client.post("/qualifications/manual/A-003/delete", json={"name": "\u9ad8\u6a4b \u4e09\u90ce"})
    assert resp.status_code == 200
    df = list_qualifications(sample_db)
    assert "A-003" not in df["license_no"].tolist()

def test_manual_print_sheet_update_preserves_dates(sample_db: Path) -> None:
    app = create_app(warehouse=sample_db)
    client = app.test_client()

    client.post(
        "/qualifications/manual",
        data={
            "name": "\u4f50\u85e4 \u592a\u90ce",
            "license_no": "M-001",
            "qualification": "JIS Z 3801",
            "registration_date": "2024-05-01",
            "first_issue_date": "2020-04-01",
            "issue_date": "2023-04-01",
            "expiry_date": "2026-04-30",
        },
    )
    resp = client.post(
        "/qualifications/manual",
        data={
            "mode": "update",
            "name": "\u4f50\u85e4 \u592a\u90ce",
            "license_no": "M-001",
            "print_sheet": "P5",
            "registration_date": "",
            "first_issue_date": "",
            "issue_date": "",
            "expiry_date": "",
        },
    )
    assert resp.status_code in (200, 303)
    df = list_qualifications(sample_db)
    row = df[df["license_no"] == "M-001"].iloc[0]
    assert row.get("registration_date") == "2024-05-01"
    assert row.get("first_issue_date") == "2020-04-01"
    assert row.get("issue_date") == "2023-04-01"
    assert row.get("expiry_date") == "2026-04-30"
    assert row.get("print_sheet") == "P5"


def test_report_registration(sample_db: Path) -> None:
    app = create_app(warehouse=sample_db)
    client = app.test_client()

    resp = client.post("/qualifications/report", json={"report_id": "inspection", "license_no": "A-001"})
    assert resp.status_code == 200
    df = list_qualifications(sample_db)
    row = df[df["license_no"] == "A-001"].iloc[0]
    assert row.get("report_ids")
    assert "inspection" in row.get("report_ids")

    resp = client.post("/qualifications/report/inspection/A-001/delete", json={})
    assert resp.status_code == 200
    df = list_qualifications(sample_db)
    row = df[df["license_no"] == "A-001"].iloc[0]
    assert not row.get("report_ids")




def test_report_definition_crud(sample_db: Path) -> None:
    app = create_app(warehouse=sample_db)
    client = app.test_client()

    resp = client.post("/qualifications/report/definitions", json={"report_id": "safety", "label": "\u5b89\u5168\u70b9\u691c", "description": "\u5916\u89b3\u30c1\u30a7\u30c3\u30af"})
    assert resp.status_code == 200

    index = client.get("/qualifications/")
    text = index.data.decode("utf-8")
    assert "\u5b89\u5168\u70b9\u691c" in text
    assert "\u5916\u89b3\u30c1\u30a7\u30c3\u30af" in text

    resp = client.post("/qualifications/report", json={"report_id": "safety", "license_no": "A-001"})
    assert resp.status_code == 200
    df = list_qualifications(sample_db)
    row = df[df["license_no"] == "A-001"].iloc[0]
    assert "safety" in row.get("report_ids")

    resp = client.post("/qualifications/report/definitions/safety/delete", json={})
    assert resp.status_code == 200
    df = list_qualifications(sample_db)
    row = df[df["license_no"] == "A-001"].iloc[0]
    assert not row.get("report_ids")
    index = client.get("/qualifications/")
    assert "\u5b89\u5168\u70b9\u691c" not in index.data.decode("utf-8")


def test_manual_update_converts_ingest(sample_db: Path) -> None:
    app = create_app(warehouse=sample_db)
    client = app.test_client()

    resp = client.post(
        "/qualifications/manual",
        json={"mode": "update", "name": "\u7530\u4e2d \u592a\u90ce", "license_no": "A-001", "qualification": "TN-V", "source_sheet": "Sheet1"},
    )
    assert resp.status_code == 200
    df = list_qualifications(sample_db)
    row = df[df["license_no"] == "A-001"].iloc[0]
    assert row["qualification"] == "TN-V"
    assert row.get("source") == "manual"
    assert row.get("source_sheet") == "Sheet1"
    assert row.get("registration_date") == "2025-01-10"


def test_column_toggle_and_sort(sample_db: Path) -> None:
    app = create_app(warehouse=sample_db)
    client = app.test_client()

    client.post(
        "/qualifications/manual",
        json={
            "name": "\u5c71\u672c \u4eac\u5b50",
            "license_no": "A-010",
            "qualification": "SC-1F",
            "category": "\u66f4\u65b0\u5bfe\u8c61",
            "expiry_date": "2026-03-01",
            "print_sheet": "P4",
        },
    )

    params = [
        ("columns", "name"),
        ("columns", "license_no"),
        ("columns", "expiry_date"),
        ("columns", "category"),
        ("sort", "expiry_date"),
        ("order", "desc"),
        ("sort", "license_no"),
        ("order", "asc"),
    ]
    resp = client.get("/qualifications/", query_string=params)
    assert resp.status_code == 200
    text = resp.data.decode("utf-8")
    assert "<th>\u5370\u5237\u30b7\u30fc\u30c8</th>" not in text
    assert "<th>\u6709\u52b9\u671f\u9650</th>" in text
    assert 'value="name" checked' in text
    assert 'value="category" checked' in text
    assert 'value="qualification" checked' not in text
    assert text.count('name="sort"') >= 3
    assert '\u512a\u51481' in text and '\u512a\u51482' in text and '\u512a\u51483' in text
    assert 'option value="expiry_date" selected' in text
    assert 'option value="license_no" selected' in text
    assert 'option value="desc" selected' in text
    assert 'option value="asc" selected' in text
    assert text.find("A-002") < text.find("A-001") < text.find("A-010")


def test_manual_add_preserves_ingest_dates(tmp_path: Path) -> None:
    db_path = tmp_path / "manual_preserve.duckdb"
    ingest_df = pd.DataFrame(
        {
            "name": ["\u4e2d\u5ddd \u9686\u53f2"],
            "license_no": ["UE2300957"],
            "qualification": ["TN-F"],
            "registration_date": ["2024-01-01"],
            "expiry_date": ["2026-12-31"],
            "print_sheet": [""],
            "source_sheet": ["Sheet1"],
        }
    )
    to_duckdb(ingest_df, db_path, table="roster")
    materialize_roster_all(db_path)

    app = create_app(warehouse=db_path)
    client = app.test_client()

    resp = client.post(
        "/qualifications/manual",
        data={
            "name": "\u4e2d\u5ddd \u9686\u53f2",
            "license_no": "UE2300957",
            "qualification": "TN-F",
            "print_sheet": "P1",
            "mode": "add",
        },
    )
    assert resp.status_code in (200, 303)

    roster = list_qualifications(db_path)
    row = roster.loc[roster["license_no"] == "UE2300957"].iloc[0]
    assert row["source"] == "ingest"
    assert row["sheet_source"] == "manual"
    assert row["registration_date"] == "2024-01-01"
    assert row["expiry_date"] == "2026-12-31"


def test_excel_import_populates_roster(tmp_path: Path) -> None:
    db_path = tmp_path / "import.duckdb"
    app = create_app(warehouse=db_path)
    client = app.test_client()

    df = pd.DataFrame(
        {
            "No.": [1],
            "\u6c0f\u540d": ["\u5c71\u7530 \u592a\u90ce"],
            "\u767b\u9332\u756a\u53f7": ["ME2500100"],
            "\u8cc7\u683c": ["A-3FV"],
            "\u8cc7\u683c\u7a2e\u985e": ["\u624b\u6eb6\u63a5"],
            "\u7d99\u7d9a": ["\u7d99\u7d9a"],
            "\u767b\u9332\u5e74\u6708\u65e5": ["2024-04-01"],
            "\u6709\u52b9\u5e74\u6708\u65e5": ["2026-03-31"],
            "\u6b21\u56de\uff7b\uff70\uff8d\uff9e\uff72\uff97\uff9d\uff7d/\u518d\u8a55\u4fa1\u53d7\u9a13\u671f\u9593": ["2025/03/01\u301c2025/08/31"],
        }
    )
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    buffer.seek(0)

    data = {
        "excel": (buffer, "roster.xlsx"),
        "sheet": "Sheet1",
    }
    resp = client.post(
        "/qualifications/import",
        data=data,
        content_type="multipart/form-data",
    )
    assert resp.status_code == 303

    roster = list_qualifications(db_path)
    assert "ME2500100" in roster["license_no"].tolist()
    row = roster.loc[roster["license_no"] == "ME2500100"].iloc[0]
    assert row["name"] == "\u5c71\u7530\u592a\u90ce"
    assert "2025/03/01" in row["next_exam_period"]
    assert "2025/08/31" in row["next_exam_period"]
    assert row["source_sheet"] == "Sheet1"
    assert row["print_sheet"] == "default"
    assert row["source"] == "ingest"

    df2 = pd.DataFrame(
        {
            "No.": [1],
            "\u6c0f\u540d": ["\u5c71\u7530 \u592a\u90ce"],
            "\u767b\u9332\u756a\u53f7": ["ME2500100"],
            "\u8cc7\u683c": ["A-3FV"],
        }
    )
    buffer2 = BytesIO()
    with pd.ExcelWriter(buffer2, engine="openpyxl") as writer:
        df2.to_excel(writer, index=False, sheet_name="Sheet1")
    buffer2.seek(0)

    resp = client.post(
        "/qualifications/import",
        data={"excel": (buffer2, "roster_blank.xlsx"), "sheet": "Sheet1"},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 303

    roster_after = list_qualifications(db_path)
    row_after = roster_after.loc[roster_after["license_no"] == "ME2500100"].iloc[0]
    assert row_after["registration_date"] == row["registration_date"]
    assert row_after["expiry_date"] == row["expiry_date"]


