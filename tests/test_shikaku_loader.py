import duckdb
import pandas as pd
import pytest

from welding_registry.shikaku_loader import (
    CANONICAL_COLUMN_MAP,
    detect_shikaku_workbook,
    load_shikaku_workbook,
)


REQUIRED_COLUMNS = list(CANONICAL_COLUMN_MAP.keys())


@pytest.fixture()
def sample_shikaku_xlsx(tmp_path):
    rows = [
        {
            "No.": 1,
            "証明番号": "ME0001",
            "資格": "A-3F",
            "資格種類": "鋼材（アーク）",
            "登録年月日": "2022/04/01",
            "継続": 2,
            "有効期限": "2025/05/31",
            "氏名": "田中 太郎",
            "生年月日": "1985/03/15",
            "自宅住所": "東京都新宿区1-1-1",
            "勤務先": "第一工業 溶接部",
            "受験申請した溶接協会": "溶接学会 立会試験",
            "次回区分": "立会試験不要",
            "次回\uff7b\uff70\uff8d\uff9e\uff72\uff97\uff9d\uff7d/\n再評価受験期間": "2025/01/01～2025/06/30",
            "次回手続き状況": "案内待ち",
            "WEB申込番号": "WEB-0001",
        },
        {
            "No.": 2,
            "証明番号": "ME0002",
            "資格": "N-2F",
            "資格種類": "鋼材（アーク）",
            "登録年月日": "2023/06/01",
            "継続": 1,
            "有効期限": "2026/07/31",
            "氏名": "佐藤 花子",
            "生年月日": "1990/11/02",
            "自宅住所": "大阪府大阪市2-2-2",
            "勤務先": "第二工業 溶接部",
            "受験申請した溶接協会": "JIS認証機関",
            "次回区分": "立会試験必要",
            "次回\uff7b\uff70\uff8d\uff9e\uff72\uff97\uff9d\uff7d/\n再評価受験期間": "2026/02/01～2026/07/31",
            "次回手続き状況": "",
            "WEB申込番号": "",
        },
    ]
    df = pd.DataFrame(rows, columns=REQUIRED_COLUMNS)
    excel_path = tmp_path / "資格一覧.xlsx"
    df.to_excel(excel_path, index=False)
    return excel_path


def test_detect_shikaku_workbook(sample_shikaku_xlsx):
    assert detect_shikaku_workbook(sample_shikaku_xlsx)


def test_load_shikaku_workbook_creates_schema(tmp_path, sample_shikaku_xlsx):
    db_path = tmp_path / "local.duckdb"
    out_dir = tmp_path / "out"

    summary = load_shikaku_workbook(
        sample_shikaku_xlsx,
        duckdb_path=db_path,
        out_dir=out_dir,
    )
    assert summary.row_count == 2
    assert (out_dir / "shikaku_canonical.csv").exists()

    con = duckdb.connect(str(db_path))
    try:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        assert {"stg_shikaku_raw", "dim_person", "dim_license", "fact_qualification"} <= tables

        df_person = con.execute("SELECT * FROM dim_person ORDER BY display_name").df()
        assert list(df_person["display_name"]) == ["佐藤 花子", "田中 太郎"]

        df_fact = con.execute(
            """
            SELECT license_no, qualification, next_exam_start, next_exam_end
            FROM vw_due_schedule
            ORDER BY license_no
            """
        ).df()
        assert list(df_fact["license_no"]) == ["ME0001", "ME0002"]
        assert pd.notna(df_fact.loc[0, "next_exam_start"])
        assert pd.notna(df_fact.loc[0, "next_exam_end"])
    finally:
        con.close()
