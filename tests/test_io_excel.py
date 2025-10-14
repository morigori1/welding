from __future__ import annotations

import pandas as pd

from welding_registry.io_excel import to_canonical


def test_to_canonical_prefers_alphanumeric_license_column() -> None:
    df = pd.DataFrame(
        {
            "No.": [1, 2, 3],
            "証明番号": ["ME2201498", "AB-1001", "CN-55"],
            "資格": ["A-3FVH", "A-3FV", "CN-F"],
            "資格種類": ["水木", "鋼構", "主要"],
            "氏名": ["甲", "乙", "丙"],
        }
    )
    result = to_canonical(df)
    assert list(result["license_no"]) == ["ME2201498", "AB-1001", "CN-55"]
    assert list(result["row_no"]) == [1, 2, 3]
    assert list(result["category"]) == ["水木", "鋼構", "主要"]


def test_to_canonical_keeps_numeric_license_when_only_candidate() -> None:
    df = pd.DataFrame({"No.": [101, 102], "氏名": ["甲", "乙"]})
    result = to_canonical(df)
    assert "row_no" in result.columns
    assert result["row_no"].tolist() == [101, 102]
    assert "license_no" not in result.columns

def test_to_canonical_maps_extended_fields() -> None:
    df = pd.DataFrame(
        {
            '氏名': ['山田 太郎'],
            '証明番号': ['ME2501234'],
            '資格種類': ['手溶接（アーク）'],
            '資格': ['A-3FVH'],
            '登録年月日': ['2024-04-01'],
            '継続': ['継続済'],
            '次回区分': ['再評価'],
            '次回ｻｰﾍﾞｲﾗﾝｽ/再評価受験期間': ['2025/03/01〜2025/08/31'],
        }
    )
    result = to_canonical(df)
    assert result['license_no'].tolist() == ['ME2501234']
    assert result['continuation_status'].tolist() == ['継続済']
    assert result['category'].tolist() == ['手溶接（アーク）']
    assert result['next_stage_label'].tolist() == ['再評価']
    assert result['next_exam_period'].tolist() == ['2025/03/01〜2025/08/31']
    reg = pd.to_datetime(result['registration_date'][0], errors='raise')
    assert reg.date().isoformat() == '2024-04-01'
