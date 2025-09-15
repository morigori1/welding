from pathlib import Path
from welding_registry.licenses import _from_text


def test_from_text_extracts_dates_and_license():
    text = """
    資格名称: JIS 半自動溶接
    登録番号: AB-12345
    交付日: R6.09.01  有効期限: 2028/09/01
    有効期間: 2024/09/01〜2028/09/01
    """
    rec = _from_text(text, Path("dummy.pdf"))
    assert rec is not None
    assert rec["license_no"] == "AB-12345"
    assert str(rec["issue_date"]).startswith("2024-09-01") or str(rec["issue_date"]).startswith(
        "2024-09-01"
    )
    assert str(rec["expiry_date"]).startswith("2028-09-01")
