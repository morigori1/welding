from welding_registry.normalize import license_key, name_key


def test_license_key_normalizes_formatting():
    assert license_key(" ab-123  45 ") == "AB12345"
    assert license_key("ａｂ１２３") == "AB123"


def test_name_key_collapses_spaces_and_width():
    assert name_key(" 山田  太郎 ") == "山田太郎"
    assert name_key("ﾔﾏﾀﾞ  ﾀﾛｳ") == "ヤマダタロウ"
