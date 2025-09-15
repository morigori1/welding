from welding_registry.field_map import get_header_map


def test_header_map_loads_yaml_and_builds_variants():
    m = get_header_map()
    # Must at least include canonical tokens like '氏名' and a normalized variant
    assert any(k for k in m.keys() if "氏名" in k)
    # Normalization keeps Unnamed columns lowercased key
    assert m.get("unnamed: 1") is None
