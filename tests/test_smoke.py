from pathlib import Path


def test_repo_has_data_folder():
    assert Path("data").exists()
