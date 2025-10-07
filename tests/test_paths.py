from welding_registry import paths


def test_resolve_duckdb_prefers_bundle(monkeypatch, tmp_path):
    portable = tmp_path / "dist" / "local-app" / "warehouse"
    portable.mkdir(parents=True)
    (portable / "local.duckdb").write_bytes(b"0")
    repo_like = tmp_path / "repo" / "warehouse"
    repo_like.mkdir(parents=True)
    (repo_like / "local.duckdb").write_bytes(b"1")

    monkeypatch.delenv("DUCKDB_DB_PATH", raising=False)
    monkeypatch.setattr(paths, "_candidate_warehouse_dirs", lambda: [portable, repo_like])

    result = paths.resolve_duckdb_path()
    assert result == portable / "local.duckdb"


def test_resolve_duckdb_uses_env_override(monkeypatch, tmp_path):
    target = tmp_path / "custom.duckdb"
    monkeypatch.setenv("DUCKDB_DB_PATH", str(target))
    monkeypatch.setattr(paths, "_candidate_warehouse_dirs", lambda: [])

    result = paths.resolve_duckdb_path()
    assert result == target


def test_resolve_duckdb_falls_back_to_user_dir(monkeypatch, tmp_path):
    monkeypatch.delenv("DUCKDB_DB_PATH", raising=False)
    monkeypatch.setattr(paths, "_candidate_warehouse_dirs", lambda: [tmp_path / "nowhere"])
    monkeypatch.setattr(paths, "_dir_is_writable", lambda _: False)
    monkeypatch.setattr(paths, "_user_data_base", lambda: tmp_path / "userbase")

    result = paths.resolve_duckdb_path()
    assert result == tmp_path / "userbase" / "warehouse" / "local.duckdb"


def test_resolve_review_db_tracks_duckdb(monkeypatch, tmp_path):
    portable = tmp_path / "bundle" / "warehouse"
    portable.mkdir(parents=True)
    (portable / "local.duckdb").write_bytes(b"0")

    monkeypatch.delenv("DUCKDB_DB_PATH", raising=False)
    monkeypatch.setattr(paths, "_candidate_warehouse_dirs", lambda: [portable])

    review = paths.resolve_review_db_path()
    assert review == portable / "review.sqlite"


def test_resolve_review_db_uses_duckdb_env_parent(monkeypatch, tmp_path):
    custom = tmp_path / "bundle" / "db" / "custom.duckdb"
    monkeypatch.setenv("DUCKDB_DB_PATH", str(custom))
    monkeypatch.setattr(paths, "_candidate_warehouse_dirs", lambda: [])

    review = paths.resolve_review_db_path()
    assert review == custom.with_name("review.sqlite")
    assert review.parent.exists()


def test_resolve_review_db_handles_env_directory(monkeypatch, tmp_path):
    env_dir = tmp_path / "portable" / "warehouse"
    env_dir.mkdir(parents=True)
    monkeypatch.setenv("DUCKDB_DB_PATH", str(env_dir))
    monkeypatch.setattr(paths, "_candidate_warehouse_dirs", lambda: [])

    review = paths.resolve_review_db_path()
    assert review == env_dir / "review.sqlite"


def test_resolve_csv_base_creates_directory(monkeypatch, tmp_path):
    portable = tmp_path / "portable" / "warehouse"
    monkeypatch.setattr(paths, "_candidate_warehouse_dirs", lambda: [portable])

    csv_dir = paths.resolve_csv_base()
    assert csv_dir == portable / "csv"
    assert csv_dir.exists()


def test_resolve_csv_base_tracks_duckdb_env(monkeypatch, tmp_path):
    custom = tmp_path / "bundle" / "db" / "custom.duckdb"
    monkeypatch.setenv("DUCKDB_DB_PATH", str(custom))
    monkeypatch.setattr(paths, "_candidate_warehouse_dirs", lambda: [])

    csv_dir = paths.resolve_csv_base()
    assert csv_dir == custom.parent / "csv"
    assert csv_dir.exists()


def test_resolve_review_and_csv_create_dirs_for_duckdb_directory(monkeypatch, tmp_path):
    custom_dir = tmp_path / "bundle" / "warehouse"
    monkeypatch.setenv("DUCKDB_DB_PATH", str(custom_dir))
    monkeypatch.setattr(paths, "_candidate_warehouse_dirs", lambda: [])

    review_path = paths.resolve_review_db_path()
    csv_dir = paths.resolve_csv_base()

    assert review_path == custom_dir / "review.sqlite"
    assert review_path.parent.exists()
    assert csv_dir == custom_dir / "csv"
    assert csv_dir.exists()
