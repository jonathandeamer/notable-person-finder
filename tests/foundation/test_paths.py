from pathlib import Path

from notable_person_finder.config.paths import resolve_paths


def test_portable_root_contains_all_mutable_paths(tmp_path: Path) -> None:
    config_file = tmp_path / "settings" / "notable.toml"
    paths = resolve_paths(config_file=config_file, portable_root=tmp_path / "portable")

    assert paths.config_file == config_file.resolve()
    assert paths.data_root == (tmp_path / "portable" / "data").resolve()
    assert paths.database == paths.data_root / "notable.sqlite3"
    assert paths.backups == paths.data_root / "backups"
    assert paths.digests == paths.data_root / "digests"
    assert paths.log_file == (tmp_path / "portable" / "logs" / "notable.jsonl").resolve()
    assert paths.cache_root == (tmp_path / "portable" / "cache").resolve()
    assert paths.lock_file == paths.data_root / "notable.lock"


def test_default_paths_do_not_depend_on_current_directory(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    paths = resolve_paths(config_file=None)

    assert paths.config_file.name == "notable.toml"
    assert tmp_path not in paths.config_file.parents
    assert tmp_path not in paths.data_root.parents
