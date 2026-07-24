from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from platformdirs import user_cache_path, user_config_path, user_data_path, user_log_path

APP_NAME = "notable-person-finder"


@dataclass(frozen=True, slots=True)
class ResolvedPaths:
    config_file: Path
    data_root: Path
    database: Path
    backups: Path
    digests: Path
    log_file: Path
    cache_root: Path
    lock_file: Path


def default_config_file() -> Path:
    return user_config_path(APP_NAME, appauthor=False) / "notable.toml"


def resolve_paths(
    config_file: Path | None,
    portable_root: Path | None = None,
) -> ResolvedPaths:
    selected_config = (config_file or default_config_file()).expanduser().resolve()
    if portable_root is None:
        data_root = user_data_path(APP_NAME, appauthor=False).resolve()
        log_root = user_log_path(APP_NAME, appauthor=False).resolve()
        cache_root = user_cache_path(APP_NAME, appauthor=False).resolve()
    else:
        root = portable_root.expanduser().resolve()
        data_root = root / "data"
        log_root = root / "logs"
        cache_root = root / "cache"

    return ResolvedPaths(
        config_file=selected_config,
        data_root=data_root,
        database=data_root / "notable.sqlite3",
        backups=data_root / "backups",
        digests=data_root / "digests",
        log_file=log_root / "notable.jsonl",
        cache_root=cache_root,
        lock_file=data_root / "notable.lock",
    )
