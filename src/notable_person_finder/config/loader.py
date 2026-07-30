from __future__ import annotations

import hashlib
import json
import os
import tomllib
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dotenv import dotenv_values
from pydantic import ValidationError

from notable_person_finder.config.models import (
    DomainProfileConfig,
    FeedsConfig,
    MainConfig,
)
from notable_person_finder.config.paths import ResolvedPaths, resolve_paths
from notable_person_finder.coverage.screening import (
    SourcePolicy,
    SourcePolicyError,
    load_source_policy,
)


@dataclass(frozen=True, slots=True)
class Credentials:
    openrouter_api_key: str | None
    brave_api_key: str | None


@dataclass(frozen=True, slots=True)
class ResolvedConfig:
    main: MainConfig
    feeds: FeedsConfig
    domain_profile: DomainProfileConfig
    source_policy: SourcePolicy
    credentials: Credentials
    paths: ResolvedPaths
    snapshot_json: str
    fingerprint: str


class ConfigLoadError(Exception):
    def __init__(self, errors: tuple[str, ...]):
        self.errors = errors
        super().__init__("configuration is invalid:\n- " + "\n- ".join(errors))


def load_config(
    config_file: Path | None,
    *,
    environ: Mapping[str, str] | None = None,
    require_secrets: bool = True,
) -> ResolvedConfig:
    initial_paths = resolve_paths(config_file)
    main_path = initial_paths.config_file
    errors: list[str] = []

    main_data = _read_toml(main_path, errors)
    if main_data is None:
        raise ConfigLoadError(tuple(errors))

    main = _validate(MainConfig, main_data, main_path, errors)
    if main is None:
        raise ConfigLoadError(tuple(errors))

    feeds_path = _source_path(main_path.parent, main.feeds_file)
    domain_profile_path = _source_path(main_path.parent, main.domain_profile_file)
    source_policy_path = _source_path(main_path.parent, main.source_policy_file)
    feeds_data = _read_toml(feeds_path, errors)
    domain_profile_data = _read_toml(domain_profile_path, errors)

    feeds = (
        _validate(FeedsConfig, feeds_data, feeds_path, errors)
        if feeds_data is not None
        else None
    )
    domain_profile = (
        _validate(DomainProfileConfig, domain_profile_data, domain_profile_path, errors)
        if domain_profile_data is not None
        else None
    )
    source_policy = _load_source_policy(source_policy_path, errors)

    portable_root = _portable_root(main_path.parent, main.paths.root)
    paths = resolve_paths(main_path, portable_root)
    credentials = _resolve_credentials(
        main, main_path, environ, require_secrets, errors
    )

    if errors:
        raise ConfigLoadError(tuple(errors))

    assert feeds is not None
    assert domain_profile is not None
    assert source_policy is not None
    snapshot = _snapshot(
        main=main,
        feeds=feeds,
        domain_profile=domain_profile,
        source_policy=source_policy,
        paths=paths,
        main_path=main_path,
        feeds_path=feeds_path,
        domain_profile_path=domain_profile_path,
        source_policy_path=source_policy_path,
        secret_availability=_secret_availability(main, credentials),
    )
    snapshot_json = json.dumps(
        snapshot,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return ResolvedConfig(
        main=main,
        feeds=feeds,
        domain_profile=domain_profile,
        source_policy=source_policy,
        credentials=credentials,
        paths=paths,
        snapshot_json=snapshot_json,
        fingerprint=hashlib.sha256(snapshot_json.encode("utf-8")).hexdigest(),
    )


def _load_source_policy(path: Path, errors: list[str]) -> SourcePolicy | None:
    try:
        return load_source_policy(path)
    except SourcePolicyError as error:
        errors.append(str(error))
        return None


def _read_toml(path: Path, errors: list[str]) -> dict[str, Any] | None:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        errors.append(f"{path}: file does not exist")
    except IsADirectoryError:
        errors.append(f"{path}: expected a TOML file, found a directory")
    except NotADirectoryError:
        errors.append(f"{path}: path component is not a directory")
    except PermissionError:
        errors.append(f"{path}: file is not readable")
    except UnicodeDecodeError:
        errors.append(f"{path}: file is not valid UTF-8")
    except OSError as error:
        errors.append(f"{path}: could not be read: {error.strerror or error}")
    except tomllib.TOMLDecodeError:
        errors.append(f"{path}: invalid TOML")
    return None


def _validate[T](
    model_type: type[T],
    data: dict[str, Any],
    path: Path,
    errors: list[str],
) -> T | None:
    try:
        return model_type.model_validate(data)  # type: ignore[attr-defined]
    except ValidationError as error:
        for detail in error.errors(include_input=False):
            location = ".".join(str(part) for part in detail["loc"])
            errors.append(f"{path}: {location}: {detail['msg']}")
    return None


def _source_path(parent: Path, configured_path: Path) -> Path:
    if configured_path.is_absolute():
        return configured_path.expanduser().resolve()
    return (parent / configured_path).resolve()


def _portable_root(parent: Path, configured_root: Path | None) -> Path | None:
    if configured_root is None:
        return None
    return _source_path(parent, configured_root)


def _resolve_credentials(
    main: MainConfig,
    main_path: Path,
    environ: Mapping[str, str] | None,
    require_secrets: bool,
    errors: list[str],
) -> Credentials:
    dotenv = _present_values(dotenv_values(main_path.parent / ".env"))
    process = _present_values(os.environ if environ is None else environ)
    # The process environment wins, but only where it supplies a real value: an
    # exported-but-blank variable must not shadow a usable .env entry.
    merged_environment = {**dotenv, **process}

    openrouter_api_key = merged_environment.get(main.secrets.openrouter_api_key)
    brave_api_key = merged_environment.get(main.secrets.brave_api_key)
    credentials = Credentials(
        openrouter_api_key=openrouter_api_key,
        brave_api_key=brave_api_key,
    )

    if require_secrets:
        for name, value in (
            (main.secrets.openrouter_api_key, credentials.openrouter_api_key),
            (main.secrets.brave_api_key, credentials.brave_api_key),
        ):
            if value is None:
                errors.append(f"missing required secret environment variable {name}")
    return credentials


def _present_values(source: Mapping[str, str | None]) -> dict[str, str]:
    present: dict[str, str] = {}
    for name, value in source.items():
        stripped = _present_value(value)
        if stripped is not None:
            present[name] = stripped
    return present


def _present_value(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _secret_availability(main: MainConfig, credentials: Credentials) -> dict[str, bool]:
    """Represent configured credentials by availability, never by value."""
    return {
        main.secrets.openrouter_api_key: credentials.openrouter_api_key is not None,
        main.secrets.brave_api_key: credentials.brave_api_key is not None,
    }


def _snapshot(
    *,
    main: MainConfig,
    feeds: FeedsConfig,
    domain_profile: DomainProfileConfig,
    source_policy: SourcePolicy,
    paths: ResolvedPaths,
    main_path: Path,
    feeds_path: Path,
    domain_profile_path: Path,
    source_policy_path: Path,
    secret_availability: dict[str, bool],
) -> dict[str, Any]:
    main_settings = main.model_dump(mode="json")
    main_settings["feeds_file"] = str(feeds_path)
    main_settings["domain_profile_file"] = str(domain_profile_path)
    main_settings["source_policy_file"] = str(source_policy_path)
    if main.paths.root is not None:
        main_settings["paths"]["root"] = str(
            _portable_root(main_path.parent, main.paths.root)
        )

    feeds_by_status = {
        "enabled": [
            feed.model_dump(mode="json") for feed in feeds.feeds if feed.enabled
        ],
        "disabled": [
            feed.model_dump(mode="json") for feed in feeds.feeds if not feed.enabled
        ],
    }
    return {
        "main": main_settings,
        "feeds": feeds_by_status,
        "domain_profile": domain_profile.model_dump(mode="json"),
        "source_policy": {
            "schema_version": source_policy.schema_version,
            "key": source_policy.key,
            "label": source_policy.label,
            "fingerprint": source_policy.fingerprint,
            "rule_count": len(source_policy.rules),
        },
        "paths": {name: str(value) for name, value in asdict(paths).items()},
        "source_files": {
            "main": str(main_path),
            "feeds": str(feeds_path),
            "domain_profile": str(domain_profile_path),
            "source_policy": str(source_policy_path),
        },
        "secret_availability": secret_availability,
    }
