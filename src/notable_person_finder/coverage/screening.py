"""Versioned source-policy load, fingerprint, and deterministic URL screening (K8).

Screening is pure once a URL / publisher key and policy are in hand. Discovery
attach decisions (K10) that need only canonicalize + policy live here so unit
tests need no service or network.
"""

from __future__ import annotations

import hashlib
import json
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator

from notable_person_finder.ingestion.urls import (
    UnusableArticleUrl,
    canonicalize_article_url,
    publisher_key,
)

DEFAULT_UNCLASSIFIED_RULE_ID = "default.unclassified"
URL_UNUSABLE_RULE_ID = "url_unusable"

RuleStatus = Literal[
    "curated_eligible",
    "curated_ineligible",
    "unclassified",
    "unusable",
]


class SourcePolicyError(ValueError):
    """Policy file missing, unreadable, or schema-invalid."""


class _StrictPolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PolicyMatch(_StrictPolicyModel):
    """At least one match criterion must be set; first matching rule wins."""

    host_exact: str | None = None
    host_suffix: str | None = None
    path_prefix: str | None = None
    publisher_key_exact: str | None = None

    @model_validator(mode="after")
    def at_least_one_criterion(self) -> PolicyMatch:
        if not any(
            (
                self.host_exact,
                self.host_suffix,
                self.path_prefix,
                self.publisher_key_exact,
            )
        ):
            raise ValueError(
                "match requires host_exact, host_suffix, path_prefix, "
                "or publisher_key_exact"
            )
        return self


class PolicyRule(_StrictPolicyModel):
    id: str = Field(min_length=1, max_length=128)
    status: Literal["curated_eligible", "curated_ineligible"]
    match: PolicyMatch
    rationale: str = Field(min_length=1, max_length=2000)
    review_date: str = Field(min_length=1, max_length=32)
    provenance_url: str | None = Field(default=None, max_length=2000)


class SourcePolicyDocument(_StrictPolicyModel):
    """Validated on-disk policy content (before fingerprint is attached)."""

    schema_version: Literal[1]
    key: str = Field(min_length=1, max_length=64)
    label: str = Field(min_length=1, max_length=200)
    rules: tuple[PolicyRule, ...] = ()

    @model_validator(mode="after")
    def unique_rule_ids(self) -> SourcePolicyDocument:
        ids = [rule.id for rule in self.rules]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate source-policy rule id")
        return self


@dataclass(frozen=True, slots=True)
class SourcePolicy:
    """Loaded policy plus stable content fingerprint (64 lowercase hex)."""

    schema_version: int
    key: str
    label: str
    rules: tuple[PolicyRule, ...]
    fingerprint: str
    path: Path | None = None


@dataclass(frozen=True, slots=True)
class ScreeningDecision:
    """Result of matching one URL against a source policy."""

    rule_id: str
    rule_status: RuleStatus
    publisher_key: str | None
    canonical_url: str | None
    raw_url: str


class DiscoveryAttachKind(StrEnum):
    """K10 pure decision for one discovery source-item URL."""

    NEITHER = "neither"  # empty/missing URL
    SCREENING_ONLY = "screening_only"  # unusable non-empty URL
    DISCOVERY_AND_SCREENING = "discovery_and_screening"  # usable + screened


@dataclass(frozen=True, slots=True)
class DiscoveryAttachDecision:
    kind: DiscoveryAttachKind
    screening: ScreeningDecision | None
    # When kind is DISCOVERY_AND_SCREENING, canonical_url is non-null.
    canonical_url: str | None = None


def load_source_policy(path: Path) -> SourcePolicy:
    """Load and validate a versioned source-policy TOML file."""
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise SourcePolicyError(f"{path}: file does not exist") from error
    except IsADirectoryError as error:
        raise SourcePolicyError(
            f"{path}: expected a TOML file, found a directory"
        ) from error
    except PermissionError as error:
        raise SourcePolicyError(f"{path}: file is not readable") from error
    except UnicodeDecodeError as error:
        raise SourcePolicyError(f"{path}: file is not valid UTF-8") from error
    except OSError as error:
        raise SourcePolicyError(
            f"{path}: could not be read: {error.strerror or error}"
        ) from error

    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        raise SourcePolicyError(f"{path}: invalid TOML") from error

    return source_policy_from_mapping(data, path=path)


def source_policy_from_mapping(
    data: Mapping[str, Any], *, path: Path | None = None
) -> SourcePolicy:
    """Validate mapping content and compute the content fingerprint."""
    try:
        document = SourcePolicyDocument.model_validate(dict(data))
    except Exception as error:
        location = str(path) if path is not None else "<policy>"
        raise SourcePolicyError(f"{location}: {error}") from error

    fingerprint = fingerprint_source_policy_document(document)
    return SourcePolicy(
        schema_version=document.schema_version,
        key=document.key,
        label=document.label,
        rules=document.rules,
        fingerprint=fingerprint,
        path=path,
    )


def fingerprint_source_policy_document(document: SourcePolicyDocument) -> str:
    """SHA-256 of canonical JSON of the validated policy (64 lowercase hex)."""
    payload = document.model_dump(mode="json")
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def fingerprint_source_policy(policy: SourcePolicy) -> str:
    """Return the policy fingerprint (already computed at load)."""
    return policy.fingerprint


def screen_url(
    policy: SourcePolicy,
    *,
    url: str,
    publisher_key_value: str | None = None,
) -> ScreeningDecision:
    """Match a usable URL against the policy (first match wins).

    Callers that already canonicalized may pass ``publisher_key_value`` to
    avoid recomputing. When omitted, this function derives the key from the
    URL via ``canonicalize_article_url`` + ``publisher_key``.

    Unusable URLs receive ``rule_status='unusable'`` and
    ``rule_id='url_unusable'`` without consulting host rules.
    """
    raw = url
    try:
        canonical = canonicalize_article_url(url)
    except UnusableArticleUrl:
        return ScreeningDecision(
            rule_id=URL_UNUSABLE_RULE_ID,
            rule_status="unusable",
            publisher_key=None,
            canonical_url=None,
            raw_url=raw,
        )

    key = (
        publisher_key_value
        if publisher_key_value is not None
        else publisher_key(canonical)
    )
    host = (urlsplit(canonical).hostname or "").lower().rstrip(".")
    path = urlsplit(canonical).path or ""

    for rule in policy.rules:
        if _rule_matches(rule, host=host, path=path, publisher_key_value=key):
            return ScreeningDecision(
                rule_id=rule.id,
                rule_status=rule.status,
                publisher_key=key,
                canonical_url=canonical,
                raw_url=raw,
            )

    return ScreeningDecision(
        rule_id=DEFAULT_UNCLASSIFIED_RULE_ID,
        rule_status="unclassified",
        publisher_key=key,
        canonical_url=canonical,
        raw_url=raw,
    )


def attach_discovery_url(
    policy: SourcePolicy, *, url: str | None
) -> DiscoveryAttachDecision:
    """K10 pure attach decision for one discovery URL (no database I/O).

    - empty/missing URL → neither screening nor discovery row
    - non-empty unusable → screening only (``unusable``)
    - usable → discovery + screening (screening_id NOT NULL path)
    """
    if url is None:
        return DiscoveryAttachDecision(kind=DiscoveryAttachKind.NEITHER, screening=None)
    stripped = url.strip()
    if not stripped:
        return DiscoveryAttachDecision(kind=DiscoveryAttachKind.NEITHER, screening=None)

    decision = screen_url(policy, url=stripped)
    if decision.rule_status == "unusable":
        return DiscoveryAttachDecision(
            kind=DiscoveryAttachKind.SCREENING_ONLY,
            screening=decision,
            canonical_url=None,
        )
    return DiscoveryAttachDecision(
        kind=DiscoveryAttachKind.DISCOVERY_AND_SCREENING,
        screening=decision,
        canonical_url=decision.canonical_url,
    )


def _rule_matches(
    rule: PolicyRule,
    *,
    host: str,
    path: str,
    publisher_key_value: str,
) -> bool:
    match = rule.match
    # All present criteria must match (AND). Absent criteria are ignored.
    if match.host_exact is not None and host != match.host_exact.lower().rstrip("."):
        return False
    if match.host_suffix is not None:
        suffix = match.host_suffix.lower().rstrip(".")
        if host != suffix and not host.endswith("." + suffix):
            return False
    if match.path_prefix is not None and not path.startswith(match.path_prefix):
        return False
    return not (
        match.publisher_key_exact is not None
        and publisher_key_value != match.publisher_key_exact
    )


def rules_in_file_order(policy: SourcePolicy) -> Sequence[PolicyRule]:
    """Expose rule order for tests (first match wins)."""
    return policy.rules
