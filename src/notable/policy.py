"""Publisher policy: TOML rules -> eligible/ineligible/unclassified, plus
same-host canonicalization for Phase 4's two-domain qualifying threshold."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

_ScreeningStatus = Literal["curated_eligible", "curated_ineligible", "unclassified"]


@dataclass(frozen=True, slots=True)
class _Rule:
    host_suffix: str
    status: Literal["curated_eligible", "curated_ineligible"]


@cache
def _rules(path: Path) -> tuple[_Rule, ...]:
    parsed = tomllib.loads(path.read_text(encoding="utf-8"))
    return tuple(
        _Rule(host_suffix=rule["match"]["host_suffix"], status=rule["status"])
        for rule in parsed["rules"]
    )


def canonical_domain(url: str) -> str:
    """The bare host, `www.` stripped -- what "the same publisher" means for
    the two-distinct-domain qualifying threshold."""
    return urlsplit(url).netloc.lower().removeprefix("www.")


def classify(url: str, policy_path: Path) -> _ScreeningStatus:
    """First-match-wins over `host_suffix` in file order. No match ->
    unclassified. `host_suffix` is the only match form the ported policy
    file uses; other forms are not implemented until something needs them.
    """
    host = canonical_domain(url)
    for rule in _rules(policy_path):
        if host == rule.host_suffix or host.endswith("." + rule.host_suffix):
            return rule.status
    return "unclassified"
