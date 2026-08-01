"""Deterministic per-person lead outcome aggregation (K7, K8, K9).

No model call: every branch reads fields milestone 5's
``person_article_assessment``/``article_assessment_signal`` already produce.
"""

from __future__ import annotations

from dataclasses import dataclass

LeadOutcomeName = str  # one of the four CHECK-constrained outcome strings


@dataclass(frozen=True, slots=True)
class ArticleEvidence:
    person_article_assessment_id: int
    domain: str
    canonical_domain: str
    same_person: bool
    coverage_depth: str | None  # 'significant' | 'passing' | 'uncertain' | None
    content_qualifying: bool
    editorially_independent: bool
    screening_status: str  # 'curated_eligible' | 'curated_ineligible' | 'unclassified'


@dataclass(frozen=True, slots=True)
class SignalEvidence:
    article_assessment_signal_id: int
    signal_kind: str  # 'attention' | 'caution'
    category: str
    transferable: bool = False


@dataclass(frozen=True, slots=True)
class LeadOutcome:
    outcome: LeadOutcomeName
    qualifying_domain_count: int
    qualifying_articles: tuple[ArticleEvidence, ...]
    contributing_signals: tuple[SignalEvidence, ...]
    wikipedia_outcome: str | None
    incompleteness_reason: str | None = None


def _is_qualifying(article: ArticleEvidence) -> bool:
    return (
        article.same_person
        and article.coverage_depth == "significant"
        and article.screening_status == "curated_eligible"
        and article.editorially_independent
        and article.content_qualifying
    )


def _qualifying_domains(articles: list[ArticleEvidence]) -> set[str]:
    return {a.canonical_domain for a in articles if _is_qualifying(a)}


def _has_useful_possible_reason(
    articles: list[ArticleEvidence], signals: list[SignalEvidence]
) -> bool:
    for article in articles:
        if (
            article.same_person
            and article.coverage_depth == "significant"
            and article.screening_status == "unclassified"
        ):
            return True
        if (
            article.same_person
            and article.coverage_depth in ("significant", "passing")
            and article.content_qualifying
            and not _is_qualifying(article)
        ):
            return True
    for signal in signals:
        if signal.signal_kind == "attention" and signal.transferable:
            return True
    return False


def aggregate_lead(
    *,
    articles: list[ArticleEvidence],
    signals: list[SignalEvidence],
    wikipedia_outcome: str | None,
    promising_domain_threshold: int,
    assessment_terminal: bool = True,
) -> LeadOutcome:
    qualifying_domains = _qualifying_domains(articles)
    if len(qualifying_domains) >= promising_domain_threshold:
        qualifying = tuple(a for a in articles if _is_qualifying(a))
        return LeadOutcome(
            outcome="promising_lead",
            qualifying_domain_count=len(qualifying_domains),
            qualifying_articles=qualifying,
            contributing_signals=(),
            wikipedia_outcome=wikipedia_outcome,
        )
    if len(qualifying_domains) >= 1 or _has_useful_possible_reason(articles, signals):
        qualifying = tuple(a for a in articles if _is_qualifying(a))
        contributing = tuple(
            s for s in signals if s.signal_kind == "attention" and s.transferable
        )
        return LeadOutcome(
            outcome="possible_lead",
            qualifying_domain_count=len(qualifying_domains),
            qualifying_articles=qualifying,
            contributing_signals=contributing,
            wikipedia_outcome=wikipedia_outcome,
        )
    if assessment_terminal:
        return LeadOutcome(
            outcome="insufficient_evidence",
            qualifying_domain_count=0,
            qualifying_articles=(),
            contributing_signals=(),
            wikipedia_outcome=wikipedia_outcome,
        )
    return LeadOutcome(
        outcome="assessment_incomplete",
        qualifying_domain_count=0,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome=wikipedia_outcome,
        incompleteness_reason="required_coverage_work_not_settled",
    )
