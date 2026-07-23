# Visual Arts Discovery Profile and Evidence Policy

**Status:** Agreed direction; detailed implementation design still in progress  
**Date:** 2026-07-23

## Purpose

The initial redesign is an English-language visual-arts pilot. It monitors a deliberately narrow set of arts publications for people who may be missing English Wikipedia biographies, then searches broadly for corroborating reliable coverage.

The discovery profile controls where the system pays attention. It does not limit where corroborating evidence may come from and does not make every article from a monitored publisher valid notability evidence.

## Discovery Sources

The profile contains eight distinct publishers. The duplicate Artforum entry in the original proposal is represented once, and the Telegraph entry is its art section rather than an individual review.

| Publisher | Section | Discovery method |
| --- | --- | --- |
| The Art Newspaper | [Homepage](https://www.theartnewspaper.com/) | [RSS](https://www.theartnewspaper.com/rss.xml) |
| ARTnews | [Homepage](https://www.artnews.com/) | [RSS](https://www.artnews.com/feed/) |
| Artnet News | [Homepage](https://news.artnet.com/) | [RSS](https://news.artnet.com/feed) |
| Artforum | [Homepage](https://www.artforum.com/) | [RSS](https://www.artforum.com/feed/) |
| The New York Times | [Art & Design](https://www.nytimes.com/international/section/arts/design) | [RSS](https://rss.nytimes.com/services/xml/rss/nyt/ArtandDesign.xml) |
| The Guardian | [Art & Design](https://www.theguardian.com/artanddesign) | [RSS](https://www.theguardian.com/artanddesign/rss) |
| ArtReview | [Homepage](https://artreview.com/) | [RSS](https://artreview.com/rss.xml) |
| The Telegraph | [Art](https://www.telegraph.co.uk/art/) | [RSS](https://www.telegraph.co.uk/art/rss.xml) |

RSS is the discovery mechanism for the initial profile because it provides a stable title, excerpt, publication date, and canonical link without scraping article pages. The application will not add publisher-specific HTML scrapers or newsletter-ingestion subsystems for the initial rewrite.

Discovery-source definitions will live in the versioned configuration file `config/discovery_profiles/art.toml`. The application core will not contain publisher-specific conditionals.

## Discovery and Evidence Are Separate

A monitored source supplies candidate events. It does not receive automatic evidentiary weight merely because an editor selected it for discovery.

An article from a discovery publisher may count as evidence only when it passes the same checks as any corroborating result:

- it is substantially about the candidate rather than a passing mention;
- it is editorial rather than sponsored, partner, advertorial, gallery, or press-release content;
- it provides meaningful biographical or career coverage rather than a listing or announcement;
- it is independent of the subject and of the other counted coverage;
- the publisher has either curated reliable status or an explicitly provisional model assessment.

Brave research may find corroboration from any publisher, including arts sources outside the discovery profile and general reliable publications.

## Evidence Reliability Tiers

### Curated reliable

The publisher domain appears in `config/reliable_sources.toml`, a checked-in and versioned policy file. Curated status is deterministic and reviewable; it is not embedded in Python constants.

Curated publisher status is necessary but not sufficient. Each result must still pass subject-focus, significance, independence, and content-type checks.

### Model-assessed provisional

An unlisted publisher may be assessed by the configured LLM task as editorially reliable for the purpose of triage. The assessment records:

- the publisher and canonical domain;
- the specific result being considered;
- the recommendation and rationale;
- the model, resolved provider, prompt version, and request parameters;
- confidence, timestamp, and complete attempt provenance.

The assessment is visibly provisional. It does not silently modify the curated reliable-source configuration and cannot produce the strongest recommendation tier.

## Recommendation Outcomes

The coverage threshold is configurable. Its initial default is two independent, significant sources.

- **`likely_notable`:** the threshold is met using only curated reliable sources.
- **`possibly_notable`:** the threshold is met only after including one or more model-assessed provisional sources.
- **`uncertain`:** the available evidence has unresolved identity, reliability, independence, or model/provider uncertainty.
- **`not_enough_evidence`:** the evaluated coverage does not meet the configured threshold.

These are attention-routing outcomes, not Wikipedia notability decisions. `Likely_notable` means that a candidate deserves stronger editor attention; it does not assert that the subject satisfies a particular guideline.

## Independence and Duplication

Different domains do not automatically constitute independent coverage. The system attempts to identify:

- syndicated wire copies;
- press-release rewrites;
- substantially identical headlines and excerpts;
- articles citing or summarizing the same original report;
- publisher aliases and regional subdomains.

When multiple results derive from the same underlying reporting, they form one evidence cluster. Thresholds count independent clusters, not raw URLs or domains.

The system preserves the individual links and explains the grouping so a human can review or override it.

## Access Boundaries

- The application does not circumvent paywalls, authentication, robots controls, or anti-bot systems.
- Feed metadata and legally accessible excerpts may be used for discovery and triage.
- A result records whether the model received a headline, feed excerpt, search snippet, accessible article text, or some combination.
- The model cannot claim to have read content that was not supplied.
- Inaccessible source URLs remain available in the digest for human review.
- Sponsored, partner, advertising, and clearly promotional content is retained for audit when encountered but does not count toward coverage thresholds.

## Scope Consequences

The profile improves the initial redesign by providing a coherent domain for prompts, Promptfoo cases, and acceptance tests. It also makes domain-specific failure modes explicit: exhibition listings, fair publicity, gallery announcements, auction snippets, group shows, and repeated press releases can resemble biographical coverage without providing significant independent sourcing.

The profile is intentionally not a general benchmark for knowledge equity or global biography discovery. It is the first editor-selected attention profile. The architecture keeps source profiles configurable so later experiments can monitor different topics, regions, languages, or communities without changing the core workflow.
