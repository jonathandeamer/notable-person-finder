# Visual Arts Discovery Profile and Evidence Policy

**Status:** Agreed direction; detailed implementation design still in progress  
**Date:** 2026-07-24

## Purpose

The initial redesign is an English-language visual-arts pilot. It monitors a deliberately narrow set of arts publications for people who may be missing English Wikipedia biographies, then searches broadly for corroborating reliable coverage.

The discovery profile controls where the system pays attention. It does not limit where corroborating evidence may come from and does not make every article from a monitored publisher valid notability evidence.

## Discovery Sources

The profile contains ten distinct publishers. The duplicate Artforum entry in the original proposal is represented once, and the Telegraph entry is its art section rather than an individual review.

| Publisher | Section | Discovery method | Last verified |
| --- | --- | --- | --- |
| The Art Newspaper | [Homepage](https://www.theartnewspaper.com/) | [RSS](https://www.theartnewspaper.com/rss.xml) | 2026-07-24 |
| ARTnews | [Homepage](https://www.artnews.com/) | [RSS](https://www.artnews.com/feed/) | 2026-07-24 |
| Artnet News | [Homepage](https://news.artnet.com/) | [RSS](https://news.artnet.com/feed) | 2026-07-24 |
| Artforum | [Homepage](https://www.artforum.com/) | [RSS](https://www.artforum.com/feed/) | 2026-07-24 |
| The New York Times | [Art & Design](https://www.nytimes.com/international/section/arts/design) | [RSS](https://rss.nytimes.com/services/xml/rss/nyt/ArtandDesign.xml) | 2026-07-24 |
| The Guardian | [Art & Design](https://www.theguardian.com/artanddesign) | [RSS](https://www.theguardian.com/artanddesign/rss) | 2026-07-24 |
| ArtReview | [Homepage](https://artreview.com/) | [RSS](https://artreview.com/rss.xml) | 2026-07-24 |
| The Telegraph | [Art](https://www.telegraph.co.uk/art/) | [RSS](https://www.telegraph.co.uk/art/rss.xml) | 2026-07-24 |
| BBC News | [Art](https://www.bbc.com/news/topics/cjnwl8q4gjnt) | [RSS](https://feeds.bbci.co.uk/news/topics/cjnwl8q4gjnt/rss.xml) | 2026-07-24 |
| Hyperallergic | [Homepage](https://hyperallergic.com/) | [RSS](https://hyperallergic.com/feed/) | 2026-07-24 |

RSS is the discovery mechanism for the initial profile because it provides a stable title, excerpt, publication date, and canonical link without scraping article pages. The application will not add publisher-specific HTML scrapers or newsletter-ingestion subsystems for the initial rewrite.

Discovery-source definitions will live in the versioned configuration file `config/discovery_profiles/art.toml`. The application core will not contain publisher-specific conditionals.

### Selection rationale

BBC News Art adds general-interest and local reporting that specialist international publications may not cover. This increases the chance of finding people outside the most prominent art-market and institutional networks. Its feed also contains video, community, and event stories, so ordinary subject-focus and significance filtering still applies.

Hyperallergic is an established specialist publication focused on contemporary art, criticism, cultural policy, and underrepresented perspectives. It broadens the profile beyond the more market- and institution-oriented publications already selected.

The ten-source limit is deliberate. It provides a varied but coherent pilot corpus for behavior review, Promptfoo cases, and acceptance testing without turning the first redesign into a general-purpose ingestion project.

## Deferred Discovery Sources

These publications are relevant enough to reconsider, but are not part of the initial profile:

| Publisher | Reason deferred | Revisit when |
| --- | --- | --- |
| Frieze | No accessible official RSS feed was found; the site combines editorial publishing with art-fair and promotional coverage. | A stable official feed or another low-maintenance, permitted discovery channel becomes available. |
| Apollo | Its expected WordPress RSS endpoint returned HTTP 500 when checked. | The official feed becomes consistently available. |
| Artsy Editorial | A working news feed exists, but Artsy combines editorial publishing with a commercial marketplace. | The behavior review defines reliable separation of editorial and commercial content. |
| Museums Journal | A working news feed exists, but its focus is museum institutions and professionals rather than artists. | Museum directors, curators, conservators, and heritage professionals are explicitly added to the pilot scope. |
| Ocula | Likely feed endpoints rejected automated access, and the publication operates alongside a commercial gallery platform. | A permitted stable feed is available and the editorial/commercial boundary is defined. |
| [AP Visual Arts](https://apnews.com/hub/visual-arts) | As checked on 2026-07-24, the current section does not advertise a public RSS or Atom feed. AP's [documented RSS delivery](https://api.ap.org/media/v/swagger/) is an authenticated product tied to an entitled plan. | AP publishes a stable public section feed suitable for personal monitoring. |
| [Reuters](https://www.reuters.com/news/picture/world-of-art-idUSRTR29A4D/) | As checked on 2026-07-24, the proposed “World of Art” URL is an individual photo feature rather than a maintained arts section. [Reuters Ready](https://reutersagency.com/content/content-types/reuters-ready/) advertises licensed content feeds, but no public arts RSS feed was found. | Reuters publishes a stable public arts feed suitable for the discovery profile. |

Deferred sources remain candidates for future editor-selected profiles. The application will not scrape them or add special-case code merely to expand the initial source count.

## Discovery and Evidence Are Separate

A monitored source supplies candidate events. It does not receive automatic evidentiary weight merely because an editor selected it for discovery.

An article from a discovery publisher may count as evidence only when it passes the same checks as any corroborating result:

- it is substantially about the candidate rather than a passing mention;
- it is editorial rather than sponsored, partner, advertorial, gallery, or press-release content;
- it provides meaningful biographical or career coverage rather than a listing or announcement;
- it is independent of the subject rather than self-published or affiliated;
- the publisher is curated eligible or is retained as an explicitly
  unclassified lead for human review.

Brave research may find corroboration from any publisher, including arts sources outside the discovery profile and general reliable publications.

## Publisher Screening States

### Curated eligible

The publisher appears in a small, versioned local policy with its rationale,
decision basis, source URL, and review date. Curated status is deterministic
and reviewable. The domain-design session will decide whether the policy is
represented as configuration, seed data, or database records.

Curated eligibility is necessary for a result to become a non-provisional
coverage lead, but it is not sufficient. Each result must still pass
subject-focus, significance, subject-independence, and content-type checks.

### Curated ineligible

Obvious poor-fit publishers and content channels, including social media,
user-generated content, press-release distribution, and clear promotion, are
retained with a deterministic rejection reason and are not fetched for model
assessment.

### Unclassified

An unlisted or contextually complicated publisher remains unclassified. A
capped number may be retrieved as fallback leads, but a model cannot promote
the publisher to “Wikipedia reliable.” Repeatedly useful publishers can be
researched and curated manually.

The [Wikipedia Reliable Sources/Perennial Sources page](https://en.wikipedia.org/wiki/Wikipedia:Reliable_sources/Perennial_sources)
is dated provenance for individual local decisions, not a runtime allowlist or
policy engine. Complex caveats remain unclassified. Updates are explicit,
manual, and independently versioned.

## Assessment Boundary

Coverage discovery returns classified articles, unresolved items, and
search-completeness metadata. It does not emit notability recommendations.
Configurable recommendation policy belongs to the subsequent assessment
capability.

The system deduplicates identical canonical URLs only. It does not infer
cross-source syndication clusters or claim that a numeric result count proves
independent sourcing. The human reviewer can judge relationships among the
presented coverage leads.

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
