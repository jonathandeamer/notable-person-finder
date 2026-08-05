# Durable Person Identity Design

## 1. The Problem
Currently, the pipeline treats every detected mention of a person as an independent entity unless their raw string exactly matches (post-normalization). The identity key is derived directly from the raw string detected in the feed (e.g., `francisco correa cordero`, `dr charles hood`). While `rank.identity_key()` already handles NFKC, casefolding, and whitespace normalization to fix smart-quotes and spacing, structural variations still cause major issues:

1. **In-Run Redundant Effort:** A person mentioned across 7 different feed items in a single run is researched 7 separate times. The system redundantly runs expensive Wikipedia matching, Brave Search, and LLM ranking for all of them.
2. **Fragile Keys (Honorifics):** While punctuation is handled, prefixes like "Dr.", "Sir", or "Lord" (`dr charles hood` vs `charles hood`) create permanently distinct identities for the exact same person.
3. **Compound Subjects:** "Jen-Hsun and Lori Huang" is extracted as a single entity. Neither individual will correctly match future mentions of their actual solo names.
4. **Single-Name Ambiguity:** Mentions like "Obama", "Trump", or "Kandinsky" leak to the digest because their Wikipedia search returns disambiguation pages instead of their actual biography. Without their full names, the matching LLM answers `uncertain`, bypassing Phase 2 safety nets.

## 2. Architectural Goals
- **Pragmatism over Enterprise Machinery:** We want 90% of the value of a durable entity registry without the cost, complexity, or irreversible failure modes of an LLM semantic merge-judge. 
- **Fix Data at the Source:** Push normalization (splitting couples, expanding names) into the Phase 1 detection prompt where it is cheapest and easiest.
- **Trivial In-Run Deduplication:** Do not process the same canonical identity twice in the same pipeline run.
- **Global Caching:** Remember the Wikipedia research outcomes for canonical identities across runs so we don't spend API budget repeatedly researching the same person.

## 3. The Solution: MVP-Shaped Deduplication

### 3.1 Phase 1 Enhancement: Name Canonicalization
The first and most powerful fix is cleaning the data before it even enters the database.
We will update the `detect_people` structured output schema to separate `raw_mention` (the exact text in the article) from a new `canonical_name` field.
The prompt will explicitly instruct the LLM to populate `canonical_name` by:
- Stripping honorifics and titles (e.g. Dr., Sir, Lord).
- Splitting compound subjects into distinct detection objects (e.g., "Jen-Hsun Huang" and "Lori Huang").
- Expanding obvious single names using article context (e.g., expanding "Obama" to "Barack Obama").

**The Grounding Invariant:** `validate_detection` will continue to strictly enforce case-sensitive, verbatim grounding *only* against `raw_mention`. Because rules like single-name expansion fundamentally break string-grounding, `canonical_name` will be left unvalidated (trusted from the LLM).
**Blast Radius:** `canonical_name` replaces `exact_name` as the input to Wikipedia search (`wiki.py:51`) and the `identity_key` generator (`rank.py:76`). This carries a massive bonus: Wikipedia and Brave search queries immediately become much higher quality.

### 3.2 Trivial In-Run Deduplication (pipeline.py)
To solve the "7 mentions in 1 run" problem, we need zero new database infrastructure. 
Inside `pipeline.py`, we will maintain a simple in-memory dictionary keyed by `identity_key(canonical_name)`. 
When iterating over the detected mentions from the feeds, if an identity key has already been processed in the current run, we **merge their `identity_facts` and `signals`**. 

This is the real win: accumulating `identity_facts` across articles perfectly solves the single-name ambiguity problem before hitting Wikipedia. (Note: `supporting_passage_ids` are item-scoped and cannot be cleanly merged, but the final `Lead` object can simply preserve a list of all source URLs).

### 3.3 The `research_cache` Table (Global Deduplication)
Instead of a complex semantic entity registry, we will introduce a simple, dumb cache table in `notable.db` to act as our cross-run memory.

**Table `research_cache`:**
- `identity_key` (TEXT PRIMARY KEY): The normalized identity key.
- `wikipedia_verdict` (TEXT): The conclusive outcome.
- `outcome_json` (TEXT): A serialized record of the Phase 2 research results (e.g., the URL of the Wikipedia page).
- `researched_at` (TEXT): Timestamp.

**Caching Rules & Staleness Policy:**
- **Only Cache Conclusive Verdicts:** We ONLY cache `matching_page` and `no_matching_page`. `uncertain` verdicts or outcomes from `Incomplete` feed items are explicitly dead ends and are never cached. Freezing a transient truncation error must be avoided.
- **Always Re-run Coverage:** The cache is *strictly* for the Phase 2 Wikipedia verdict. We never cache Phase 3 (Coverage Research) or Phase 4 (Rank). A person with no Wikipedia page must be re-evaluated against fresh news so that a small story today doesn't permanently suppress a major story about them a month from now.

**Integration:**
- **Read:** Before executing Phase 2 for a canonical identity, check `research_cache`. If a definitive verdict exists, skip the MediaWiki API and use the cached result.
- **Write:** Writes to `research_cache` will strictly follow `store.py`'s existing transactional discipline. State will land in one transaction *after* the digest is written, ensuring a run crashing mid-loop does not leave a half-updated cache.

## 4. Unaddressed & Deferred Items
- **Semantic LLM Merging:** Deferred. Two distinct people sharing a name might incorrectly share a cache entry, but at hobby scale, the cost of occasional false-merges is acceptable. If data eventually shows "same-person-different-name" (e.g. Jensen vs Jen-Hsun) happening frequently, we can run a manual weekly database review or implement semantic merging later.
- **Schema Migration:** No complex migration is needed for existing `notable.db` data. `identity_key` remains structurally identical. The `research_cache` table is purely additive and can be created with `CREATE TABLE IF NOT EXISTS`.
