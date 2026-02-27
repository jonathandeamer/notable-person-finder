# Troubleshooting

## Codex model errors

Error:
`model is not supported when using Codex with a ChatGPT account`

Fix:
Use a Codex-supported model or omit `--model` to use the default. The current default in
`scripts/llm_gate1_runner.py` is `gpt-5.2`.

## Invalid schema error in batch mode

Error:
`schema must be a JSON Schema of type "object", got "array"`

Fix:
Batch output must be wrapped in an object with `items`. This is already enforced in
`scripts/llm_gate1_runner.py`.

## Empty or missing outputs

Possible causes:
- Codex stream disconnects
- API key not set (OpenAI API backend)

Fix:
Retry. The script now retries `codex exec` failures (default 3 attempts).

## MediaWiki candidates returns zero records

Likely cause:
Input has no usable `subject_name_full`. Ensure your Gate 1 results include a parsed output with
`subject_name_full`. The script now falls back to `parsed_output` automatically.

## MediaWiki run appears stuck

Likely cause:
Each record may fetch up to `--search-max-results` page details, and throttling can make a single
record take minutes.

Checks:
- Watch progress output (e.g., `progress: 5/30`).
- Monitor output growth: `wc -l /Users/jonathan/new-wikipedia-article-checker/state/wiki_candidates.jsonl`
- Use the request log: `tail -f /Users/jonathan/new-wikipedia-article-checker/state/mw_candidates.log`

Mitigations:
- Lower `--search-max-results` or `--srlimit`.
- Reduce `--throttle-ms` carefully if you need faster throughput.

## Too many search results

Fix:
Use the hard cap `--search-max-results` and/or lower `--srlimit`.

Example:
```bash
--search-max-results 10 --srlimit 10
```

## HTTP 403 / DNS / bad JSON (MediaWiki)

Likely cause:
Missing or blocked User-Agent, or network/DNS issues.

Fix:
- Set an explicit User-Agent (Wikipedia expects contact info):
```bash
--user-agent "WikiNotabilityFinder/0.1 (https://en.wikipedia.org/wiki/User:Jonathan_Deamer; bot)"
```
- Capture request/response errors with `--log-file`.

## Gate 2 has-page filter is too aggressive

Likely cause:
Exact title match + biography heuristics flagged a clear page match.

Fix:
Inspect `state/wiki_candidates_skip.jsonl` and adjust similarity thresholds or rules in
`scripts/det_gate2_has_page.py` if false positives appear.

## Gate 2 has-page filter is too conservative

Likely cause:
Strict exact title match and similarity check prevented skipping.

Fix:
Inspect `state/wiki_candidates_pass.jsonl` and consider adjusting Levenshtein threshold or
allowing redirects or other signals.

## Gate 4b not marking someone as likely notable?

Gate 4b now requires coverage on at least two distinct reliable Brave domains before promoting `LIKELY_NOTABLE`. If you see `POSSIBLY_NOTABLE` instead:

- Inspect `state/gate4b_llm_results.jsonl` for the `first_pass_domains`/`second_pass_domains` lists and the recorded domain count.
- Ensure `state/gate4_reliable_coverage.jsonl` collected enough distinct domains (cut down on duplicate sources or config missing reliable domains).
- The digest (`scripts/daily_notability_digest_report.py`) surfaces these domain counts at a glance.
