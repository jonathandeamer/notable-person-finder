# SDD ledger — plan: docs/superpowers/plans/2026-07-30-coverage-evidence.md

**Design:** `docs/superpowers/specs/2026-07-30-coverage-evidence-design.md` (revision 4)  
**Branch:** `feat/coverage-evidence`  
**Worktree:** `.worktrees/feat-coverage-evidence`

## Task status

| Task | Name | Status |
| --- | --- | --- |
| 1 | Migration 0007 + repository | complete |
| 2 | Brave WebSearchClient | complete |
| 3 | ArticleFetcher + Trafilatura | complete |
| 4 | Policy, selection, queries, eligibility | complete |
| 5 | PassageSelector + assess contract + config | complete |
| 6a | Plan lifecycle + brave_web_search | complete |
| 6b | fetch_article + views | complete |
| 7 | assess_article + multi-model inspect | complete |
| 8 | Seed, supersede, merge | pending |
| 9 | Digest/status/CLI + gate | pending |

## Ledger lines

(appended as tasks complete)

## Mutation evidence

(record at Task 9: rule → mutation → killer test name)

Task 1: fix round 1/5 (3 addressed, 0 open — current_assessment FK, K10 discovery test isolation, completed CHECK matrix; commits 41079c3..4fcbad0)
Task 1: complete (commits 6080b48..4fcbad0, review clean)

Task 2: fix round 1/5 (2 addressed, 0 open — provider_result_id + endpoint wiring tests; commits d4b1927..b71df3c)
Task 2: complete (commits 4fcbad0..b71df3c, review clean)

Task 3: complete (commit ab7776d — ArticleFetcher + Trafilatura extractor; 22 tests; mutation evidence in task-3-report.md)
Task 3: fix round 1/5 (2 Important addressed, 0 open — 410 not_found + HTML-ish content-type accept tests; commit d1f27ad)

Task 3: fix round 1/5 (2 addressed, 0 open; commits ab7776d..d1f27ad)
Task 3: complete (commits b71df3c..d1f27ad, review clean)

Task 4: complete — screening/selection/queries/eligibility pure modules;
source_policy_file required on MainConfig; K5 matrix + K10/K34 unit tests;
mutation evidence in task-4-report.md

Task 4: fix round 1/5 (2 addressed, 0 open; commits 7f31e9c..66b524d)
Task 4: complete (commits d1f27ad..66b524d, review clean)

Task 5: fix round 1/5 (2 addressed, 0 open; commits af7f8bd..618b856)
Task 5: complete (commits 66b524d..618b856, review clean)

Task 6a: complete — plan open (K10 discovery + exact forms), brave_web_search
handler (one search_web / UNIQUE attempt), offset semantics, maybe_advance
stages + T1–T11 terminalize; CLI registers Brave only (fetch/assess later);
mutation evidence in task-6a-report.md
Task 6a: fix round 1/5 (3 Important addressed — positive alias/context gate,
fetched-without-assess terminalize block, expanded mutation evidence;
commit 34b0d1a)

Task 6a: fix round 1/5 (3 addressed, 0 open; commits f6f7a5f..34b0d1a)
Task 6a: complete (commits 618b856..34b0d1a, review clean)

Task 6b: complete — build_fetch_article_handler (one GET + in-process extract,
K3 HTML ban), article_view full/partial/snippets, K28 snippets fallback, K31
person_article, schedule_assess_article when view ready; CLI registers fetch;
mutation evidence in task-6b-report.md
Task 6b: fix round 1/5 (1 Important addressed — permanent-failure K28 split
tests with-snippets vs no-text; mutation-killed)

Task 6b: fix round 1/5 (1 addressed, 0 open; commits a714305..8ade6e8)
Task 6b: complete (commits 34b0d1a..8ade6e8, review clean)

Task 7: complete — build_assess_article_handler (PassageSelector + validate,
K25 completed-only pointer), assess_model_needed + mid-run ensure (K20),
permanent preflight settler for assess_article (K23); 10 new tests;
mutation evidence in task-7-report.md; gate 214 passed
