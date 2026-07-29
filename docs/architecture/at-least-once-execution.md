# At-Least-Once External Execution

**Status:** Current
**Applies to:** the rewrite run engine (`src/notable_person_finder/runs/`)

## The window

Batch execution and each external call run in these steps:

1. `claim_batch` commits up to the worker limit as `running` in one brief
   transaction;
2. for each claimed item, the application thread checks provider pause state
   and runs handler `prepare` with no transaction open;
3. for each item that will actually be called, `start_attempt` reserves budget
   and creates the attempt in a second brief transaction;
4. the external call runs with no database transaction open;
5. a brief transaction stores the attempt's outcome and reconciles its cost
   (`finish_attempt`); and
6. a second brief transaction completes or defers the work item, including
   trusted handler domain persistence (`complete_work`).

Steps 5 and 6 are **two** transactions, not one. There are therefore two
distinct crash windows in which a provider may already have accepted — and
charged for — a request whose result the run never finished recording. They
leave different attempt rows behind, but in both the next ordinary
`notable run` returns the work item to `pending` and performs the request
again.

Because step 1 commits before steps 2 and 3, a process can also die with a
claimed work item but no attempt row. That is a no-call recovery window: the
provider was never contacted, no spend can be duplicated, and the startup
sweep simply returns the claim to `pending`. Deliberate no-call settlements
(for example, a paused provider or a preparation failure) likewise create no
attempt row. There is no separate resume mode and no manual recovery step:
the sweep at the start of every run is the recovery path.

## What the application does and does not promise

The application guarantees **reuse after successful persistence**: once a
result is committed, no later run repeats that call while its fingerprint is
unchanged. It does **not** promise exactly-once execution across a remote API
and a local database. That would require a usable provider idempotency key,
which version one does not assume.

## What a crash looks like on disk

Whatever ends the process — `SIGKILL`, power loss, an unhandled error — the
database is left exactly as its last committed transaction left it. In both
windows the crashed run row is still `running`, with `finished_at` NULL and no
digest recorded, and the work item is still `running` and stamped
`claimed_by_run_id` for that run. The attempt row is what differs.

### Window one: the call was in flight (crash during step 4)

The attempt row exists with `outcome` and `finished_at` NULL.

The next run's sweep turns that into: the run `interrupted`, the attempt
`interrupted`, and the work item back to `pending` with no
`completed_by_run_id`. It returns to `pending` rather than `deferred`
deliberately — a deferral stamped by the crashed run would keep the item out of
the recovering run's own selection, silently converting "retry the interrupted
call" into "skip it".

### Window two: the outcome was stored but the item was never settled (crash between steps 5 and 6)

The attempt row is already `succeeded`, with a `finished_at`, a reconciled
cost, and a request fingerprint. Only the work item is unsettled.

The sweep updates attempts `WHERE outcome IS NULL`, so **this attempt is never
marked `interrupted`**. The work item still returns to `pending` and the call
is still repeated, so nothing is lost — but afterwards the two calls appear in
the `attempt` table as two ordinary `succeeded` rows against the same request
fingerprint, with nothing to distinguish the duplicate from a legitimate
re-evaluation.

In this window the duplicate is traceable **only** through the predecessor
run's `interrupted` state, never through the attempt rows. It is the narrower
of the two windows but the more operationally dangerous one, because it leaves
no marker to search for.

The same sweep also covers unexpected execute failures that leave the same
in-flight shape: a non-`ProviderFailure` escaping the worker propagates out of
the engine, so the run row stays `running` and the next sweep marks it
`interrupted`. Preparation and persistence faults do not normally abort the
run now. An ordinary exception from `prepare` settles only that claimed item
`failed_permanent` before any attempt or call; an exception from handler
`persist` rolls back its domain writes and first settlement, then settles only
that item `failed_permanent` without those writes. An interrupt during the
digest write still exits 130. The observed run-transition sequence across a
process crash and its recovery is `running, interrupted, running, complete`.

## Consequences for the operator

- A crash during a paid generation can cost that generation twice. In window
  one the lost call's attempt keeps its `reserved_nano_usd` but never gets an
  `actual_nano_usd`, and the crashed run's `run.budget_actual_nano_usd` is
  never advanced for it, so that spend is recorded nowhere.
- The spend cap is per run. A crash-and-retry cycle therefore spans two runs
  and can spend up to two runs' worth of the configured limit; the limit bounds
  a single run, not a recovery sequence.
- No manual cleanup is needed. Recovery is another ordinary `notable run`, and
  nothing needs to be deleted or reset by hand first.
- A crash is always visible at the run level, whichever window it fell in.
  `notable status` reports the latest run's state, and the recovering run's
  digest lists the predecessor runs it swept under "Interrupted predecessor
  runs recorded". A run recorded `interrupted` means one call may have been
  paid for twice.
- A crash is visible at the *attempt* level only in window one, where the
  `attempt` table carries `outcome = 'interrupted'` alongside the provider,
  operation, ordinal and request fingerprint. In window two there is no such
  row; start from the `interrupted` run instead and treat its work item's
  repeated attempts as suspect.
- The dated digest is named for both the local date and the run
  (`<date>-run-<id>.md`), so a recovering run never collides with the digest of
  the run it recovered. A crash *during* a digest write can leave a dated file
  on disk for a run that is subsequently recorded `interrupted` with no
  `digest_path`; that file is the crashed run's own artifact and is never
  rewritten.

## Verification

`tests/run_engine/test_crash_boundary.py` covers both windows directly. For
each, it crashes in-process and then repeats the same scenario in a child
process that `SIGKILL`s itself at the same point, and asserts that the two
leave identical durable state — every column of every row in `run`,
`work_item`, `attempt` and `run_transition`. On top of that it pins the two
on-disk shapes described above, the interrupted-attempt record, the repeated
call in the next run, the absence of any `interrupted` attempt row in window
two, and the guarantee that a persisted result is never repeated — including
when the crash happens after some of the run's work has already been
committed.

Both external-call on-disk shapes in this document are asserted by that file.
If a future change fuses steps 5 and 6 into one transaction, window two
disappears and its tests fail; the tests and this document must then be
corrected together.
