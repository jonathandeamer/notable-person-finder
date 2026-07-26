# At-Least-Once External Execution

**Status:** Current
**Applies to:** the rewrite run engine (`src/notable_person_finder/runs/`)

## The window

External work runs in three steps:

1. a brief transaction claims the work item, creates the attempt, and reserves
   budget;
2. the external call runs with no database transaction open; and
3. a brief transaction stores the attempt's outcome and completes or defers the
   work item.

If the process dies between steps 2 and 3, a provider may already have accepted
— and charged for — a request whose response was never stored. The next
ordinary `notable run` marks the abandoned attempt `interrupted`, returns the
work item to `pending`, and performs the request again.

Step 1 is one transaction, so a crash cannot leave a claim without an attempt
row or an attempt row without a claim. There is no separate resume mode and no
manual recovery step: the sweep at the start of every run is the recovery path.

## What the application does and does not promise

The application guarantees **reuse after successful persistence**: once a
result is committed, no later run repeats that call while its fingerprint is
unchanged. It does **not** promise exactly-once execution across a remote API
and a local database. That would require a usable provider idempotency key,
which version one does not assume.

## What a crash looks like on disk

Whatever ends the process — `SIGKILL`, power loss, an unhandled error — the
database is left exactly as its last committed transaction left it:

- the crashed run row is still `running`, with `finished_at` NULL and no
  digest recorded;
- the work item is `running` and stamped `claimed_by_run_id` for that run;
- the attempt row exists with `outcome` and `finished_at` NULL.

The next run's sweep turns that into: the run `interrupted`, the attempt
`interrupted`, and the work item back to `pending` with no
`completed_by_run_id`. It returns to `pending` rather than `deferred`
deliberately — a deferral stamped by the crashed run would keep the item out of
the recovering run's own selection, silently converting "retry the interrupted
call" into "skip it".

The same sweep also covers the aborts that are not process death but leave the
same in-flight shape. A handler that returns a work state which does not settle
the item raises `ValueError` and aborts the whole run rather than isolating the
item; the run row stays `running` and the next sweep marks it `interrupted`.
So does an interrupt during the digest write, which exits 130. The observed
run-transition sequence across a crash and its recovery is
`running, interrupted, running, complete`.

## Consequences for the operator

- A crash during a paid generation can cost that generation twice. The
  interrupted attempt records `reserved_nano_usd` but never an
  `actual_nano_usd`, so the money spent on the lost call is **not** included in
  any run's reported spend.
- The spend cap is per run. A crash-and-retry cycle therefore spans two runs
  and can spend up to two runs' worth of the configured limit; the limit bounds
  a single run, not a recovery sequence.
- No manual cleanup is needed. Recovery is another ordinary `notable run`, and
  nothing needs to be deleted or reset by hand first.
- Interrupted work is always traceable. `notable status` reports the latest
  run's state, an interrupted attempt is recorded in the `attempt` table with
  `outcome = 'interrupted'` alongside its provider, operation, ordinal and
  request fingerprint, and the recovering run's digest lists the predecessor
  runs it swept under "Interrupted predecessor runs recorded".
- The dated digest is named for both the local date and the run
  (`<date>-run-<id>.md`), so a recovering run never collides with the digest of
  the run it recovered. A crash *during* a digest write can leave a dated file
  on disk for a run that is subsequently recorded `interrupted` with no
  `digest_path`; that file is the crashed run's own artifact and is never
  rewritten.

## Verification

`tests/run_engine/test_crash_boundary.py` covers the window directly: a
simulated crash inside step 2, a real `SIGKILL` inside step 2 in a child
process, an assertion that the two leave byte-identical durable state, the
interrupted-attempt record, the repeated call in the next run, and the
guarantee that a persisted result is never repeated — including when the crash
happens after some of the run's work has already been committed.
