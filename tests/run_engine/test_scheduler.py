from __future__ import annotations

import threading
from collections.abc import Iterable, Iterator

import pytest

from notable_person_finder.runs.scheduler import BoundedScheduler, Completion


def ok_results[I](completions: Iterable[Completion[I, int]]) -> list[int]:
    """Extract results from completions already known to have succeeded.

    `Completion.result` is `int | None` in general -- `None` on a failed
    item -- so pulling it out unconditionally would be exactly the kind of
    unchecked optional access this checker is right to catch. Asserting
    `error is None` up front documents and enforces the invariant these
    tests actually rely on: every completion here succeeded, so its result
    is present.
    """
    values: list[int] = []
    for completion in completions:
        assert completion.error is None
        assert completion.result is not None
        values.append(completion.result)
    return values


def test_all_items_are_processed_and_results_returned() -> None:
    with BoundedScheduler(max_workers=3) as scheduler:
        completions = list(scheduler.run(range(5), lambda value: value * 2))
    assert sorted(ok_results(completions)) == [0, 2, 4, 6, 8]


def test_completion_carries_the_originating_item() -> None:
    with BoundedScheduler(max_workers=2) as scheduler:
        completions = list(scheduler.run(["a", "b"], str.upper))
    assert {(c.item, c.result) for c in completions} == {("a", "A"), ("b", "B")}


def test_in_flight_work_never_exceeds_the_worker_limit() -> None:
    # `run()` is lazy: nothing is submitted until the returned iterator is
    # pulled. Setting `release` from the calling thread before consuming the
    # iterator (as opposed to synchronizing on actual worker starts) would let
    # every worker sail through `release.wait()` immediately, so `peak` would
    # never climb above 1 and the assertion would pass for a fully
    # serializing scheduler too. A `Barrier` forces exactly `max_workers`
    # workers to be in flight simultaneously before anything is released,
    # so the test proves the cap is both reached and never exceeded.
    in_flight = 0
    peak = 0
    guard = threading.Lock()
    started = threading.Barrier(2)
    both_started = threading.Event()
    release = threading.Event()

    def worker(value: int) -> int:
        nonlocal in_flight, peak
        with guard:
            in_flight += 1
            peak = max(peak, in_flight)
        started.wait(timeout=2)
        both_started.set()
        release.wait(timeout=2)
        with guard:
            in_flight -= 1
        return value

    completions: list[Completion[int, int]] = []
    with BoundedScheduler(max_workers=2) as scheduler:
        consumer = threading.Thread(
            target=lambda: completions.extend(scheduler.run(range(10), worker))
        )
        consumer.start()
        assert both_started.wait(timeout=2)
        release.set()
        consumer.join(timeout=5)

    assert not consumer.is_alive()
    assert peak == 2
    assert len(completions) == 10


def test_submission_stays_bounded_while_more_input_remains() -> None:
    # `ThreadPoolExecutor(max_workers=N)` caps concurrent *execution* for
    # free, even if every item were submitted eagerly up front -- that alone
    # does not prove `run()` only pulls from `items` lazily, bounded by the
    # cap. Count how many items the source iterable has yielded and check
    # that count right after the very first completion is drained: an eager
    # implementation (e.g. `{executor.submit(worker, i): i for i in
    # list(items)}`) would already have pulled every item by then, so this
    # discriminates a bound on submission, not just on concurrency.
    pulled = 0
    guard = threading.Lock()

    def counting_items() -> Iterator[int]:
        nonlocal pulled
        for value in range(1_000):
            with guard:
                pulled += 1
            yield value

    with BoundedScheduler(max_workers=2) as scheduler:
        completions = scheduler.run(counting_items(), lambda value: value)
        next(completions)
        with guard:
            observed = pulled

    assert observed <= 2


def test_max_workers_below_one_is_rejected() -> None:
    with pytest.raises(ValueError):
        BoundedScheduler(max_workers=0)


def test_early_abandonment_still_shuts_the_pool_down() -> None:
    # Breaking out of a `for` loop (or any other early exit) abandons the
    # `run()` generator with futures still pending. The context manager's
    # `__exit__` must still join every worker thread the pool created --
    # otherwise a caller that stops iterating early leaks threads for the
    # life of the process.
    baseline = {thread.ident for thread in threading.enumerate()}

    def worker(value: int) -> int:
        return value

    with BoundedScheduler(max_workers=3) as scheduler:
        for completion in scheduler.run(range(20), worker):
            assert completion.error is None
            break

    leaked = {thread.ident for thread in threading.enumerate()} - baseline
    assert leaked == set()


def test_a_worker_failure_is_returned_not_raised() -> None:
    def worker(value: int) -> int:
        if value == 2:
            raise ValueError("bad item")
        return value

    with BoundedScheduler(max_workers=2) as scheduler:
        completions = list(scheduler.run(range(4), worker))

    failures = [c for c in completions if c.error is not None]
    assert len(failures) == 1
    assert isinstance(failures[0].error, ValueError)
    successes = [c for c in completions if c.error is None]
    assert sorted(ok_results(successes)) == [0, 1, 3]


def test_one_failure_does_not_cancel_unrelated_work() -> None:
    def worker(value: int) -> int:
        if value % 2 == 0:
            raise RuntimeError("even values fail")
        return value

    with BoundedScheduler(max_workers=4) as scheduler:
        completions = list(scheduler.run(range(10), worker))
    assert len(completions) == 10
    successes = [c for c in completions if c.error is None]
    assert sorted(ok_results(successes)) == [1, 3, 5, 7, 9]


def test_results_are_yielded_on_the_calling_thread() -> None:
    caller = threading.get_ident()
    observed: list[int] = []

    with BoundedScheduler(max_workers=3) as scheduler:
        for _completion in scheduler.run(range(6), lambda value: value):
            observed.append(threading.get_ident())
    assert set(observed) == {caller}


def test_workers_run_off_the_calling_thread() -> None:
    caller = threading.get_ident()
    worker_threads: set[int] = set()

    def worker(value: int) -> int:
        worker_threads.add(threading.get_ident())
        return value

    with BoundedScheduler(max_workers=2) as scheduler:
        list(scheduler.run(range(6), worker))
    assert caller not in worker_threads


def test_an_empty_item_stream_completes_immediately() -> None:
    with BoundedScheduler(max_workers=2) as scheduler:
        assert list(scheduler.run([], lambda value: value)) == []


def test_single_worker_serializes_work() -> None:
    order: list[int] = []
    with BoundedScheduler(max_workers=1) as scheduler:
        list(scheduler.run(range(4), lambda value: order.append(value) or value))
    assert order == [0, 1, 2, 3]
