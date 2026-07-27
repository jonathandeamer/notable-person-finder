from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from types import TracebackType
from typing import cast


@dataclass(frozen=True, slots=True)
class Completion[I, R]:
    item: I
    result: R | None
    error: BaseException | None

    def unwrap(self) -> R:
        """Return the worker's result, or re-raise the failure it recorded.

        `result` is `R | None` because a failed completion carries no result,
        so reading it directly is an unchecked optional access at exactly the
        point a caller is draining results. This is the narrowing the type
        cannot express on its own: `error` and `result` are never both set,
        and never both absent, so a completion without an error necessarily
        carries the `R` its worker returned -- including `None`, when that is
        what `R` is. A failed completion re-raises the worker's own exception
        with its traceback intact, rather than inventing a new one.
        """
        if self.error is not None:
            raise self.error
        return cast(R, self.result)


class BoundedScheduler:
    """Runs independent external calls with a hard in-flight cap.

    Results are yielded on the calling thread. Callers claim work and persist
    results there too, so a worker thread never touches SQLite.
    """

    def __init__(self, max_workers: int) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        self._max_workers = max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    @property
    def max_workers(self) -> int:
        """The hard in-flight cap, so callers size their batches from it.

        Exposed so the run engine claims exactly as much work as it can have
        in flight, instead of carrying a second copy of the operator's
        `concurrency.http_workers` that could drift out of step with the pool.
        """
        return self._max_workers

    def __enter__(self) -> BoundedScheduler:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._executor.shutdown(wait=True)

    def run[I, R](
        self, items: Iterable[I], worker: Callable[[I], R]
    ) -> Iterator[Completion[I, R]]:
        pending: dict[Future[R], I] = {}
        stream = iter(items)
        exhausted = False

        while True:
            while not exhausted and len(pending) < self._max_workers:
                try:
                    item = next(stream)
                except StopIteration:
                    exhausted = True
                    break
                pending[self._executor.submit(worker, item)] = item

            if not pending:
                return

            done, _ = wait(set(pending), return_when=FIRST_COMPLETED)
            for future in done:
                item = pending.pop(future)
                error = future.exception()
                yield Completion(
                    item=item,
                    result=None if error is not None else future.result(),
                    error=error,
                )
