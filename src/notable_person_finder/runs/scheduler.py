from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from types import TracebackType


@dataclass(frozen=True, slots=True)
class Completion[I, R]:
    item: I
    result: R | None
    error: BaseException | None


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
