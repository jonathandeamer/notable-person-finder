"""The hash-keyed response cache that carries the recovery guarantee."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

CACHE_FORMAT_VERSION = 1


def cache_key(
    *,
    provider: str,
    method: str,
    url: str,
    body: object,
    transport_profile: str,
    extra: dict[str, object] | None = None,
) -> str:
    """A SHA-256 over everything that can change the response.

    `extra` carries per-provider discriminators -- for model calls, the model
    id and the structured-output schema. Authorization headers and API keys are
    never included: a rotated key must not invalidate the cache, and no secret
    may reach disk.
    """
    material = {
        "version": CACHE_FORMAT_VERSION,
        "provider": provider,
        "method": method.upper(),
        "url": url,
        "body": body,
        "transport_profile": transport_profile,
        "extra": extra or {},
    }
    canonical = json.dumps(
        material, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class Cache:
    """Content-addressed response storage on disk."""

    def __init__(
        self,
        root: Path,
        clock: Callable[[], float] = time.time,
        *,
        ignore_ttl: bool = False,
    ) -> None:
        self._root = root
        self._clock = clock
        # Replay only. TTLs govern production; a recorded fixture stays valid
        # indefinitely, or it decays into a timed test failure.
        self._ignore_ttl = ignore_ttl

    def path_for(self, key: str) -> Path:
        # Shard by the first two hex characters: a year of daily runs would
        # otherwise put tens of thousands of files in one directory.
        return self._root / key[:2] / f"{key}.json"

    def get(self, key: str, *, ttl_seconds: int | None) -> dict[str, Any] | None:
        path = self.path_for(key)
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError:
            return None

        try:
            envelope = json.loads(raw)
            stored_at = float(envelope["stored_at"])
            payload = envelope["payload"]
            if not isinstance(payload, dict):
                raise TypeError("payload is not an object")
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            # A cache is an optimization; it may never be a source of failure.
            # Delete rather than leave a poisoned entry to be re-read forever.
            # A corrupt cache is still a miss if cleanup itself is unavailable;
            # the optimization must never become a failure.
            with contextlib.suppress(OSError):
                path.unlink(missing_ok=True)
            return None

        if (
            not self._ignore_ttl
            and ttl_seconds is not None
            and self._clock() - stored_at > ttl_seconds
        ):
            return None
        return payload

    def put(self, key: str, payload: dict[str, Any]) -> None:
        path = self.path_for(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        envelope = {"stored_at": self._clock(), "payload": payload}
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=path.parent,
                prefix=f"{key}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temporary = Path(handle.name)
                json.dump(envelope, handle, ensure_ascii=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        except BaseException:
            if temporary is not None:
                with contextlib.suppress(OSError):
                    temporary.unlink(missing_ok=True)
            raise
