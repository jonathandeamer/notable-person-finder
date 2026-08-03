"""Argument parsing and command dispatch."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import httpx

from notable import __version__
from notable.cache import Cache
from notable.config import load_config
from notable.http import Transport
from notable.llm import LlmClient
from notable.pipeline import Providers, RunResult, run
from notable.store import Store


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="notable",
        description="Find inspectable Wikipedia biography leads from RSS feeds.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)
    run_cmd = commands.add_parser("run", help="Fetch feeds and write a digest.")
    run_cmd.add_argument(
        "--fresh-feeds",
        action="store_true",
        help="Bypass the feed cache and refetch every feed.",
    )
    run_cmd.add_argument(
        "--config",
        default="config/mvp.local.toml",
        help="Path to the configuration file.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.command != "run":
        parser.error(f"unknown command: {args.command}")
        return 2

    try:
        config = load_config(Path(args.config))
    except ValueError as error:
        print(f"configuration error: {error}")
        return 2

    store = Store(config.data_dir / "notable.db")
    try:
        with httpx.Client(follow_redirects=True) as client:
            transport = Transport(
                config.transport, Cache(config.cache.dir), client=client
            )
            providers = Providers(
                transport=transport,
                llm=LlmClient(
                    transport,
                    config.openrouter,
                    api_key=config.openrouter_api_key,
                    budget_usd=config.budget_usd,
                ),
            )
            result = run(config, store, providers, fresh_feeds=args.fresh_feeds)
            report = _report(providers, result)
    finally:
        store.close()

    print(result.digest_path.read_text(encoding="utf-8"))
    print(report)
    return 0


def _report(providers: Providers, result: RunResult) -> str:
    """What the run actually did, in numbers.

    The live gate has to measure model calls, truncations, validation
    failures, cache hits and misses, retries, 429s and real spend. None of
    those are recoverable by grepping run output afterwards: a recovered 429
    raises nothing and logs nothing, and a cache-file count is not a spend
    figure.

    Every number comes from this run's in-memory state. Reading them back from
    the `run` table would make a log whose writes may silently fail into the
    authority on what happened, so a failed log write would print the previous
    run's figures as though they were this one's.
    """
    stats = providers.transport.stats
    summary = result.summary
    return "\n".join(
        [
            "--- run report ---",
            f"status:            {summary.status}",
            f"items settled:     {len(summary.settled)}",
            f"items incomplete:  {len(summary.incomplete)}",
            # "stopped at cap", not "capped": a run *has* a cap configured
            # (always, for the live run) and separately may or may not have
            # *hit* it. Conflating the two makes the fixture gate ambiguous.
            f"stopped at cap:    {summary.capped}",
            f"model calls:       {providers.llm.calls}",
            f"truncated:         {providers.llm.truncations}",
            f"cache hits/misses: {providers.transport.cache_hits}/"
            f"{providers.transport.cache_misses}",
            f"http attempts:     {stats.attempts} ({stats.retries} retries)",
            f"rate limited:      {stats.rate_limited}",
            f"measured spend:    ${summary.cost_usd}",
            f"digest:            {result.digest_path}",
        ]
    )
