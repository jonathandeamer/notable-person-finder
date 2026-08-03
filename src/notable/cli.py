"""Argument parsing and command dispatch."""

from __future__ import annotations

import argparse

from notable import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="notable",
        description="Find inspectable Wikipedia biography leads from RSS feeds.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="Fetch feeds and write a digest.")
    run.add_argument(
        "--fresh-feeds",
        action="store_true",
        help="Bypass the feed cache and refetch every feed.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "run":
        raise SystemExit("notable run is not implemented until Phase 1")
    parser.error(f"unknown command: {args.command}")
    return 2
