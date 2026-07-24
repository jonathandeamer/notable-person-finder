from __future__ import annotations

import argparse
from collections.abc import Sequence
from typing import NoReturn


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="notable")
    parser.add_argument("--config", type=str, help="path to notable.toml")
    parser.add_argument("--verbose", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)

    config = commands.add_parser("config")
    config.add_subparsers(dest="config_command", required=True).add_parser("validate")
    commands.add_parser("paths")
    db = commands.add_parser("db")
    db.add_subparsers(dest="db_command", required=True).add_parser("migrate")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    build_parser().parse_args(argv)
    return 0


def entrypoint() -> NoReturn:
    raise SystemExit(main())
