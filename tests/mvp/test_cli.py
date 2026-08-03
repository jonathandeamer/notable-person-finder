import pytest

from notable import __version__
from notable.cli import build_parser, main


def test_version_flag_reports_the_package_version(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["--version"])
    assert exit_info.value.code == 0
    assert capsys.readouterr().out.strip() == __version__


def test_run_accepts_fresh_feeds_flag():
    args = build_parser().parse_args(["run", "--fresh-feeds"])
    assert args.command == "run"
    assert args.fresh_feeds is True


def test_no_command_is_an_error():
    with pytest.raises(SystemExit) as exit_info:
        build_parser().parse_args([])
    assert exit_info.value.code == 2
