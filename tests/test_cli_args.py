import argparse
from welding_registry.__main__ import build_parser


def test_ingest_has_expiry_args():
    parser = build_parser()
    # Find the subparser for 'ingest'
    sub_actions = [a for a in parser._actions if isinstance(a, argparse._SubParsersAction)]
    assert sub_actions, "no subparsers action found"
    ingest_parser = sub_actions[0].choices.get("ingest")
    assert ingest_parser is not None, "ingest subcommand missing"
    help_text = ingest_parser.format_help()
    assert "--expiry-from" in help_text
    assert "--valid-years" in help_text
    assert "--header-row" in help_text
