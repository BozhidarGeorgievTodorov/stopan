from __future__ import annotations

import argparse
from collections.abc import Sequence

from stopan.cli.config_utils import add_config_args
from stopan.cli.metadata_args_graph import add_graph_group
from stopan.cli.metadata_args_identity import add_identity_commands
from stopan.cli.metadata_args_pack import add_pack_group
from stopan.cli.metadata_validation import validate_metadata_args


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stopan metadata",
        allow_abbrev=False,
        description="Gestiona el metadata vault cifrado local.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    _add_status_command(subparsers)
    add_identity_commands(subparsers)
    add_graph_group(subparsers)
    add_pack_group(subparsers)

    args = parser.parse_args(argv)
    validate_metadata_args(parser, args)
    return args


def _add_status_command(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    status_parser = subparsers.add_parser(
        "status",
        allow_abbrev=False,
        help="Muestra configuración efectiva y estado operativo del metadata vault.",
    )
    add_config_args(status_parser)
