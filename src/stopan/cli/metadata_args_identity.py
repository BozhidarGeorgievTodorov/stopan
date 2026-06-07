from __future__ import annotations

import argparse

from stopan.cli.config_utils import add_config_args


def add_identity_commands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    identity_show_parser = subparsers.add_parser(
        "identity-show",
        allow_abbrev=False,
        help="Muestra el owner_id efectivo desde CLI/config/identity file.",
    )
    add_config_args(identity_show_parser)
    identity_show_parser.add_argument(
        "--owner-id",
        default=None,
        help="Owner ID explícito 64-hex lowercase. Tiene prioridad sobre config y fichero.",
    )
    identity_show_parser.add_argument(
        "--identity-file",
        default=None,
        help="Ruta de identity file. Default: metadata.identity_file.",
    )
