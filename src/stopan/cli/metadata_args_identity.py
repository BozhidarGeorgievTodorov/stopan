from __future__ import annotations

import argparse

from stopan.cli.config_utils import add_config_args
from stopan.cli.metadata_args_common import add_scrypt_override_args


def add_identity_commands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    identity_create_parser = subparsers.add_parser(
        "identity-create",
        allow_abbrev=False,
        help="Crea una identidad criptográfica local Ed25519+X25519 para metadata distribuida.",
    )
    add_config_args(identity_create_parser)
    identity_create_parser.add_argument(
        "--out",
        "--identity-file",
        dest="identity_file",
        default=None,
        help="Ruta donde escribir la identidad. Default: metadata.identity_file.",
    )
    identity_create_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase para cifrar la private key desde un fichero privado. Default: metadata.passphrase_file o prompt.",
    )
    identity_create_parser.add_argument(
        "--force",
        action="store_true",
        help="Sobrescribe el identity file si ya existe.",
    )
    add_scrypt_override_args(identity_create_parser)

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

