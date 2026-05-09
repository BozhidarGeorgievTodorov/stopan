from __future__ import annotations

import argparse
from typing import Any

from stopan.metadata.identity.passphrase import ScryptCost
from stopan.metadata.objects.graph import MetadataObjectGraphAutoExport


def add_metadata_auto_export_args(parser: argparse.ArgumentParser, *, context: str) -> None:
    """Add common metadata auto-export flags for commands that mutate MetadataDB."""
    parser.add_argument(
        "--metadata-object-graph",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            f"Exporta el metadata object graph cifrado después de {context}. "
            "Usa --no-metadata-object-graph para desactivarlo explícitamente."
        ),
    )
    parser.add_argument(
        "--metadata-object-store",
        default=None,
        help=(
            "Directorio del metadata object store cifrado. "
            "Si se define, activa metadata object graph para este comando."
        ),
    )
    parser.add_argument(
        "--metadata-passphrase-file",
        default=None,
        help=(
            "Fichero privado con la passphrase del metadata object store. "
            "Default: metadata.passphrase_file."
        ),
    )
    parser.add_argument(
        "--metadata-identity-file",
        default=None,
        help=(
            "Identity file usado para crear .stopanmetapack si se activa auto-pack. "
            "Default: metadata.identity_file."
        ),
    )
    parser.add_argument(
        "--metadata-object-pack",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Crea un .stopanmetapack después de exportar el object graph. "
            "Usa --no-metadata-object-pack para desactivarlo explícitamente."
        ),
    )
    parser.add_argument(
        "--metadata-object-pack-dir",
        default=None,
        help=(
            "Directorio donde escribir el .stopanmetapack generado. "
            "Si se define, activa metadata object pack para este comando."
        ),
    )


def build_metadata_object_graph_auto_export(args: argparse.Namespace, cfg: Any) -> MetadataObjectGraphAutoExport | None:
    """Build auto-export settings from CLI overrides and StopanConfig.

    Precedence:
      1. --metadata-object-store implies object graph export.
      2. --metadata-object-graph / --no-metadata-object-graph overrides config.
      3. metadata.object_graph_auto_export decides otherwise.

    Pack creation is only meaningful when object graph export is enabled.
    """
    cli_graph = getattr(args, "metadata_object_graph", None)
    cli_store = getattr(args, "metadata_object_store", None)
    cli_pack = getattr(args, "metadata_object_pack", None)
    cli_pack_dir = getattr(args, "metadata_object_pack_dir", None)

    if cli_store and cli_graph is False:
        raise ValueError("--metadata-object-store y --no-metadata-object-graph son incompatibles.")

    if cli_store:
        enabled = True
    elif cli_graph is not None:
        enabled = bool(cli_graph)
    else:
        enabled = bool(cfg.metadata.object_graph_auto_export)

    if not enabled:
        if cli_pack or cli_pack_dir:
            raise ValueError(
                "--metadata-object-pack/--metadata-object-pack-dir requieren metadata object graph activo."
            )
        return None

    if cli_pack_dir and cli_pack is False:
        raise ValueError("--metadata-object-pack-dir y --no-metadata-object-pack son incompatibles.")

    if cli_pack_dir:
        auto_pack = True
    elif cli_pack is not None:
        auto_pack = bool(cli_pack)
    else:
        auto_pack = bool(cfg.metadata.object_graph_auto_pack)

    object_store_dir = str(cli_store or cfg.metadata.object_store_dir)
    passphrase_file = str(getattr(args, "metadata_passphrase_file", None) or cfg.metadata.passphrase_file)
    pack_dir = str(cli_pack_dir or cfg.metadata.object_pack_dir or "") or None
    identity_file = str(getattr(args, "metadata_identity_file", None) or cfg.metadata.identity_file or "")

    return MetadataObjectGraphAutoExport(
        enabled=True,
        object_store_dir=object_store_dir,
        passphrase_file=passphrase_file,
        scrypt_cost=ScryptCost(
            n=int(cfg.metadata.scrypt_n),
            r=int(cfg.metadata.scrypt_r),
            p=int(cfg.metadata.scrypt_p),
            key_length=int(cfg.metadata.key_length),
        ),
        include_protection=bool(cfg.metadata.object_graph_include_protection),
        auto_pack=auto_pack,
        pack_dir=pack_dir,
        identity_file=identity_file,
    )
