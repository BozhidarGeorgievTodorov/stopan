from __future__ import annotations

from collections.abc import Callable, Sequence

from stopan.cli.metadata_args import parse_args
from stopan.errors import StopanUsageError


MetadataCommandHandler = Callable[[object], int]


def _command_handlers() -> dict[str, MetadataCommandHandler]:
    from stopan.cli.metadata_commands_graph import (
        cmd_export_object_graph,
        cmd_import_object_graph,
        cmd_object_store_status,
    )
    from stopan.cli.metadata_commands_identity import (
        cmd_identity_show,
        cmd_status,
    )
    from stopan.cli.metadata_commands_pack import (
        cmd_discover_metadata_packs,
        cmd_import_object_pack,
        cmd_inspect_object_pack,
        cmd_list_object_packs,
        cmd_local_list_packs,
        cmd_local_retrieve_pack,
        cmd_local_store_pack,
        cmd_pack_object_graph,
        cmd_push,
        cmd_recover,
        cmd_verify_metadata_packs,
    )

    return {
        "status": cmd_status,
        "identity-show": cmd_identity_show,
        "graph.status": cmd_object_store_status,
        "graph.export": cmd_export_object_graph,
        "graph.import": cmd_import_object_graph,
        "pack.create": cmd_pack_object_graph,
        "pack.inspect": cmd_inspect_object_pack,
        "pack.list": cmd_list_object_packs,
        "pack.import": cmd_import_object_pack,
        "pack.push": cmd_push,
        "pack.discover": cmd_discover_metadata_packs,
        "pack.verify": cmd_verify_metadata_packs,
        "pack.recover": cmd_recover,
        "pack.local-store": cmd_local_store_pack,
        "pack.local-list": cmd_local_list_packs,
        "pack.local-retrieve": cmd_local_retrieve_pack,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        handler = _command_handlers()[args.command]
    except KeyError as exc:
        raise StopanUsageError(f"Comando metadata desconocido: {args.command!r}") from exc
    return handler(args)
