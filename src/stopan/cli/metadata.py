from __future__ import annotations

import argparse
from collections.abc import Sequence

from stopan.cli.config_utils import add_config_args
from stopan.cli.validation import (
    Flag,
    FloatRange,
    IntRange,
    reject_together,
    require_dependency,
    validate_float_ranges,
    validate_int_ranges,
    validate_scrypt_overrides,
)
from stopan.cli.metadata_commands import (
    cmd_export_object_graph,
    cmd_gc_object_store,
    cmd_identity_create,
    cmd_identity_show,
    cmd_import_object_graph,
    cmd_import_object_pack,
    cmd_inspect_object_pack,
    cmd_list_object_packs,
    cmd_local_list_packs,
    cmd_local_retrieve_pack,
    cmd_local_store_pack,
    cmd_object_store_status,
    cmd_pack_object_graph,
    cmd_push,
    cmd_recover,
    cmd_status,
)


_DEFAULT_RECOVER_MAX_CANDIDATES = 20


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stopan metadata",
        allow_abbrev=False,
        description="Gestiona el metadata vault cifrado local.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    status_parser = subparsers.add_parser(
        "status",
        allow_abbrev=False,
        help="Muestra configuración efectiva y estado operativo del metadata vault.",
    )
    add_config_args(status_parser)

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
    _add_scrypt_override_args(identity_create_parser)

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

    object_status_parser = subparsers.add_parser(
        "object-store-status",
        allow_abbrev=False,
        help="[avanzado] Muestra estado del metadata object store incremental cifrado.",
    )
    add_config_args(object_status_parser)
    object_status_parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store cifrado. Default: metadata.object_store_dir.",
    )
    object_status_parser.add_argument(
        "--decrypt-latest",
        action="store_true",
        help="Descifra y muestra el latest pointer del store.",
    )
    object_status_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase desde un fichero privado para --decrypt-latest.",
    )

    export_graph_parser = subparsers.add_parser(
        "export-graph",
        allow_abbrev=False,
        help="Exporta el estado actual de _metadata.db a un object store incremental cifrado.",
    )
    add_config_args(export_graph_parser)
    export_graph_parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store cifrado. Default: metadata.object_store_dir.",
    )
    export_graph_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase desde un fichero privado. Evita pasar secretos por argv.",
    )
    export_graph_parser.add_argument(
        "--identity-file",
        default=None,
        help="Identity file usado para cifrar el pack si se usa --pack. Default: metadata.identity_file.",
    )
    export_graph_parser.add_argument(
        "--no-protection",
        action="store_true",
        help="No incluir chunk_protection en el grafo exportado.",
    )
    export_graph_parser.add_argument(
        "--pack",
        action="store_true",
        help="Después de exportar el object graph, crea también un .stopanmetapack transportable.",
    )
    export_graph_parser.add_argument(
        "--pack-out",
        default=None,
        help="Ruta exacta del .stopanmetapack si se usa --pack.",
    )
    export_graph_parser.add_argument(
        "--pack-dir",
        default=None,
        help="Directorio de salida del pack si se usa --pack y se omite --pack-out. Default: <object-store>/packs.",
    )
    _add_scrypt_override_args(export_graph_parser)

    import_graph_parser = subparsers.add_parser(
        "import-graph",
        allow_abbrev=False,
        help="Reconstruye una _metadata.db vacía desde el latest del object store cifrado.",
    )
    add_config_args(import_graph_parser)
    import_graph_parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store cifrado. Default: metadata.object_store_dir.",
    )
    import_graph_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase desde un fichero privado. Evita pasar secretos por argv.",
    )
    import_graph_parser.add_argument(
        "--no-protection",
        action="store_true",
        help="No importar chunk_protection del graph; crea filas PENDING para chunks conocidos.",
    )
    import_graph_parser.add_argument(
        "--default-desired-remote-copies",
        type=int,
        default=None,
        help=(
            "Copias remotas deseadas para filas PENDING si se usa --no-protection "
            "o no hay protection_index. Puede ser 0. Default: protection.remote_copies."
        ),
    )

    pack_graph_parser = subparsers.add_parser(
        "pack-graph",
        allow_abbrev=False,
        help="Crea un pack cifrado transportable con el latest del metadata object store.",
    )
    add_config_args(pack_graph_parser)
    pack_graph_parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store cifrado origen. Default: metadata.object_store_dir.",
    )
    pack_graph_parser.add_argument(
        "--out",
        default=None,
        help="Ruta exacta de salida .stopanmetapack. Si se omite, usa <object-store>/packs/pack-<hash>.stopanmetapack.",
    )
    pack_graph_parser.add_argument(
        "--pack-dir",
        default=None,
        help="Directorio de salida si se omite --out. Default: <object-store>/packs.",
    )
    pack_graph_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase desde un fichero privado. Evita pasar secretos por argv.",
    )
    pack_graph_parser.add_argument(
        "--identity-file",
        default=None,
        help="Identity file usado para cifrar el pack. Default: metadata.identity_file.",
    )
    _add_scrypt_override_args(pack_graph_parser)

    inspect_pack_parser = subparsers.add_parser(
        "inspect-pack",
        allow_abbrev=False,
        help="[avanzado] Inspecciona un metadata object pack cifrado.",
    )
    add_config_args(inspect_pack_parser)
    inspect_pack_parser.add_argument("path", help="Ruta del fichero .stopanmetapack")
    inspect_pack_parser.add_argument(
        "--decrypt",
        action="store_true",
        help="Descifra el pack y muestra resumen del latest que contiene.",
    )
    inspect_pack_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase desde un fichero privado para --decrypt.",
    )
    inspect_pack_parser.add_argument(
        "--identity-file",
        default=None,
        help="Identity file usado para descifrar el pack con --decrypt. Default: metadata.identity_file.",
    )

    list_packs_parser = subparsers.add_parser(
        "list-object-packs",
        allow_abbrev=False,
        help="[avanzado] Lista metadata object packs locales sin descifrarlos.",
    )
    add_config_args(list_packs_parser)
    list_packs_parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store. Si se usa, lista <object-store>/packs.",
    )
    list_packs_parser.add_argument(
        "--pack-dir",
        default=None,
        help="Directorio exacto de packs a listar. Tiene prioridad sobre --object-store.",
    )

    local_store_pack_parser = subparsers.add_parser(
        "local-store-pack",
        allow_abbrev=False,
        help="[avanzado] Guarda un metadata object pack cifrado en el pack store local por owner_id.",
    )
    add_config_args(local_store_pack_parser)
    local_store_pack_parser.add_argument("path", help="Ruta del fichero .stopanmetapack")
    local_store_pack_parser.add_argument(
        "--owner-id",
        default=None,
        help="Owner ID 64-hex lowercase. Default: metadata.owner_id o metadata.identity_file.",
    )
    local_store_pack_parser.add_argument(
        "--identity-file",
        default=None,
        help="Ruta de identity file. Default: metadata.identity_file.",
    )
    local_store_pack_parser.add_argument(
        "--pack-store",
        default=None,
        help="Directorio local de packs distribuidos. Default: metadata.distributed_pack_store_dir.",
    )
    local_store_pack_parser.add_argument(
        "--expected-pack-hash",
        default=None,
        help="Pack hash esperado. Si se pasa, se verifica contra el fichero.",
    )
    local_store_pack_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase para firmar el metadata pack con la identity private key.",
    )

    local_list_packs_parser = subparsers.add_parser(
        "local-list-packs",
        allow_abbrev=False,
        help="[avanzado] Lista metadata packs guardados en el pack store local para un owner_id.",
    )
    add_config_args(local_list_packs_parser)
    local_list_packs_parser.add_argument(
        "--owner-id",
        default=None,
        help="Owner ID 64-hex lowercase. Default: metadata.owner_id o metadata.identity_file.",
    )
    local_list_packs_parser.add_argument(
        "--identity-file",
        default=None,
        help="Ruta de identity file. Default: metadata.identity_file.",
    )
    local_list_packs_parser.add_argument(
        "--pack-store",
        default=None,
        help="Directorio local de packs distribuidos. Default: metadata.distributed_pack_store_dir.",
    )

    local_retrieve_pack_parser = subparsers.add_parser(
        "local-retrieve-pack",
        allow_abbrev=False,
        help="[avanzado] Extrae un metadata pack del pack store local a una ruta de salida.",
    )
    add_config_args(local_retrieve_pack_parser)
    local_retrieve_pack_parser.add_argument(
        "--owner-id",
        default=None,
        help="Owner ID 64-hex lowercase. Default: metadata.owner_id o metadata.identity_file.",
    )
    local_retrieve_pack_parser.add_argument(
        "--identity-file",
        default=None,
        help="Ruta de identity file. Default: metadata.identity_file.",
    )
    local_retrieve_pack_parser.add_argument(
        "--pack-hash",
        required=True,
        help="Pack hash 64-hex lowercase a recuperar.",
    )
    local_retrieve_pack_parser.add_argument(
        "--out",
        required=True,
        help="Ruta exacta donde escribir el .stopanmetapack recuperado.",
    )
    local_retrieve_pack_parser.add_argument(
        "--pack-store",
        default=None,
        help="Directorio local de packs distribuidos. Default: metadata.distributed_pack_store_dir.",
    )

    push_parser = subparsers.add_parser(
        "push",
        allow_abbrev=False,
        help=(
            "Distribuye metadata a nodos remotos. Por defecto crea un pack del latest object graph; "
            "con --pack-in distribuye un .stopanmetapack existente."
        ),
    )
    add_config_args(push_parser)
    push_parser.add_argument(
        "--pack-in",
        default=None,
        help="Ruta de un .stopanmetapack existente a distribuir. Si se omite, se crea desde el latest object graph.",
    )
    push_parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store origen cuando no se usa --pack-in. Default: metadata.object_store_dir.",
    )
    push_parser.add_argument(
        "--passphrase-file",
        default=None,
        help=(
            "Lee la passphrase para crear el pack latest o firmar un pack existente. "
            "Default: prompt interactivo."
        ),
    )
    push_parser.add_argument(
        "--pack-out",
        default=None,
        help="Ruta exacta de salida .stopanmetapack antes de distribuirlo cuando no se usa --pack-in.",
    )
    push_parser.add_argument(
        "--pack-dir",
        default=None,
        help=(
            "Directorio de salida del pack si se omite --pack-out y no se usa --pack-in. "
            "Default: metadata.object_pack_dir o <object-store>/packs."
        ),
    )
    push_parser.add_argument(
        "--owner-id",
        default=None,
        help="Owner ID 64-hex lowercase. Default: metadata.owner_id o metadata.identity_file.",
    )
    push_parser.add_argument(
        "--identity-file",
        default=None,
        help="Ruta de identity file. Default: metadata.identity_file.",
    )
    _add_scrypt_override_args(push_parser)
    _add_metadata_pack_push_args(push_parser)

    recover_parser = subparsers.add_parser(
        "recover",
        allow_abbrev=False,
        help="Recupera metadata desde packs distribuidos remotos, importa el pack y reconstruye la DB local.",
    )
    add_config_args(recover_parser)
    recover_parser.add_argument(
        "--owner-id",
        default=None,
        help="Owner ID 64-hex lowercase. Default: metadata.owner_id o metadata.identity_file.",
    )
    recover_parser.add_argument(
        "--identity-file",
        default=None,
        help="Ruta de identity file. Default: metadata.identity_file.",
    )
    recover_parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store destino. Default: metadata.object_store_dir.",
    )
    recover_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase para descifrar packs y el object store local.",
    )
    recover_parser.add_argument(
        "--membership-seed",
        default=None,
        help="Seed de membership para descubrir nodos remotos. Default: cluster.seeds[0].",
    )
    recover_parser.add_argument(
        "--target-parallelism",
        type=int,
        default=None,
        help="Número de nodos remotos consultados en paralelo. Default: replication.target_parallelism.",
    )
    recover_parser.add_argument(
        "--rpc-timeout-s",
        type=float,
        default=None,
        help="Timeout de RPC List/RetrieveMetadataPack. Default: replication.stream_timeout_s.",
    )
    recover_parser.add_argument(
        "--max-message-bytes",
        type=int,
        default=None,
        help="Límite gRPC de mensaje para descargar packs. Default: grpc.max_message_bytes.",
    )
    recover_parser.add_argument(
        "--download-dir",
        default=None,
        help="Directorio donde guardar el pack descargado. Default: <object-store>/recovered_packs.",
    )
    recover_parser.add_argument(
        "--pack-out",
        default=None,
        help="Ruta exacta donde guardar el pack descargado elegido.",
    )
    recover_parser.add_argument(
        "--max-candidates",
        type=int,
        default=_DEFAULT_RECOVER_MAX_CANDIDATES,
        help="Máximo de pack_hash candidatos a intentar, ordenados por stored_at remoto. Default: 20.",
    )
    recover_parser.add_argument(
        "--no-import-db",
        dest="import_db",
        action="store_false",
        default=True,
        help="Solo importa el pack al object store local; no reconstruye _metadata.db.",
    )
    recover_parser.add_argument(
        "--no-protection",
        action="store_true",
        help="Al reconstruir DB, no importar chunk_protection del graph; crea filas PENDING.",
    )
    recover_parser.add_argument(
        "--default-desired-remote-copies",
        type=int,
        default=None,
        help=(
            "Copias remotas deseadas para filas PENDING si se usa --no-protection "
            "o no hay protection_index. Puede ser 0. Default: protection.remote_copies."
        ),
    )
    _add_scrypt_override_args(recover_parser)

    import_pack_parser = subparsers.add_parser(
        "import-pack",
        allow_abbrev=False,
        help="Importa un metadata object pack cifrado a un object store local.",
    )
    add_config_args(import_pack_parser)
    import_pack_parser.add_argument("path", help="Ruta del fichero .stopanmetapack")
    import_pack_parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store destino. Default: metadata.object_store_dir.",
    )
    import_pack_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase desde un fichero privado. Evita pasar secretos por argv.",
    )
    import_pack_parser.add_argument(
        "--identity-file",
        default=None,
        help="Identity file usado para descifrar el pack. Default: metadata.identity_file.",
    )
    _add_scrypt_override_args(import_pack_parser)

    gc_parser = subparsers.add_parser(
        "gc",
        allow_abbrev=False,
        help="[avanzado] Ejecuta Mark & Sweep sobre el metadata object store cifrado.",
    )
    add_config_args(gc_parser)
    gc_parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store cifrado. Default: metadata.object_store_dir.",
    )
    gc_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase desde un fichero privado. Evita pasar secretos por argv.",
    )
    gc_parser.add_argument(
        "--identity-file",
        default=None,
        help="Identity file usado para descifrar packs locales durante GC. Default: metadata.identity_file.",
    )
    gc_parser.add_argument(
        "--object-grace-hours",
        type=float,
        default=None,
        help=(
            "Periodo de gracia antes de borrar objetos cifrados no alcanzables. "
            "Default: gc.metadata_object_store_grace_hours."
        ),
    )
    gc_parser.add_argument(
        "--pack-grace-hours",
        type=float,
        default=None,
        help=(
            "Periodo de gracia antes de borrar .stopanmetapack locales obsoletos. "
            "Default: gc.metadata_object_pack_grace_hours."
        ),
    )
    gc_apply_group = gc_parser.add_mutually_exclusive_group()
    gc_apply_group.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Muestra lo que se borraría sin borrar nada. Es el default.",
    )
    gc_apply_group.add_argument(
        "--apply",
        dest="dry_run",
        action="store_false",
        help="Ejecuta el borrado real.",
    )
    gc_parser.add_argument(
        "--objects-only",
        action="store_true",
        help="Solo recolecta objetos huérfanos; no toca packs.",
    )
    gc_parser.add_argument(
        "--packs-only",
        action="store_true",
        help="Solo recolecta packs obsoletos; no toca objetos.",
    )
    gc_parser.add_argument(
        "--pack-dir",
        default=None,
        help="Directorio de packs a limpiar. Default: <object-store>/packs.",
    )

    args = parser.parse_args(argv)
    _validate_args(parser, args)
    return args


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    validate_scrypt_overrides(parser, args)

    if args.command == "object-store-status":
        require_dependency(
            parser,
            args,
            Flag("passphrase_file", "--passphrase-file"),
            Flag("decrypt_latest", "--decrypt-latest"),
        )

    if args.command == "export-graph":
        require_dependency(parser, args, Flag("pack_out", "--pack-out"), Flag("pack", "--pack"))
        require_dependency(parser, args, Flag("pack_dir", "--pack-dir"), Flag("pack", "--pack"))
        require_dependency(parser, args, Flag("identity_file", "--identity-file"), Flag("pack", "--pack"))
        reject_together(parser, args, Flag("pack_out", "--pack-out"), Flag("pack_dir", "--pack-dir"))

    if args.command == "pack-graph":
        reject_together(parser, args, Flag("out", "--out"), Flag("pack_dir", "--pack-dir"))

    if args.command == "inspect-pack":
        require_dependency(parser, args, Flag("passphrase_file", "--passphrase-file"), Flag("decrypt", "--decrypt"))
        require_dependency(parser, args, Flag("identity_file", "--identity-file"), Flag("decrypt", "--decrypt"))

    if args.command == "push":
        reject_together(parser, args, Flag("pack_in", "--pack-in"), Flag("object_store", "--object-store"))
        reject_together(parser, args, Flag("pack_in", "--pack-in"), Flag("pack_out", "--pack-out"))
        reject_together(parser, args, Flag("pack_in", "--pack-in"), Flag("pack_dir", "--pack-dir"))
        reject_together(parser, args, Flag("pack_out", "--pack-out"), Flag("pack_dir", "--pack-dir"))
        validate_int_ranges(
            parser,
            args,
            (
                IntRange("pack_copies", "--pack-copies", 0),
                IntRange("target_parallelism", "--target-parallelism", 1),
                IntRange("max_message_bytes", "--max-message-bytes", 1),
            ),
        )
        validate_float_ranges(
            parser,
            args,
            (FloatRange("rpc_timeout_s", "--rpc-timeout-s", 0.0, inclusive=False),),
        )

    if args.command in {"import-graph", "recover"}:
        validate_int_ranges(
            parser,
            args,
            (IntRange("default_desired_remote_copies", "--default-desired-remote-copies", 0),),
        )

    if args.command == "recover":
        validate_int_ranges(
            parser,
            args,
            (
                IntRange("target_parallelism", "--target-parallelism", 1),
                IntRange("max_message_bytes", "--max-message-bytes", 1),
                IntRange("max_candidates", "--max-candidates", 1),
            ),
        )
        validate_float_ranges(
            parser,
            args,
            (FloatRange("rpc_timeout_s", "--rpc-timeout-s", 0.0, inclusive=False),),
        )
        reject_together(parser, args, Flag("pack_out", "--pack-out"), Flag("download_dir", "--download-dir"))
        if not args.import_db and args.no_protection:
            parser.error("'--no-import-db' y '--no-protection' son incompatibles")
        if not args.import_db and args.default_desired_remote_copies is not None:
            parser.error("--default-desired-remote-copies requiere importar la DB")

    if args.command == "gc":
        validate_float_ranges(
            parser,
            args,
            (
                FloatRange("object_grace_hours", "--object-grace-hours", 0.0),
                FloatRange("pack_grace_hours", "--pack-grace-hours", 0.0),
            ),
        )
        reject_together(parser, args, Flag("objects_only", "--objects-only"), Flag("packs_only", "--packs-only"))


def _add_scrypt_override_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--scrypt-n", type=int, default=None, help="Sobrescribe metadata.scrypt_n.")
    parser.add_argument("--scrypt-r", type=int, default=None, help="Sobrescribe metadata.scrypt_r.")
    parser.add_argument("--scrypt-p", type=int, default=None, help="Sobrescribe metadata.scrypt_p.")
    parser.add_argument("--metadata-key-length", type=int, default=None, help="Sobrescribe metadata.key_length.")


def _add_metadata_pack_push_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--membership-seed",
        default=None,
        help="Seed de membership para obtener la vista del cluster elegible.",
    )
    parser.add_argument(
        "--pack-copies",
        type=int,
        default=None,
        help="Copias remotas requeridas para distribuir el metadata pack. Usa 0 para no enviarlo. Default: metadata.pack_copies.",
    )
    parser.add_argument(
        "--strict-pack-copies",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Exige suficientes targets remotos elegibles antes de enviar. "
            "Con --pack-copies=0 no se envía nada. Default: metadata.strict_pack_copies."
        ),
    )
    parser.add_argument(
        "--target-parallelism",
        type=int,
        default=None,
        help="Número de targets remotos procesados en paralelo. Default: replication.target_parallelism.",
    )
    parser.add_argument(
        "--rpc-timeout-s",
        type=float,
        default=None,
        help="Timeout del RPC StoreMetadataPack en segundos. Default: replication.stream_timeout_s.",
    )
    parser.add_argument(
        "--max-message-bytes",
        type=int,
        default=None,
        help="Límite gRPC de mensaje para enviar el pack. Default: grpc.max_message_bytes.",
    )


COMMAND_HANDLERS = {
    "status": cmd_status,
    "identity-create": cmd_identity_create,
    "identity-show": cmd_identity_show,
    "object-store-status": cmd_object_store_status,
    "export-graph": cmd_export_object_graph,
    "import-graph": cmd_import_object_graph,
    "list-object-packs": cmd_list_object_packs,
    "local-store-pack": cmd_local_store_pack,
    "local-list-packs": cmd_local_list_packs,
    "local-retrieve-pack": cmd_local_retrieve_pack,
    "push": cmd_push,
    "pack-graph": cmd_pack_object_graph,
    "inspect-pack": cmd_inspect_object_pack,
    "recover": cmd_recover,
    "import-pack": cmd_import_object_pack,
    "gc": cmd_gc_object_store,
}


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        handler = COMMAND_HANDLERS[args.command]
    except KeyError as exc:
        raise ValueError(f"Comando metadata desconocido: {args.command!r}") from exc
    return handler(args)
