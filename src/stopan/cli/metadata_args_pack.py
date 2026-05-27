from __future__ import annotations

import argparse

from stopan.cli.config_utils import add_config_args
from stopan.cli.metadata_args_common import (
    add_metadata_pack_push_args,
    add_scrypt_override_args,
)


def add_pack_group(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    pack_parser = subparsers.add_parser(
        "pack",
        allow_abbrev=False,
        help="Gestiona metadata packs locales y distribuidos.",
    )
    pack_subparsers = pack_parser.add_subparsers(dest="pack_command", required=True)

    create_parser = pack_subparsers.add_parser(
        "create",
        allow_abbrev=False,
        help="Crea un pack cifrado transportable con el latest del metadata object store.",
    )
    create_parser.set_defaults(command="pack.create")
    add_config_args(create_parser)
    create_parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store cifrado origen. Default: metadata.object_store_dir.",
    )
    create_parser.add_argument(
        "--out",
        default=None,
        help="Ruta exacta de salida .stopanmetapack. Si se omite, usa <object-store>/packs/pack-<hash>.stopanmetapack.",
    )
    create_parser.add_argument(
        "--pack-dir",
        default=None,
        help="Directorio de salida si se omite --out. Default: <object-store>/packs.",
    )
    create_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase desde un fichero privado. Evita pasar secretos por argv.",
    )
    create_parser.add_argument(
        "--identity-file",
        default=None,
        help="Identity file usado para cifrar el pack. Default: metadata.identity_file.",
    )
    add_scrypt_override_args(create_parser)

    inspect_parser = pack_subparsers.add_parser(
        "inspect",
        allow_abbrev=False,
        help="[avanzado] Inspecciona un metadata object pack cifrado.",
    )
    inspect_parser.set_defaults(command="pack.inspect")
    add_config_args(inspect_parser)
    inspect_parser.add_argument("path", help="Ruta del fichero .stopanmetapack")
    inspect_parser.add_argument(
        "--decrypt",
        action="store_true",
        help="Descifra el pack y muestra resumen del latest que contiene.",
    )
    inspect_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase desde un fichero privado para --decrypt.",
    )
    inspect_parser.add_argument(
        "--identity-file",
        default=None,
        help="Identity file usado para descifrar el pack con --decrypt. Default: metadata.identity_file.",
    )

    list_parser = pack_subparsers.add_parser(
        "list",
        allow_abbrev=False,
        help="[avanzado] Lista metadata object packs locales sin descifrarlos.",
    )
    list_parser.set_defaults(command="pack.list")
    add_config_args(list_parser)
    list_parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store. Si se usa, lista <object-store>/packs.",
    )
    list_parser.add_argument(
        "--pack-dir",
        default=None,
        help="Directorio exacto de packs a listar. Tiene prioridad sobre --object-store.",
    )

    import_parser = pack_subparsers.add_parser(
        "import",
        allow_abbrev=False,
        help="Importa un metadata object pack cifrado a un object store local.",
    )
    import_parser.set_defaults(command="pack.import")
    add_config_args(import_parser)
    import_parser.add_argument("path", help="Ruta del fichero .stopanmetapack")
    import_parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store destino. Default: metadata.object_store_dir.",
    )
    import_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase desde un fichero privado. Evita pasar secretos por argv.",
    )
    import_parser.add_argument(
        "--identity-file",
        default=None,
        help="Identity file usado para descifrar el pack. Default: metadata.identity_file.",
    )
    add_scrypt_override_args(import_parser)

    push_parser = pack_subparsers.add_parser(
        "push",
        allow_abbrev=False,
        help=(
            "Distribuye metadata a nodos remotos. Por defecto crea un pack del latest object graph; "
            "con --pack-in distribuye un .stopanmetapack existente."
        ),
    )
    push_parser.set_defaults(command="pack.push")
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
    add_scrypt_override_args(push_parser)
    add_metadata_pack_push_args(push_parser)

    discover_parser = pack_subparsers.add_parser(
        "discover",
        allow_abbrev=False,
        help="Descubre metadata packs distribuidos del owner y muestra su estado de protección por presencia.",
    )
    discover_parser.set_defaults(command="pack.discover")
    add_config_args(discover_parser)
    discover_parser.add_argument(
        "--owner-id",
        default=None,
        help="Owner ID 64-hex lowercase. Default: metadata.owner_id o metadata.identity_file.",
    )
    discover_parser.add_argument(
        "--identity-file",
        default=None,
        help="Ruta de identity file. Default: metadata.identity_file.",
    )
    discover_parser.add_argument(
        "--membership-seed",
        default=None,
        help="Seed de membership para descubrir nodos remotos. Default: cluster.seeds[0].",
    )
    discover_parser.add_argument(
        "--target-parallelism",
        type=int,
        default=None,
        help="Número de nodos remotos consultados en paralelo. Default: replication.target_parallelism.",
    )
    discover_parser.add_argument(
        "--rpc-timeout-s",
        type=float,
        default=None,
        help="Timeout del RPC ListMetadataPacks en segundos. Default: replication.stream_timeout_s.",
    )
    discover_parser.add_argument(
        "--max-message-bytes",
        type=int,
        default=None,
        help="Límite gRPC de mensaje. Default: grpc.max_message_bytes.",
    )
    discover_parser.add_argument(
        "--max-candidates",
        type=int,
        default=None,
        help="Máximo de pack_hash a mostrar, ordenados por stored_at remoto. Default: metadata.pack_discovery_max_candidates.",
    )
    discover_parser.add_argument(
        "--show-sources",
        action="store_true",
        help="Muestra las copias remotas concretas de cada pack.",
    )

    verify_parser = pack_subparsers.add_parser(
        "verify",
        allow_abbrev=False,
        help="Verifica presencia remota de metadata packs distribuidos del owner usando política local si existe.",
    )
    verify_parser.set_defaults(command="pack.verify")
    add_config_args(verify_parser)
    verify_parser.add_argument(
        "--owner-id",
        default=None,
        help="Owner ID 64-hex lowercase. Default: metadata.owner_id o metadata.identity_file.",
    )
    verify_parser.add_argument(
        "--identity-file",
        default=None,
        help="Ruta de identity file. Default: metadata.identity_file.",
    )
    verify_parser.add_argument(
        "--membership-seed",
        default=None,
        help="Seed de membership para descubrir nodos remotos. Default: cluster.seeds[0].",
    )
    verify_parser.add_argument(
        "--pack-hash",
        default=None,
        help="Pack hash concreto a verificar. Si se omite, verifica el latest descubierto.",
    )
    verify_parser.add_argument(
        "--all",
        action="store_true",
        help="Verifica todos los packs descubiertos hasta --max-candidates.",
    )
    verify_parser.add_argument(
        "--target-parallelism",
        type=int,
        default=None,
        help="Número de nodos remotos consultados en paralelo. Default: replication.target_parallelism.",
    )
    verify_parser.add_argument(
        "--rpc-timeout-s",
        type=float,
        default=None,
        help="Timeout del RPC ListMetadataPacks en segundos. Default: replication.stream_timeout_s.",
    )
    verify_parser.add_argument(
        "--max-message-bytes",
        type=int,
        default=None,
        help="Límite gRPC de mensaje. Default: grpc.max_message_bytes.",
    )
    verify_parser.add_argument(
        "--max-candidates",
        type=int,
        default=None,
        help="Máximo de pack_hash a verificar cuando no se usa --pack-hash. Default: metadata.pack_discovery_max_candidates.",
    )
    verify_parser.add_argument(
        "--show-sources",
        action="store_true",
        help="Muestra las copias remotas concretas usadas como evidencia.",
    )

    recover_parser = pack_subparsers.add_parser(
        "recover",
        allow_abbrev=False,
        help="Recupera metadata desde packs distribuidos remotos, importa el pack y reconstruye la DB local.",
    )
    recover_parser.set_defaults(command="pack.recover")
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
        "--target-hash",
        default=None,
        help="Pack hash concreto a recuperar. Si se omite, se recupera el latest válido descubierto.",
    )
    recover_parser.add_argument(
        "--max-candidates",
        type=int,
        default=None,
        help="Máximo de pack_hash candidatos a intentar, ordenados por stored_at remoto. Default: metadata.pack_discovery_max_candidates.",
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
    add_scrypt_override_args(recover_parser)

    local_store_parser = pack_subparsers.add_parser(
        "local-store",
        allow_abbrev=False,
        help="[avanzado] Guarda un metadata pack cifrado en el pack store local por owner_id.",
    )
    local_store_parser.set_defaults(command="pack.local-store")
    add_config_args(local_store_parser)
    local_store_parser.add_argument("path", help="Ruta del fichero .stopanmetapack")
    local_store_parser.add_argument(
        "--owner-id",
        default=None,
        help="Owner ID 64-hex lowercase. Default: metadata.owner_id o metadata.identity_file.",
    )
    local_store_parser.add_argument(
        "--identity-file",
        default=None,
        help="Ruta de identity file. Default: metadata.identity_file.",
    )
    local_store_parser.add_argument(
        "--pack-store",
        default=None,
        help="Directorio local de packs distribuidos. Default: metadata.distributed_pack_store_dir.",
    )
    local_store_parser.add_argument(
        "--expected-pack-hash",
        default=None,
        help="Pack hash esperado. Si se pasa, se verifica contra el fichero.",
    )
    local_store_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase para firmar el metadata pack con la identity private key.",
    )

    local_list_parser = pack_subparsers.add_parser(
        "local-list",
        allow_abbrev=False,
        help="[avanzado] Lista metadata packs guardados en el pack store local para un owner_id.",
    )
    local_list_parser.set_defaults(command="pack.local-list")
    add_config_args(local_list_parser)
    local_list_parser.add_argument(
        "--owner-id",
        default=None,
        help="Owner ID 64-hex lowercase. Default: metadata.owner_id o metadata.identity_file.",
    )
    local_list_parser.add_argument(
        "--identity-file",
        default=None,
        help="Ruta de identity file. Default: metadata.identity_file.",
    )
    local_list_parser.add_argument(
        "--pack-store",
        default=None,
        help="Directorio local de packs distribuidos. Default: metadata.distributed_pack_store_dir.",
    )

    local_retrieve_parser = pack_subparsers.add_parser(
        "local-retrieve",
        allow_abbrev=False,
        help="[avanzado] Extrae un metadata pack del pack store local a una ruta de salida.",
    )
    local_retrieve_parser.set_defaults(command="pack.local-retrieve")
    add_config_args(local_retrieve_parser)
    local_retrieve_parser.add_argument(
        "--owner-id",
        default=None,
        help="Owner ID 64-hex lowercase. Default: metadata.owner_id o metadata.identity_file.",
    )
    local_retrieve_parser.add_argument(
        "--identity-file",
        default=None,
        help="Ruta de identity file. Default: metadata.identity_file.",
    )
    local_retrieve_parser.add_argument(
        "--pack-hash",
        required=True,
        help="Pack hash 64-hex lowercase a recuperar.",
    )
    local_retrieve_parser.add_argument(
        "--out",
        required=True,
        help="Ruta exacta donde escribir el .stopanmetapack recuperado.",
    )
    local_retrieve_parser.add_argument(
        "--pack-store",
        default=None,
        help="Directorio local de packs distribuidos. Default: metadata.distributed_pack_store_dir.",
    )

