from __future__ import annotations

import argparse

from stopan.cli.config_utils import add_config_args
from stopan.cli.metadata_args_common import add_scrypt_override_args


def add_graph_group(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    graph_parser = subparsers.add_parser(
        "graph",
        allow_abbrev=False,
        help="Gestiona el metadata object graph cifrado.",
    )
    graph_subparsers = graph_parser.add_subparsers(dest="graph_command", required=True)

    status_parser = graph_subparsers.add_parser(
        "status",
        allow_abbrev=False,
        help="[avanzado] Muestra estado del metadata object store incremental cifrado.",
    )
    status_parser.set_defaults(command="graph.status")
    add_config_args(status_parser)
    status_parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store cifrado. Default: metadata.object_store_dir.",
    )
    status_parser.add_argument(
        "--decrypt-latest",
        action="store_true",
        help="Descifra y muestra el latest pointer del store.",
    )
    status_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase desde un fichero privado para --decrypt-latest. Default: metadata.passphrase_file si existe.",
    )

    export_parser = graph_subparsers.add_parser(
        "export",
        allow_abbrev=False,
        help="Exporta el estado actual de _metadata.db a un object store incremental cifrado.",
    )
    export_parser.set_defaults(command="graph.export")
    add_config_args(export_parser)
    export_parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store cifrado. Default: metadata.object_store_dir.",
    )
    export_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase desde un fichero privado. Default: metadata.passphrase_file si existe; si no, prompt interactivo.",
    )
    export_parser.add_argument(
        "--identity-file",
        default=None,
        help="Identity file usado para cifrar el pack si se usa --pack. Default: metadata.identity_file.",
    )
    export_parser.add_argument(
        "--no-protection",
        action="store_true",
        help="No incluir chunk_protection en el grafo exportado.",
    )
    export_parser.add_argument(
        "--pack",
        action="store_true",
        help="Después de exportar el object graph, crea también un .stopanmetapack transportable.",
    )
    export_parser.add_argument(
        "--pack-out",
        default=None,
        help="Ruta exacta del .stopanmetapack si se usa --pack.",
    )
    export_parser.add_argument(
        "--pack-dir",
        default=None,
        help=(
            "Directorio de salida del pack si se usa --pack y se omite --pack-out. "
            "Default: metadata.object_pack_dir o <object-store>/packs."
        ),
    )
    add_scrypt_override_args(export_parser)

    import_parser = graph_subparsers.add_parser(
        "import",
        allow_abbrev=False,
        help="Reconstruye una _metadata.db vacía desde el latest del object store cifrado.",
    )
    import_parser.set_defaults(command="graph.import")
    add_config_args(import_parser)
    import_parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store cifrado. Default: metadata.object_store_dir.",
    )
    import_parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase desde un fichero privado. Default: metadata.passphrase_file si existe; si no, prompt interactivo.",
    )
    import_parser.add_argument(
        "--no-protection",
        action="store_true",
        help="No importar chunk_protection del graph; crea filas PENDING para chunks conocidos.",
    )
    import_parser.add_argument(
        "--default-desired-remote-copies",
        type=int,
        default=None,
        help=(
            "Copias remotas deseadas para filas PENDING si se usa --no-protection "
            "o no hay protection_index. Puede ser 0. Default: protection.remote_copies."
        ),
    )
