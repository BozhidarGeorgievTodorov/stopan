from __future__ import annotations

import argparse



def add_scrypt_override_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--scrypt-n", type=int, default=None, help="Sobrescribe metadata.scrypt_n.")
    parser.add_argument("--scrypt-r", type=int, default=None, help="Sobrescribe metadata.scrypt_r.")
    parser.add_argument("--scrypt-p", type=int, default=None, help="Sobrescribe metadata.scrypt_p.")
    parser.add_argument("--metadata-key-length", type=int, default=None, help="Sobrescribe metadata.key_length.")


def add_metadata_pack_push_args(parser: argparse.ArgumentParser) -> None:
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
        help="Número de targets remotos procesados en paralelo. Default: metadata.pack_target_parallelism.",
    )
    parser.add_argument(
        "--rpc-timeout-s",
        type=float,
        default=None,
        help="Timeout de los RPC ProbeMetadataPack/StoreMetadataPack. Default: metadata.pack_rpc_timeout_s.",
    )
    parser.add_argument(
        "--max-message-bytes",
        type=int,
        default=None,
        help="Límite gRPC por mensaje durante el flujo del pack. Default: grpc.max_message_bytes.",
    )

