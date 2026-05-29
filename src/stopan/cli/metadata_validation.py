from __future__ import annotations

import argparse

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


def validate_metadata_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    validate_scrypt_overrides(parser, args)

    if args.command == "graph.status":
        require_dependency(
            parser,
            args,
            Flag("passphrase_file", "--passphrase-file"),
            Flag("decrypt_latest", "--decrypt-latest"),
        )

    if args.command == "graph.export":
        require_dependency(parser, args, Flag("pack_out", "--pack-out"), Flag("pack", "--pack"))
        require_dependency(parser, args, Flag("pack_dir", "--pack-dir"), Flag("pack", "--pack"))
        require_dependency(parser, args, Flag("identity_file", "--identity-file"), Flag("pack", "--pack"))
        reject_together(parser, args, Flag("pack_out", "--pack-out"), Flag("pack_dir", "--pack-dir"))

    if args.command == "pack.create":
        reject_together(parser, args, Flag("out", "--out"), Flag("pack_dir", "--pack-dir"))

    if args.command == "pack.inspect":
        require_dependency(parser, args, Flag("passphrase_file", "--passphrase-file"), Flag("decrypt", "--decrypt"))
        require_dependency(parser, args, Flag("identity_file", "--identity-file"), Flag("decrypt", "--decrypt"))

    if args.command == "pack.push":
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

    if args.command in {"graph.import", "pack.recover"}:
        validate_int_ranges(
            parser,
            args,
            (IntRange("default_desired_remote_copies", "--default-desired-remote-copies", 0),),
        )

    if args.command == "pack.recover":
        validate_int_ranges(
            parser,
            args,
            (
                IntRange("target_parallelism", "--target-parallelism", 1),
                IntRange("max_message_bytes", "--max-message-bytes", 1),
            ),
        )
        validate_float_ranges(
            parser,
            args,
            (FloatRange("rpc_timeout_s", "--rpc-timeout-s", 0.0, inclusive=False),),
        )
        reject_together(parser, args, Flag("pack_out", "--pack-out"), Flag("download_dir", "--download-dir"))
        if args.download_only and not args.import_db:
            parser.error("'--download-only' y '--no-import-db' son incompatibles")
        if args.download_only and args.no_protection:
            parser.error("'--download-only' y '--no-protection' son incompatibles")
        if args.download_only and args.default_desired_remote_copies is not None:
            parser.error("--default-desired-remote-copies requiere importar la DB")
        if not args.import_db and args.no_protection:
            parser.error("'--no-import-db' y '--no-protection' son incompatibles")
        if not args.import_db and args.default_desired_remote_copies is not None:
            parser.error("--default-desired-remote-copies requiere importar la DB")

    if args.command in {"pack.discover", "pack.verify"}:
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

    if args.command == "pack.verify":
        reject_together(parser, args, Flag("pack_hash", "--pack-hash"), Flag("all", "--all"))

    if args.command == "graph.gc":
        validate_float_ranges(
            parser,
            args,
            (
                FloatRange("object_grace_hours", "--object-grace-hours", 0.0),
                FloatRange("pack_grace_hours", "--pack-grace-hours", 0.0),
            ),
        )
        reject_together(parser, args, Flag("objects_only", "--objects-only"), Flag("packs_only", "--packs-only"))


