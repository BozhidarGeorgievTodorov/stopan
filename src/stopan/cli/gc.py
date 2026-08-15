from __future__ import annotations

import argparse
import tempfile
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from pathlib import Path

from stopan.cli.config_utils import add_config_args, choose, load_runtime_config
from stopan.cli.output import format_bytes
from stopan.cli.validation import FloatRange, IntRange, validate_float_ranges, validate_int_ranges
from stopan.errors import StopanUsageError
from stopan.gc.models import LocalFileGarbageCollectionResult

GcCommandHandler = Callable[[argparse.Namespace], int]

_SECONDS_PER_HOUR = 3600
_SECONDS_PER_DAY = 24 * _SECONDS_PER_HOUR


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stopan gc",
        allow_abbrev=False,
        description="Ejecuta GC local sobre almacenes de Stopan.",
    )
    subparsers = parser.add_subparsers(dest="target", required=True)

    _add_file_target(
        subparsers,
        "generated-chunks",
        help_text="Limpia chunks propios en storage.local_chunk_dir.",
        path_arg="chunk_store",
        path_option="--chunk-store",
        path_help="CAS de chunks propios. Default: storage.local_chunk_dir.",
        age_kind="grace-hours",
    )
    _add_file_target(
        subparsers,
        "received-chunks",
        help_text="Limpia chunks recibidos en storage.custody_dir/chunks.",
        path_arg="chunk_store",
        path_option="--chunk-store",
        path_help="CAS de chunks recibidos. Default: storage.custody_dir/chunks.",
        age_kind="max-age-days",
    )
    _add_file_target(
        subparsers,
        "received-ec",
        help_text="Limpia shards EC recibidos en storage.custody_dir/ec_shards.",
        path_arg="ec_store",
        path_option="--ec-store",
        path_help="Almacén EC recibido. Default: storage.custody_dir/ec_shards.",
        age_kind="max-age-days",
    )

    _add_metadata_graph_target(
        subparsers,
        "generated-metadata-graph",
        help_text="Limpia objetos huérfanos del metadata graph generado localmente.",
        include_objects=True,
        include_packs=False,
    )
    _add_metadata_graph_target(
        subparsers,
        "generated-metadata-packs",
        help_text="Limpia metadata packs generados localmente.",
        include_objects=False,
        include_packs=True,
    )
    _add_received_metadata_packs_target(subparsers)
    _add_recovered_metadata_packs_target(subparsers)
    _add_all_target(subparsers)

    args = parser.parse_args(argv)
    _validate_args(parser, args)
    return args


def _add_apply_args(parser: argparse.ArgumentParser) -> None:
    apply_group = parser.add_mutually_exclusive_group()
    apply_group.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Muestra lo que se borraría sin borrar nada. Es el default.",
    )
    apply_group.add_argument(
        "--apply",
        dest="dry_run",
        action="store_false",
        help="Ejecuta el borrado real.",
    )


def _add_file_target(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    name: str,
    *,
    help_text: str,
    path_arg: str,
    path_option: str,
    path_help: str,
    age_kind: str,
) -> None:
    parser = subparsers.add_parser(name, allow_abbrev=False, help=help_text, description=help_text)
    parser.set_defaults(command=name)
    add_config_args(parser)
    parser.add_argument(path_option, dest=path_arg, default=None, help=path_help)
    if age_kind == "grace-hours":
        parser.add_argument(
            "--grace-hours",
            type=float,
            default=None,
            help="Periodo de gracia antes de borrar ficheros antiguos. Default: gc.generated_chunk_grace_hours.",
        )
    else:
        parser.add_argument(
            "--max-age-days",
            type=int,
            default=None,
            help="Edad máxima antes de borrar ficheros antiguos. 0 desactiva borrado por edad.",
        )
    _add_apply_args(parser)


def _add_metadata_graph_target(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    name: str,
    *,
    help_text: str,
    include_objects: bool,
    include_packs: bool,
) -> None:
    parser = subparsers.add_parser(name, allow_abbrev=False, help=help_text, description=help_text)
    parser.set_defaults(command=name, include_objects=include_objects, include_packs=include_packs)
    add_config_args(parser)
    parser.add_argument(
        "--object-store",
        default=None,
        help="Directorio del metadata object store cifrado. Default: metadata.object_store_dir.",
    )
    parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase desde un fichero privado. Evita pasar secretos por argv.",
    )
    if include_packs:
        parser.add_argument(
            "--identity-file",
            default=None,
            help="Identity file usado para descifrar packs locales. Default: metadata.identity_file.",
        )
    if include_objects:
        parser.add_argument(
            "--object-grace-hours",
            type=float,
            default=None,
            help="Periodo de gracia para objetos huérfanos. Default: gc.generated_metadata_graph_grace_hours.",
        )
    if include_packs:
        parser.add_argument(
            "--pack-grace-hours",
            type=float,
            default=None,
            help="Periodo de gracia para packs obsoletos. Default: gc.generated_metadata_pack_grace_hours.",
        )
        parser.add_argument(
            "--pack-dir",
            default=None,
            help="Directorio de packs generados a limpiar. Default: metadata.generated_pack_dir.",
        )
    _add_apply_args(parser)


def _add_received_metadata_packs_target(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "received-metadata-packs",
        allow_abbrev=False,
        help="Limpia metadata packs recibidos en el distributed metadata pack store.",
    )
    parser.set_defaults(command="received-metadata-packs")
    add_config_args(parser)
    parser.add_argument(
        "--pack-store",
        default=None,
        help="Directorio del distributed metadata pack store. Default: metadata.custody_pack_store_dir.",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=None,
        help="Sobrescribe gc.received_metadata_pack_max_age_days. 0 desactiva borrado por edad.",
    )
    _add_apply_args(parser)


def _add_recovered_metadata_packs_target(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "recovered-metadata-packs",
        allow_abbrev=False,
        help="Limpia metadata packs descargados por metadata pack recover.",
    )
    parser.set_defaults(command="recovered-metadata-packs")
    add_config_args(parser)
    parser.add_argument(
        "--object-store",
        default=None,
        help="Metadata object store usado durante recover. Default: metadata.object_store_dir.",
    )
    parser.add_argument(
        "--pack-dir",
        default=None,
        help="Directorio de packs recuperados. Default: metadata.recovered_pack_dir.",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=None,
        help="Sobrescribe gc.recovered_metadata_pack_max_age_days. 0 desactiva borrado por edad.",
    )
    _add_apply_args(parser)


def _add_all_target(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "all",
        allow_abbrev=False,
        help="Ejecuta todos los GC locales configurados.",
        description="Ejecuta todos los GC locales configurados.",
    )
    parser.set_defaults(command="all")
    add_config_args(parser)
    parser.add_argument(
        "--passphrase-file",
        default=None,
        help="Lee la passphrase para metadata graph/packs generados desde un fichero privado.",
    )
    parser.add_argument(
        "--identity-file",
        default=None,
        help="Identity file para metadata packs generados. Default: metadata.identity_file.",
    )
    _add_apply_args(parser)


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if hasattr(args, "max_age_days"):
        validate_int_ranges(parser, args, (IntRange("max_age_days", "--max-age-days", 0),))
    if hasattr(args, "grace_hours"):
        validate_float_ranges(parser, args, (FloatRange("grace_hours", "--grace-hours", 0.0),))
    ranges = []
    if hasattr(args, "object_grace_hours"):
        ranges.append(FloatRange("object_grace_hours", "--object-grace-hours", 0.0))
    if hasattr(args, "pack_grace_hours"):
        ranges.append(FloatRange("pack_grace_hours", "--pack-grace-hours", 0.0))
    if ranges:
        validate_float_ranges(parser, args, tuple(ranges))


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        handler = _command_handlers()[args.command]
    except KeyError as exc:
        raise StopanUsageError(f"Target GC desconocido: {args.command!r}") from exc
    return handler(args)


def _command_handlers() -> dict[str, GcCommandHandler]:
    return {
        "generated-chunks": _cmd_generated_chunks,
        "received-chunks": _cmd_received_chunks,
        "received-ec": _cmd_received_ec,
        "generated-metadata-graph": _cmd_metadata_object_store,
        "generated-metadata-packs": _cmd_metadata_object_store,
        "received-metadata-packs": _cmd_received_metadata_packs,
        "recovered-metadata-packs": _cmd_recovered_metadata_packs,
        "all": _cmd_all,
    }


def _cmd_generated_chunks(args: argparse.Namespace) -> int:
    from stopan.cas.gc import collect_cas_chunks
    from stopan.metadata.database import MetadataDB, MetadataDBAccessMode

    cfg = load_runtime_config(args)
    db = MetadataDB(
        cfg.node.catalog_file,
        init_schema=False,
        access_mode=MetadataDBAccessMode.READ_ONLY,
    )
    try:
        reachable_hashes = db.gc_protected_chunk_hashes()
    finally:
        db.close()

    with _CandidateSpool(enabled=bool(args.dry_run)) as candidates:
        result = collect_cas_chunks(
            target=args.command,
            root_dir=args.chunk_store or cfg.storage.local_chunk_dir,
            max_age_seconds=int(float(choose(args.grace_hours, cfg.gc.generated_chunk_grace_hours)) * _SECONDS_PER_HOUR),
            dry_run=bool(args.dry_run),
            reachable_hashes=reachable_hashes,
            candidate_reporter=candidates.report,
        )
        _print_local_file_result(result, candidates=candidates)
    return 0


def _cmd_received_chunks(args: argparse.Namespace) -> int:
    from stopan.cas.gc import collect_cas_chunks

    cfg = load_runtime_config(args)
    max_age_days = int(choose(args.max_age_days, cfg.gc.received_chunk_max_age_days))
    with _CandidateSpool(enabled=bool(args.dry_run)) as candidates:
        result = collect_cas_chunks(
            target=args.command,
            root_dir=args.chunk_store or (Path(cfg.storage.custody_dir) / "chunks"),
            max_age_seconds=None if max_age_days == 0 else max_age_days * _SECONDS_PER_DAY,
            dry_run=bool(args.dry_run),
            candidate_reporter=candidates.report,
        )
        _print_local_file_result(result, candidates=candidates)
    return 0


def _cmd_received_ec(args: argparse.Namespace) -> int:
    from stopan.node.storage.ec_gc import collect_ec_shards

    cfg = load_runtime_config(args)
    max_age_days = int(choose(args.max_age_days, cfg.gc.received_ec_max_age_days))
    with _CandidateSpool(enabled=bool(args.dry_run)) as candidates:
        result = collect_ec_shards(
            target=args.command,
            root_dir=args.ec_store or (Path(cfg.storage.custody_dir) / "ec_shards"),
            max_age_seconds=None if max_age_days == 0 else max_age_days * _SECONDS_PER_DAY,
            dry_run=bool(args.dry_run),
            candidate_reporter=candidates.report,
        )
        _print_local_file_result(result, candidates=candidates)
    return 0


def _cmd_metadata_object_store(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    object_store_dir = getattr(args, "object_store", None) or cfg.metadata.object_store_dir
    if not object_store_dir:
        raise StopanUsageError("Se requiere --object-store o metadata.object_store_dir.")

    object_store_root = Path(object_store_dir).expanduser().resolve()
    if not (object_store_root / "store.json").is_file():
        _print_metadata_object_gc_skipped(
            args.command,
            object_store_root,
            reason="metadata object store no inicializado: falta store.json",
        )
        return 0

    from stopan.cli.metadata_helpers import (
        identity_file_from_args,
        passphrase_for_decrypt,
        scrypt_cost_from_config,
    )
    from stopan.metadata.objects.gc import MetadataObjectGarbageCollector

    include_objects = bool(args.include_objects)
    include_packs = bool(args.include_packs)
    pack_dir = (getattr(args, "pack_dir", None) or cfg.metadata.generated_pack_dir) if include_packs else None
    identity_file = identity_file_from_args(args, cfg) if include_packs else (cfg.metadata.identity_file or "")
    object_grace_default = cfg.gc.generated_metadata_graph_grace_hours
    object_grace_hours = float(choose(getattr(args, "object_grace_hours", None), object_grace_default))
    pack_grace_hours = float(
        choose(getattr(args, "pack_grace_hours", None), cfg.gc.generated_metadata_pack_grace_hours)
    )

    collector = MetadataObjectGarbageCollector(scrypt_cost=scrypt_cost_from_config(cfg))
    with _CandidateSpool(enabled=bool(args.dry_run)) as candidates:
        result = collector.collect(
            object_store_dir=object_store_root,
            identity_file=identity_file,
            passphrase=passphrase_for_decrypt(args, cfg),
            object_grace_seconds=int(max(object_grace_hours, 0.0) * _SECONDS_PER_HOUR),
            pack_grace_seconds=int(max(pack_grace_hours, 0.0) * _SECONDS_PER_HOUR),
            dry_run=bool(args.dry_run),
            include_objects=include_objects,
            include_packs=include_packs,
            pack_dir=pack_dir,
            candidate_reporter=candidates.report,
        )
        _print_metadata_object_gc_result(args.command, result, candidates=candidates)
    return 0


def _cmd_received_metadata_packs(args: argparse.Namespace) -> int:
    from stopan.metadata.packs.gc import collect_received_metadata_packs

    cfg = load_runtime_config(args)
    with _CandidateSpool(enabled=bool(args.dry_run)) as candidates:
        result = collect_received_metadata_packs(
            cfg=cfg,
            pack_store=args.pack_store,
            max_age_days=args.max_age_days,
            dry_run=bool(args.dry_run),
            candidate_reporter=candidates.report,
        )
        _print_received_metadata_pack_result(result, cfg=cfg, candidates=candidates)
    return 0


def _cmd_recovered_metadata_packs(args: argparse.Namespace) -> int:
    from stopan.metadata.packs.gc import collect_recovered_metadata_packs

    cfg = load_runtime_config(args)
    with _CandidateSpool(enabled=bool(args.dry_run)) as candidates:
        result = collect_recovered_metadata_packs(
            cfg=cfg,
            object_store=args.object_store,
            pack_dir=args.pack_dir,
            max_age_days=args.max_age_days,
            dry_run=bool(args.dry_run),
            candidate_reporter=candidates.report,
        )
        _print_local_file_result(result, candidates=candidates)
    return 0


def _cmd_all(args: argparse.Namespace) -> int:
    targets = list(_all_target_args(args))
    for index, target_args in enumerate(targets, start=1):
        if index > 1:
            print()
        print(f"[{index}/{len(targets)}]")
        handler = _command_handlers()[target_args.command]
        handler(target_args)
    return 0


def _all_target_args(args: argparse.Namespace) -> list[argparse.Namespace]:
    base = {
        "config": getattr(args, "config", None),
        "dry_run": bool(args.dry_run),
        "passphrase_file": getattr(args, "passphrase_file", None),
        "identity_file": getattr(args, "identity_file", None),
    }
    targets = [
        argparse.Namespace(**base, command="generated-chunks", chunk_store=None, grace_hours=None),
        argparse.Namespace(**base, command="received-chunks", chunk_store=None, max_age_days=None),
        argparse.Namespace(**base, command="received-ec", ec_store=None, max_age_days=None),
        argparse.Namespace(
            **base,
            command="generated-metadata-graph",
            object_store=None,
            object_grace_hours=None,
            include_objects=True,
            include_packs=False,
        ),
        argparse.Namespace(
            **base,
            command="generated-metadata-packs",
            object_store=None,
            object_grace_hours=None,
            pack_grace_hours=None,
            pack_dir=None,
            include_objects=False,
            include_packs=True,
        ),
        argparse.Namespace(**base, command="received-metadata-packs", pack_store=None, max_age_days=None),
        argparse.Namespace(**base, command="recovered-metadata-packs", object_store=None, pack_dir=None, max_age_days=None),
    ]
    return targets


def _print_local_file_result(
    result: LocalFileGarbageCollectionResult,
    *,
    candidates: "_CandidateSpool | None" = None,
) -> None:
    print(f"GC {result.target}")
    print(f"   root_dir: {result.root_dir}")
    print(f"   dry_run: {result.dry_run}")
    print(f"   enabled_by_age: {result.enabled}")
    print(f"   max_age_seconds: {result.max_age_seconds if result.max_age_seconds is not None else 'disabled'}")
    print(f"   age_cutoff: {_format_optional_time(result.cutoff_unix)}")
    print("Scan")
    print(f"   files_seen: {result.files_seen}")
    print(f"   files_retained_by_policy: {result.files_retained_by_policy}")
    print(f"   files_skipped_by_age: {result.files_skipped_by_age}")
    print("Prune")
    print(f"   files_collectable: {result.files_collectable}")
    print(f"   bytes_collectable: {format_bytes(result.bytes_collectable)}")
    print(f"   files_deleted: {result.files_deleted}")
    print(f"   bytes_deleted: {format_bytes(result.bytes_deleted)}")
    if candidates is not None:
        candidates.print_items()
    _print_items("Errors / skipped items", result.errors)
    _print_dry_run_hint(result.dry_run)


def _print_metadata_object_gc_skipped(target: str, object_store_dir: Path, *, reason: str) -> None:
    print(f"GC {target}")
    print(f"   object_store: {object_store_dir}")
    print("   skipped: True")
    print(f"   reason: {reason}")


def _print_metadata_object_gc_result(
    target: str,
    result,
    *,
    candidates: "_CandidateSpool | None" = None,
) -> None:
    print(f"GC {target}")
    print(f"   object_store: {result.root_dir}")
    print(f"   catalog_hash: {result.catalog_hash}")
    print(f"   dry_run: {result.dry_run}")
    print(f"   object_grace_seconds: {result.object_grace_seconds}")
    print(f"   pack_grace_seconds: {result.pack_grace_seconds}")
    print("Objects")
    print(f"   live_objects: {result.live_objects}")
    print(f"   object_files_seen: {result.object_files_seen}")
    print(f"   object_files_live: {result.object_files_live}")
    print(f"   object_files_collectable: {result.object_files_collectable}")
    print(f"   object_files_deleted: {result.object_files_deleted}")
    print(f"   object_files_skipped_by_grace: {result.object_files_skipped_by_grace}")
    print(f"   object_files_malformed: {result.object_files_malformed}")
    print(f"   object_bytes_collectable: {format_bytes(result.object_bytes_collectable)}")
    print(f"   object_bytes_deleted: {format_bytes(result.object_bytes_deleted)}")
    print("Packs")
    print(f"   pack_dir: {result.pack_dir if result.pack_dir is not None else '(not used)'}")
    print(f"   pack_files_seen: {result.pack_files_seen}")
    print(f"   pack_files_latest: {result.pack_files_latest}")
    print(f"   pack_files_collectable: {result.pack_files_collectable}")
    print(f"   pack_files_deleted: {result.pack_files_deleted}")
    print(f"   pack_files_skipped_by_grace: {result.pack_files_skipped_by_grace}")
    print(f"   pack_files_unreadable: {result.pack_files_unreadable}")
    print(f"   pack_bytes_collectable: {format_bytes(result.pack_bytes_collectable)}")
    print(f"   pack_bytes_deleted: {format_bytes(result.pack_bytes_deleted)}")
    if candidates is not None:
        candidates.print_items()
    _print_items("Errors / skipped items", result.errors)
    _print_dry_run_hint(result.dry_run)


def _print_received_metadata_pack_result(
    result,
    *,
    cfg,
    candidates: "_CandidateSpool | None" = None,
) -> None:
    print("GC received-metadata-packs")
    print(f"   pack_store: {result.root_dir}")
    print(f"   dry_run: {result.dry_run}")
    print(f"   max_age_days: {result.max_age_days}")
    print(f"   age_cutoff: {_format_optional_time(result.cutoff_unix)}")
    print("Limits")
    print(f"   max_pack_bytes: {format_bytes(int(cfg.metadata.max_distributed_pack_bytes))}")
    print(f"   max_packs_per_owner: {int(cfg.metadata.max_distributed_packs_per_owner)}")
    print(f"   max_bytes_per_owner: {format_bytes(int(cfg.metadata.max_distributed_pack_bytes_per_owner))}")
    print(f"   max_store_bytes: {format_bytes(int(cfg.metadata.max_distributed_pack_store_bytes))}")
    print("Scan")
    print(f"   packs_seen: {result.packs_seen}")
    print(f"   owners_seen: {result.owners_seen}")
    print("Prune")
    print(f"   expired_packs: {result.expired_packs}")
    print(f"   quota_packs: {result.quota_packs}")
    print(f"   pruned_packs: {result.pruned_packs}")
    print(f"   pruned_bytes: {format_bytes(result.pruned_bytes)}")
    if candidates is not None:
        candidates.print_items()
    _print_items("Errors / skipped items", result.errors)
    _print_dry_run_hint(result.dry_run)



def format_time(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).astimezone().isoformat(timespec="seconds")


def _format_optional_time(ts: float | None) -> str:
    return "disabled" if ts is None else format_time(float(ts))


def _print_items(title: str, items: Sequence[str] | tuple[str, ...], *, limit: int = 20) -> None:
    values = tuple(items or ())
    if not values:
        return
    print(title)
    for item in values[:limit]:
        print(f"- {item}")
    remaining = len(values) - limit
    if remaining > 0:
        print(f"- ... {remaining} elementos más omitidos")


def _print_dry_run_hint(dry_run: bool) -> None:
    if dry_run:
        print("\nDry-run activo: no se borró nada. Usa --apply para ejecutar el borrado real.")


class _CandidateSpool:
    """Acumula el listado de dry-run sin retenerlo completo en memoria."""

    def __init__(self, *, enabled: bool, max_memory_bytes: int = 1024 * 1024):
        self.enabled = bool(enabled)
        self._count = 0
        self._spool = tempfile.SpooledTemporaryFile(
            mode="w+t",
            max_size=max_memory_bytes,
            encoding="utf-8",
            newline="\n",
        )

    def __enter__(self) -> "_CandidateSpool":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._spool.close()

    def report(self, path: Path) -> None:
        if not self.enabled:
            return
        self._spool.write(f"{path}\n")
        self._count += 1

    def print_items(self) -> None:
        if not self.enabled or self._count == 0:
            return
        print("Candidates")
        self._spool.seek(0)
        for line in self._spool:
            print(f"- {line.rstrip()}")


if __name__ == "__main__":
    raise SystemExit(main())
