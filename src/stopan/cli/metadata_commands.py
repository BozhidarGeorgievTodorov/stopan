from __future__ import annotations

import argparse
from pathlib import Path

from stopan.cli.config_utils import choose, first_seed, load_runtime_config
from stopan.errors import StopanDataError, StopanStorageError, StopanUsageError
from stopan.cli.metadata_helpers import (
    distributed_pack_store_from_args,
    dir_status,
    file_status,
    format_bytes,
    format_time,
    identity_file_from_args,
    object_graph_service_from_config,
    object_graph_service_from_config_defaults,
    object_pack_service_from_config,
    object_pack_service_from_config_defaults,
    object_store_dir_from_args,
    owner_id_from_args,
    passphrase_for_decrypt,
    passphrase_for_object_store_export,
    scrypt_cost_from_args,
)
from stopan.metadata.identity import (
    load_metadata_identity_file,
    sign_metadata_pack_hash,
    validate_owner_id,
)
from stopan.metadata.packs.hashes import calculate_pack_hash, calculate_pack_hash_file, validate_pack_hash
from stopan.metadata.packs.object_pack import MetadataObjectPackService
from stopan.metadata.database import MetadataDB, MetadataDBAccessMode


def _configured_warning_limit(cfg) -> int:
    return max(1, int(cfg.metadata.cli_warning_limit))


def _metadata_pack_network_kwargs(args: argparse.Namespace, cfg) -> dict[str, object]:
    return {
        "membership_seed": args.membership_seed or first_seed(cfg),
        "self_addr": cfg.node.advertise_addr,
        "cluster_token": cfg.cluster.token,
        "membership_timeout_s": float(cfg.membership.rpc_timeout_s),
        "rpc_timeout_s": float(choose(args.rpc_timeout_s, cfg.metadata.pack_rpc_timeout_s)),
        "target_parallelism": int(choose(args.target_parallelism, cfg.metadata.pack_target_parallelism)),
        "max_message_bytes": int(choose(args.max_message_bytes, cfg.grpc.max_message_bytes)),
        "grpc_keepalive_time_ms": int(cfg.grpc.keepalive_time_ms),
        "grpc_keepalive_timeout_ms": int(cfg.grpc.keepalive_timeout_ms),
        "grpc_keepalive_permit_without_calls": bool(cfg.grpc.keepalive_permit_without_calls),
    }


def _metadata_pack_push_kwargs(args: argparse.Namespace, cfg) -> dict[str, object]:
    values = _metadata_pack_network_kwargs(args, cfg)
    values.update(
        max_pack_bytes=int(cfg.metadata.max_distributed_pack_bytes),
        rf=int(choose(args.pack_copies, cfg.metadata.pack_copies)),
        strict_rf=bool(choose(args.strict_pack_copies, cfg.metadata.strict_pack_copies)),
    )
    return values


def _print_limited_items(title: str, items, *, limit: int) -> None:
    values = tuple(items or ())
    if not values:
        return
    print(title)
    for item in values[:limit]:
        print(f"   - {item}")
    if len(values) > limit:
        print(f"   ... (+{len(values) - limit} más)")


def _format_desired_copies(value: int | None) -> str:
    return str(int(value)) if value is not None else "unknown"


def _print_local_pushed_at(local_pushed_at_by_hash, pack_hash: str) -> None:
    pushed_at = local_pushed_at_by_hash.get(pack_hash) if local_pushed_at_by_hash else None
    if pushed_at is not None:
        print(f"      local_pushed_at: {format_time(pushed_at)}")


def _print_metadata_pack_sources(sources) -> None:
    for source in sources:
        node = source.node_id[:8] or "unknown"
        stored_at = format_time(source.stored_at_unix) if source.stored_at_unix else "-"
        print(f"         - {node}@{source.address} stored_at={stored_at} size={format_bytes(source.size_bytes)}")


def _print_metadata_pack_discovery_entries(
    entries,
    *,
    show_sources: bool,
    local_pushed_at_by_hash=None,
) -> None:
    for entry in entries:
        print(f"   pack_hash: {entry.pack_hash}")
        print("      check: presence")
        print(f"      presence_state: {entry.presence_state.value}")
        print(f"      copies: {entry.copies_seen}/{_format_desired_copies(entry.desired_copies)}")
        print(f"      desired_copies_source: {entry.desired_copies_source}")
        if entry.desired_copies is None:
            print("      note: no hay publicación local con copias esperadas; estado UNKNOWN")
        print(f"      size: {format_bytes(entry.size_bytes)}")
        _print_local_pushed_at(local_pushed_at_by_hash, entry.pack_hash)
        if entry.newest_stored_at_unix:
            print(f"      newest_stored_at: {format_time(entry.newest_stored_at_unix)}")
        if entry.oldest_stored_at_unix and entry.oldest_stored_at_unix != entry.newest_stored_at_unix:
            print(f"      oldest_stored_at: {format_time(entry.oldest_stored_at_unix)}")
        if show_sources:
            print("      sources:")
            _print_metadata_pack_sources(entry.sources)


def _print_metadata_pack_verification_results(
    results,
    *,
    show_sources: bool,
    local_pushed_at_by_hash=None,
) -> None:
    for result in results:
        print(f"   pack_hash: {result.pack_hash}")
        print(f"      check: {result.details.check_kind}")
        print(f"      presence_state: {result.state.value}")
        print(f"      copies: {result.copies_seen}/{_format_desired_copies(result.desired_copies)}")
        print(f"      desired_copies_source: {result.details.desired_copies_source}")
        if result.details.reason:
            print(f"      reason: {result.details.reason}")
        if result.size_bytes:
            print(f"      size: {format_bytes(result.size_bytes)}")
        _print_local_pushed_at(local_pushed_at_by_hash, result.pack_hash)
        if result.newest_stored_at_unix:
            print(f"      newest_stored_at: {format_time(result.newest_stored_at_unix)}")
        if result.oldest_stored_at_unix and result.oldest_stored_at_unix != result.newest_stored_at_unix:
            print(f"      oldest_stored_at: {format_time(result.oldest_stored_at_unix)}")
        if show_sources and result.sources:
            print("      sources:")
            _print_metadata_pack_sources(result.sources)


def cmd_status(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)

    print("Metadata vault status")
    print(f"   db_file: {cfg.node.db_file}")

    print("Incremental metadata object graph")
    print(f"   auto_export: {bool(cfg.metadata.object_graph_auto_export)}")
    print(f"   object_store_dir: {cfg.metadata.object_store_dir}")
    print(f"   object_store_dir_status: {dir_status(cfg.metadata.object_store_dir)}")
    print(f"   include_protection: {bool(cfg.metadata.object_graph_include_protection)}")
    print(f"   auto_pack: {bool(cfg.metadata.object_graph_auto_pack)}")
    print(f"   pack_dir: {cfg.metadata.object_pack_dir or '<object_store_dir>/packs'}")

    print("Metadata secret material")
    print(f"   passphrase_file: {cfg.metadata.passphrase_file or '(not configured)'}")
    print(f"   passphrase_file_status: {file_status(cfg.metadata.passphrase_file)}")
    print(
        "   scrypt: "
        f"n={cfg.metadata.scrypt_n} r={cfg.metadata.scrypt_r} "
        f"p={cfg.metadata.scrypt_p} key_length={cfg.metadata.key_length}"
    )

    identity_owner_id = None
    identity_owner_error = None
    if cfg.metadata.identity_file:
        identity_path = Path(cfg.metadata.identity_file).expanduser()
        if identity_path.is_file():
            try:
                identity_owner_id = load_metadata_identity_file(identity_path).owner_id
            except Exception as exc:
                identity_owner_error = str(exc)

    effective_owner_id = cfg.metadata.owner_id or identity_owner_id or ""

    print("Metadata identity / distribution")
    print(f"   owner_id: {effective_owner_id or '(not configured)'}")
    print(f"   owner_id_source: {'metadata.owner_id' if cfg.metadata.owner_id else ('identity_file' if identity_owner_id else 'missing')}")
    if cfg.metadata.owner_id and identity_owner_id and cfg.metadata.owner_id != identity_owner_id:
        print(f"   owner_id_warning: metadata.owner_id no coincide con identity_file ({identity_owner_id})")
    if identity_owner_error:
        print(f"   identity_owner_id_error: {identity_owner_error}")
    print(f"   identity_file: {cfg.metadata.identity_file or '(not configured)'}")
    print(f"   identity_file_status: {file_status(cfg.metadata.identity_file)}")
    print(f"   distributed_pack_store_dir: {cfg.metadata.distributed_pack_store_dir}")
    print(f"   distributed_pack_store_status: {dir_status(cfg.metadata.distributed_pack_store_dir)}")
    print(f"   pack_copies: {int(cfg.metadata.pack_copies)}")
    print(f"   strict_pack_copies: {bool(cfg.metadata.strict_pack_copies)}")
    print(f"   pack_discovery_max_candidates: {int(cfg.metadata.pack_discovery_max_candidates)}")
    print(f"   cli_warning_limit: {int(cfg.metadata.cli_warning_limit)}")
    print(f"   max_distributed_pack_bytes: {format_bytes(int(cfg.metadata.max_distributed_pack_bytes))}")
    print(f"   max_distributed_packs_per_owner: {int(cfg.metadata.max_distributed_packs_per_owner)}")
    print(f"   max_distributed_pack_bytes_per_owner: {format_bytes(int(cfg.metadata.max_distributed_pack_bytes_per_owner))}")
    print(f"   max_distributed_pack_store_bytes: {format_bytes(int(cfg.metadata.max_distributed_pack_store_bytes))}")

    print("Metadata graph GC policy")
    print(f"   generated_graph_grace_hours: {cfg.gc.generated_metadata_graph_grace_hours}")
    print(f"   generated_pack_grace_hours: {cfg.gc.generated_metadata_pack_grace_hours}")

    print("Received metadata pack store GC policy")
    print(f"   max_age_days: {cfg.gc.received_metadata_pack_max_age_days}")

    print("Recovered metadata pack GC policy")
    print(f"   max_age_days: {cfg.gc.recovered_metadata_pack_max_age_days}")

    print("\nHow backup/push/verify decide whether to update the metadata vault:")
    print("   1. --metadata-object-store implies object graph export for that command")
    print("   2. --metadata-object-graph enables it for that command")
    print("   3. --no-metadata-object-graph disables it for that command")
    print("   4. otherwise metadata.object_graph_auto_export decides")
    print("\nHow pack creation is decided after object graph export:")
    print("   1. --metadata-object-pack enables pack creation for that command")
    print("   2. --no-metadata-object-pack disables it for that command")
    print("   3. --metadata-object-pack-dir implies pack creation")
    print("   4. otherwise metadata.object_graph_auto_pack decides")
    return 0

def cmd_identity_show(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)

    if args.owner_id:
        owner_id = validate_owner_id(args.owner_id, name="--owner-id")
        print("Metadata identity")
        print("   source: --owner-id")
        print(f"   owner_id: {owner_id}")
        return 0

    if cfg.metadata.owner_id:
        owner_id = validate_owner_id(cfg.metadata.owner_id, name="metadata.owner_id")
        print("Metadata identity")
        print("   source: metadata.owner_id")
        print(f"   owner_id: {owner_id}")
        return 0

    identity_file = identity_file_from_args(args, cfg)
    identity = load_metadata_identity_file(identity_file)
    print("Metadata identity")
    print(f"   source: {identity.path}")
    print(f"   algorithm: {identity.algorithm}")
    print(f"   owner_id: {identity.owner_id}")
    print(f"   signing_public_key_b64: {identity.signing_public_key_b64}")
    print(f"   encryption_public_key_b64: {identity.encryption_public_key_b64}")
    print(f"   private_keys: {'encrypted' if identity.private_keys_encrypted else 'not encrypted'}")
    print(f"   created_at: {identity.created_at}")
    return 0


def cmd_local_store_pack(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    owner_id = owner_id_from_args(args, cfg)
    identity_file = identity_file_from_args(args, cfg)
    pack_path = Path(args.path).expanduser().resolve()
    try:
        calculated_pack_hash = validate_pack_hash(calculate_pack_hash_file(pack_path))
    except OSError as exc:
        raise StopanStorageError(f"No se pudo leer el metadata pack {pack_path}: {exc}") from exc
    if args.expected_pack_hash is not None and validate_pack_hash(args.expected_pack_hash) != calculated_pack_hash:
        raise StopanDataError(
            f"pack_hash esperado no coincide con fichero: "
            f"esperado={args.expected_pack_hash} calculado={calculated_pack_hash}"
        )
    signed_owner_id, public_key_b64, signature_b64 = sign_metadata_pack_hash(
        identity_file=identity_file,
        passphrase=passphrase_for_decrypt(args, cfg),
        pack_hash=calculated_pack_hash,
        expected_owner_id=owner_id,
    )
    store = distributed_pack_store_from_args(args, cfg)
    result = store.put_pack_file(
        owner_id=signed_owner_id,
        path=args.path,
        expected_pack_hash=calculated_pack_hash,
        public_key_b64=public_key_b64,
        signature_b64=signature_b64,
    )

    print("Metadata pack guardado en pack store local")
    print(f"   owner_id: {result.owner_id}")
    print(f"   pack_hash: {result.pack_hash}")
    print(f"   path: {result.path}")
    print(f"   size: {format_bytes(result.size_bytes)}")
    print(f"   stored: {result.stored}")
    print(f"   already_present: {result.already_present}")
    return 0


def cmd_local_list_packs(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    owner_id = owner_id_from_args(args, cfg)
    store = distributed_pack_store_from_args(args, cfg)
    records = store.list_packs(owner_id=owner_id)

    print("Local distributed metadata packs")
    print(f"   owner_id: {owner_id}")
    print(f"   pack_store: {store.root_dir}")
    if not records:
        print("   (none)")
        return 0

    for record in records:
        print(f"- {record.pack_hash}")
        print(f"   path: {record.path}")
        print(f"   size: {format_bytes(record.size_bytes)}")
        print(f"   stored_at: {format_time(record.stored_at_unix)}")
        print(f"   signed: {bool(getattr(record, 'signed', False))}")
    return 0


def cmd_local_retrieve_pack(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    owner_id = owner_id_from_args(args, cfg)
    pack_hash = validate_pack_hash(args.pack_hash)
    store = distributed_pack_store_from_args(args, cfg)
    out_path = store.retrieve_pack_to_file(
        owner_id=owner_id,
        pack_hash=pack_hash,
        out_path=args.out,
    )
    try:
        size = out_path.stat().st_size
        calculated = calculate_pack_hash(out_path.read_bytes())
    except OSError as exc:
        raise StopanStorageError(f"No se pudo verificar el metadata pack recuperado {out_path}: {exc}") from exc
    if calculated != pack_hash:
        raise StopanDataError(f"pack_hash recuperado no coincide: esperado={pack_hash} calculado={calculated}")

    print("Metadata pack recuperado del pack store local")
    print(f"   owner_id: {owner_id}")
    print(f"   pack_hash: {pack_hash}")
    print(f"   out: {out_path}")
    print(f"   size: {format_bytes(size)}")
    return 0


def cmd_object_store_status(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    object_store_dir = object_store_dir_from_args(args, cfg)
    service = object_graph_service_from_config_defaults(cfg)
    inspection = service.inspect_store(
        object_store_dir=object_store_dir,
        passphrase=(passphrase_for_decrypt(args, cfg) if args.decrypt_latest else None),
        decrypt_latest=bool(args.decrypt_latest),
    )

    header = inspection.header
    print("Metadata object store")
    print(f"   root_dir: {header.root_dir}")
    print(f"   object_count_on_disk: {header.object_count_on_disk}")
    print(f"   has_latest: {header.has_latest}")
    print(
        "   scrypt: "
        f"n={header.scrypt_n} r={header.scrypt_r} "
        f"p={header.scrypt_p} key_length={header.key_length}"
    )

    if inspection.latest is not None:
        latest = inspection.latest
        print("Latest metadata state")
        print(f"   vault_id: {latest.vault_id}")
        print(f"   catalog_hash: {latest.catalog_hash}")
        print(f"   state_digest: {latest.state_digest}")
        print(f"   objects: {latest.object_count}")
        print(f"   canonical_bytes: {format_bytes(latest.total_canonical_bytes)}")
        print(f"   snapshots: {latest.snapshot_count}")
        print(f"   known_chunks: {latest.known_chunk_count}")
        print(f"   protection_records: {latest.protection_record_count}")

    return 0


def cmd_export_object_graph(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    object_store_dir = object_store_dir_from_args(args, cfg)
    passphrase = passphrase_for_object_store_export(args, object_store_dir=object_store_dir, cfg=cfg)

    service = object_graph_service_from_config(args, cfg)
    result = service.export_current_state(
        object_store_dir=object_store_dir,
        passphrase=passphrase,
        include_protection=not bool(args.no_protection),
    )

    print("Metadata object graph exportado")
    print(f"   object_store: {result.root_dir}")
    print(f"   vault_id: {result.vault_id}")
    print(f"   catalog_hash: {result.catalog_hash}")
    print(f"   state_digest: {result.state_digest}")
    stats = result.stats
    print(f"   objects_total: {stats.objects_total}")
    print(f"   objects_written: {stats.objects_written}")
    print(f"   objects_reused: {stats.objects_reused}")
    print(f"   canonical_bytes: {format_bytes(stats.total_canonical_bytes)}")
    print(f"   snapshots: {stats.snapshot_count}")
    print(f"   known_chunks: {stats.known_chunk_count}")
    print(f"   protection_records: {stats.protection_record_count}")

    if bool(args.pack):
        pack_service = object_pack_service_from_config(args, cfg)
        pack_dir = args.pack_dir or cfg.metadata.object_pack_dir or None
        pack_result = pack_service.export_latest_pack(
            object_store_dir=object_store_dir,
            passphrase=passphrase,
            identity_file=identity_file_from_args(args, cfg),
            out_path=args.pack_out,
            pack_dir=pack_dir,
        )

        print("Metadata object pack creado")
        print(f"   path: {pack_result.path}")
        print(f"   pack_hash: {pack_result.pack_hash}")
        print(f"   vault_id: {pack_result.vault_id}")
        print(f"   vault_generation: {pack_result.vault_generation}")
        print(f"   pack_created_at: {format_time(pack_result.pack_created_at_unix)}")
        print(f"   catalog_hash: {pack_result.catalog_hash}")
        print(f"   state_digest: {pack_result.state_digest}")
        pack_stats = pack_result.stats
        print(f"   objects_packed: {pack_stats.objects_packed}")
        print(f"   canonical_bytes: {format_bytes(pack_stats.total_canonical_bytes)}")
        print(f"   pack_plaintext: {format_bytes(pack_stats.plaintext_bytes)}")
        print(f"   compressed: {format_bytes(pack_stats.compressed_bytes)}")
        print(f"   ciphertext: {format_bytes(pack_stats.ciphertext_bytes)}")
        print(f"   snapshots: {pack_stats.snapshot_count}")
        print(f"   known_chunks: {pack_stats.known_chunk_count}")
        print(f"   protection_records: {pack_stats.protection_record_count}")

    return 0

def cmd_import_object_graph(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    object_store_dir = object_store_dir_from_args(args, cfg)
    service = object_graph_service_from_config_defaults(cfg)
    result = service.import_latest_state(
        object_store_dir=object_store_dir,
        passphrase=passphrase_for_decrypt(args, cfg),
        include_protection=not bool(args.no_protection),
        default_desired_rf=int(
            args.default_desired_remote_copies
            if args.default_desired_remote_copies is not None
            else cfg.protection.remote_copies
        ),
    )

    print("Metadata object graph importado")
    print(f"   db_file: {result.db_file}")
    print(f"   object_store: {result.object_store_dir}")
    print(f"   vault_id: {result.vault_id}")
    print(f"   catalog_hash: {result.catalog_hash}")
    print(f"   state_digest: {result.state_digest}")
    stats = result.stats
    print(f"   objects_read: {stats.objects_read}")
    print(f"   canonical_bytes: {format_bytes(stats.total_canonical_bytes)}")
    print(f"   snapshots_imported: {stats.snapshots_imported}")
    print(f"   complete_snapshots_imported: {stats.complete_snapshots_imported}")
    print(f"   items_imported: {stats.items_imported}")
    print(
        f"   recipes: imported={stats.recipes_imported} "
        f"reused={stats.recipes_reused}"
    )
    print(f"   known_chunks_imported: {stats.chunks_imported}")
    print(f"   protection_records_imported: {stats.protection_records_imported}")
    print(f"   pending_protection_records_created: {stats.pending_protection_records_created}")
    print(f"   erasure_data_packs_imported: {stats.erasure_data_packs_imported}")
    print(f"   erasure_pack_chunks_imported: {stats.erasure_pack_chunks_imported}")
    print(f"   erasure_pack_shards_imported: {stats.erasure_pack_shards_imported}")
    return 0



def cmd_list_object_packs(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)

    if args.pack_dir:
        pack_dir = Path(args.pack_dir).expanduser().resolve()
    elif args.object_store:
        pack_dir = (Path(args.object_store).expanduser() / "packs").resolve()
    else:
        pack_dir = (Path(cfg.metadata.object_store_dir).expanduser() / "packs").resolve()

    if not pack_dir.exists():
        raise StopanUsageError(f"Directorio de metadata object packs no existe: {pack_dir}")
    if not pack_dir.is_dir():
        raise StopanUsageError(f"No es un directorio de metadata object packs: {pack_dir}")

    service = object_pack_service_from_config_defaults(cfg)
    paths = sorted(pack_dir.glob("*.stopanmetapack"), key=lambda item: item.name)
    print(f"Metadata object packs in {pack_dir}")
    if not paths:
        print("   (none)")
        return 0

    for path in paths:
        try:
            inspection = service.inspect_pack_header(path)
            header = inspection.header
            stat_result = path.stat()
            print(f"- {path.name}")
            print(f"   path: {path}")
            print(f"   size: {format_bytes(stat_result.st_size)}")
            print(f"   mtime: {format_time(stat_result.st_mtime)}")
            print(f"   pack_hash: {header.pack_hash}")
            print(f"   format: {header.format} v{header.version}")
            print(f"   encryption: {header.encryption}")
            print(f"   recipients: {header.recipient_count}")
            print(f"   aead: {header.aead_name}")
            print(f"   compression: {header.compression}")
            print(f"   ciphertext: {format_bytes(header.ciphertext_bytes)}")
        except Exception as exc:
            print(f"- {path.name}")
            print(f"   path: {path}")
            print(f"   error: {exc}")

    return 0


def print_metadata_pack_push_result(result) -> int:
    print("Metadata pack push finalizado" if result.protected else "Metadata pack push incompleto")
    print(f"   owner_id: {result.owner_id}")
    print(f"   pack_hash: {result.pack_hash}")
    print(f"   pack_path: {result.pack_path}")
    print(f"   pack_size: {format_bytes(result.pack_size_bytes)}")
    stats = result.stats
    print(f"   pack_copies: {stats.desired_rf}")
    print(f"   remote_candidates: {stats.remote_candidates}")
    print(f"   attempted_targets: {stats.attempted_targets}")
    print(f"   successful_targets: {stats.successful_targets}")
    print(f"   stored_targets: {stats.stored_targets}")
    print(f"   already_present_targets: {stats.already_present_targets}")
    print(f"   failed_targets: {stats.failed_targets}")

    if stats.insufficient_remote_targets:
        print("   insufficient_remote_targets: true")

    if result.target_results:
        print("Targets")
        for item in result.target_results:
            state = "stored" if item.stored else "already_present" if item.already_present else "failed"
            print(f"- {item.address}")
            print(f"   node_id: {item.node_id or '(unknown)'}")
            print(f"   result: {state}")
            print(f"   status: {item.status}")
            print(f"   detail: {item.detail or 'ok'}")
            if item.size_bytes:
                print(f"   size: {format_bytes(item.size_bytes)}")
            if item.stored_at_unix:
                print(f"   stored_at: {format_time(item.stored_at_unix)}")

    return 0 if result.protected else 2

def find_reusable_latest_object_pack(
    *,
    cfg,
    pack_service: MetadataObjectPackService,
    object_store_dir: str,
    pack_dir: str | None,
    identity_file: str,
    passphrase: str | bytes,
):
    resolved_pack_dir = (
        Path(pack_dir).expanduser().resolve()
        if pack_dir is not None
        else (Path(object_store_dir).expanduser() / "packs").resolve()
    )

    if not resolved_pack_dir.exists():
        return None

    if not resolved_pack_dir.is_dir():
        raise StopanUsageError(f"No es un directorio de metadata packs: {resolved_pack_dir}")

    graph_service = object_graph_service_from_config_defaults(cfg)
    store_inspection = graph_service.inspect_store(
        object_store_dir=object_store_dir,
        passphrase=passphrase,
        decrypt_latest=True,
    )
    latest = store_inspection.latest
    if latest is None:
        raise StopanDataError(f"El metadata object store no tiene latest: {object_store_dir}")

    candidates = []

    for path in sorted(resolved_pack_dir.glob("*.stopanmetapack")):
        try:
            inspection = pack_service.inspect_pack_summary(
                path,
                identity_file=identity_file,
                passphrase=passphrase,
            )
            if inspection.decrypted is None:
                continue

            summary = inspection.decrypted
            if summary.catalog_hash != latest.catalog_hash:
                continue
            if summary.state_digest != latest.state_digest:
                continue

            candidates.append(
                (
                    int(summary.vault_generation),
                    str(inspection.header.pack_hash),
                    path,
                    summary,
                )
            )
        except Exception:
            # Pack ilegible, de otra identity, corrupto o antiguo: no bloquea push.
            continue

    if not candidates:
        return None

    _generation, _pack_hash, path, summary = max(
        candidates,
        key=lambda item: (item[0], item[1]),
    )
    return path, summary


def metadata_pack_publication_maps(cfg, *, owner_id: str) -> tuple[dict[str, int], dict[str, float]]:
    db = MetadataDB(
        cfg.node.db_file,
        init_schema=False,
        access_mode=MetadataDBAccessMode.READ_ONLY,
    )
    try:
        publications = db.get_metadata_pack_publications(owner_id=owner_id)
        return (
            {publication.pack_hash: int(publication.desired_copies) for publication in publications},
            {publication.pack_hash: float(publication.pushed_at) for publication in publications},
        )
    finally:
        db.close()


def metadata_pack_publication_maps_best_effort(
    cfg,
    *,
    owner_id: str,
) -> tuple[dict[str, int], dict[str, float], str | None]:
    """
    Consulta publicaciones locales sin bloquear el descubrimiento remoto.

    El estado persistido solo sirve para comparar las copias observadas con la
    intención local. Si SQLite no puede consultarse, discovery todavía puede
    listar y validar las referencias remotas, aunque la presencia quede UNKNOWN.
    """

    try:
        desired_copies_by_hash, pushed_at_by_hash = metadata_pack_publication_maps(
            cfg,
            owner_id=owner_id,
        )
    except Exception as exc:
        return (
            {},
            {},
            f"No se pudieron consultar las publicaciones locales: {exc}",
        )

    return desired_copies_by_hash, pushed_at_by_hash, None


def record_metadata_pack_publication(cfg, result) -> None:
    stats = result.stats
    desired = int(stats.desired_rf)
    if desired < 1:
        return

    db = MetadataDB(
        cfg.node.db_file,
        access_mode=MetadataDBAccessMode.READ_WRITE,
    )
    try:
        db.record_metadata_pack_publication(
            owner_id=result.owner_id,
            pack_hash=result.pack_hash,
            desired_copies=desired,
            pack_size_bytes=int(result.pack_size_bytes),
            attempted_targets=int(stats.attempted_targets),
            successful_targets=int(stats.successful_targets),
            stored_targets=int(stats.stored_targets),
            already_present_targets=int(stats.already_present_targets),
            failed_targets=int(stats.failed_targets),
        )
    finally:
        db.close()


def push_pack_result_from_path(args: argparse.Namespace, cfg, *, pack_path: str | Path):
    owner_id = owner_id_from_args(args, cfg)
    identity_file = identity_file_from_args(args, cfg)
    identity_passphrase = passphrase_for_decrypt(args, cfg)

    from stopan.metadata.packs.pusher import push_metadata_pack_to_network

    result = push_metadata_pack_to_network(
        pack_path=pack_path,
        owner_id=owner_id,
        identity_file=identity_file,
        identity_passphrase=identity_passphrase,
        **_metadata_pack_push_kwargs(args, cfg),
    )
    record_metadata_pack_publication(cfg, result)
    return result


def cmd_push(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)

    if args.pack_in:
        if args.object_store:
            raise StopanUsageError("'--pack-in' y '--object-store' son incompatibles.")
        if args.pack_out:
            raise StopanUsageError("'--pack-in' y '--pack-out' son incompatibles.")
        if args.pack_dir:
            raise StopanUsageError("'--pack-in' y '--pack-dir' son incompatibles.")

        result = push_pack_result_from_path(args, cfg, pack_path=args.pack_in)
        return print_metadata_pack_push_result(result)

    if args.pack_out and args.pack_dir:
        raise StopanUsageError("'--pack-out' y '--pack-dir' son incompatibles.")

    object_store_dir = args.object_store or cfg.metadata.object_store_dir
    if not object_store_dir:
        raise StopanUsageError("Se requiere --object-store o metadata.object_store_dir.")

    passphrase = passphrase_for_decrypt(args, cfg)
    identity_file = identity_file_from_args(args, cfg)
    pack_service = object_pack_service_from_config(args, cfg)
    pack_dir = args.pack_dir or cfg.metadata.object_pack_dir or None

    reusable = None
    if not args.pack_out:
        reusable = find_reusable_latest_object_pack(
            cfg=cfg,
            pack_service=pack_service,
            object_store_dir=object_store_dir,
            pack_dir=pack_dir,
            identity_file=identity_file,
            passphrase=passphrase,
        )

    if reusable is not None:
        pack_path, summary = reusable

        print("Reutilizando metadata object pack local para distribución")
        print(f"   path: {pack_path}")
        print(f"   vault_id: {summary.vault_id}")
        print(f"   vault_generation: {summary.vault_generation}")
        print(f"   pack_created_at: {format_time(summary.pack_created_at_unix)}")
        print(f"   catalog_hash: {summary.catalog_hash}")
        print(f"   state_digest: {summary.state_digest}")
        print(f"   objects_packed: {summary.object_count}")
        print("   ciphertext: reutilizado")

        result = push_pack_result_from_path(args, cfg, pack_path=pack_path)
        return print_metadata_pack_push_result(result)

    pack_result = pack_service.export_latest_pack(
        object_store_dir=object_store_dir,
        passphrase=passphrase,
        identity_file=identity_file,
        out_path=args.pack_out,
        pack_dir=pack_dir,
    )

    print("Metadata object pack creado para distribución")
    print(f"   path: {pack_result.path}")
    print(f"   pack_hash: {pack_result.pack_hash}")
    print(f"   vault_id: {pack_result.vault_id}")
    print(f"   vault_generation: {pack_result.vault_generation}")
    print(f"   pack_created_at: {format_time(pack_result.pack_created_at_unix)}")
    print(f"   catalog_hash: {pack_result.catalog_hash}")
    print(f"   state_digest: {pack_result.state_digest}")
    pack_stats = pack_result.stats
    print(f"   objects_packed: {pack_stats.objects_packed}")
    print(f"   ciphertext: {format_bytes(pack_stats.ciphertext_bytes)}")

    result = push_pack_result_from_path(args, cfg, pack_path=pack_result.path)
    return print_metadata_pack_push_result(result)


def cmd_recover(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    owner_id = owner_id_from_args(args, cfg)
    object_store_dir = args.object_store or cfg.metadata.object_store_dir
    if not object_store_dir and not args.download_only:
        raise StopanUsageError("Se requiere --object-store o metadata.object_store_dir.")
    if not object_store_dir and not args.pack_out and not args.download_dir:
        raise StopanUsageError(
            "Se requiere --pack-out, --download-dir o --object-store para guardar el pack recuperado."
        )

    passphrase = passphrase_for_decrypt(args, cfg)

    if args.vault_id is not None:
        from stopan.metadata.packs.payload import require_vault_id

        try:
            require_vault_id("vault_id", args.vault_id)
        except Exception as exc:
            raise StopanUsageError(str(exc)) from exc

    from stopan.metadata.packs.recover import recover_metadata_from_network

    result = recover_metadata_from_network(
        owner_id=owner_id,
        identity_file=identity_file_from_args(args, cfg),
        object_store_dir=object_store_dir,
        passphrase=passphrase,
        **_metadata_pack_network_kwargs(args, cfg),
        max_pack_bytes=int(cfg.metadata.max_distributed_pack_bytes),
        scrypt_cost=scrypt_cost_from_args(args, cfg),
        db_file=cfg.node.db_file,
        import_db=bool(args.import_db),
        include_protection=not bool(args.no_protection),
        download_only=bool(args.download_only),
        default_desired_rf=int(
            args.default_desired_remote_copies
            if args.default_desired_remote_copies is not None
            else cfg.protection.remote_copies
        ),
        download_dir=args.download_dir,
        pack_out=args.pack_out,
        target_hash=args.target_hash,
        vault_id=args.vault_id,
    )

    print("Metadata recover remoto completado")
    print(f"   owner_id: {result.owner_id}")
    if result.object_store_dir is not None:
        print(f"   object_store: {result.object_store_dir}")
    else:
        print("   object_store: skipped (--download-only)")
    stats = result.stats
    print(f"   list_targets: {stats.list_targets_succeeded}/{stats.list_targets_attempted}")
    print(f"   candidates_seen: {stats.candidates_seen}")
    print(f"   unique_packs_seen: {stats.unique_packs_seen}")
    print(f"   downloads_attempted: {stats.downloads_attempted}")
    if args.target_hash:
        print(f"   target_hash: {args.target_hash}")
    if args.vault_id:
        print(f"   target_vault_id: {args.vault_id}")
    print(f"   recovered_pack_hash: {result.recovered_pack_hash}")
    print(f"   vault_id: {result.vault_id}")
    print(f"   recovered_pack_path: {result.recovered_pack_path}")
    print(f"   recovered_from: {result.recovered_from_node_id[:8] or 'unknown'}@{result.recovered_from_address}")
    if result.remote_stored_at_unix:
        print(f"   remote_stored_at: {format_time(result.remote_stored_at_unix)}")
    print(f"   vault_generation: {result.vault_generation}")
    if result.pack_created_at_unix:
        print(f"   pack_created_at: {format_time(result.pack_created_at_unix)}")

    print("Recovered latest metadata")
    print(f"   catalog_hash: {result.pack_catalog_hash}")
    print(f"   state_digest: {result.pack_state_digest}")
    print(f"   objects: {result.pack_object_count}")
    print(f"   snapshots: {result.pack_snapshot_count}")
    print(f"   known_chunks: {result.pack_known_chunk_count}")
    print(f"   protection_records: {result.pack_protection_record_count}")

    pack_import = result.pack_import_result
    if pack_import is not None:
        pack_import_stats = pack_import.stats
        print("Imported object pack")
        print(f"   objects_total: {pack_import_stats.objects_total}")
        print(f"   objects_written: {pack_import_stats.objects_written}")
        print(f"   objects_reused: {pack_import_stats.objects_reused}")
    else:
        print("Imported object pack: skipped (--download-only)")

    if result.db_import_result is not None:
        db_import = result.db_import_result
        print("Rebuilt local metadata DB")
        print(f"   db_file: {db_import.db_file}")
        db_import_stats = db_import.stats
        print(f"   snapshots_imported: {db_import_stats.snapshots_imported}")
        print(f"   complete_snapshots_imported: {db_import_stats.complete_snapshots_imported}")
        print(f"   items_imported: {db_import_stats.items_imported}")
        print(f"   known_chunks_imported: {db_import_stats.chunks_imported}")
        print(f"   protection_records_imported: {db_import_stats.protection_records_imported}")
        print(f"   pending_protection_records_created: {db_import_stats.pending_protection_records_created}")
        print(f"   erasure_data_packs_imported: {db_import_stats.erasure_data_packs_imported}")
        print(f"   erasure_pack_chunks_imported: {db_import_stats.erasure_pack_chunks_imported}")
        print(f"   erasure_pack_shards_imported: {db_import_stats.erasure_pack_shards_imported}")
    else:
        reason = "--download-only" if args.download_only else "--no-import-db"
        print(f"Rebuilt local metadata DB: skipped ({reason})")

    warning_limit = _configured_warning_limit(cfg)
    _print_limited_items("List warnings", stats.list_errors, limit=warning_limit)
    _print_limited_items("Download warnings", stats.download_errors, limit=warning_limit)

    return 0



def cmd_discover_metadata_packs(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    owner_id = owner_id_from_args(args, cfg)

    from stopan.metadata.packs.discovery import discover_metadata_packs_from_network

    (
        desired_copies_by_hash,
        local_pushed_at_by_hash,
        local_publication_warning,
    ) = metadata_pack_publication_maps_best_effort(
        cfg,
        owner_id=owner_id,
    )

    result = discover_metadata_packs_from_network(
        owner_id=owner_id,
        **_metadata_pack_network_kwargs(args, cfg),
        desired_copies_by_hash=desired_copies_by_hash,
        max_candidates=int(choose(args.max_candidates, cfg.metadata.pack_discovery_max_candidates)),
    )

    stats = result.stats
    print("Metadata packs distribuidos")
    print(f"   owner_id: {result.owner_id}")
    print(f"   list_targets: {stats.list_targets_succeeded}/{stats.list_targets_attempted}")
    print(f"   sources_seen: {stats.sources_seen}")
    print(f"   unique_packs_seen: {stats.unique_packs_seen}")
    print(f"   publications_known: {stats.publications_known}")

    if not result.entries:
        print("   packs: (none)")
    else:
        print("Packs")
        _print_metadata_pack_discovery_entries(
            result.entries,
            show_sources=bool(args.show_sources),
            local_pushed_at_by_hash=local_pushed_at_by_hash,
        )

    if local_publication_warning:
        _print_limited_items(
            "Advertencias de publicaciones locales",
            (local_publication_warning,),
            limit=1,
        )
    _print_limited_items("List warnings", stats.list_errors, limit=_configured_warning_limit(cfg))

    return 0


def cmd_verify_metadata_packs(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    owner_id = owner_id_from_args(args, cfg)

    from stopan.metadata.packs.verifier import verify_metadata_packs_from_network

    desired_copies_by_hash, local_pushed_at_by_hash = metadata_pack_publication_maps(
        cfg,
        owner_id=owner_id,
    )

    result = verify_metadata_packs_from_network(
        owner_id=owner_id,
        **_metadata_pack_network_kwargs(args, cfg),
        desired_copies_by_hash=desired_copies_by_hash,
        pack_hash=args.pack_hash,
        verify_all=bool(args.all),
        max_candidates=int(choose(args.max_candidates, cfg.metadata.pack_discovery_max_candidates)),
    )

    stats = result.stats
    print("Verificación de metadata packs distribuidos")
    print(f"   owner_id: {result.owner_id}")
    print(f"   list_targets: {stats.list_targets_succeeded}/{stats.list_targets_attempted}")
    print(f"   sources_seen: {stats.sources_seen}")
    print(f"   candidates_checked: {stats.candidates_checked}")
    print(f"   publications_known: {stats.publications_known}")
    print("   check: presence")
    print(f"   presence_verified: {stats.verified}")
    print(f"   presence_degraded: {stats.degraded}")
    print(f"   presence_failed: {stats.failed}")
    print(f"   presence_unknown: {stats.unknown}")

    if not result.results:
        print("   packs: (none)")
    else:
        print("Packs")
        _print_metadata_pack_verification_results(
            result.results,
            show_sources=bool(args.show_sources),
            local_pushed_at_by_hash=local_pushed_at_by_hash,
        )

    _print_limited_items("List warnings", stats.list_errors, limit=_configured_warning_limit(cfg))

    return 0 if result.results and stats.failed == 0 else 1


def cmd_pack_object_graph(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    object_store_dir = object_store_dir_from_args(args, cfg)
    service = object_pack_service_from_config(args, cfg)
    result = service.export_latest_pack(
        object_store_dir=object_store_dir,
        passphrase=passphrase_for_decrypt(args, cfg),
        identity_file=identity_file_from_args(args, cfg),
        out_path=args.out,
        pack_dir=args.pack_dir,
    )

    print("Metadata object pack creado")
    print(f"   path: {result.path}")
    print(f"   pack_hash: {result.pack_hash}")
    print(f"   vault_id: {result.vault_id}")
    print(f"   vault_generation: {result.vault_generation}")
    print(f"   catalog_hash: {result.catalog_hash}")
    print(f"   state_digest: {result.state_digest}")
    stats = result.stats
    print(f"   objects_packed: {stats.objects_packed}")
    print(f"   canonical_bytes: {format_bytes(stats.total_canonical_bytes)}")
    print(f"   pack_plaintext: {format_bytes(stats.plaintext_bytes)}")
    print(f"   compressed: {format_bytes(stats.compressed_bytes)}")
    print(f"   ciphertext: {format_bytes(stats.ciphertext_bytes)}")
    print(f"   snapshots: {stats.snapshot_count}")
    print(f"   known_chunks: {stats.known_chunk_count}")
    print(f"   protection_records: {stats.protection_record_count}")
    return 0


def cmd_inspect_object_pack(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    service = object_pack_service_from_config_defaults(cfg)
    decrypt = bool(args.decrypt)
    full_validation = bool(args.full_validation)

    if full_validation and not decrypt:
        raise StopanUsageError("--full-validation requiere --decrypt")

    if not decrypt:
        inspection = service.inspect_pack_header(args.path)
    else:
        identity_file = identity_file_from_args(args, cfg)
        passphrase = passphrase_for_decrypt(args, cfg)
        if full_validation:
            inspection = service.validate_pack(
                args.path,
                identity_file=identity_file,
                passphrase=passphrase,
            )
        else:
            inspection = service.inspect_pack_summary(
                args.path,
                identity_file=identity_file,
                passphrase=passphrase,
            )

    header = inspection.header
    print("Metadata object pack")
    print(f"   path: {header.path}")
    print(f"   pack_hash: {header.pack_hash}")
    print(f"   format: {header.format} v{header.version}")
    print(f"   encryption: {header.encryption}")
    print(f"   recipients: {header.recipient_count}")
    print(f"   aead: {header.aead_name}")
    print(f"   compression: {header.compression}")
    print(f"   ciphertext: {format_bytes(header.ciphertext_bytes)}")

    if inspection.decrypted is not None:
        summary = inspection.decrypted
        print("Decrypted pack")
        print(f"   object_validation: {'full' if full_validation else 'summary'}")
        print(f"   vault_id: {summary.vault_id}")
        print(f"   vault_generation: {summary.vault_generation}")
        print(f"   pack_created_at: {format_time(summary.pack_created_at_unix)}")
        print(f"   catalog_hash: {summary.catalog_hash}")
        print(f"   state_digest: {summary.state_digest}")
        print(f"   objects: {summary.object_count}")
        print(f"   canonical_bytes: {format_bytes(summary.total_canonical_bytes)}")
        print(f"   pack_plaintext: {format_bytes(summary.plaintext_bytes)}")
        print(f"   compressed: {format_bytes(summary.compressed_bytes)}")
        print(f"   snapshots: {summary.snapshot_count}")
        print(f"   known_chunks: {summary.known_chunk_count}")
        print(f"   protection_records: {summary.protection_record_count}")

    return 0


def cmd_import_object_pack(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    service = object_pack_service_from_config(args, cfg)
    result = service.import_pack(
        args.path,
        object_store_dir=object_store_dir_from_args(args, cfg),
        identity_file=identity_file_from_args(args, cfg),
        passphrase=passphrase_for_decrypt(args, cfg),
    )

    print("Metadata object pack importado")
    print(f"   path: {result.path}")
    print(f"   pack_hash: {result.pack_hash}")
    print(f"   vault_id: {result.vault_id}")
    print(f"   vault_generation: {result.vault_generation}")
    print(f"   pack_created_at: {format_time(result.pack_created_at_unix)}")
    print(f"   object_store: {result.object_store_dir}")
    print(f"   catalog_hash: {result.catalog_hash}")
    print(f"   state_digest: {result.state_digest}")
    stats = result.stats
    print(f"   objects_total: {stats.objects_total}")
    print(f"   objects_written: {stats.objects_written}")
    print(f"   objects_reused: {stats.objects_reused}")
    print(f"   canonical_bytes: {format_bytes(stats.total_canonical_bytes)}")
    print(f"   snapshots: {stats.snapshot_count}")
    print(f"   known_chunks: {stats.known_chunk_count}")
    print(f"   protection_records: {stats.protection_record_count}")
    return 0
