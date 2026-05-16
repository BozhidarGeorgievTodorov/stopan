from __future__ import annotations

import argparse
from pathlib import Path

from stopan.cli.config_utils import choose, first_seed, load_runtime_config
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
    passphrase_for_export,
    passphrase_for_object_store_export,
    scrypt_cost_from_args,
    scrypt_cost_from_config,
)
from stopan.metadata.identity import (
    create_metadata_identity_file,
    load_metadata_identity_file,
    sign_metadata_pack_hash,
    validate_owner_id,
)
from stopan.metadata.objects.gc import MetadataObjectGarbageCollector
from stopan.metadata.packs.hashes import calculate_pack_hash, validate_pack_hash
from stopan.metadata.packs.object_pack import MetadataObjectPackService


_MAX_GC_ERRORS_TO_PRINT = 20
_MAX_RECOVER_WARNINGS_TO_PRINT = 10


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

    print("Metadata identity / distribution")
    print(f"   owner_id: {cfg.metadata.owner_id or '(not configured in config)'}")
    print(f"   identity_file: {cfg.metadata.identity_file or '(not configured)'}")
    print(f"   identity_file_status: {file_status(cfg.metadata.identity_file)}")
    print(f"   distributed_pack_store_dir: {cfg.metadata.distributed_pack_store_dir}")
    print(f"   distributed_pack_store_status: {dir_status(cfg.metadata.distributed_pack_store_dir)}")
    print(f"   pack_copies: {int(cfg.metadata.pack_copies)}")
    print(f"   strict_pack_copies: {bool(cfg.metadata.strict_pack_copies)}")
    print(f"   max_distributed_pack_bytes: {format_bytes(int(cfg.metadata.max_distributed_pack_bytes))}")
    print(f"   max_distributed_packs_per_owner: {int(cfg.metadata.max_distributed_packs_per_owner)}")
    print(f"   max_distributed_pack_bytes_per_owner: {format_bytes(int(cfg.metadata.max_distributed_pack_bytes_per_owner))}")
    print(f"   max_distributed_pack_store_bytes: {format_bytes(int(cfg.metadata.max_distributed_pack_store_bytes))}")

    print("Metadata object store GC policy")
    print(f"   object_grace_hours: {cfg.gc.metadata_object_store_grace_hours}")
    print(f"   pack_grace_hours: {cfg.gc.metadata_object_pack_grace_hours}")

    print("Distributed metadata pack store GC policy")
    print(f"   max_age_days: {cfg.gc.distributed_pack_max_age_days}")

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

def cmd_identity_create(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    identity_file = identity_file_from_args(args, cfg)
    identity = create_metadata_identity_file(
        identity_file,
        passphrase=passphrase_for_export(args),
        scrypt_cost=scrypt_cost_from_args(args, cfg),
        force=bool(args.force),
    )

    print("Metadata identity criptográfica creada")
    print(f"   path: {identity.path}")
    print(f"   algorithm: {identity.algorithm}")
    print(f"   owner_id: {identity.owner_id}")
    print(f"   signing_public_key_b64: {identity.signing_public_key_b64}")
    print(f"   encryption_public_key_b64: {identity.encryption_public_key_b64}")
    print(f"   private_keys: encrypted")
    print(f"   created_at: {identity.created_at}")
    print("   note: owner_id = BLAKE3(signing_public_key); no es secreto y sirve para descubrir packs.")
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
    pack_data = Path(args.path).expanduser().resolve().read_bytes()
    calculated_pack_hash = validate_pack_hash(calculate_pack_hash(pack_data))
    if args.expected_pack_hash is not None and validate_pack_hash(args.expected_pack_hash) != calculated_pack_hash:
        raise RuntimeError(
            f"pack_hash esperado no coincide con fichero: "
            f"esperado={args.expected_pack_hash} calculado={calculated_pack_hash}"
        )
    signed_owner_id, public_key_b64, signature_b64 = sign_metadata_pack_hash(
        identity_file=identity_file,
        passphrase=passphrase_for_decrypt(args),
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
    size = out_path.stat().st_size
    calculated = calculate_pack_hash(out_path.read_bytes())
    if calculated != pack_hash:
        raise RuntimeError(f"pack_hash recuperado no coincide: esperado={pack_hash} calculado={calculated}")

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
        passphrase=(passphrase_for_decrypt(args) if args.decrypt_latest else None),
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
    passphrase = passphrase_for_object_store_export(args, object_store_dir=object_store_dir)

    service = object_graph_service_from_config(args, cfg)
    result = service.export_current_state(
        object_store_dir=object_store_dir,
        passphrase=passphrase,
        include_protection=not bool(args.no_protection),
    )

    print("Metadata object graph exportado")
    print(f"   object_store: {result.root_dir}")
    print(f"   catalog_hash: {result.catalog_hash}")
    print(f"   state_digest: {result.state_digest}")
    print(f"   objects_total: {result.objects_total}")
    print(f"   objects_written: {result.objects_written}")
    print(f"   objects_reused: {result.objects_reused}")
    print(f"   canonical_bytes: {format_bytes(result.total_canonical_bytes)}")
    print(f"   snapshots: {result.snapshot_count}")
    print(f"   known_chunks: {result.known_chunk_count}")
    print(f"   protection_records: {result.protection_record_count}")

    if bool(args.pack):
        pack_service = object_pack_service_from_config(args, cfg)
        pack_result = pack_service.export_latest_pack(
            object_store_dir=object_store_dir,
            passphrase=passphrase,
            identity_file=identity_file_from_args(args, cfg),
            out_path=args.pack_out,
            pack_dir=args.pack_dir,
        )

        print("Metadata object pack creado")
        print(f"   path: {pack_result.path}")
        print(f"   pack_hash: {pack_result.pack_hash}")
        print(f"   vault_generation: {pack_result.vault_generation}")
        print(f"   pack_created_at: {format_time(pack_result.pack_created_at_unix)}")
        print(f"   catalog_hash: {pack_result.catalog_hash}")
        print(f"   state_digest: {pack_result.state_digest}")
        print(f"   objects_packed: {pack_result.objects_packed}")
        print(f"   canonical_bytes: {format_bytes(pack_result.total_canonical_bytes)}")
        print(f"   pack_plaintext: {format_bytes(pack_result.plaintext_bytes)}")
        print(f"   compressed: {format_bytes(pack_result.compressed_bytes)}")
        print(f"   ciphertext: {format_bytes(pack_result.ciphertext_bytes)}")
        print(f"   snapshots: {pack_result.snapshot_count}")
        print(f"   known_chunks: {pack_result.known_chunk_count}")
        print(f"   protection_records: {pack_result.protection_record_count}")

    return 0

def cmd_import_object_graph(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    object_store_dir = object_store_dir_from_args(args, cfg)
    service = object_graph_service_from_config_defaults(cfg)
    result = service.import_latest_state(
        object_store_dir=object_store_dir,
        passphrase=passphrase_for_decrypt(args),
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
    print(f"   catalog_hash: {result.catalog_hash}")
    print(f"   state_digest: {result.state_digest}")
    print(f"   objects_read: {result.objects_read}")
    print(f"   canonical_bytes: {format_bytes(result.total_canonical_bytes)}")
    print(f"   snapshots_imported: {result.snapshots_imported}")
    print(f"   complete_snapshots_imported: {result.complete_snapshots_imported}")
    print(f"   items_imported: {result.items_imported}")
    print(
        f"   recipes: imported={result.recipes_imported} "
        f"reused={result.recipes_reused}"
    )
    print(f"   known_chunks_imported: {result.chunks_imported}")
    print(f"   protection_records_imported: {result.protection_records_imported}")
    print(f"   pending_protection_records_created: {result.pending_protection_records_created}")
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
        raise FileNotFoundError(f"Directorio de metadata object packs no existe: {pack_dir}")
    if not pack_dir.is_dir():
        raise NotADirectoryError(f"No es un directorio de metadata object packs: {pack_dir}")

    service = object_pack_service_from_config_defaults(cfg)
    paths = sorted(pack_dir.glob("*.stopanmetapack"), key=lambda item: item.name)
    print(f"Metadata object packs in {pack_dir}")
    if not paths:
        print("   (none)")
        return 0

    for path in paths:
        try:
            inspection = service.inspect_pack(path, decrypt=False)
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


def cmd_gc_object_store(args: argparse.Namespace) -> int:
    if args.objects_only and args.packs_only:
        raise ValueError("'--objects-only' y '--packs-only' son incompatibles.")

    cfg = load_runtime_config(args)
    object_store_dir = object_store_dir_from_args(args, cfg)

    object_grace_hours = float(
        choose(args.object_grace_hours, cfg.gc.metadata_object_store_grace_hours)
    )
    pack_grace_hours = float(
        choose(args.pack_grace_hours, cfg.gc.metadata_object_pack_grace_hours)
    )

    collector = MetadataObjectGarbageCollector(
        scrypt_cost=scrypt_cost_from_config(cfg)
    )
    result = collector.collect(
        object_store_dir=object_store_dir,
        identity_file=identity_file_from_args(args, cfg),
        passphrase=passphrase_for_decrypt(args),
        object_grace_seconds=int(max(object_grace_hours, 0.0) * 3600),
        pack_grace_seconds=int(max(pack_grace_hours, 0.0) * 3600),
        dry_run=bool(args.dry_run),
        include_objects=not bool(args.packs_only),
        include_packs=not bool(args.objects_only),
        pack_dir=args.pack_dir,
    )

    print("Metadata object store GC")
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
    print(f"   object_bytes_deleted: {format_bytes(result.object_bytes_deleted)}")
    print("Packs")
    print(f"   pack_dir: {result.pack_dir}")
    print(f"   pack_files_seen: {result.pack_files_seen}")
    print(f"   pack_files_latest: {result.pack_files_latest}")
    print(f"   pack_files_collectable: {result.pack_files_collectable}")
    print(f"   pack_files_deleted: {result.pack_files_deleted}")
    print(f"   pack_files_skipped_by_grace: {result.pack_files_skipped_by_grace}")
    print(f"   pack_files_unreadable: {result.pack_files_unreadable}")
    print(f"   pack_bytes_deleted: {format_bytes(result.pack_bytes_deleted)}")

    if result.errors:
        print("Errors / skipped items")
        for error in result.errors[:_MAX_GC_ERRORS_TO_PRINT]:
            print(f"   - {error}")
        if len(result.errors) > _MAX_GC_ERRORS_TO_PRINT:
            print(f"   ... (+{len(result.errors) - _MAX_GC_ERRORS_TO_PRINT} más)")

    if result.dry_run:
        print("\nDry-run activo: no se borró nada. Usa --apply para ejecutar el borrado real.")

    return 0


def print_metadata_pack_push_result(result) -> int:
    print("Metadata pack push finalizado" if result.protected else "Metadata pack push incompleto")
    print(f"   owner_id: {result.owner_id}")
    print(f"   pack_hash: {result.pack_hash}")
    print(f"   pack_path: {result.pack_path}")
    print(f"   pack_size: {format_bytes(result.pack_size_bytes)}")
    print(f"   pack_copies: {result.desired_rf}")
    print(f"   remote_candidates: {result.remote_candidates}")
    print(f"   attempted_targets: {result.attempted_targets}")
    print(f"   successful_targets: {result.successful_targets}")
    print(f"   stored_targets: {result.stored_targets}")
    print(f"   already_present_targets: {result.already_present_targets}")
    print(f"   failed_targets: {result.failed_targets}")

    if result.insufficient_remote_targets:
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
        raise NotADirectoryError(f"No es un directorio de metadata packs: {resolved_pack_dir}")

    graph_service = object_graph_service_from_config_defaults(cfg)
    store_inspection = graph_service.inspect_store(
        object_store_dir=object_store_dir,
        passphrase=passphrase,
        decrypt_latest=True,
    )
    latest = store_inspection.latest
    if latest is None:
        raise RuntimeError(f"El metadata object store no tiene latest: {object_store_dir}")

    candidates = []

    for path in sorted(resolved_pack_dir.glob("*.stopanmetapack")):
        try:
            inspection = pack_service.inspect_pack(
                path,
                identity_file=identity_file,
                passphrase=passphrase,
                decrypt=True,
            )
            if inspection.decrypted is None:
                continue

            summary = inspection.decrypted
            if summary.catalog_hash != latest.catalog_hash:
                continue
            if summary.state_digest != latest.state_digest:
                continue

            try:
                mtime = float(path.stat().st_mtime)
            except OSError:
                mtime = 0.0

            candidates.append(
                (
                    int(summary.vault_generation),
                    float(summary.pack_created_at_unix),
                    mtime,
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

    _generation, _created_at, _mtime, _pack_hash, path, summary = max(
        candidates,
        key=lambda item: (item[0], item[1], item[2], item[3]),
    )
    return path, summary

def push_pack_result_from_path(args: argparse.Namespace, cfg, *, pack_path: str | Path):
    owner_id = owner_id_from_args(args, cfg)
    identity_file = identity_file_from_args(args, cfg)
    identity_passphrase = passphrase_for_decrypt(args)

    from stopan.metadata.packs.pusher import push_metadata_pack_to_network

    return push_metadata_pack_to_network(
        pack_path=pack_path,
        owner_id=owner_id,
        identity_file=identity_file,
        identity_passphrase=identity_passphrase,
        membership_seed=args.membership_seed or first_seed(cfg),
        rf=int(choose(args.pack_copies, cfg.metadata.pack_copies)),
        strict_rf=bool(choose(args.strict_pack_copies, cfg.metadata.strict_pack_copies)),
        target_parallelism=int(choose(args.target_parallelism, cfg.replication.target_parallelism)),
        rpc_timeout_s=float(choose(args.rpc_timeout_s, cfg.replication.stream_timeout_s)),
        membership_timeout_s=float(cfg.membership.rpc_timeout_s),
        max_message_bytes=int(choose(args.max_message_bytes, cfg.grpc.max_message_bytes)),
        grpc_keepalive_time_ms=int(cfg.grpc.keepalive_time_ms),
        grpc_keepalive_timeout_ms=int(cfg.grpc.keepalive_timeout_ms),
        grpc_keepalive_permit_without_calls=bool(cfg.grpc.keepalive_permit_without_calls),
        self_addr=cfg.node.advertise_addr,
        cluster_token=cfg.cluster.token,
    )


def cmd_push(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)

    if args.pack_in:
        if args.object_store:
            raise ValueError("'--pack-in' y '--object-store' son incompatibles.")
        if args.pack_out:
            raise ValueError("'--pack-in' y '--pack-out' son incompatibles.")
        if args.pack_dir:
            raise ValueError("'--pack-in' y '--pack-dir' son incompatibles.")

        result = push_pack_result_from_path(args, cfg, pack_path=args.pack_in)
        return print_metadata_pack_push_result(result)

    if args.pack_out and args.pack_dir:
        raise ValueError("'--pack-out' y '--pack-dir' son incompatibles.")

    object_store_dir = args.object_store or cfg.metadata.object_store_dir
    if not object_store_dir:
        raise ValueError("Se requiere --object-store o metadata.object_store_dir.")

    passphrase = passphrase_for_decrypt(args)
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
    print(f"   vault_generation: {pack_result.vault_generation}")
    print(f"   pack_created_at: {format_time(pack_result.pack_created_at_unix)}")
    print(f"   catalog_hash: {pack_result.catalog_hash}")
    print(f"   state_digest: {pack_result.state_digest}")
    print(f"   objects_packed: {pack_result.objects_packed}")
    print(f"   ciphertext: {format_bytes(pack_result.ciphertext_bytes)}")

    result = push_pack_result_from_path(args, cfg, pack_path=pack_result.path)
    return print_metadata_pack_push_result(result)


def cmd_recover(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    owner_id = owner_id_from_args(args, cfg)
    object_store_dir = args.object_store or cfg.metadata.object_store_dir
    if not object_store_dir:
        raise ValueError("Se requiere --object-store o metadata.object_store_dir.")

    passphrase = passphrase_for_decrypt(args)

    from stopan.metadata.packs.recover import recover_metadata_from_network

    result = recover_metadata_from_network(
        owner_id=owner_id,
        identity_file=identity_file_from_args(args, cfg),
        object_store_dir=object_store_dir,
        passphrase=passphrase,
        membership_seed=args.membership_seed or first_seed(cfg),
        self_addr=cfg.node.advertise_addr,
        cluster_token=cfg.cluster.token,
        membership_timeout_s=float(cfg.membership.rpc_timeout_s),
        rpc_timeout_s=float(choose(args.rpc_timeout_s, cfg.replication.stream_timeout_s)),
        target_parallelism=int(choose(args.target_parallelism, cfg.replication.target_parallelism)),
        max_message_bytes=int(choose(args.max_message_bytes, cfg.grpc.max_message_bytes)),
        grpc_keepalive_time_ms=int(cfg.grpc.keepalive_time_ms),
        grpc_keepalive_timeout_ms=int(cfg.grpc.keepalive_timeout_ms),
        grpc_keepalive_permit_without_calls=bool(cfg.grpc.keepalive_permit_without_calls),
        scrypt_cost=scrypt_cost_from_args(args, cfg),
        db_file=cfg.node.db_file,
        import_db=bool(args.import_db),
        include_protection=not bool(args.no_protection),
        default_desired_rf=int(
            args.default_desired_remote_copies
            if args.default_desired_remote_copies is not None
            else cfg.protection.remote_copies
        ),
        download_dir=args.download_dir,
        pack_out=args.pack_out,
        max_candidates=int(args.max_candidates),
    )

    print("Metadata recover remoto completado")
    print(f"   owner_id: {result.owner_id}")
    print(f"   object_store: {result.object_store_dir}")
    print(f"   list_targets: {result.list_targets_succeeded}/{result.list_targets_attempted}")
    print(f"   candidates_seen: {result.candidates_seen}")
    print(f"   unique_packs_seen: {result.unique_packs_seen}")
    print(f"   downloads_attempted: {result.downloads_attempted}")
    print(f"   recovered_pack_hash: {result.recovered_pack_hash}")
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
    print("Imported object pack")
    print(f"   objects_total: {pack_import.objects_total}")
    print(f"   objects_written: {pack_import.objects_written}")
    print(f"   objects_reused: {pack_import.objects_reused}")

    if result.db_import_result is not None:
        db_import = result.db_import_result
        print("Rebuilt local metadata DB")
        print(f"   db_file: {db_import.db_file}")
        print(f"   snapshots_imported: {db_import.snapshots_imported}")
        print(f"   complete_snapshots_imported: {db_import.complete_snapshots_imported}")
        print(f"   items_imported: {db_import.items_imported}")
        print(f"   known_chunks_imported: {db_import.chunks_imported}")
        print(f"   protection_records_imported: {db_import.protection_records_imported}")
        print(f"   pending_protection_records_created: {db_import.pending_protection_records_created}")
    else:
        print("Rebuilt local metadata DB: skipped (--no-import-db)")

    if result.list_errors:
        print("List warnings")
        for item in result.list_errors[:_MAX_RECOVER_WARNINGS_TO_PRINT]:
            print(f"   - {item}")
        if len(result.list_errors) > _MAX_RECOVER_WARNINGS_TO_PRINT:
            print(f"   ... (+{len(result.list_errors) - _MAX_RECOVER_WARNINGS_TO_PRINT} más)")

    if result.download_errors:
        print("Download warnings")
        for item in result.download_errors[:_MAX_RECOVER_WARNINGS_TO_PRINT]:
            print(f"   - {item}")
        if len(result.download_errors) > _MAX_RECOVER_WARNINGS_TO_PRINT:
            print(f"   ... (+{len(result.download_errors) - _MAX_RECOVER_WARNINGS_TO_PRINT} más)")

    return 0


def cmd_pack_object_graph(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    object_store_dir = object_store_dir_from_args(args, cfg)
    service = object_pack_service_from_config(args, cfg)
    result = service.export_latest_pack(
        object_store_dir=object_store_dir,
        passphrase=passphrase_for_decrypt(args),
        identity_file=identity_file_from_args(args, cfg),
        out_path=args.out,
        pack_dir=args.pack_dir,
    )

    print("Metadata object pack creado")
    print(f"   path: {result.path}")
    print(f"   pack_hash: {result.pack_hash}")
    print(f"   catalog_hash: {result.catalog_hash}")
    print(f"   state_digest: {result.state_digest}")
    print(f"   objects_packed: {result.objects_packed}")
    print(f"   canonical_bytes: {format_bytes(result.total_canonical_bytes)}")
    print(f"   pack_plaintext: {format_bytes(result.plaintext_bytes)}")
    print(f"   compressed: {format_bytes(result.compressed_bytes)}")
    print(f"   ciphertext: {format_bytes(result.ciphertext_bytes)}")
    print(f"   snapshots: {result.snapshot_count}")
    print(f"   known_chunks: {result.known_chunk_count}")
    print(f"   protection_records: {result.protection_record_count}")
    return 0


def cmd_inspect_object_pack(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    service = object_pack_service_from_config_defaults(cfg)
    inspection = service.inspect_pack(
        args.path,
        identity_file=(identity_file_from_args(args, cfg) if args.decrypt else None),
        passphrase=(passphrase_for_decrypt(args) if args.decrypt else None),
        decrypt=bool(args.decrypt),
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
        passphrase=passphrase_for_decrypt(args),
    )

    print("Metadata object pack importado")
    print(f"   path: {result.path}")
    print(f"   pack_hash: {result.pack_hash}")
    print(f"   vault_generation: {result.vault_generation}")
    print(f"   pack_created_at: {format_time(result.pack_created_at_unix)}")
    print(f"   object_store: {result.object_store_dir}")
    print(f"   catalog_hash: {result.catalog_hash}")
    print(f"   state_digest: {result.state_digest}")
    print(f"   objects_total: {result.objects_total}")
    print(f"   objects_written: {result.objects_written}")
    print(f"   objects_reused: {result.objects_reused}")
    print(f"   canonical_bytes: {format_bytes(result.total_canonical_bytes)}")
    print(f"   snapshots: {result.snapshot_count}")
    print(f"   known_chunks: {result.known_chunk_count}")
    print(f"   protection_records: {result.protection_record_count}")
    return 0
