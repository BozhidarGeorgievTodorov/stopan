from __future__ import annotations

import getpass
import os
import stat
from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from stopan.cli.config_utils import choose
from stopan.errors import StopanUsageError
from stopan.metadata.identity import resolve_owner_id
from stopan.metadata.identity.passphrase import ScryptCost, read_passphrase_file
from stopan.metadata.objects.service import MetadataObjectGraphStoreService
from stopan.metadata.packs.distributed_store import MetadataPackStore
from stopan.metadata.packs.object_pack import MetadataObjectPackService


def prompt_new_passphrase() -> str:
    first = getpass.getpass("Metadata vault passphrase: ")
    second = getpass.getpass("Repeat passphrase: ")
    if first != second:
        raise StopanUsageError("Las passphrases no coinciden.")
    if not first:
        raise StopanUsageError("La passphrase no puede estar vacía.")
    return first


def prompt_existing_passphrase() -> str:
    value = getpass.getpass("Metadata vault passphrase: ")
    if not value:
        raise StopanUsageError("La passphrase no puede estar vacía.")
    return value


def passphrase_for_export(args: Namespace) -> str | bytes:
    if getattr(args, "passphrase_file", None):
        return read_passphrase_file(args.passphrase_file)
    return prompt_new_passphrase()


def passphrase_for_decrypt(args: Namespace) -> str | bytes:
    if getattr(args, "passphrase_file", None):
        return read_passphrase_file(args.passphrase_file)
    return prompt_existing_passphrase()


def passphrase_for_object_store_export(args: Namespace, *, object_store_dir: str) -> str | bytes:
    if getattr(args, "passphrase_file", None):
        return read_passphrase_file(args.passphrase_file)

    store_json = Path(object_store_dir).expanduser() / "store.json"
    if store_json.exists():
        return prompt_existing_passphrase()
    return prompt_new_passphrase()


def object_store_dir_from_args(args: Namespace, cfg: Any) -> str:
    object_store_dir = getattr(args, "object_store", None) or cfg.metadata.object_store_dir
    if not object_store_dir:
        raise StopanUsageError("Se requiere --object-store o metadata.object_store_dir.")
    return str(object_store_dir)


def scrypt_cost_from_args(args: Namespace, cfg: Any) -> ScryptCost:
    return ScryptCost(
        n=int(choose(getattr(args, "scrypt_n", None), cfg.metadata.scrypt_n)),
        r=int(choose(getattr(args, "scrypt_r", None), cfg.metadata.scrypt_r)),
        p=int(choose(getattr(args, "scrypt_p", None), cfg.metadata.scrypt_p)),
        key_length=int(choose(getattr(args, "metadata_key_length", None), cfg.metadata.key_length)),
    )


def scrypt_cost_from_config(cfg: Any) -> ScryptCost:
    return ScryptCost(
        n=int(cfg.metadata.scrypt_n),
        r=int(cfg.metadata.scrypt_r),
        p=int(cfg.metadata.scrypt_p),
        key_length=int(cfg.metadata.key_length),
    )


def object_graph_service_from_config(args: Namespace, cfg: Any) -> MetadataObjectGraphStoreService:
    return MetadataObjectGraphStoreService(
        db_file=cfg.node.db_file,
        scrypt_cost=scrypt_cost_from_args(args, cfg),
    )


def object_graph_service_from_config_defaults(cfg: Any) -> MetadataObjectGraphStoreService:
    return MetadataObjectGraphStoreService(
        db_file=cfg.node.db_file,
        scrypt_cost=scrypt_cost_from_config(cfg),
    )


def object_pack_service_from_config(args: Namespace, cfg: Any) -> MetadataObjectPackService:
    return MetadataObjectPackService(
        scrypt_cost=scrypt_cost_from_args(args, cfg),
    )


def object_pack_service_from_config_defaults(cfg: Any) -> MetadataObjectPackService:
    return MetadataObjectPackService(
        scrypt_cost=scrypt_cost_from_config(cfg),
    )


def format_bytes(value: int) -> str:
    if value < 1024:
        return f"{value} B"
    if value < 1024 * 1024:
        return f"{value / 1024:.2f} KiB"
    if value < 1024 * 1024 * 1024:
        return f"{value / (1024 * 1024):.2f} MiB"
    return f"{value / (1024 * 1024 * 1024):.2f} GiB"


def format_time(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).astimezone().isoformat(timespec="seconds")


def mode_octal(path: Path) -> str:
    try:
        return oct(stat.S_IMODE(path.stat().st_mode))
    except OSError:
        return "n/a"


def file_status(path_text: str) -> str:
    if not path_text:
        return "not configured"
    path = Path(path_text).expanduser()
    if not path.exists():
        return f"missing ({path})"
    if not path.is_file():
        return f"not a file ({path})"
    mode = mode_octal(path)
    readable = os.access(path, os.R_OK)
    secure_hint = "ok" if mode in {"0o600", "0o400"} else "check permissions"
    return f"present mode={mode} readable={readable} {secure_hint}"


def dir_status(path_text: str) -> str:
    path = Path(path_text).expanduser()
    if not path.exists():
        parent = path.parent if str(path.parent) else Path(".")
        writable_parent = parent.exists() and os.access(parent, os.W_OK)
        return f"missing; parent_writable={writable_parent} ({path})"
    if not path.is_dir():
        return f"not a directory ({path})"
    mode = mode_octal(path)
    writable = os.access(path, os.W_OK)
    secure_hint = "ok" if mode == "0o700" else "check permissions"
    return f"present mode={mode} writable={writable} {secure_hint}"


def identity_file_from_args(args: Namespace, cfg: Any) -> str:
    identity_file = getattr(args, "identity_file", None) or cfg.metadata.identity_file
    if not identity_file:
        raise StopanUsageError("Se requiere --identity-file o metadata.identity_file.")
    return str(identity_file)


def owner_id_from_args(args: Namespace, cfg: Any) -> str:
    return resolve_owner_id(
        explicit_owner_id=getattr(args, "owner_id", None),
        explicit_identity_file=getattr(args, "identity_file", None),
        config_owner_id=cfg.metadata.owner_id,
        config_identity_file=cfg.metadata.identity_file,
    )


def distributed_pack_store_from_args(args: Namespace, cfg: Any) -> MetadataPackStore:
    root_dir = getattr(args, "pack_store", None) or cfg.metadata.distributed_pack_store_dir
    return MetadataPackStore(
        root_dir,
        max_pack_bytes=int(cfg.metadata.max_distributed_pack_bytes),
        max_packs_per_owner=int(cfg.metadata.max_distributed_packs_per_owner),
        max_total_bytes_per_owner=int(cfg.metadata.max_distributed_pack_bytes_per_owner),
        max_total_store_bytes=int(cfg.metadata.max_distributed_pack_store_bytes),
        max_age_days=int(cfg.gc.received_metadata_pack_max_age_days),
    )
