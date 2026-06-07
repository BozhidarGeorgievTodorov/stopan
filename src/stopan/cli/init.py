from __future__ import annotations

import argparse
import grp
import os
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import yaml

from stopan.cli.validation import IntRange, validate_int_ranges
from stopan.common.fs import atomic_write_bytes
from stopan.config.defaults import (
    DEFAULT_CLUSTER_TOKEN,
    DEFAULT_NODE_BIND_ADDR,
    DEFAULT_PROTECTION_REMOTE_COPIES,
    DEFAULT_STOPAN_CONFIG,
    DEFAULT_SYSTEM_DB_FILE,
    DEFAULT_SYSTEM_LOCAL_SHARD_DIR,
    DEFAULT_SYSTEM_REPO_STORE_DIR,
)
from stopan.config.loader import load_config
from stopan.config.model import StopanConfig
from stopan.errors import StopanStorageError, StopanUsageError


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stopan init",
        allow_abbrev=False,
        description="Inicializa configuración, secretos y directorios de Stopan.",
    )

    sub = parser.add_subparsers(dest="target", required=True)

    node = sub.add_parser(
        "node",
        allow_abbrev=False,
        help="Configura la sección node/cluster del fichero principal de Stopan.",
    )
    node.add_argument(
        "--config",
        default=DEFAULT_STOPAN_CONFIG,
        help=f"Ruta del YAML principal a actualizar. Por defecto: {DEFAULT_STOPAN_CONFIG}.",
    )
    node.add_argument(
        "--advertise-addr",
        required=True,
        help="Dirección pública/anunciada del nodo, por ejemplo 192.168.1.42:50051.",
    )
    node.add_argument(
        "--bind-addr",
        default=DEFAULT_NODE_BIND_ADDR,
        help=f"Dirección local de escucha gRPC. Por defecto: {DEFAULT_NODE_BIND_ADDR}.",
    )
    node.add_argument(
        "--token",
        default=DEFAULT_CLUSTER_TOKEN,
        help="Token lógico del cluster. Si se omite, queda vacío.",
    )
    node.add_argument(
        "--seed",
        action="append",
        default=[],
        help="Seed de membership. Puede repetirse. Si se omite, se usa advertise-addr.",
    )
    node.add_argument(
        "--repo-store-dir",
        default=DEFAULT_SYSTEM_REPO_STORE_DIR,
        help=f"Directorio del almacén P2P del nodo. Por defecto: {DEFAULT_SYSTEM_REPO_STORE_DIR}.",
    )
    node.add_argument(
        "--local-shard-dir",
        default=DEFAULT_SYSTEM_LOCAL_SHARD_DIR,
        help=f"Directorio del CAS local. Por defecto: {DEFAULT_SYSTEM_LOCAL_SHARD_DIR}.",
    )
    node.add_argument(
        "--db-file",
        default=DEFAULT_SYSTEM_DB_FILE,
        help=f"Ruta de metadata local. Por defecto: {DEFAULT_SYSTEM_DB_FILE}.",
    )
    node.add_argument(
        "--remote-copies",
        type=int,
        default=None,
        help=(
            "Copias remotas completas por chunk. Si se omite, conserva el valor existente "
            f"o usa el default {DEFAULT_PROTECTION_REMOTE_COPIES}."
        ),
    )
    node.add_argument(
        "--strict-remote-copies",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Exige suficientes nodos remotos para todas las copias. Si se omite, conserva el valor existente.",
    )

    metadata = sub.add_parser(
        "metadata",
        allow_abbrev=False,
        help="Inicializa passphrase, identidad criptográfica y owner_id de metadata.",
    )
    metadata.add_argument(
        "--config",
        default=DEFAULT_STOPAN_CONFIG,
        help=f"Ruta del YAML principal a actualizar. Por defecto: {DEFAULT_STOPAN_CONFIG}.",
    )

    args = parser.parse_args(argv)

    if args.target == "node" and args.remote_copies is not None:
        validate_int_ranges(parser, args, (IntRange("remote_copies", "--remote-copies", 0),))

    return args


def _plain_data(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _plain_data(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_plain_data(item) for item in value]
    if isinstance(value, list):
        return [_plain_data(item) for item in value]
    return value


def _build_node_config(args: argparse.Namespace, base_cfg: StopanConfig | None = None) -> StopanConfig:
    base = base_cfg or StopanConfig()
    seeds = tuple(args.seed) if args.seed else (args.advertise_addr,)
    protection_overrides: dict[str, Any] = {}
    if args.remote_copies is not None:
        protection_overrides["remote_copies"] = args.remote_copies
    if args.strict_remote_copies is not None:
        protection_overrides["strict_remote_copies"] = bool(args.strict_remote_copies)

    return base.with_overrides(
        node={
            "bind_addr": args.bind_addr,
            "advertise_addr": args.advertise_addr,
            "repo_store_dir": args.repo_store_dir,
            "local_shard_dir": args.local_shard_dir,
            "db_file": args.db_file,
        },
        cluster={
            "token": args.token,
            "seeds": seeds,
        },
        protection=protection_overrides,
    )


def _stopan_group_gid() -> int | None:
    try:
        return grp.getgrnam("stopan").gr_gid
    except KeyError:
        return None


def _apply_service_file_permissions(path: Path, *, mode: int = 0o640) -> None:
    try:
        os.chmod(path, mode)
    except OSError as exc:
        raise StopanStorageError(f"No se pudieron ajustar permisos de {path}: {exc}") from exc

    gid = _stopan_group_gid()
    if gid is not None:
        try:
            os.chown(path, -1, gid)
        except PermissionError:
            pass
        except OSError as exc:
            raise StopanStorageError(f"No se pudo asignar el grupo stopan a {path}: {exc}") from exc


def _write_yaml_atomic(path: Path, data: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise StopanStorageError(f"No se pudo crear el directorio de configuración {path.parent}: {exc}") from exc

    rendered = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)
    tmp_path: Path | None = None

    try:
        with NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=str(path.parent),
            prefix=f".{path.name}.",
            delete=False,
        ) as tmp:
            tmp.write(rendered)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = Path(tmp.name)

        os.chmod(tmp_path, 0o640)
        os.replace(tmp_path, path)
        _apply_service_file_permissions(path, mode=0o640)
    except OSError as exc:
        raise StopanStorageError(f"No se pudo escribir la configuración {path}: {exc}") from exc
    finally:
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def _load_config_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        return _plain_data(asdict(StopanConfig()))
    try:
        with open(path, "r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle)
    except OSError as exc:
        raise StopanStorageError(f"No se pudo leer la configuración {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise StopanUsageError(f"YAML inválido en {path}: {exc}") from exc

    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise StopanUsageError(f"La configuración {path} debe contener un objeto YAML en la raíz.")
    return raw


def _ensure_mapping(root: dict[str, Any], section: str) -> dict[str, Any]:
    current = root.get(section)
    if current is None:
        current = {}
        root[section] = current
    if not isinstance(current, dict):
        raise StopanUsageError(f"La sección {section!r} de la configuración debe ser un objeto YAML.")
    return current


def _prepare_node_directories(cfg: StopanConfig) -> list[Path]:
    directories = [
        Path(cfg.node.repo_store_dir),
        Path(cfg.node.local_shard_dir),
        Path(cfg.node.db_file).parent,
    ]
    for directory in directories:
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise StopanStorageError(f"No se pudo preparar el directorio {directory}: {exc}") from exc
    return directories


def _init_node(args: argparse.Namespace) -> int:
    config_path = Path(args.config).expanduser().resolve()
    base_cfg = load_config(str(config_path)) if config_path.exists() else StopanConfig()
    cfg = _build_node_config(args, base_cfg)

    data = _load_config_document(config_path)
    node = _ensure_mapping(data, "node")
    node.update(
        {
            "bind_addr": args.bind_addr,
            "advertise_addr": args.advertise_addr,
            "repo_store_dir": args.repo_store_dir,
            "local_shard_dir": args.local_shard_dir,
            "db_file": args.db_file,
        }
    )
    cluster = _ensure_mapping(data, "cluster")
    cluster.update(
        {
            "token": args.token,
            "seeds": list(tuple(args.seed) if args.seed else (args.advertise_addr,)),
        }
    )
    if args.remote_copies is not None or args.strict_remote_copies is not None:
        protection = _ensure_mapping(data, "protection")
        if args.remote_copies is not None:
            protection["remote_copies"] = int(args.remote_copies)
        if args.strict_remote_copies is not None:
            protection["strict_remote_copies"] = bool(args.strict_remote_copies)

    _write_yaml_atomic(config_path, data)
    prepared_dirs = _prepare_node_directories(cfg)
    load_config(str(config_path))

    print(f"Configuración actualizada: {config_path}")
    print("Nodo")
    print(f"   bind_addr: {cfg.node.bind_addr}")
    print(f"   advertise_addr: {cfg.node.advertise_addr}")
    print("Cluster")
    print(f"   token: {'configured' if cfg.cluster.token else '(empty)'}")
    print("   seeds:")
    for seed in cfg.cluster.seeds:
        print(f"      - {seed}")
    print("Directorios preparados:")
    for directory in prepared_dirs:
        print(f"  {directory}")
    print("Validación correcta.")
    print("Arranque:")
    if str(config_path) == DEFAULT_STOPAN_CONFIG:
        print("  stopan node")
    else:
        print(f"  stopan node --config {config_path}")
    return 0


def _prompt_metadata_passphrase(*, identity_exists: bool, target_path: Path) -> str:
    from stopan.cli.metadata_helpers import prompt_existing_passphrase, prompt_new_passphrase

    try:
        if identity_exists:
            return prompt_existing_passphrase()
        return prompt_new_passphrase()
    except (EOFError, KeyboardInterrupt) as exc:
        raise StopanUsageError(
            f"No existe {target_path} y no se pudo leer una passphrase de forma interactiva. "
            "Ejecuta 'sudo stopan init metadata' en una terminal o crea el archivo "
            "metadata.passphrase de forma segura antes de inicializar metadata."
        ) from exc


def _ensure_passphrase_file(path: Path, *, identity_exists: bool) -> tuple[str, bool]:
    from stopan.metadata.identity.passphrase import read_passphrase_file

    if path.exists():
        if not path.is_file():
            raise StopanUsageError(f"metadata.passphrase_file no es un archivo regular: {path}")
        _apply_service_file_permissions(path, mode=0o640)
        return read_passphrase_file(path), False

    passphrase = _prompt_metadata_passphrase(identity_exists=identity_exists, target_path=path)
    atomic_write_bytes(path, (passphrase + "\n").encode("utf-8"), mode=0o640)
    _apply_service_file_permissions(path, mode=0o640)
    return passphrase, True


def _init_metadata(args: argparse.Namespace) -> int:
    config_path = Path(args.config).expanduser().resolve()
    if not config_path.exists():
        raise StopanUsageError(
            f"No existe {config_path}. Inicializa primero la configuración con 'sudo stopan init node ...' "
            "o instala el paquete Stopan."
        )

    from stopan.metadata.identity import create_metadata_identity_file
    from stopan.metadata.identity.files import load_metadata_private_identity_file
    from stopan.metadata.identity.passphrase import ScryptCost

    cfg = load_config(str(config_path))
    if not cfg.metadata.passphrase_file:
        raise StopanUsageError("metadata.passphrase_file debe estar configurado para inicializar metadata.")
    if not cfg.metadata.identity_file:
        raise StopanUsageError("metadata.identity_file debe estar configurado para inicializar metadata.")

    passphrase_path = Path(cfg.metadata.passphrase_file).expanduser().resolve()
    identity_path = Path(cfg.metadata.identity_file).expanduser().resolve()

    identity_exists = identity_path.exists()
    passphrase, passphrase_created = _ensure_passphrase_file(passphrase_path, identity_exists=identity_exists)

    identity_created = False
    if identity_exists:
        identity = load_metadata_private_identity_file(identity_path, passphrase=passphrase).identity
    else:
        identity = create_metadata_identity_file(
            identity_path,
            passphrase=passphrase,
            scrypt_cost=ScryptCost(
                n=int(cfg.metadata.scrypt_n),
                r=int(cfg.metadata.scrypt_r),
                p=int(cfg.metadata.scrypt_p),
                key_length=int(cfg.metadata.key_length),
            ),
            force=False,
        )
        identity_created = True

    _apply_service_file_permissions(identity_path, mode=0o640)

    if cfg.metadata.owner_id and cfg.metadata.owner_id != identity.owner_id:
        raise StopanUsageError(
            "metadata.owner_id no coincide con el archivo de identidad de metadata: "
            f"metadata.owner_id={cfg.metadata.owner_id} identity_owner_id={identity.owner_id}. "
            "No se sobrescribe la configuración automáticamente."
        )

    data = _load_config_document(config_path)
    metadata = _ensure_mapping(data, "metadata")
    metadata["passphrase_file"] = str(passphrase_path)
    metadata["identity_file"] = str(identity_path)
    metadata["owner_id"] = identity.owner_id

    _write_yaml_atomic(config_path, data)
    load_config(str(config_path))

    print("Metadata inicializada")
    print(f"   passphrase_file: {passphrase_path}")
    print(f"   passphrase_file_created: {passphrase_created}")
    if passphrase_created and not identity_created:
        print("   passphrase_source: introducida por terminal y guardada para desbloquear la identidad existente")
    elif passphrase_created:
        print("   passphrase_source: introducida por terminal y guardada para esta nueva identidad")
    else:
        print("   passphrase_source: archivo existente")
    print(f"   identity_file: {identity_path}")
    print(f"   identity_file_created: {identity_created}")
    print(f"   algorithm: {identity.algorithm}")
    print(f"   owner_id: {identity.owner_id}")
    print(f"   signing_public_key_b64: {identity.signing_public_key_b64}")
    print(f"   encryption_public_key_b64: {identity.encryption_public_key_b64}")
    print(f"   config_updated: {config_path}")
    print("   note: conserva de forma segura metadata.passphrase e identity_file; owner_id solo no permite descifrar packs.")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    if args.target == "node":
        return _init_node(args)
    if args.target == "metadata":
        return _init_metadata(args)

    raise StopanUsageError(f"Target desconocido: {args.target}")


if __name__ == "__main__":
    raise SystemExit(main())
