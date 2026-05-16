from __future__ import annotations

import argparse
import os
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import yaml

from stopan.cli.validation import IntRange, validate_int_ranges
from stopan.errors import StopanStorageError, StopanUsageError
from stopan.config.defaults import (
    DEFAULT_NODE_CONFIG,
    DEFAULT_SYSTEM_DB_FILE,
    DEFAULT_SYSTEM_LOCAL_SHARD_DIR,
    DEFAULT_SYSTEM_REPO_STORE_DIR,
)
from stopan.config.loader import load_config
from stopan.config.model import StopanConfig



def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stopan init",
        allow_abbrev=False,
        description="Inicializa configuración y directorios de Stopan.",
    )

    sub = parser.add_subparsers(dest="target", required=True)

    node = sub.add_parser(
        "node",
        allow_abbrev=False,
        help="Inicializa la configuración de un nodo P2P",
    )
    node.add_argument(
        "--config",
        default=DEFAULT_NODE_CONFIG,
        help=f"Ruta del YAML a crear. Por defecto: {DEFAULT_NODE_CONFIG}.",
    )
    node.add_argument(
        "--advertise-addr",
        required=True,
        help="Dirección pública/anunciada del nodo, por ejemplo 192.168.1.42:50051.",
    )
    node.add_argument(
        "--bind-addr",
        default="[::]:50051",
        help="Dirección local de escucha gRPC. Por defecto: [::]:50051.",
    )
    node.add_argument(
        "--token",
        default="",
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
    node.add_argument("--remote-copies", type=int, default=3, help="Copias remotas completas por chunk. Por defecto: 3.")
    node.add_argument(
        "--strict-remote-copies",
        action="store_true",
        help="Falla si no se pueden alcanzar todas las copias remotas al proteger chunks.",
    )
    node.add_argument(
        "--force",
        action="store_true",
        help="Sobrescribe el YAML si ya existe.",
    )

    args = parser.parse_args(argv)

    if args.target == "node":
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


def _build_node_config(args: argparse.Namespace) -> StopanConfig:
    seeds = tuple(args.seed) if args.seed else (args.advertise_addr,)
    return StopanConfig().with_overrides(
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
        protection={
            "remote_copies": args.remote_copies,
            "strict_remote_copies": args.strict_remote_copies,
        },
    )


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

        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, path)
    except OSError as exc:
        raise StopanStorageError(f"No se pudo escribir la configuración {path}: {exc}") from exc
    finally:
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


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
    config_path = Path(args.config)
    if config_path.exists() and not args.force:
        raise StopanUsageError(
            f"Ya existe {config_path}. Usa --force si quieres sobrescribirlo."
        )

    cfg = _build_node_config(args)
    data = _plain_data(asdict(cfg))

    _write_yaml_atomic(config_path, data)
    prepared_dirs = _prepare_node_directories(cfg)

    # Relee el fichero escrito para validar que el YAML persistido coincide con el modelo.
    load_config(str(config_path))

    print(f"Configuración creada: {config_path}")
    print("Directorios preparados:")
    for directory in prepared_dirs:
        print(f"  {directory}")
    print("Validación correcta.")
    print("Arranque:")
    if str(config_path) == DEFAULT_NODE_CONFIG:
        print("  python -m stopan node")
    else:
        print(f"  python -m stopan node --config {config_path}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    if args.target == "node":
        return _init_node(args)

    raise StopanUsageError(f"Target desconocido: {args.target}")


if __name__ == "__main__":
    raise SystemExit(main())
