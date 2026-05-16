from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from stopan.config.defaults import DEFAULT_NODE_CONFIG
from stopan.errors import StopanConfigError
from stopan.config.loader import load_config
from stopan.config.model import StopanConfig


def add_config_args(
    parser: argparse.ArgumentParser,
    *,
    default_config: str | None = None,
    required: bool = False,
) -> None:
    help_text = "Ruta al fichero YAML de configuración Stopan."
    if default_config is not None:
        help_text += f" Por defecto: {default_config}."

    parser.add_argument(
        "--config",
        default=default_config,
        required=required,
        help=help_text,
    )


def load_runtime_config(args: argparse.Namespace) -> StopanConfig:
    config_path = getattr(args, "config", None)
    if config_path == DEFAULT_NODE_CONFIG and not Path(config_path).exists():
        raise StopanConfigError(
            f"No existe el fichero de configuración por defecto: {DEFAULT_NODE_CONFIG}. "
            "Inicializa el nodo con: sudo python -m stopan init node "
            "--advertise-addr HOST:50051 [--token TOKEN] [--seed HOST:50051]"
        )
    return load_config(config_path)


def require_config_file(path: str | None) -> str:
    if not path:
        raise StopanConfigError("Falta --config con la ruta del fichero YAML de configuración.")

    config_path = Path(path)
    if not config_path.exists():
        raise StopanConfigError(f"No existe el fichero de configuración Stopan: {config_path}")

    return str(config_path)


def first_seed(cfg: StopanConfig) -> str | None:
    return cfg.cluster.seeds[0] if cfg.cluster.seeds else None


def choose(cli_value: Any, config_value: Any) -> Any:
    return config_value if cli_value is None else cli_value
