from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from stopan.config.example import EXAMPLE_CONFIG
from stopan.config.loader import load_config
from stopan.errors import StopanStorageError


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stopan config",
        allow_abbrev=False,
        description="Herramientas de configuración de Stopan.",
    )
    sub = parser.add_subparsers(dest="action", required=True)

    example = sub.add_parser("example", allow_abbrev=False, help="Imprime o escribe un node.yaml de ejemplo.")
    example.add_argument("--out", default=None, help="Ruta donde escribir el ejemplo. Si se omite, imprime stdout.")

    validate = sub.add_parser("validate", allow_abbrev=False, help="Valida un fichero de configuración.")
    validate.add_argument("config_path", help="Ruta del YAML de configuración a validar.")

    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    if args.action == "example":
        if args.out:
            path = Path(args.out)
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(EXAMPLE_CONFIG, encoding="utf-8")
            except OSError as exc:
                raise StopanStorageError(f"No se pudo escribir la configuración de ejemplo en {path}: {exc}") from exc
            print(f"Configuración de ejemplo escrita en: {path}")
        else:
            print(EXAMPLE_CONFIG)
        return 0

    if args.action == "validate":
        cfg = load_config(args.config_path)
        print("Configuración válida.")
        print(f"   advertise_addr={cfg.node.advertise_addr or '<empty>'}")
        print(f"   db_file={cfg.node.db_file}")
        print(f"   repo_store_dir={cfg.node.repo_store_dir}")
        print(f"   remote_copies={cfg.protection.remote_copies}")
        print(f"   seeds={list(cfg.cluster.seeds)}")
        return 0

    print(f"Acción desconocida: {args.action}", file=sys.stderr)
    return 2
