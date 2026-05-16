from __future__ import annotations

import argparse
from collections.abc import Sequence

from stopan.cli.config_utils import add_config_args, load_runtime_config
from stopan.config.defaults import DEFAULT_NODE_CONFIG


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stopan node",
        allow_abbrev=False,
        description="Arranca un nodo Stopan de almacenamiento P2P + membership.",
    )
    add_config_args(parser, default_config=DEFAULT_NODE_CONFIG)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_runtime_config(args)

    if not cfg.node.advertise_addr:
        raise ValueError(
            "Falta node.advertise_addr. Define advertise_addr en el fichero de configuración "
            f"usado con --config o en {DEFAULT_NODE_CONFIG}."
        )

    from stopan.node.server import serve

    serve(cfg)
    return 0
