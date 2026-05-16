from __future__ import annotations

import argparse
import importlib
import os
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from stopan.errors import StopanError


CommandMain = Callable[[Sequence[str] | None], int]


@dataclass(frozen=True, slots=True)
class CommandSpec:
    module_name: str
    description: str


COMMANDS: dict[str, CommandSpec] = {
    "backup": CommandSpec(
        module_name="stopan.cli.backup",
        description="Crea un snapshot local desde una carpeta",
    ),
    "push": CommandSpec(
        module_name="stopan.cli.push",
        description="Protege chunks pendientes en nodos remotos",
    ),
    "restore": CommandSpec(
        module_name="stopan.cli.restore",
        description="Restaura un snapshot desde CAS local y/o red",
    ),
    "verify": CommandSpec(
        module_name="stopan.cli.verify",
        description="Verifica la protección remota",
    ),
    "node": CommandSpec(
        module_name="stopan.cli.node",
        description="Arranca un nodo de almacenamiento y membership",
    ),
    "init": CommandSpec(
        module_name="stopan.cli.init",
        description="Inicializa configuración y directorios de Stopan",
    ),
    "config": CommandSpec(
        module_name="stopan.cli.config",
        description="Genera o valida ficheros de configuración Stopan",
    ),
    "metadata": CommandSpec(
        module_name="stopan.cli.metadata",
        description="Gestiona metadata cifrada local",
    ),
    "metadata-store-gc": CommandSpec(
        module_name="stopan.cli.metadata_store_gc",
        description="Limpia el distributed metadata pack store",
    ),
}

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="stopan",
        allow_abbrev=False,
        description="Stopan - backup distribuido content-addressed",
    )
    subparsers = parser.add_subparsers(
        dest="command",
        metavar="<command>",
        required=True,
    )

    for name, spec in COMMANDS.items():
        subparsers.add_parser(
            name,
            allow_abbrev=False,
            help=spec.description,
            description=spec.description,
            add_help=False,
        )

    return parser


def _normalize_help_args(args: list[str]) -> list[str]:
    if not args or args[0] != "help":
        return args
    if len(args) == 1:
        return ["--help"]
    return [args[1], "--help", *args[2:]]


def _load_command_main(spec: CommandSpec) -> CommandMain:
    module = importlib.import_module(spec.module_name)
    main_func = getattr(module, "main", None)
    if main_func is None or not callable(main_func):
        raise RuntimeError(f"Command module {spec.module_name!r} does not expose callable main(argv).")
    return main_func


def _dispatch(command: str, args: Sequence[str]) -> int:
    spec = COMMANDS[command]
    main_func = _load_command_main(spec)
    return int(main_func(list(args)))


def _debug_tracebacks_enabled() -> bool:
    value = os.getenv("STOPAN_DEBUG", "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _print_error(prefix: str, exc: BaseException) -> None:
    message = str(exc).strip() or exc.__class__.__name__
    print(f"{prefix}: {message}", file=sys.stderr)


def print_help() -> None:
    _build_parser().print_help()


def _main(argv: Sequence[str] | None = None) -> int:
    args = _normalize_help_args(list(sys.argv[1:] if argv is None else argv))
    parser = _build_parser()

    try:
        parsed, forwarded_args = parser.parse_known_args(args)
    except SystemExit as exc:
        return int(exc.code or 0)

    return _dispatch(parsed.command, forwarded_args)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return _main(argv)
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.", file=sys.stderr)
        return 130
    except StopanError as exc:
        if _debug_tracebacks_enabled():
            raise
        _print_error(exc.prefix, exc)
        return int(exc.exit_code)
    except Exception as exc:
        if _debug_tracebacks_enabled():
            raise
        _print_error("Error de Stopan", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
