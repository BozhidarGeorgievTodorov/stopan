from __future__ import annotations

import sys
from collections.abc import Callable


_COMMANDS = {
    "backup": ("stopan.cli.backup", "stopan backup"),
    "push": ("stopan.cli.push", "stopan push"),
    "restore": ("stopan.cli.restore", "stopan restore"),
    "verify": ("stopan.cli.verify", "stopan verify"),
}


def _dispatch(module_name: str, argv0: str, forwarded_args: list[str]) -> int:
    module = __import__(module_name, fromlist=["main"])
    main_func: Callable[[], int] = getattr(module, "main")

    old_argv = sys.argv[:]
    try:
        sys.argv = [argv0, *forwarded_args]
        return int(main_func())
    finally:
        sys.argv = old_argv


def print_help() -> None:
    print(
        """Stopan - sistema de backup distribuido direccionado por contenido

Uso:
  python -m stopan <comando> [args...]

Comandos:
  backup      Crea un snapshot local de una carpeta
  push        Protege chunks pendientes en nodos remotos
  restore     Restaura un snapshot desde CAS local y/o red
  verify      Audita la protección remota con ProbeMissingChunks
  node        Arranca un nodo de almacenamiento y membership

Ayuda:
  python -m stopan <comando> --help
"""
    )


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] in {"-h", "--help", "help"}:
        print_help()
        return 0

    command = sys.argv[1]
    args = sys.argv[2:]

    if command in _COMMANDS:
        module_name, argv0 = _COMMANDS[command]
        return _dispatch(module_name, argv0, args)

    if command == "node":
        if args:
            if args[0] in {"-h", "--help"}:
                print("Uso: python -m stopan node")
                print("Arranca el nodo P2P de almacenamiento y membership usando variables de entorno.")
                return 0
            print(f"Unknown node argument: {args[0]}", file=sys.stderr)
            return 2

        from stopan.node.server import serve

        serve()
        return 0

    print(f"Unknown command: {command}", file=sys.stderr)
    print("Run: python -m stopan --help", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
