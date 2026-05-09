from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class JsonFileError(RuntimeError):
    pass


def canonical_json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def load_json_file(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise JsonFileError(f"No se pudo leer JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise JsonFileError(f"{path} no contiene un objeto JSON")
    return value
