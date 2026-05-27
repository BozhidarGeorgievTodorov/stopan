"""Validadores escalares pequeños compartidos.

Estas funciones no conocen los dominios de Stopan: reciben una fábrica de
excepción para que cada capa conserve su propio tipo de error.
"""

from __future__ import annotations

from collections.abc import Callable

ErrorFactory = Callable[[str], Exception]


def require_strict_int(
    name: str,
    value: object,
    *,
    min_value: int,
    error_factory: ErrorFactory,
    type_label: str = "int",
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise error_factory(f"{name} debe ser {type_label}; recibido {type(value).__name__}")
    if value < min_value:
        raise error_factory(f"{name} debe ser >= {min_value}; recibido {value}")
    return value


def require_strict_non_negative_int(
    name: str,
    value: object,
    *,
    error_factory: ErrorFactory,
    type_label: str = "int",
) -> int:
    return require_strict_int(
        name,
        value,
        min_value=0,
        error_factory=error_factory,
        type_label=type_label,
    )


def require_strict_positive_int(
    name: str,
    value: object,
    *,
    error_factory: ErrorFactory,
    type_label: str = "int",
) -> int:
    number = require_strict_non_negative_int(
        name,
        value,
        error_factory=error_factory,
        type_label=type_label,
    )
    if number <= 0:
        raise error_factory(f"{name} debe ser > 0; recibido {number}")
    return number
