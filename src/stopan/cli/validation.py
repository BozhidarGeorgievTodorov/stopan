from __future__ import annotations

import argparse
from collections.abc import Iterable
from dataclasses import dataclass
from typing import NoReturn

from stopan.errors import StopanUsageError


@dataclass(frozen=True, slots=True)
class IntRange:
    attr: str
    flag: str
    min_value: int


@dataclass(frozen=True, slots=True)
class FloatRange:
    attr: str
    flag: str
    min_value: float
    inclusive: bool = True


@dataclass(frozen=True, slots=True)
class Flag:
    attr: str
    flag: str


class CLIUsageError(StopanUsageError):
    """Error de uso detectado después de mezclar CLI y configuración."""


def parser_error(parser: argparse.ArgumentParser, message: str) -> NoReturn:
    parser.error(message)
    raise AssertionError("argparse.ArgumentParser.error no ha terminado la ejecución")


def is_present(value: object) -> bool:
    return value is not None and value is not False


def require_int_at_least(
    parser: argparse.ArgumentParser,
    value: int | None,
    *,
    flag: str,
    min_value: int,
) -> None:
    if value is not None and value < min_value:
        parser_error(parser, f"{flag} debe ser >= {min_value}")


def require_float_at_least(
    parser: argparse.ArgumentParser,
    value: float | None,
    *,
    flag: str,
    min_value: float,
    inclusive: bool,
) -> None:
    if value is None:
        return
    if inclusive:
        if value < min_value:
            parser_error(parser, f"{flag} debe ser >= {min_value:g}")
    elif value <= min_value:
        parser_error(parser, f"{flag} debe ser > {min_value:g}")


def validate_int_ranges(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    rules: Iterable[IntRange],
) -> None:
    for rule in rules:
        require_int_at_least(
            parser,
            getattr(args, rule.attr, None),
            flag=rule.flag,
            min_value=rule.min_value,
        )


def validate_float_ranges(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    rules: Iterable[FloatRange],
) -> None:
    for rule in rules:
        require_float_at_least(
            parser,
            getattr(args, rule.attr, None),
            flag=rule.flag,
            min_value=rule.min_value,
            inclusive=rule.inclusive,
        )


def require_power_of_two(
    parser: argparse.ArgumentParser,
    value: int | None,
    *,
    flag: str,
    min_value: int = 2,
) -> None:
    if value is None:
        return
    if value < min_value:
        parser_error(parser, f"{flag} debe ser >= {min_value}")
    if value & (value - 1) != 0:
        parser_error(parser, f"{flag} debe ser potencia de dos")


def require_present(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    item: Flag,
    message: str,
) -> None:
    if getattr(args, item.attr, None) is None:
        parser_error(parser, message)


def reject_present(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    items: Iterable[Flag],
    message: str,
) -> None:
    for item in items:
        if is_present(getattr(args, item.attr, None)):
            parser_error(parser, message.format(flag=item.flag))


def reject_together(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    left: Flag,
    right: Flag,
    message: str | None = None,
) -> None:
    if is_present(getattr(args, left.attr, None)) and is_present(getattr(args, right.attr, None)):
        parser_error(parser, message or f"'{left.flag}' y '{right.flag}' son incompatibles")


def require_dependency(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    dependent: Flag,
    required: Flag,
    message: str | None = None,
) -> None:
    if is_present(getattr(args, dependent.attr, None)) and not is_present(getattr(args, required.attr, None)):
        parser_error(parser, message or f"{dependent.flag} requiere {required.flag}")


def validate_scrypt_overrides(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    require_power_of_two(parser, getattr(args, "scrypt_n", None), flag="--scrypt-n")
    validate_int_ranges(
        parser,
        args,
        (
            IntRange("scrypt_r", "--scrypt-r", 1),
            IntRange("scrypt_p", "--scrypt-p", 1),
            IntRange("metadata_key_length", "--metadata-key-length", 32),
        ),
    )
