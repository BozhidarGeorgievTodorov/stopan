from __future__ import annotations

import argparse


def require_int_at_least(
    parser: argparse.ArgumentParser,
    value: int | None,
    *,
    flag: str,
    min_value: int,
) -> None:
    if value is not None and value < min_value:
        parser.error(f"{flag} debe ser >= {min_value}")


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
            parser.error(f"{flag} debe ser >= {min_value:g}")
    elif value <= min_value:
        parser.error(f"{flag} debe ser > {min_value:g}")


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
        parser.error(f"{flag} debe ser >= {min_value}")
    if value & (value - 1) != 0:
        parser.error(f"{flag} debe ser potencia de dos")


def validate_scrypt_overrides(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    require_power_of_two(parser, getattr(args, "scrypt_n", None), flag="--scrypt-n")
    require_int_at_least(parser, getattr(args, "scrypt_r", None), flag="--scrypt-r", min_value=1)
    require_int_at_least(parser, getattr(args, "scrypt_p", None), flag="--scrypt-p", min_value=1)
    require_int_at_least(
        parser,
        getattr(args, "metadata_key_length", None),
        flag="--metadata-key-length",
        min_value=32,
    )
