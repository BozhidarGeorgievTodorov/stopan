from __future__ import annotations

import argparse
import os
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timezone

from stopan.cli.config_utils import add_config_args, load_runtime_config
from stopan.cli.output import format_bytes
from stopan.cli.validation import IntRange, validate_int_ranges
from stopan.errors import StopanDataError
from stopan.metadata.database import MetadataDB, MetadataDBAccessMode, SnapshotRecord


_SNAPSHOT_STATUSES = ("CREATING", "COMPLETE", "FAILED")


@dataclass(frozen=True, slots=True)
class _ParsedTime:
    instant_utc: datetime
    sqlite_value: str
    date_only: bool


def _snapshot_selector(value: str) -> int | str:
    text = str(value).strip()
    if text.isdecimal():
        snapshot_id = int(text)
        if snapshot_id < 1:
            raise argparse.ArgumentTypeError("el ID del snapshot debe ser >= 1")
        return snapshot_id

    try:
        return str(uuid.UUID(text))
    except (ValueError, AttributeError) as exc:
        raise argparse.ArgumentTypeError(
            "debe ser un ID local positivo o un UUID de snapshot válido"
        ) from exc


def _parse_time(value: str, *, flag: str) -> _ParsedTime:
    text = str(value).strip()
    try:
        if len(text) == 10 and text[4] == "-" and text[7] == "-":
            day = date.fromisoformat(text)
            instant = datetime.combine(day, time.min, tzinfo=timezone.utc)
            return _ParsedTime(
                instant_utc=instant,
                sqlite_value=_sqlite_utc_timestamp(instant),
                date_only=True,
            )

        if "T" not in text:
            raise ValueError
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        instant = datetime.fromisoformat(normalized)
        if instant.tzinfo is None or instant.utcoffset() is None:
            raise ValueError
        instant_utc = instant.astimezone(timezone.utc)
        return _ParsedTime(
            instant_utc=instant_utc,
            sqlite_value=_sqlite_utc_timestamp(instant_utc),
            date_only=False,
        )
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"{flag} debe ser YYYY-MM-DD o un timestamp ISO 8601 con zona horaria "
            "(por ejemplo 2026-09-03T18:00:00Z o 2026-09-03T20:00:00+02:00)"
        ) from exc


def _sqlite_utc_timestamp(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    if utc_value.microsecond:
        return utc_value.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0")
    return utc_value.strftime("%Y-%m-%d %H:%M:%S")


def _format_catalog_timestamp(value: str) -> str:
    text = str(value).strip()
    if not text:
        return "<desconocida>"
    if text.endswith("Z") or "+" in text[10:] or "-" in text[10:]:
        try:
            normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is not None and parsed.utcoffset() is not None:
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
                text = parsed.isoformat(sep=" ", timespec="seconds")
        except ValueError:
            pass
    return f"{text}Z"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="stopan snapshot",
        allow_abbrev=False,
        description="Consulta snapshots registrados en el catálogo local.",
    )
    subparsers = parser.add_subparsers(dest="action", required=True)

    list_parser = subparsers.add_parser(
        "list",
        allow_abbrev=False,
        help="Lista snapshots del catálogo con filtros opcionales.",
    )
    add_config_args(list_parser)
    list_parser.add_argument(
        "--status",
        action="append",
        choices=_SNAPSHOT_STATUSES,
        default=None,
        help="Filtra por estado. Puede repetirse para combinar estados.",
    )
    list_parser.add_argument(
        "--root",
        default=None,
        help="Filtra por la ruta raíz exacta del snapshot.",
    )
    list_parser.add_argument(
        "--from",
        dest="created_from",
        default=None,
        help=(
            "Límite temporal inferior. Acepta YYYY-MM-DD (día UTC) o timestamp "
            "ISO 8601 con zona horaria."
        ),
    )
    list_parser.add_argument(
        "--to",
        dest="created_to",
        default=None,
        help=(
            "Límite temporal superior. YYYY-MM-DD incluye el día UTC completo. "
            "Los timestamps deben incluir zona horaria."
        ),
    )
    list_parser.add_argument(
        "--last",
        type=int,
        default=None,
        help="Limita el resultado a los N snapshots más recientes tras aplicar los filtros.",
    )

    show_parser = subparsers.add_parser(
        "show",
        allow_abbrev=False,
        help="Muestra los datos de un snapshot por ID local o UUID.",
    )
    add_config_args(show_parser)
    show_parser.add_argument(
        "snapshot",
        type=_snapshot_selector,
        help="ID local positivo o UUID estable del snapshot.",
    )

    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.action == "list":
        validate_int_ranges(parser, args, (IntRange("last", "--last", 1),))
        try:
            args.created_from_parsed = (
                _parse_time(args.created_from, flag="--from")
                if args.created_from
                else None
            )
            args.created_to_parsed = (
                _parse_time(args.created_to, flag="--to")
                if args.created_to
                else None
            )
        except argparse.ArgumentTypeError as exc:
            parser.error(str(exc))

        if args.created_to_parsed is not None and args.created_to_parsed.date_only:
            upper_instant = datetime.combine(
                args.created_to_parsed.instant_utc.date(),
                time.max,
                tzinfo=timezone.utc,
            )
            args.created_to_parsed = _ParsedTime(
                instant_utc=upper_instant,
                sqlite_value=_sqlite_utc_timestamp(upper_instant),
                date_only=True,
            )

        if args.created_from_parsed is not None and args.created_to_parsed is not None:
            if args.created_from_parsed.instant_utc > args.created_to_parsed.instant_utc:
                parser.error("--from no puede ser posterior a --to")

    return args


def _print_table(records: Sequence[SnapshotRecord]) -> None:
    headers = ("ID", "CREADA (UTC)", "ESTADO", "ARCHIVOS", "TAMAÑO", "RAÍZ")
    rows = [
        (
            str(record.id),
            _format_catalog_timestamp(record.created_at),
            record.status,
            str(record.total_files),
            format_bytes(record.total_size),
            record.root_path,
        )
        for record in records
    ]
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(len(headers))
    ]

    def render(row: Sequence[str], *, header: bool = False) -> str:
        cells: list[str] = []
        for index, value in enumerate(row):
            if not header and index in {0, 3, 4}:
                cells.append(value.rjust(widths[index]))
            else:
                cells.append(value.ljust(widths[index]))
        return "  ".join(cells).rstrip()

    print(render(headers, header=True))
    for row in rows:
        print(render(row))


def _cmd_list(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    db = MetadataDB(
        cfg.node.catalog_file,
        init_schema=False,
        access_mode=MetadataDBAccessMode.READ_ONLY,
    )
    try:
        with db.read_snapshot():
            records = db.list_snapshots(
                statuses=args.status,
                root_path=os.path.abspath(args.root) if args.root is not None else None,
                created_from_utc=(
                    args.created_from_parsed.sqlite_value
                    if args.created_from_parsed is not None
                    else None
                ),
                created_to_utc=(
                    args.created_to_parsed.sqlite_value
                    if args.created_to_parsed is not None
                    else None
                ),
                limit=args.last,
            )
    finally:
        db.close()

    if not records:
        filtered = any(
            value is not None
            for value in (args.status, args.root, args.created_from, args.created_to, args.last)
        )
        print(
            "No hay snapshots que coincidan con los filtros."
            if filtered
            else "No hay snapshots en el catálogo."
        )
        return 0

    _print_table(records)
    return 0


def _cmd_show(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)
    db = MetadataDB(
        cfg.node.catalog_file,
        init_schema=False,
        access_mode=MetadataDBAccessMode.READ_ONLY,
    )
    try:
        with db.read_snapshot():
            record = db.get_snapshot(args.snapshot)
    finally:
        db.close()

    if record is None:
        raise StopanDataError(f"snapshot no encontrado: {args.snapshot}")

    print(f"Snapshot {record.id}")
    print(f"   UUID: {record.uuid}")
    print(f"   Estado: {record.status}")
    print(f"   Creado (UTC): {_format_catalog_timestamp(record.created_at)}")
    print(f"   Ruta raíz: {record.root_path}")
    print(f"   Nodo origen: {record.origin_node_id}")
    print(f"   Archivos: {record.total_files}")
    print(f"   Tamaño: {format_bytes(record.total_size)}")
    if record.error:
        print(f"   Error: {record.error}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.action == "list":
        return _cmd_list(args)
    if args.action == "show":
        return _cmd_show(args)
    raise AssertionError(f"acción snapshot desconocida: {args.action!r}")


if __name__ == "__main__":
    raise SystemExit(main())
