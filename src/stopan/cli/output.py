from __future__ import annotations


def format_bytes(value: int) -> str:
    if value < 1024:
        return f"{value} B"
    if value < 1024 * 1024:
        return f"{value / 1024:.2f} KiB"
    if value < 1024 * 1024 * 1024:
        return f"{value / (1024 * 1024):.2f} MiB"
    return f"{value / (1024 * 1024 * 1024):.2f} GiB"


def format_duration(seconds: float) -> str:
    return f"{seconds:.2f} segundos"


def format_speed(total_size: int, elapsed: float) -> str:
    if elapsed <= 0:
        return "n/a"

    mb_per_second = total_size / (1024 * 1024) / elapsed
    return f"{mb_per_second:.2f} MB/s"


def print_timing_summary(*, processed_bytes: int, elapsed: float) -> None:
    print(f"Tamaño procesado: {processed_bytes} bytes")
    print(f"Tiempo: {format_duration(elapsed)}")
    print(f"Velocidad: {format_speed(processed_bytes, elapsed)}")
