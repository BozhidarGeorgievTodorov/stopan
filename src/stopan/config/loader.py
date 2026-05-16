from __future__ import annotations

from dataclasses import fields
from pathlib import Path
from typing import Any, Callable

import yaml

from stopan.errors import StopanConfigError
from stopan.config.model import (
    ClusterConfig,
    GcConfig,
    GrpcConfig,
    MembershipConfig,
    MetadataConfig,
    NodeConfig,
    ProtectionConfig,
    ReplicationConfig,
    RestoreConfig,
    StopanConfig,
    StorageConfig,
    VerifyConfig,
)

_SECTION_TYPES = {
    "node": NodeConfig,
    "cluster": ClusterConfig,
    "protection": ProtectionConfig,
    "grpc": GrpcConfig,
    "storage": StorageConfig,
    "replication": ReplicationConfig,
    "verify": VerifyConfig,
    "restore": RestoreConfig,
    "membership": MembershipConfig,
    "metadata": MetadataConfig,
    "gc": GcConfig,
}


def load_config(path: str | None = None) -> StopanConfig:
    """
    Carga configuración Stopan.

    Precedencia:
      defaults -> yaml

    Los argumentos explícitos de CLI deben aplicarse después de esta función.
    """
    cfg = StopanConfig()

    if path:
        try:
            cfg = _apply_sections(cfg, _read_yaml(Path(path)), source=str(path))
        except ValueError as exc:
            raise StopanConfigError(str(exc)) from exc

    return cfg


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise StopanConfigError(f"No existe el fichero de configuración Stopan: {path}")

    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise StopanConfigError(f"No se pudo leer el fichero de configuración Stopan {path}: {exc}") from exc

    try:
        data = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise StopanConfigError(f"YAML de configuración inválido en {path}: {exc}") from exc
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise StopanConfigError(f"El fichero de configuración debe contener un objeto YAML: {path}")
    return data


def _apply_sections(cfg: StopanConfig, raw: dict[str, Any], *, source: str) -> StopanConfig:
    sections: dict[str, dict[str, Any]] = {}

    for section_name, values in raw.items():
        if section_name not in _SECTION_TYPES:
            raise StopanConfigError(f"Sección desconocida en configuración {source}: {section_name}")

        if values is None:
            continue
        if not isinstance(values, dict):
            raise StopanConfigError(f"La sección '{section_name}' en {source} debe ser un objeto/mapa.")

        sections[section_name] = _validate_section(
            section_name=section_name,
            values=values,
            source=source,
        )

    return cfg.with_overrides(**sections)


def _validate_section(*, section_name: str, values: dict[str, Any], source: str) -> dict[str, Any]:
    section_type = _SECTION_TYPES[section_name]
    valid_fields = {field.name for field in fields(section_type)}

    unknown = sorted(set(values) - valid_fields)
    if unknown:
        raise StopanConfigError(f"Campos desconocidos en sección '{section_name}' de {source}: {unknown}")

    converted: dict[str, Any] = {}
    for field_name, value in values.items():
        converted[field_name] = _coerce_field(
            section_name=section_name,
            field_name=field_name,
            value=value,
            source=source,
        )

    return converted


def _coerce_field(*, section_name: str, field_name: str, value: Any, source: str) -> Any:
    if section_name == "cluster" and field_name == "seeds":
        return _coerce_seeds(value, source=source)

    converter = _FIELD_CONVERTERS.get(section_name, {}).get(field_name)
    if converter is None:
        return value

    try:
        return converter(value)
    except (TypeError, ValueError) as exc:
        raise StopanConfigError(
            f"Valor inválido para {section_name}.{field_name} en {source}: {value!r}"
        ) from exc


def _coerce_seeds(value: Any, *, source: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(seed.strip() for seed in value.split(",") if seed.strip())
    if isinstance(value, (list, tuple)):
        return tuple(str(seed).strip() for seed in value if str(seed).strip())
    raise StopanConfigError(f"cluster.seeds en {source} debe ser lista, tupla o string separado por comas")


def _as_str(value: Any) -> str:
    if not isinstance(value, str):
        raise TypeError("expected str")
    return value


def _as_int(value: Any) -> int:
    if isinstance(value, bool):
        raise TypeError("expected int")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value.strip())
    raise TypeError("expected int")


def _as_float(value: Any) -> float:
    if isinstance(value, bool):
        raise TypeError("expected float")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value.strip())
    raise TypeError("expected float")


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    raise TypeError("expected bool")


_FIELD_CONVERTERS: dict[str, dict[str, Callable[[Any], Any]]] = {
    "node": {
        "bind_addr": _as_str,
        "advertise_addr": _as_str,
        "repo_store_dir": _as_str,
        "local_shard_dir": _as_str,
        "db_file": _as_str,
    },
    "cluster": {
        "token": _as_str,
    },
    "protection": {
        "remote_copies": _as_int,
        "strict_remote_copies": _as_bool,
    },
    "grpc": {
        "max_message_bytes": _as_int,
        "keepalive_time_ms": _as_int,
        "keepalive_timeout_ms": _as_int,
        "keepalive_permit_without_calls": _as_bool,
    },
    "storage": {
        "rpc_workers": _as_int,
        "commit_workers": _as_int,
        "commit_queue_items": _as_int,
        "max_chunk_size": _as_int,
    },
    "replication": {
        "target_parallelism": _as_int,
        "probe_batch_hashes": _as_int,
        "stream_inflight": _as_int,
        "probe_timeout_s": _as_float,
        "stream_timeout_s": _as_float,
        "commit_every": _as_int,
    },
    "verify": {
        "target_parallelism": _as_int,
        "probe_batch_hashes": _as_int,
        "probe_timeout_s": _as_float,
    },
    "restore": {
        "batch_target_parallelism": _as_int,
        "prefetch_window": _as_int,
        "rpc_timeout_s": _as_float,
    },
    "membership": {
        "protocol_period_s": _as_float,
        "ping_timeout_s": _as_float,
        "suspect_timeout_s": _as_float,
        "indirect_ping_fanout": _as_int,
        "max_gossip_events": _as_int,
        "gossip_ttl_s": _as_float,
        "rpc_timeout_s": _as_float,
    },
    "metadata": {
        "passphrase_file": _as_str,
        "owner_id": _as_str,
        "identity_file": _as_str,
        "object_graph_auto_export": _as_bool,
        "object_store_dir": _as_str,
        "object_graph_include_protection": _as_bool,
        "object_graph_auto_pack": _as_bool,
        "object_pack_dir": _as_str,
        "distributed_pack_store_dir": _as_str,
        "pack_copies": _as_int,
        "strict_pack_copies": _as_bool,
        "max_distributed_pack_bytes": _as_int,
        "max_distributed_packs_per_owner": _as_int,
        "max_distributed_pack_bytes_per_owner": _as_int,
        "max_distributed_pack_store_bytes": _as_int,
        "scrypt_n": _as_int,
        "scrypt_r": _as_int,
        "scrypt_p": _as_int,
        "key_length": _as_int,
    },
    "gc": {
        "local_cas_grace_hours": _as_float,
        "restore_output_max_age_days": _as_int,
        "node_cas_max_age_days": _as_int,
        "metadata_object_store_grace_hours": _as_float,
        "metadata_object_pack_grace_hours": _as_float,
        "distributed_pack_max_age_days": _as_int,
    },
}
