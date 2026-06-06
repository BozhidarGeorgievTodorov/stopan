from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

from stopan.errors import StopanConfigTypeError, StopanConfigValueError

from stopan.config.defaults import (
    DEFAULT_BACKUP_WORKERS,
    DEFAULT_CLUSTER_SEEDS,
    DEFAULT_CLUSTER_TOKEN,
    DEFAULT_GC_DISTRIBUTED_PACK_MAX_AGE_DAYS,
    DEFAULT_GC_LOCAL_CAS_GRACE_HOURS,
    DEFAULT_GC_METADATA_OBJECT_PACK_GRACE_HOURS,
    DEFAULT_GC_METADATA_OBJECT_STORE_GRACE_HOURS,
    DEFAULT_GC_NODE_CAS_MAX_AGE_DAYS,
    DEFAULT_GC_RESTORE_OUTPUT_MAX_AGE_DAYS,
    DEFAULT_EC_PACK_SIZE_BYTES,
    DEFAULT_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS,
    DEFAULT_GRPC_KEEPALIVE_TIME_MS,
    DEFAULT_GRPC_KEEPALIVE_TIMEOUT_MS,
    DEFAULT_GRPC_MAX_MESSAGE_BYTES,
    DEFAULT_MEMBERSHIP_GOSSIP_TTL_S,
    DEFAULT_MEMBERSHIP_INDIRECT_PING_FANOUT,
    DEFAULT_MEMBERSHIP_MAX_GOSSIP_EVENTS,
    DEFAULT_MEMBERSHIP_PING_TIMEOUT_S,
    DEFAULT_MEMBERSHIP_PROTOCOL_PERIOD_S,
    DEFAULT_MEMBERSHIP_RPC_TIMEOUT_S,
    DEFAULT_MEMBERSHIP_SUSPECT_TIMEOUT_S,
    DEFAULT_METADATA_DISTRIBUTED_PACK_STORE_DIR,
    DEFAULT_METADATA_IDENTITY_FILE,
    DEFAULT_METADATA_KEY_LENGTH,
    DEFAULT_METADATA_MAX_DISTRIBUTED_PACK_BYTES,
    DEFAULT_METADATA_MAX_DISTRIBUTED_PACK_BYTES_PER_OWNER,
    DEFAULT_METADATA_MAX_DISTRIBUTED_PACK_STORE_BYTES,
    DEFAULT_METADATA_MAX_DISTRIBUTED_PACKS_PER_OWNER,
    DEFAULT_METADATA_OBJECT_GRAPH_AUTO_EXPORT,
    DEFAULT_METADATA_OBJECT_GRAPH_AUTO_PACK,
    DEFAULT_METADATA_OBJECT_GRAPH_INCLUDE_PROTECTION,
    DEFAULT_METADATA_OBJECT_PACK_DIR,
    DEFAULT_METADATA_OBJECT_STORE_DIR,
    DEFAULT_METADATA_OWNER_ID,
    DEFAULT_METADATA_PACK_COPIES,
    DEFAULT_METADATA_PACK_DISCOVERY_MAX_CANDIDATES,
    DEFAULT_METADATA_PACK_TARGET_PARALLELISM,
    DEFAULT_METADATA_PACK_RPC_TIMEOUT_S,
    DEFAULT_METADATA_CLI_WARNING_LIMIT,
    DEFAULT_METADATA_PASSPHRASE_FILE,
    DEFAULT_METADATA_SCRYPT_N,
    DEFAULT_METADATA_STRICT_PACK_COPIES,
    DEFAULT_METADATA_SCRYPT_P,
    DEFAULT_METADATA_SCRYPT_R,
    DEFAULT_NODE_ADVERTISE_ADDR,
    DEFAULT_NODE_BIND_ADDR,
    DEFAULT_NODE_DB_FILE,
    DEFAULT_NODE_LOCAL_SHARD_DIR,
    DEFAULT_NODE_REPO_STORE_DIR,
    DEFAULT_PROTECTION_REMOTE_COPIES,
    DEFAULT_PROTECTION_STRICT_REMOTE_COPIES,
    DEFAULT_REPLICATION_COMMIT_EVERY,
    DEFAULT_REPLICATION_PROBE_BATCH_HASHES,
    DEFAULT_REPLICATION_PROBE_TIMEOUT_S,
    DEFAULT_REPLICATION_STREAM_INFLIGHT,
    DEFAULT_REPLICATION_STREAM_TIMEOUT_S,
    DEFAULT_REPLICATION_TARGET_PARALLELISM,
    DEFAULT_RESTORE_BATCH_TARGET_PARALLELISM,
    DEFAULT_RESTORE_PREFETCH_WINDOW,
    DEFAULT_RESTORE_RPC_TIMEOUT_S,
    DEFAULT_STORAGE_COMMIT_QUEUE_ITEMS,
    DEFAULT_STORAGE_COMMIT_WORKERS,
    DEFAULT_STORAGE_MAX_CHUNK_SIZE,
    DEFAULT_STORAGE_RPC_WORKERS,
    DEFAULT_VERIFY_PROBE_BATCH_HASHES,
    DEFAULT_VERIFY_PROBE_TIMEOUT_S,
    DEFAULT_VERIFY_TARGET_PARALLELISM,
)


def _require_str(name: str, value: object, *, allow_empty: bool = False) -> None:
    if not isinstance(value, str):
        raise StopanConfigTypeError(f"{name} debe ser string; recibido {type(value).__name__}")
    if not allow_empty and not value.strip():
        raise StopanConfigValueError(f"{name} no puede estar vacío")


def _require_bool(name: str, value: object) -> None:
    if not isinstance(value, bool):
        raise StopanConfigTypeError(f"{name} debe ser booleano; recibido {type(value).__name__}")


def _require_int(name: str, value: object, *, min_value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise StopanConfigTypeError(f"{name} debe ser entero; recibido {type(value).__name__}")
    if value < min_value:
        raise StopanConfigValueError(f"{name} debe ser >= {min_value}; recibido {value}")


def _require_float(name: str, value: object, *, min_value: float, inclusive: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise StopanConfigTypeError(f"{name} debe ser numérico; recibido {type(value).__name__}")

    numeric = float(value)
    if inclusive:
        if numeric < min_value:
            raise StopanConfigValueError(f"{name} debe ser >= {min_value}; recibido {value}")
    elif numeric <= min_value:
        raise StopanConfigValueError(f"{name} debe ser > {min_value}; recibido {value}")


def _require_str_tuple(name: str, value: object) -> None:
    if not isinstance(value, tuple):
        raise StopanConfigTypeError(f"{name} debe ser una tupla de strings; recibido {type(value).__name__}")
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip():
            raise StopanConfigValueError(f"{name}[{index}] debe ser string no vacío")


def _require_hex64(name: str, value: str) -> None:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise StopanConfigValueError(f"{name} debe ser hex lowercase de 64 caracteres")


@dataclass(frozen=True)
class NodeConfig:
    bind_addr: str = DEFAULT_NODE_BIND_ADDR
    advertise_addr: str = DEFAULT_NODE_ADVERTISE_ADDR
    repo_store_dir: str = DEFAULT_NODE_REPO_STORE_DIR
    local_shard_dir: str = DEFAULT_NODE_LOCAL_SHARD_DIR
    db_file: str = DEFAULT_NODE_DB_FILE

    def __post_init__(self) -> None:
        _require_str("node.bind_addr", self.bind_addr)
        _require_str("node.advertise_addr", self.advertise_addr, allow_empty=True)
        _require_str("node.repo_store_dir", self.repo_store_dir)
        _require_str("node.local_shard_dir", self.local_shard_dir)
        _require_str("node.db_file", self.db_file)


@dataclass(frozen=True)
class ClusterConfig:
    token: str = DEFAULT_CLUSTER_TOKEN
    seeds: tuple[str, ...] = DEFAULT_CLUSTER_SEEDS

    def __post_init__(self) -> None:
        _require_str("cluster.token", self.token, allow_empty=True)
        _require_str_tuple("cluster.seeds", self.seeds)


@dataclass(frozen=True)
class ProtectionConfig:
    remote_copies: int = DEFAULT_PROTECTION_REMOTE_COPIES
    strict_remote_copies: bool = DEFAULT_PROTECTION_STRICT_REMOTE_COPIES
    ec_pack_size_bytes: int = DEFAULT_EC_PACK_SIZE_BYTES

    def __post_init__(self) -> None:
        _require_int("protection.remote_copies", self.remote_copies, min_value=0)
        _require_bool("protection.strict_remote_copies", self.strict_remote_copies)
        _require_int("protection.ec_pack_size_bytes", self.ec_pack_size_bytes, min_value=1)


@dataclass(frozen=True)
class BackupConfig:
    workers: int = DEFAULT_BACKUP_WORKERS

    def __post_init__(self) -> None:
        _require_int("backup.workers", self.workers, min_value=1)


@dataclass(frozen=True)
class GrpcConfig:
    max_message_bytes: int = DEFAULT_GRPC_MAX_MESSAGE_BYTES
    keepalive_time_ms: int = DEFAULT_GRPC_KEEPALIVE_TIME_MS
    keepalive_timeout_ms: int = DEFAULT_GRPC_KEEPALIVE_TIMEOUT_MS
    keepalive_permit_without_calls: bool = DEFAULT_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS

    def __post_init__(self) -> None:
        _require_int("grpc.max_message_bytes", self.max_message_bytes, min_value=1)
        _require_int("grpc.keepalive_time_ms", self.keepalive_time_ms, min_value=1)
        _require_int("grpc.keepalive_timeout_ms", self.keepalive_timeout_ms, min_value=1)
        _require_bool("grpc.keepalive_permit_without_calls", self.keepalive_permit_without_calls)


@dataclass(frozen=True)
class StorageConfig:
    rpc_workers: int = DEFAULT_STORAGE_RPC_WORKERS
    commit_workers: int = DEFAULT_STORAGE_COMMIT_WORKERS
    commit_queue_items: int = DEFAULT_STORAGE_COMMIT_QUEUE_ITEMS
    max_chunk_size: int = DEFAULT_STORAGE_MAX_CHUNK_SIZE

    def __post_init__(self) -> None:
        _require_int("storage.rpc_workers", self.rpc_workers, min_value=1)
        _require_int("storage.commit_workers", self.commit_workers, min_value=1)
        _require_int("storage.commit_queue_items", self.commit_queue_items, min_value=1)
        _require_int("storage.max_chunk_size", self.max_chunk_size, min_value=1)


@dataclass(frozen=True)
class ReplicationConfig:
    target_parallelism: int = DEFAULT_REPLICATION_TARGET_PARALLELISM
    probe_batch_hashes: int = DEFAULT_REPLICATION_PROBE_BATCH_HASHES
    stream_inflight: int = DEFAULT_REPLICATION_STREAM_INFLIGHT
    probe_timeout_s: float = DEFAULT_REPLICATION_PROBE_TIMEOUT_S
    stream_timeout_s: float = DEFAULT_REPLICATION_STREAM_TIMEOUT_S
    commit_every: int = DEFAULT_REPLICATION_COMMIT_EVERY

    def __post_init__(self) -> None:
        _require_int("replication.target_parallelism", self.target_parallelism, min_value=1)
        _require_int("replication.probe_batch_hashes", self.probe_batch_hashes, min_value=1)
        _require_int("replication.stream_inflight", self.stream_inflight, min_value=1)
        _require_float("replication.probe_timeout_s", self.probe_timeout_s, min_value=0.0)
        _require_float("replication.stream_timeout_s", self.stream_timeout_s, min_value=0.0)
        _require_int("replication.commit_every", self.commit_every, min_value=1)


@dataclass(frozen=True)
class VerifyConfig:
    target_parallelism: int = DEFAULT_VERIFY_TARGET_PARALLELISM
    probe_batch_hashes: int = DEFAULT_VERIFY_PROBE_BATCH_HASHES
    probe_timeout_s: float = DEFAULT_VERIFY_PROBE_TIMEOUT_S

    def __post_init__(self) -> None:
        _require_int("verify.target_parallelism", self.target_parallelism, min_value=1)
        _require_int("verify.probe_batch_hashes", self.probe_batch_hashes, min_value=1)
        _require_float("verify.probe_timeout_s", self.probe_timeout_s, min_value=0.0)


@dataclass(frozen=True)
class RestoreConfig:
    batch_target_parallelism: int = DEFAULT_RESTORE_BATCH_TARGET_PARALLELISM
    prefetch_window: int = DEFAULT_RESTORE_PREFETCH_WINDOW
    rpc_timeout_s: float = DEFAULT_RESTORE_RPC_TIMEOUT_S

    def __post_init__(self) -> None:
        _require_int("restore.batch_target_parallelism", self.batch_target_parallelism, min_value=1)
        _require_int("restore.prefetch_window", self.prefetch_window, min_value=1)
        _require_float("restore.rpc_timeout_s", self.rpc_timeout_s, min_value=0.0)


@dataclass(frozen=True)
class MembershipConfig:
    protocol_period_s: float = DEFAULT_MEMBERSHIP_PROTOCOL_PERIOD_S
    ping_timeout_s: float = DEFAULT_MEMBERSHIP_PING_TIMEOUT_S
    rpc_timeout_s: float = DEFAULT_MEMBERSHIP_RPC_TIMEOUT_S
    suspect_timeout_s: float = DEFAULT_MEMBERSHIP_SUSPECT_TIMEOUT_S
    indirect_ping_fanout: int = DEFAULT_MEMBERSHIP_INDIRECT_PING_FANOUT
    max_gossip_events: int = DEFAULT_MEMBERSHIP_MAX_GOSSIP_EVENTS
    gossip_ttl_s: float = DEFAULT_MEMBERSHIP_GOSSIP_TTL_S

    def __post_init__(self) -> None:
        _require_float("membership.protocol_period_s", self.protocol_period_s, min_value=0.0)
        _require_float("membership.ping_timeout_s", self.ping_timeout_s, min_value=0.0)
        _require_float("membership.rpc_timeout_s", self.rpc_timeout_s, min_value=0.0)
        _require_float("membership.suspect_timeout_s", self.suspect_timeout_s, min_value=0.0)
        _require_int("membership.indirect_ping_fanout", self.indirect_ping_fanout, min_value=0)
        _require_int("membership.max_gossip_events", self.max_gossip_events, min_value=0)
        _require_float("membership.gossip_ttl_s", self.gossip_ttl_s, min_value=0.0)


@dataclass(frozen=True)
class MetadataConfig:
    passphrase_file: str = DEFAULT_METADATA_PASSPHRASE_FILE
    owner_id: str = DEFAULT_METADATA_OWNER_ID
    identity_file: str = DEFAULT_METADATA_IDENTITY_FILE
    object_graph_auto_export: bool = DEFAULT_METADATA_OBJECT_GRAPH_AUTO_EXPORT
    object_store_dir: str = DEFAULT_METADATA_OBJECT_STORE_DIR
    object_graph_include_protection: bool = DEFAULT_METADATA_OBJECT_GRAPH_INCLUDE_PROTECTION
    object_graph_auto_pack: bool = DEFAULT_METADATA_OBJECT_GRAPH_AUTO_PACK
    object_pack_dir: str = DEFAULT_METADATA_OBJECT_PACK_DIR
    distributed_pack_store_dir: str = DEFAULT_METADATA_DISTRIBUTED_PACK_STORE_DIR
    pack_copies: int = DEFAULT_METADATA_PACK_COPIES
    strict_pack_copies: bool = DEFAULT_METADATA_STRICT_PACK_COPIES
    pack_discovery_max_candidates: int = DEFAULT_METADATA_PACK_DISCOVERY_MAX_CANDIDATES
    pack_target_parallelism: int = DEFAULT_METADATA_PACK_TARGET_PARALLELISM
    pack_rpc_timeout_s: float = DEFAULT_METADATA_PACK_RPC_TIMEOUT_S
    cli_warning_limit: int = DEFAULT_METADATA_CLI_WARNING_LIMIT
    max_distributed_pack_bytes: int = DEFAULT_METADATA_MAX_DISTRIBUTED_PACK_BYTES
    max_distributed_packs_per_owner: int = DEFAULT_METADATA_MAX_DISTRIBUTED_PACKS_PER_OWNER
    max_distributed_pack_bytes_per_owner: int = DEFAULT_METADATA_MAX_DISTRIBUTED_PACK_BYTES_PER_OWNER
    max_distributed_pack_store_bytes: int = DEFAULT_METADATA_MAX_DISTRIBUTED_PACK_STORE_BYTES
    scrypt_n: int = DEFAULT_METADATA_SCRYPT_N
    scrypt_r: int = DEFAULT_METADATA_SCRYPT_R
    scrypt_p: int = DEFAULT_METADATA_SCRYPT_P
    key_length: int = DEFAULT_METADATA_KEY_LENGTH

    def __post_init__(self) -> None:
        _require_str("metadata.passphrase_file", self.passphrase_file, allow_empty=True)
        _require_str("metadata.owner_id", self.owner_id, allow_empty=True)
        if self.owner_id:
            _require_hex64("metadata.owner_id", self.owner_id)
        _require_str("metadata.identity_file", self.identity_file, allow_empty=True)
        _require_bool("metadata.object_graph_auto_export", self.object_graph_auto_export)
        _require_str("metadata.object_store_dir", self.object_store_dir)
        _require_bool("metadata.object_graph_include_protection", self.object_graph_include_protection)
        _require_bool("metadata.object_graph_auto_pack", self.object_graph_auto_pack)
        _require_str("metadata.object_pack_dir", self.object_pack_dir, allow_empty=True)
        _require_str("metadata.distributed_pack_store_dir", self.distributed_pack_store_dir)
        _require_int("metadata.pack_copies", self.pack_copies, min_value=0)
        _require_bool("metadata.strict_pack_copies", self.strict_pack_copies)
        _require_int("metadata.pack_discovery_max_candidates", self.pack_discovery_max_candidates, min_value=1)
        _require_int("metadata.pack_target_parallelism", self.pack_target_parallelism, min_value=1)
        _require_float("metadata.pack_rpc_timeout_s", self.pack_rpc_timeout_s, min_value=0.0)
        _require_int("metadata.cli_warning_limit", self.cli_warning_limit, min_value=1)
        _require_int("metadata.max_distributed_pack_bytes", self.max_distributed_pack_bytes, min_value=1)
        _require_int("metadata.max_distributed_packs_per_owner", self.max_distributed_packs_per_owner, min_value=1)
        _require_int(
            "metadata.max_distributed_pack_bytes_per_owner",
            self.max_distributed_pack_bytes_per_owner,
            min_value=1,
        )
        _require_int(
            "metadata.max_distributed_pack_store_bytes",
            self.max_distributed_pack_store_bytes,
            min_value=1,
        )
        _require_int("metadata.scrypt_n", self.scrypt_n, min_value=2)
        if self.scrypt_n & (self.scrypt_n - 1) != 0:
            raise StopanConfigValueError("metadata.scrypt_n debe ser potencia de dos")
        _require_int("metadata.scrypt_r", self.scrypt_r, min_value=1)
        _require_int("metadata.scrypt_p", self.scrypt_p, min_value=1)
        _require_int("metadata.key_length", self.key_length, min_value=32)

        if self.max_distributed_pack_bytes_per_owner < self.max_distributed_pack_bytes:
            raise StopanConfigValueError(
                "metadata.max_distributed_pack_bytes_per_owner debe ser >= "
                "metadata.max_distributed_pack_bytes"
            )
        if self.max_distributed_pack_store_bytes < self.max_distributed_pack_bytes:
            raise StopanConfigValueError(
                "metadata.max_distributed_pack_store_bytes debe ser >= "
                "metadata.max_distributed_pack_bytes"
            )


@dataclass(frozen=True)
class GcConfig:
    local_cas_grace_hours: float = DEFAULT_GC_LOCAL_CAS_GRACE_HOURS
    restore_output_max_age_days: int = DEFAULT_GC_RESTORE_OUTPUT_MAX_AGE_DAYS
    node_cas_max_age_days: int = DEFAULT_GC_NODE_CAS_MAX_AGE_DAYS
    metadata_object_store_grace_hours: float = DEFAULT_GC_METADATA_OBJECT_STORE_GRACE_HOURS
    metadata_object_pack_grace_hours: float = DEFAULT_GC_METADATA_OBJECT_PACK_GRACE_HOURS
    distributed_pack_max_age_days: int = DEFAULT_GC_DISTRIBUTED_PACK_MAX_AGE_DAYS

    def __post_init__(self) -> None:
        _require_float("gc.local_cas_grace_hours", self.local_cas_grace_hours, min_value=0.0, inclusive=True)
        _require_int("gc.restore_output_max_age_days", self.restore_output_max_age_days, min_value=0)
        _require_int("gc.node_cas_max_age_days", self.node_cas_max_age_days, min_value=0)
        _require_float(
            "gc.metadata_object_store_grace_hours",
            self.metadata_object_store_grace_hours,
            min_value=0.0,
            inclusive=True,
        )
        _require_float(
            "gc.metadata_object_pack_grace_hours",
            self.metadata_object_pack_grace_hours,
            min_value=0.0,
            inclusive=True,
        )
        _require_int("gc.distributed_pack_max_age_days", self.distributed_pack_max_age_days, min_value=0)


@dataclass(frozen=True)
class StopanConfig:
    node: NodeConfig = field(default_factory=NodeConfig)
    cluster: ClusterConfig = field(default_factory=ClusterConfig)
    protection: ProtectionConfig = field(default_factory=ProtectionConfig)
    backup: BackupConfig = field(default_factory=BackupConfig)
    grpc: GrpcConfig = field(default_factory=GrpcConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    replication: ReplicationConfig = field(default_factory=ReplicationConfig)
    verify: VerifyConfig = field(default_factory=VerifyConfig)
    restore: RestoreConfig = field(default_factory=RestoreConfig)
    membership: MembershipConfig = field(default_factory=MembershipConfig)
    metadata: MetadataConfig = field(default_factory=MetadataConfig)
    gc: GcConfig = field(default_factory=GcConfig)

    def with_overrides(self, **sections: Any) -> StopanConfig:
        current = self
        for section_name, values in sections.items():
            if values is None:
                continue
            section = getattr(current, section_name)
            current = replace(current, **{section_name: replace(section, **values)})
        return current
