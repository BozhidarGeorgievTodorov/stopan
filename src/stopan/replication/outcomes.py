"""
Modelos de resultado para replicación remota de chunks.

Estas estructuras conservan ACKs por target, errores de transporte y outcomes
por chunk.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from stopan.errors import StopanNetworkError
from stopan.protos import p2p_storage_pb2


@dataclass(frozen=True, slots=True)
class TargetAck:
    """ACK recibido desde un target remoto para un chunk concreto."""

    chunk_hash: str
    node_id: str
    address: str
    status: int
    detail: str

    @property
    def is_success(self) -> bool:
        return self.status in (
            p2p_storage_pb2.STORE_STATUS_STORED,
            p2p_storage_pb2.STORE_STATUS_ALREADY_PRESENT,
        )

    @property
    def is_already_present(self) -> bool:
        return self.status == p2p_storage_pb2.STORE_STATUS_ALREADY_PRESENT

    @property
    def is_stored(self) -> bool:
        return self.status == p2p_storage_pb2.STORE_STATUS_STORED


@dataclass(frozen=True, slots=True)
class TargetExecutionResult:
    """Resultado de ejecutar probe/stream contra un target remoto."""
    node_id: str
    address: str
    acks: dict[str, TargetAck]
    transport_error: str | None = None


class StreamingReplicationError(StopanNetworkError, RuntimeError):
    """Error de stream que conserva los ACKs recibidos antes del fallo."""

    def __init__(self, message: str, *, acks: dict[str, TargetAck]):
        super().__init__(message)
        self.acks = dict(acks)


@dataclass(frozen=True, slots=True)
class ChunkReplicationOutcome:
    """Resultado agregado de replicar un chunk en sus targets remotos."""
    chunk_hash: str
    success: bool
    required_remote_copies: int
    protected_remote_copies: int
    stored_remote_copies: int
    already_present_remote_copies: int
    error: str | None = None


@dataclass(slots=True)
class ChunkAccumulator:
    """
    Acumulador mutable usado mientras llegan ACKs de distintos targets.

    Un chunk queda protegido cuando stored_remote_copies +
    already_present_remote_copies alcanza required_remote_copies.
    """
    required_remote_copies: int
    stored_remote_copies: int = 0
    already_present_remote_copies: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def protected_remote_copies(self) -> int:
        return self.stored_remote_copies + self.already_present_remote_copies

    @property
    def success(self) -> bool:
        return self.protected_remote_copies >= self.required_remote_copies
