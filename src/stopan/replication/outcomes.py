from __future__ import annotations

from dataclasses import dataclass, field

from stopan.protos import p2p_storage_pb2


@dataclass(frozen=True)
class TargetAck:
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


@dataclass(frozen=True)
class TargetExecutionResult:
    node_id: str
    address: str
    acks: dict[str, TargetAck]
    transport_error: str | None = None


class StreamingReplicationError(RuntimeError):
    
    def __init__(self, message: str, *, acks: dict[str, TargetAck]):
        super().__init__(message)
        self.acks = dict(acks)


@dataclass(frozen=True)
class ChunkReplicationOutcome:
    chunk_hash: str
    success: bool
    required_remote_copies: int
    protected_remote_copies: int
    stored_remote_copies: int
    already_present_remote_copies: int
    error: str | None = None


@dataclass
class ChunkAccumulator:
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
