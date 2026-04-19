from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ProbeExecutionResult:
    node_id: str
    address: str
    requested_hashes: tuple[str, ...]
    present_hashes: frozenset[str]
    transport_error: str | None = None


@dataclass(frozen=True)
class VerificationOutcome:
    chunk_hash: str
    desired_rf: int
    placement_epoch: str
    success: bool
    required_remote_copies: int
    verified_remote_copies: int
    error: str | None = None


@dataclass(frozen=True)
class VerificationStats:
    candidates: int = 0
    verified: int = 0
    degraded: int = 0
    rpc_failures: int = 0


@dataclass
class VerificationAccumulator:
    desired_rf: int
    placement_epoch: str
    required_remote_copies: int
    verified_remote_copies: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.verified_remote_copies >= self.required_remote_copies

    @property
    def error_summary(self) -> str | None:
        if self.success:
            return None

        prefix = (
            f"verified_remote_copies={self.verified_remote_copies}/"
            f"{self.required_remote_copies}"
        )
        if not self.errors:
            return prefix

        return f"{prefix}; " + "; ".join(self.errors)[:1800]
