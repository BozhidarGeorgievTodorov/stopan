"""
Modelos de verificación de protección remota.

Estas estructuras agrupan outcomes por chunk y contadores agregados del verifier.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class VerificationOutcome:
    """Resultado de verificación para un chunk concreto."""

    chunk_hash: str
    desired_rf: int
    placement_epoch: str
    required_remote_copies: int
    verified_remote_copies: int
    error: str | None = None


@dataclass(frozen=True, slots=True)
class VerificationStats:
    """Contadores agregados de una ejecución del verifier."""

    candidates: int = 0
    verified: int = 0
    degraded: int = 0
    rpc_failed_calls: int = 0
    rpc_failed_targets: int = 0
    rpc_unverified_assignments: int = 0


@dataclass(slots=True)
class VerificationAccumulator:
    """
    Acumulador mutable usado mientras se verifican los targets de un chunk.

    Registra copias remotas confirmadas y errores por nodo hasta decidir si el
    chunk cumple el RF remoto requerido.
    """

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
