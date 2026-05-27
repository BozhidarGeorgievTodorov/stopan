"""Base común para clientes remotos de protección P2P."""

from __future__ import annotations

from collections.abc import Callable

from stopan.rpc.p2p_storage_client import P2PStorageClientRuntime


class P2PStorageProtectionClient:
    """Envoltura mínima de P2PStorageClientRuntime para clientes de protección."""

    def __init__(
        self,
        *,
        max_message_bytes: int,
        closed_message: str,
        closed_error_factory: Callable[[str], Exception],
    ):
        self.max_message_bytes = max(int(max_message_bytes), 1)
        self._runtime = P2PStorageClientRuntime(
            max_message_bytes=self.max_message_bytes,
            closed_message=closed_message,
            closed_error_factory=closed_error_factory,
        )

    def _ensure_open(self) -> None:
        self._runtime.ensure_open()

    def _ensure_runtime(self) -> None:
        self._runtime.ensure_runtime()

    @property
    def _pb(self):
        return self._runtime.pb

    def _get_stub(self, address: str):
        return self._runtime.get_stub(address)

    def close(self) -> None:
        self._runtime.close()
