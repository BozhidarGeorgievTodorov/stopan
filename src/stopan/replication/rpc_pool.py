from __future__ import annotations

import threading

import grpc

from stopan.protos import p2p_storage_pb2_grpc


class StorageRpcPool:
    """
    Pool reutilizable de canales y stubs gRPC para replicación de chunks.
    """

    def __init__(self, *, max_message_bytes: int):
        self._lock = threading.Lock()
        self._channels: dict[str, grpc.Channel] = {}
        self._stubs: dict[str, p2p_storage_pb2_grpc.P2PStorageStub] = {}
        self._options = [
            ("grpc.max_send_message_length", int(max_message_bytes)),
            ("grpc.max_receive_message_length", int(max_message_bytes)),
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 10_000),
            ("grpc.http2.max_pings_without_data", 0),
            ("grpc.keepalive_permit_without_calls", 1),
        ]

    def get_stub(self, address: str) -> p2p_storage_pb2_grpc.P2PStorageStub:
        with self._lock:
            stub = self._stubs.get(address)
            if stub is not None:
                return stub

            channel = grpc.insecure_channel(address, options=self._options)
            stub = p2p_storage_pb2_grpc.P2PStorageStub(channel)
            self._channels[address] = channel
            self._stubs[address] = stub
            return stub

    def close(self) -> None:
        with self._lock:
            for channel in self._channels.values():
                channel.close()
            self._channels.clear()
            self._stubs.clear()
