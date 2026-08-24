from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from stopan.cli.config_utils import add_config_args, first_seed, load_runtime_config
from stopan.config.defaults import DEFAULT_STOPAN_CONFIG
from stopan.config.model import StopanConfig
from stopan.errors import StopanConfigError
from stopan.rpc.errors import format_rpc_error


_NODE_STATUS_RPC_TIMEOUT_S = 2.0


@dataclass(frozen=True, slots=True)
class LocalPathStatus:
    path: Path
    exists: bool
    directory: bool
    writable: bool


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="stopan node",
        allow_abbrev=False,
        description="Arranca, detiene o consulta el estado operativo de un nodo Stopan.",
    )
    subparsers = parser.add_subparsers(dest="node_command", metavar="<subcommand>")

    subparsers.add_parser(
        "stop",
        allow_abbrev=False,
        help="Solicita una parada ordenada del nodo local y espera al drenaje.",
        description=(
            "Cierra la admisión de trabajo nuevo, anuncia LEFT al clúster y espera "
            "las operaciones ya iniciadas antes de detener el nodo local."
        ),
    )

    status_parser = subparsers.add_parser(
        "status",
        allow_abbrev=False,
        help="Consulta si el nodo está sirviendo y qué miembros ve.",
        description="Consulta el estado local y la vista de membership del nodo configurado.",
    )
    add_config_args(status_parser)
    status_parser.add_argument(
        "--address",
        help="Dirección gRPC a consultar. Por defecto usa node.advertise_addr.",
    )
    add_config_args(parser)
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    return _build_parser().parse_args(argv)


def _read_node_identity(identity_file: str) -> tuple[str | None, int | None, str | None]:
    path = Path(identity_file)
    if not path.exists():
        return None, None, f"missing ({path})"

    try:
        lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    except OSError as exc:
        return None, None, f"unreadable ({path}): {exc}"

    if len(lines) != 2:
        return None, None, f"invalid format ({path})"

    node_id = lines[0]
    try:
        incarnation = int(lines[1])
    except ValueError:
        return node_id or None, None, f"invalid incarnation ({path})"

    return node_id or None, incarnation, str(path)


def _path_status(path: str) -> LocalPathStatus:
    resolved = Path(path).expanduser()
    exists = resolved.exists()
    directory = resolved.is_dir()
    writable = directory and os.access(resolved, os.W_OK)
    return LocalPathStatus(
        path=resolved,
        exists=exists,
        directory=directory,
        writable=writable,
    )


def _format_path_status(status: LocalPathStatus) -> str:
    if not status.exists:
        return f"missing ({status.path})"
    if not status.directory:
        return f"not a directory ({status.path})"
    writable = "writable" if status.writable else "not writable"
    return f"present {writable} ({status.path})"


def _short_node_id(node_id: str | None) -> str:
    value = str(node_id or "").strip()
    return value[:8] if value else "-"


def _check_membership_rpc(
    *,
    address: str,
    cfg: StopanConfig,
    timeout_s: float,
):
    import grpc

    from stopan.protos import membership_pb2, membership_pb2_grpc
    from stopan.rpc.channels import temporary_insecure_channel

    try:
        with temporary_insecure_channel(
            address,
            max_message_bytes=cfg.grpc.max_message_bytes,
            keepalive_time_ms=cfg.grpc.keepalive_time_ms,
            keepalive_timeout_ms=cfg.grpc.keepalive_timeout_ms,
            keepalive_permit_without_calls=cfg.grpc.keepalive_permit_without_calls,
        ) as channel:
            stub = membership_pb2_grpc.MembershipStub(channel)
            response = stub.GetMembers(
                membership_pb2.GetMembersRequest(cluster_token=cfg.cluster.token),
                timeout=timeout_s,
            )
    except grpc.RpcError as exc:
        return False, f"unreachable: {format_rpc_error(exc)}", ()

    members = tuple(response.members)
    return True, "reachable", members


def _check_storage_rpc(
    *,
    address: str,
    cfg: StopanConfig,
    timeout_s: float,
) -> tuple[bool, str]:
    import grpc

    from stopan.protos import p2p_storage_pb2, p2p_storage_pb2_grpc
    from stopan.rpc.auth import cluster_token_metadata
    from stopan.rpc.channels import temporary_insecure_channel

    try:
        with temporary_insecure_channel(
            address,
            max_message_bytes=cfg.grpc.max_message_bytes,
            keepalive_time_ms=cfg.grpc.keepalive_time_ms,
            keepalive_timeout_ms=cfg.grpc.keepalive_timeout_ms,
            keepalive_permit_without_calls=cfg.grpc.keepalive_permit_without_calls,
        ) as channel:
            stub = p2p_storage_pb2_grpc.P2PStorageStub(channel)
            stub.ProbeMissingChunks(
                p2p_storage_pb2.ProbeMissingChunksRequest(chunk_hashes=[]),
                timeout=timeout_s,
                metadata=cluster_token_metadata(cfg.cluster.token),
            )
    except grpc.RpcError as exc:
        return False, f"unreachable: {format_rpc_error(exc)}"

    return True, "reachable"


def _print_members(
    members: Sequence[object],
    *,
    local_node_id: str | None = None,
    target_address: str | None = None,
) -> None:
    print(f"   members_alive: {len(members)}")
    if not members:
        return

    print("   members:")
    normalized_target = str(target_address or "").strip()
    for member in sorted(members, key=lambda item: str(getattr(item, "address", ""))):
        node_id = str(getattr(member, "node_id", "") or "")
        address = str(getattr(member, "address", "") or "")
        incarnation = int(getattr(member, "incarnation", 0) or 0)
        marker = ""
        if local_node_id and node_id == local_node_id:
            marker = " self"
        elif normalized_target and address == normalized_target:
            marker = " target"
        print(f"      {_short_node_id(node_id)}  {address}  incarnation={incarnation}{marker}")


def _status(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)

    explicit_address = str(args.address or "").strip()
    remote_query = bool(explicit_address)
    address = explicit_address or str(cfg.node.advertise_addr or first_seed(cfg) or "").strip()
    timeout_s = _NODE_STATUS_RPC_TIMEOUT_S

    local_node_id: str | None = None
    if not remote_query:
        local_node_id, incarnation, identity_status = _read_node_identity(cfg.node.identity_file)

    print("Node status")
    print("Config")
    print(f"   config_file: {args.config}")
    if remote_query:
        print("   mode: remote")
        print(f"   rpc_target: {address}")
    else:
        print("   mode: local")
        print(f"   bind_addr: {cfg.node.bind_addr}")
        print(f"   advertise_addr: {cfg.node.advertise_addr or '(not configured)'}")
        print(f"   rpc_target: {address or '(not configured)'}")

        print("Local identity")
        print(f"   node_id: {local_node_id or '(not initialized)'}")
        print(f"   incarnation: {incarnation if incarnation is not None else '(not initialized)'}")
        print(f"   identity_file: {identity_status}")

        print("Local storage")
        print(f"   identity_file: {_format_path_status(_path_status(cfg.node.identity_file))}")
        print(f"   local_chunk_dir: {_format_path_status(_path_status(cfg.storage.local_chunk_dir))}")
        print(f"   custody_dir: {_format_path_status(_path_status(cfg.storage.custody_dir))}")
        print(f"   catalog_file: {cfg.node.catalog_file}")

    if not address:
        print("RPC")
        print("   membership: skipped (node.advertise_addr no configurado)")
        print("   storage: skipped (node.advertise_addr no configurado)")
        return 1

    print("RPC")
    membership_ok, membership_detail, members = _check_membership_rpc(
        address=address,
        cfg=cfg,
        timeout_s=timeout_s,
    )
    print(f"   target: {address}")
    print(f"   membership: {membership_detail}")

    storage_ok, storage_detail = _check_storage_rpc(
        address=address,
        cfg=cfg,
        timeout_s=timeout_s,
    )
    print(f"   storage: {storage_detail}")
    print("   metadata_pack_storage: same gRPC server")

    print("Cluster view")
    _print_members(
        members,
        local_node_id=local_node_id if not remote_query else None,
        target_address=address if remote_query else None,
    )

    if membership_ok and storage_ok:
        print("Summary")
        print("   node_serving: yes")
        print("   can_receive_data: yes")
        return 0

    print("Summary")
    print("   node_serving: no")
    print("   can_receive_data: no")
    return 1


def _stop() -> int:
    from stopan.node.lifecycle import request_local_node_stop

    print("Solicitando parada ordenada del nodo local...", flush=True)
    request_local_node_stop()
    print("Nodo local detenido.")
    return 0


def _serve(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)

    if not cfg.node.advertise_addr:
        raise StopanConfigError(
            "Falta node.advertise_addr. Define advertise_addr en el fichero de configuración "
            f"usado con --config o en {DEFAULT_STOPAN_CONFIG}."
        )

    from stopan.node.server import serve

    serve(cfg)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    node_command = getattr(args, "node_command", None)
    if node_command == "status":
        return _status(args)
    if node_command == "stop":
        return _stop()
    return _serve(args)
