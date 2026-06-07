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
        description="Arranca un nodo Stopan o consulta su estado operativo.",
    )
    subparsers = parser.add_subparsers(dest="node_command", metavar="<subcommand>")

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


def _read_node_identity(repo_store_dir: str) -> tuple[str | None, int | None, str | None]:
    path = Path(repo_store_dir) / "node_id.txt"
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
            )
    except grpc.RpcError as exc:
        return False, f"unreachable: {format_rpc_error(exc)}"

    return True, "reachable"


def _print_members(members: Sequence[object], *, local_node_id: str | None) -> None:
    print(f"   members_alive: {len(members)}")
    if not members:
        return

    print("   members:")
    for member in sorted(members, key=lambda item: str(getattr(item, "address", ""))):
        node_id = str(getattr(member, "node_id", "") or "")
        address = str(getattr(member, "address", "") or "")
        incarnation = int(getattr(member, "incarnation", 0) or 0)
        marker = " self" if local_node_id and node_id == local_node_id else ""
        print(f"      {_short_node_id(node_id)}  {address}  incarnation={incarnation}{marker}")


def _status(args: argparse.Namespace) -> int:
    cfg = load_runtime_config(args)

    local_node_id, incarnation, identity_status = _read_node_identity(cfg.node.repo_store_dir)
    address = str(args.address or cfg.node.advertise_addr or first_seed(cfg) or "").strip()
    timeout_s = _NODE_STATUS_RPC_TIMEOUT_S

    print("Node status")
    print("Config")
    print(f"   config_file: {args.config}")
    print(f"   bind_addr: {cfg.node.bind_addr}")
    print(f"   advertise_addr: {cfg.node.advertise_addr or '(not configured)'}")
    print(f"   rpc_target: {address or '(not configured)'}")

    print("Local identity")
    print(f"   node_id: {local_node_id or '(not initialized)'}")
    print(f"   incarnation: {incarnation if incarnation is not None else '(not initialized)'}")
    print(f"   identity_file: {identity_status}")

    print("Local storage")
    print(f"   repo_store_dir: {_format_path_status(_path_status(cfg.node.repo_store_dir))}")
    print(f"   local_shard_dir: {_format_path_status(_path_status(cfg.node.local_shard_dir))}")
    print(f"   db_file: {cfg.node.db_file}")

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
    _print_members(members, local_node_id=local_node_id)

    if membership_ok and storage_ok:
        print("Summary")
        print("   node_serving: yes")
        print("   can_receive_data: yes")
        return 0

    print("Summary")
    print("   node_serving: no")
    print("   can_receive_data: no")
    return 1


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
    if getattr(args, "node_command", None) == "status":
        return _status(args)
    return _serve(args)
