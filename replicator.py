import argparse
import os

import grpc

from core.cluster_view import ClusterMembershipClient
from core.database import MetadataDB
from core.repository import CASRepository
from protos import p2p_storage_pb2
from protos import p2p_storage_pb2_grpc


LOCAL_SHARD_DIR = os.getenv("LOCAL_SHARD_DIR", "_data_chunks")
DB_FILE = os.getenv("DB_FILE", "_metadata.db")
CLUSTER_TOKEN = os.getenv("CLUSTER_TOKEN", "")
DEFAULT_RF = int(os.getenv("RF", os.getenv("REPLICATION_FACTOR", "3")))
DEFAULT_SEED = os.getenv("MEMBERSHIP_SEED", "localhost:50051")
COMMIT_EVERY = 100


class StorageStubPool:
    """Reutiliza conexiones gRPC durante el push."""

    def __init__(self):
        self.channels = {}
        self.stubs = {}

    def get(self, address):
        if address not in self.stubs:
            channel = grpc.insecure_channel(address)
            self.channels[address] = channel
            self.stubs[address] = p2p_storage_pb2_grpc.P2PStorageStub(channel)
        return self.stubs[address]

    def close(self):
        for channel in self.channels.values():
            channel.close()
        self.channels.clear()
        self.stubs.clear()


def push(seed=DEFAULT_SEED, *, rf=DEFAULT_RF, limit=None):
    """Envía chunks pendientes hasta cubrir el replication factor indicado."""
    repo = CASRepository(LOCAL_SHARD_DIR)
    db = MetadataDB(DB_FILE)
    stubs = StorageStubPool()

    sent = 0
    attempted = 0
    pending_chunks = []

    try:
        self_addr = os.getenv("ADVERTISE_ADDR", "")
        cluster = ClusterMembershipClient(seed, self_addr=self_addr).get_cluster_view()
        if not cluster.members:
            print(f"No eligible members returned by seed {seed}.")
            return False

        placement_epoch = cluster.placement_epoch(
            desired_rf=rf,
            cluster_token=CLUSTER_TOKEN,
        )
        db.mark_stale_protection(desired_rf=rf, current_epoch=placement_epoch)
        pending_chunks = db.get_pending_protection_chunks(
            desired_rf=rf,
            current_epoch=placement_epoch,
            limit=limit,
        )

        if not pending_chunks:
            print("No chunks pending for the current protection policy.")
            return True

        print(f"Protecting {len(pending_chunks)} chunks")
        print(f"Replication factor: {min(max(rf, 1), len(cluster.members))}")
        print(f"Placement epoch: {placement_epoch[:12]}")

        for chunk_hash in pending_chunks:
            attempted += 1
            targets = cluster.hrw_remote_targets(chunk_hash, rf=rf, salt=CLUSTER_TOKEN)

            if not targets:
                db.mark_chunk_placed(
                    chunk_hash,
                    desired_rf=rf,
                    protected_remote_copies=0,
                    placement_epoch=placement_epoch,
                )
                sent += 1
                continue

            try:
                compressed_data = repo.get_compressed(chunk_hash)
            except Exception as exc:
                db.mark_chunk_failed(
                    chunk_hash,
                    desired_rf=rf,
                    protected_remote_copies=0,
                    placement_epoch=placement_epoch,
                    error=f"Could not read local chunk: {exc}",
                )
                continue

            ok_count = 0
            failures = []

            for member in targets:
                request = p2p_storage_pb2.StoreRequest(
                    chunk_hash=chunk_hash,
                    chunk_data=compressed_data,
                )

                try:
                    response = stubs.get(member.address).StoreChunk(request, timeout=10)
                    if response.success:
                        ok_count += 1
                    else:
                        failures.append(f"{member.address}: {response.message}")

                except grpc.RpcError as exc:
                    failures.append(f"{member.address}: RPC {exc.details()}")

            if ok_count == len(targets):
                db.mark_chunk_placed(
                    chunk_hash,
                    desired_rf=rf,
                    protected_remote_copies=ok_count,
                    placement_epoch=placement_epoch,
                )
                sent += 1
                if sent % COMMIT_EVERY == 0:
                    db.commit()
            else:
                db.mark_chunk_failed(
                    chunk_hash,
                    desired_rf=rf,
                    protected_remote_copies=ok_count,
                    placement_epoch=placement_epoch,
                    error="; ".join(failures)[:2000],
                )

        return True

    except KeyboardInterrupt:
        print("Push interrupted.")
        return False

    finally:
        db.commit()
        db.close()
        stubs.close()
        print(f"Push finished: {sent}/{len(pending_chunks)} chunks protected (attempted={attempted}).")


def _build_parser():
    parser = argparse.ArgumentParser(description="Replica chunks pendientes en nodos P2P.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    push_parser = subparsers.add_parser("push")
    push_parser.add_argument("--seed", default=DEFAULT_SEED, help="Nodo seed de membership")
    push_parser.add_argument("--rf", type=int, default=DEFAULT_RF, help="Replication factor")
    push_parser.add_argument("--limit", type=int, default=None, help="Máximo de chunks a procesar")

    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    if args.command == "push":
        ok = push(seed=args.seed, rf=args.rf, limit=args.limit)
        if not ok:
            raise SystemExit(1)
