from __future__ import annotations

import concurrent.futures
from collections.abc import Sequence

import blake3
import zstandard as zstd

from stopan.cas.repository import CASRepository
from stopan.restore.cluster import LazyClusterResolver
from stopan.restore.config import CLUSTER_TOKEN
from stopan.restore.remote_client import RemoteStorageClientPool


class ChunkFetchService:
    """
    Lee chunks con prioridad local y recuperación remota por lotes bajo demanda.
    """

    def __init__(
        self,
        *,
        repo: CASRepository,
        p2p_local_repo: CASRepository | None,
        cluster_resolver: LazyClusterResolver,
        remote_pool: RemoteStorageClientPool,
        rf: int,
    ):
        self.repo = repo
        self.p2p_local_repo = p2p_local_repo
        self.cluster_resolver = cluster_resolver
        self.remote_pool = remote_pool
        self.rf = max(int(rf), 1)

    def fetch_many_raw_chunks(
        self,
        chunk_hashes: Sequence[str],
        *,
        target_parallelism: int,
    ) -> dict[str, bytes | Exception]:
        ordered_hashes = list(chunk_hashes)
        results: dict[str, bytes | Exception] = {}
        missing_hashes: list[str] = []

        for chunk_hash in ordered_hashes:
            try:
                results[chunk_hash] = self.repo.get(chunk_hash)
                continue
            except FileNotFoundError:
                pass
            except Exception as exc:
                results[chunk_hash] = exc
                continue

            if self.p2p_local_repo is not None:
                try:
                    results[chunk_hash] = self.p2p_local_repo.get(chunk_hash)
                    continue
                except FileNotFoundError:
                    pass
                except Exception as exc:
                    results[chunk_hash] = exc
                    continue

            missing_hashes.append(chunk_hash)

        if missing_hashes:
            results.update(
                self._fetch_missing_many_from_remote(
                    missing_hashes,
                    target_parallelism=max(int(target_parallelism), 1),
                )
            )

        return results

    @staticmethod
    def _validate_remote_compressed_chunk(chunk_hash: str, compressed_data: bytes) -> bytes:
        try:
            raw_data = zstd.ZstdDecompressor().decompress(compressed_data)
        except zstd.ZstdError as exc:
            raise ValueError(f"Remote chunk has corrupt zstd data: {chunk_hash}") from exc

        calculated_hash = blake3.blake3(raw_data).hexdigest()
        if calculated_hash != chunk_hash:
            raise ValueError(
                f"Remote chunk hash mismatch: expected {chunk_hash}, got {calculated_hash}"
            )

        return raw_data

    def _fetch_missing_many_from_remote(
        self,
        missing_hashes: list[str],
        *,
        target_parallelism: int,
    ) -> dict[str, bytes | Exception]:
        cluster = self.cluster_resolver.get_cluster()
        self.cluster_resolver.announce_once()

        result_map: dict[str, bytes | Exception] = {}
        error_map: dict[str, list[str]] = {chunk_hash: [] for chunk_hash in missing_hashes}
        target_lists: dict[str, list] = {}
        excluded_node_ids = {self.cluster_resolver.origin_node_id}

        for chunk_hash in missing_hashes:
            remote_targets = cluster.hrw_targets_excluding(
                chunk_hash,
                rf=self.rf,
                salt=CLUSTER_TOKEN,
                excluded_node_ids=excluded_node_ids,
            )
            target_lists[chunk_hash] = remote_targets

            if not remote_targets:
                result_map[chunk_hash] = FileNotFoundError(
                    f"Chunk {chunk_hash[:8]} is not local and has no usable HRW targets."
                )

        unresolved = {chunk_hash for chunk_hash in missing_hashes if chunk_hash not in result_map}

        if cluster.self_node_id and self.p2p_local_repo is not None:
            for chunk_hash in list(unresolved):
                has_self_target = any(
                    member.node_id == cluster.self_node_id
                    for member in target_lists[chunk_hash]
                )
                if not has_self_target:
                    continue

                try:
                    compressed_data = self.p2p_local_repo.get_compressed(chunk_hash)
                    raw_data = self._validate_remote_compressed_chunk(chunk_hash, compressed_data)
                    try:
                        self.repo.put_compressed(chunk_hash, compressed_data)
                    except Exception as exc:
                        error_map[chunk_hash].append(f"local P2P store cache: {exc}")
                    result_map[chunk_hash] = raw_data
                    unresolved.discard(chunk_hash)
                except FileNotFoundError:
                    error_map[chunk_hash].append("local P2P store: missing chunk")
                except Exception as exc:
                    error_map[chunk_hash].append(f"local P2P store: {exc}")

        max_depth = max((len(target_lists[chunk_hash]) for chunk_hash in unresolved), default=0)

        for rank in range(max_depth):
            if not unresolved:
                break

            groups: dict[str, tuple[object, list[str]]] = {}
            for chunk_hash in list(unresolved):
                targets = target_lists[chunk_hash]
                if rank >= len(targets):
                    continue

                member = targets[rank]
                existing = groups.get(member.address)
                if existing is None:
                    groups[member.address] = (member, [chunk_hash])
                else:
                    existing[1].append(chunk_hash)

            if not groups:
                break

            max_workers = min(max(int(target_parallelism), 1), len(groups))
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_map = {
                    executor.submit(
                        self.remote_pool.retrieve_chunk_batch,
                        address=member.address,
                        chunk_hashes=group_hashes,
                    ): (member, group_hashes)
                    for member, group_hashes in groups.values()
                }

                for future in concurrent.futures.as_completed(future_map):
                    member, group_hashes = future_map[future]

                    try:
                        batch_results = future.result()
                    except Exception as exc:
                        if self.remote_pool.is_rpc_error(exc):
                            details = getattr(exc, "details", lambda: str(exc))()
                            message = f"{member.address}: RPC {details}"
                        else:
                            message = f"{member.address}: {exc}"

                        for chunk_hash in group_hashes:
                            error_map[chunk_hash].append(message)
                        continue

                    for chunk_hash in group_hashes:
                        item = batch_results.get(chunk_hash)
                        if item is None:
                            error_map[chunk_hash].append(f"{member.address}: missing batch result")
                            continue

                        if not item.is_found(self.remote_pool.retrieve_status_found):
                            error_map[chunk_hash].append(f"{member.address}: {item.detail}")
                            continue

                        try:
                            raw_data = self._validate_remote_compressed_chunk(
                                chunk_hash,
                                item.chunk_data,
                            )
                        except Exception as exc:
                            error_map[chunk_hash].append(f"{member.address}: corrupt chunk: {exc}")
                            print(
                                f"Corrupt replica ignored: "
                                f"{member.address} chunk={chunk_hash[:8]} error={exc}"
                            )
                            continue

                        try:
                            self.repo.put_compressed(chunk_hash, item.chunk_data)
                        except Exception as exc:
                            error_map[chunk_hash].append(f"{member.address}: local cache failed: {exc}")

                        result_map[chunk_hash] = raw_data
                        unresolved.discard(chunk_hash)

        for chunk_hash in list(unresolved):
            result_map[chunk_hash] = FileNotFoundError(
                f"No HRW target returned chunk {chunk_hash[:8]}. "
                + " | ".join(error_map[chunk_hash])
            )

        return result_map
