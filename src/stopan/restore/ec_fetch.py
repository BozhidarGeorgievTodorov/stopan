from __future__ import annotations

import concurrent.futures
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass

from stopan.restore.cluster import LazyClusterResolver
from stopan.restore.errors import ChunkUnavailableError, RestoreDataError
from stopan.cas.repository import CASRepository
from stopan.metadata.database import MetadataDB, ErasureDataPackShardRecord
from stopan.protection.ec.manifest import DataPackManifest
from stopan.protection.ec.metadata_adapter import manifest_from_erasure_metadata
from stopan.protection.ec.models import DataPackShard
from stopan.protection.ec.packer import extract_pack_chunks, reconstruct_payload
from stopan.protection.ec.remote_client import RemoteDataPackShardClientPool
from stopan.protection.ec.shard_targets import group_erasure_shard_refs_by_address
from stopan.progress import ProgressReporter, suspend_progress


@dataclass(frozen=True)
class _ErasurePackRecoveryJob:
    pack_hash: str
    manifest: DataPackManifest
    shard_rows: tuple[ErasureDataPackShardRecord, ...]
    wanted_hashes: tuple[str, ...]


class ErasureChunkRecoveryService:
    """
    Recupera chunks faltantes reconstruyendo data packs EC.

    Las lecturas SQLite se hacen en el hilo llamador. Los workers solo hacen
    I/O remoto, decodificación y validación de hashes.
    """

    def __init__(
        self,
        *,
        db: MetadataDB,
        repo: CASRepository,
        remote_pool: RemoteDataPackShardClientPool,
        cluster_resolver: LazyClusterResolver,
        progress: ProgressReporter | None = None,
    ):
        self.db = db
        self.repo = repo
        self.remote_pool = remote_pool
        self._announced = False
        self.cluster_resolver = cluster_resolver
        self.progress = progress

    def recover_many_raw_chunks(
        self,
        chunk_hashes: Sequence[str],
        *,
        target_parallelism: int,
    ) -> dict[str, bytes | Exception]:
        ordered_hashes = list(dict.fromkeys(chunk_hashes))
        if not ordered_hashes:
            return {}

        result_map: dict[str, bytes | Exception] = {}
        chunks_by_pack = self._group_missing_chunks_by_pack(ordered_hashes, result_map)
        if not chunks_by_pack:
            return result_map

        jobs = self._build_recovery_jobs(chunks_by_pack, result_map)
        if not jobs:
            return result_map

        chunk_count = sum(len(job.wanted_hashes) for job in jobs)
        if self.progress is not None:
            self.progress.update(
                detail=f"EC: recuperando {len(jobs)} data packs para {chunk_count} chunks"
            )
        self._announce_once(len(jobs), chunk_count)

        max_workers = min(max(int(target_parallelism), 1), len(jobs))
        completed_packs = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(self._recover_pack_chunks, job): job
                for job in jobs
            }

            for future in concurrent.futures.as_completed(future_map):
                job = future_map[future]
                completed_packs += 1
                if self.progress is not None:
                    self.progress.update(
                        detail=(
                            "EC: "
                            f"{completed_packs}/{len(jobs)} data packs procesados "
                            f"({chunk_count} chunks solicitados)"
                        )
                    )
                try:
                    recovered = future.result()
                except Exception as exc:
                    for chunk_hash in job.wanted_hashes:
                        result_map[chunk_hash] = ChunkUnavailableError(
                            f"No se pudo reconstruir data pack EC {job.pack_hash[:8]} "
                            f"para chunk {chunk_hash[:8]}: {exc}"
                        )
                    continue

                self._cache_reconstructed_pack_chunks(job.pack_hash, recovered)

                for chunk_hash in job.wanted_hashes:
                    value = recovered.get(chunk_hash)
                    if value is None:
                        result_map[chunk_hash] = ChunkUnavailableError(
                            f"Data pack EC {job.pack_hash[:8]} no devolvió chunk {chunk_hash[:8]}"
                        )
                        continue

                    result_map[chunk_hash] = value

        return result_map

    def _group_missing_chunks_by_pack(
        self,
        ordered_hashes: list[str],
        result_map: dict[str, bytes | Exception],
    ) -> dict[str, list[str]]:
        locations = self.db.get_erasure_chunk_locations(ordered_hashes)
        chunks_by_pack: dict[str, list[str]] = defaultdict(list)

        for chunk_hash in ordered_hashes:
            location = locations.get(chunk_hash)
            if location is None:
                result_map[chunk_hash] = ChunkUnavailableError(
                    f"El chunk {chunk_hash[:8]} no pertenece a ningún data pack EC"
                )
                continue
            chunks_by_pack[location.pack_hash].append(chunk_hash)

        return chunks_by_pack

    def _build_recovery_jobs(
        self,
        chunks_by_pack: dict[str, list[str]],
        result_map: dict[str, bytes | Exception],
    ) -> list[_ErasurePackRecoveryJob]:
        jobs: list[_ErasurePackRecoveryJob] = []

        for pack_hash, wanted_hashes in chunks_by_pack.items():
            try:
                jobs.append(self._build_recovery_job(pack_hash, wanted_hashes))
            except Exception as exc:
                for chunk_hash in wanted_hashes:
                    result_map[chunk_hash] = ChunkUnavailableError(
                        f"No se pudo preparar metadata EC del pack {pack_hash[:8]} "
                        f"para chunk {chunk_hash[:8]}: {exc}"
                    )

        return jobs

    def _build_recovery_job(
        self,
        pack_hash: str,
        wanted_hashes: list[str],
    ) -> _ErasurePackRecoveryJob:
        pack = self.db.get_erasure_data_pack(pack_hash)
        if pack is None:
            raise RestoreDataError(f"metadata de data pack EC no encontrada: {pack_hash[:8]}")

        pack_chunks = self.db.get_erasure_pack_chunks(pack_hash)
        pack_shards = self.db.get_erasure_pack_shards(pack_hash)
        if not pack_chunks:
            raise RestoreDataError(f"data pack EC sin chunks: {pack_hash[:8]}")
        if len(pack_shards) < pack.data_shards:
            raise RestoreDataError(
                f"data pack EC sin suficientes shards registrados: "
                f"{len(pack_shards)}/{pack.data_shards}"
            )

        manifest = manifest_from_erasure_metadata(pack=pack, chunks=pack_chunks)
        return _ErasurePackRecoveryJob(
            pack_hash=pack_hash,
            manifest=manifest,
            shard_rows=tuple(pack_shards),
            wanted_hashes=tuple(wanted_hashes),
        )

    def _recover_pack_chunks(self, job: _ErasurePackRecoveryJob) -> dict[str, bytes]:
        shards = self._retrieve_pack_shards(
            job.pack_hash,
            job.shard_rows,
            required=job.manifest.spec.data_shards,
        )
        payload = reconstruct_payload(manifest=job.manifest, shards=shards)

        # La reconstrucción EC trabaja a nivel de data pack completo. Aunque la
        # ventana actual del restore solo haya pedido unos pocos chunks, extraer
        # el pack entero permite cachear todos sus chunks en el CAS local y evita
        # descargar/reconstruir el mismo pack una vez por ventana de prefetch.
        return extract_pack_chunks(payload=payload, manifest=job.manifest)

    def _cache_reconstructed_pack_chunks(
        self,
        pack_hash: str,
        recovered_chunks: dict[str, bytes],
    ) -> None:
        failed_cache = 0
        first_error: Exception | None = None

        for chunk_hash, data in recovered_chunks.items():
            try:
                self.repo.put(chunk_hash, data)
            except Exception as exc:
                failed_cache += 1
                if first_error is None:
                    first_error = exc

        if failed_cache:
            with suspend_progress(self.progress):
                print(
                    f"   EC restore: no pude cachear {failed_cache} chunks del data pack "
                    f"{pack_hash[:8]} en CAS local: {first_error}"
                )

    def _retrieve_pack_shards(
        self,
        pack_hash: str,
        shard_rows: tuple[ErasureDataPackShardRecord, ...],
        *,
        required: int,
    ) -> list[DataPackShard]:
        cluster = self.cluster_resolver.get_cluster()
        active_nodes = {m.node_id: m.address for m in cluster.members}
        target_groups = group_erasure_shard_refs_by_address(
            shard_rows=shard_rows,
            node_addresses=active_nodes,
            short_node_ids_in_errors=False,
        )
        refs_by_addr = target_groups.refs_by_address
        found: list[DataPackShard] = []
        errors = list(target_groups.offline_errors)

        if len(refs_by_addr) < required:
            raise ChunkUnavailableError(
                f"data pack EC {pack_hash[:8]}: nodos online insuficientes para K shards "
                f"({len(refs_by_addr)}/{required}). " + " | ".join(errors)
            )

        max_workers = min(len(refs_by_addr), max(int(required), 1))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(
                    self.remote_pool.retrieve_shard_batch,
                    addr=address,
                    refs=refs,
                ): address
                for address, refs in refs_by_addr.items()
            }

            for future in concurrent.futures.as_completed(future_map):
                address = future_map[future]
                try:
                    results = future.result()
                except Exception as exc:
                    errors.append(f"{address}: {exc}")
                    continue

                for item in results.values():
                    if not item.is_found(self.remote_pool.retrieve_status_found):
                        errors.append(f"{address}: shard={item.ref.shard_index}: {item.detail}")
                        continue

                    try:
                        found.append(
                            DataPackShard(
                                pack_hash=item.ref.pack_hash,
                                shard_index=item.ref.shard_index,
                                data=item.data,
                                shard_hash=item.ref.shard_hash,
                            )
                        )
                    except Exception as exc:
                        errors.append(f"{address}: shard={item.ref.shard_index}: {exc}")

        found.sort(key=lambda item: item.shard_index)
        if len(found) < required:
            raise ChunkUnavailableError(
                f"data pack EC {pack_hash[:8]} no tiene K shards recuperables: "
                f"{len(found)}/{required}. " + " | ".join(errors)
            )

        return found[:required]

    def _announce_once(self, pack_count: int, chunk_count: int) -> None:
        if self._announced:
            return
        self._announced = True
        with suspend_progress(self.progress):
            print(
                "Activando recuperación por erasure coding. "
                f"data_packs={pack_count} chunks={chunk_count}"
            )
