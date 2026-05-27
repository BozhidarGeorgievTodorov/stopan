"""
Servicio de lectura de chunks para restore.

La lectura sigue una cadena conservadora:
  1. CAS local;
  2. CAS P2P local;
  3. red remota mediante RetrieveChunkBatch (si está habilitado);
  4. reconstrucción por data packs EC si sigue faltando el chunk (si está habilitado).

Todo chunk recuperado fuera del CAS local principal se valida con Zstandard y
BLAKE3 antes de aceptarse.
"""

from __future__ import annotations

import concurrent.futures
from collections.abc import Sequence

import blake3
import zstandard as zstd

from stopan.cas.repository import CASRepository
from stopan.restore.cluster import LazyClusterResolver
from stopan.restore.ec_fetch import ErasureChunkRecoveryService
from stopan.restore.remote_client import RemoteStorageClientPool
from stopan.protection.policy import normalize_remote_rf
from stopan.rpc.errors import format_rpc_error, is_rpc_error
from stopan.restore.errors import ChunkUnavailableError, RestoreDataError
from stopan.restore.models import RestoreRunStats


class ChunkFetchService:
    """
    Política de lectura de chunks durante restore.

    Prioridad:
      1. CAS local principal;
      2. CAS P2P local del nodo, si existe;
      3. red bajo demanda con RetrieveChunkBatch (si remote_chunk_recovery es True);
      4. reconstrucción por data packs EC si sigue faltando el chunk y ec_recovery_service está presente.

          rf=0 desactiva la búsqueda remota. Los blobs recuperados desde P2P local o
    red se descomprimen y se validan contra el BLAKE3 esperado antes de usarse.
    """

    def __init__(
        self,
        *,
        repo: CASRepository,
        p2p_local_repo: CASRepository | None,
        cluster_resolver: LazyClusterResolver | None,
        remote_pool: RemoteStorageClientPool | None,
        rf: int,
        cluster_token: str,
        max_chunk_size: int,
        ec_recovery_service: ErasureChunkRecoveryService | None = None,
        remote_chunk_recovery: bool = True,
    ):
        self.repo = repo
        self.p2p_local_repo = p2p_local_repo
        self.cluster_resolver = cluster_resolver
        self.remote_pool = remote_pool
        self.rf = normalize_remote_rf(rf)
        self.cluster_token = str(cluster_token or "").strip()
        self.max_chunk_size = max(int(max_chunk_size), 1)
        self.ec_recovery_service = ec_recovery_service
        self.remote_chunk_recovery = bool(remote_chunk_recovery)
        self.stats = RestoreRunStats()

    def fetch_many_raw_chunks(
        self,
        chunk_hashes: Sequence[str],
        *,
        target_parallelism: int,
    ) -> dict[str, bytes | Exception]:
        """
        Recupera varios chunks en formato raw siguiendo el orden de prioridades configurado.

        Devuelve bytes para los chunks recuperados y Exception para los que no se
        pudieron resolver. No lanza por fallo individual de chunk.
        """
        ordered_hashes = list(chunk_hashes)
        self.stats.chunks_requested += len(ordered_hashes)
        results: dict[str, bytes | Exception] = {}
        missing_hashes: list[str] = []
        local_errors: dict[str, list[str]] = {}

        for chunk_hash in ordered_hashes:
            try:
                results[chunk_hash] = self.repo.get(chunk_hash)
                self.stats.chunks_from_local_cas += 1
                continue
            except FileNotFoundError:
                pass
            except Exception as exc:
                local_errors.setdefault(chunk_hash, []).append(f"CAS local: {exc}")

            if self.p2p_local_repo is not None:
                try:
                    compressed_data = self.p2p_local_repo.get_compressed(chunk_hash)
                    raw_data = self._validate_compressed_chunk(
                        chunk_hash,
                        compressed_data,
                        source_label="chunk local P2P",
                    )
                    try:
                        self.repo.put_compressed(chunk_hash, compressed_data)
                    except Exception as exc:
                        local_errors.setdefault(chunk_hash, []).append(
                            f"CAS local cache desde P2P: {exc}"
                        )
                    results[chunk_hash] = raw_data
                    self.stats.chunks_from_local_p2p_cas += 1
                    continue
                except FileNotFoundError:
                    pass
                except Exception as exc:
                    local_errors.setdefault(chunk_hash, []).append(f"CAS P2P local: {exc}")

            missing_hashes.append(chunk_hash)

        if missing_hashes:
            if self.remote_chunk_recovery:
                remote_results = self._fetch_missing_many_from_remote(
                    missing_hashes,
                    target_parallelism=max(int(target_parallelism), 1),
                    initial_errors=local_errors,
                )
            else:
                remote_results = self._remote_chunk_recovery_disabled_results(
                    missing_hashes,
                    initial_errors=local_errors,
                )

            if self.ec_recovery_service is not None:
                remote_results = self._recover_unresolved_with_ec(
                    missing_hashes,
                    remote_results,
                    target_parallelism=max(int(target_parallelism), 1),
                )
            results.update(remote_results)
            self.stats.chunks_failed += sum(
                1 for chunk_hash in missing_hashes
                if isinstance(remote_results.get(chunk_hash), Exception)
            )

        return results

    def _remote_chunk_recovery_disabled_results(
        self,
        missing_hashes: list[str],
        *,
        initial_errors: dict[str, list[str]] | None = None,
    ) -> dict[str, bytes | Exception]:
        error_map = {
            chunk_hash: list((initial_errors or {}).get(chunk_hash, []))
            for chunk_hash in missing_hashes
        }
        return {
            chunk_hash: ChunkUnavailableError(
                f"El chunk {chunk_hash[:8]} no está localmente y la recuperación "
                "remota por chunks está desactivada. "
                + " | ".join(error_map[chunk_hash])
            )
            for chunk_hash in missing_hashes
        }

    def _recover_unresolved_with_ec(
        self,
        missing_hashes: list[str],
        remote_results: dict[str, bytes | Exception],
        *,
        target_parallelism: int,
    ) -> dict[str, bytes | Exception]:
        unresolved = [
            chunk_hash
            for chunk_hash in missing_hashes
            if isinstance(remote_results.get(chunk_hash), Exception)
        ]
        if not unresolved:
            return remote_results

        ec_results = self.ec_recovery_service.recover_many_raw_chunks(
            unresolved,
            target_parallelism=target_parallelism,
        )

        for chunk_hash in unresolved:
            value = ec_results.get(chunk_hash)
            if value is None:
                continue
            if not isinstance(value, Exception):
                remote_results[chunk_hash] = value
                self.stats.chunks_from_ec += 1
                continue

            previous = remote_results.get(chunk_hash)
            if isinstance(previous, Exception):
                remote_results[chunk_hash] = ChunkUnavailableError(
                    f"{previous} | EC: {value}"
                )
                self.stats.ec_recovery_failures += 1
            else:
                remote_results[chunk_hash] = value

        return remote_results

    def _validate_compressed_chunk(
        self,
        chunk_hash: str,
        compressed_data: bytes,
        *,
        source_label: str,
    ) -> bytes:
        try:
            raw_data = zstd.ZstdDecompressor().decompress(
                compressed_data,
                max_output_size=self.max_chunk_size,
            )
        except zstd.ZstdError as exc:
            raise RestoreDataError(f"{source_label} corrupto al descomprimir: {chunk_hash[:8]}: {exc}") from exc
        except Exception as exc:
            raise RestoreDataError(
                f"{source_label} inválido o demasiado grande al descomprimir: {chunk_hash[:8]}: {exc}"
            ) from exc

        calculated_hash = blake3.blake3(raw_data).hexdigest()
        if calculated_hash != chunk_hash:
            raise RestoreDataError(
                f"hash inválido en {source_label} {chunk_hash[:8]}: calculado {calculated_hash}"
            )

        return raw_data

    def _fetch_missing_many_from_remote(
        self,
        missing_hashes: list[str],
        *,
        target_parallelism: int,
        initial_errors: dict[str, list[str]] | None = None,
    ) -> dict[str, bytes | Exception]:
        """
        Recupera chunks ausentes desde targets HRW remotos.

        Consulta targets por rondas de rank HRW. En cuanto un chunk se recupera y
        valida correctamente, deja de consultarse en targets posteriores.
        """
        
        result_map: dict[str, bytes | Exception] = {}
        error_map: dict[str, list[str]] = {
            chunk_hash: list((initial_errors or {}).get(chunk_hash, []))
            for chunk_hash in missing_hashes
        }

        if self.rf == 0 or self.cluster_resolver is None or self.remote_pool is None:
            for chunk_hash in missing_hashes:
                result_map[chunk_hash] = ChunkUnavailableError(
                    f"El chunk {chunk_hash[:8]} no está localmente y la recuperación "
                    "remota por chunks no está disponible o no hay targets de replicación. "
                    + " | ".join(error_map[chunk_hash])
                )
            return result_map

        cluster = self.cluster_resolver.get_cluster()
        self.cluster_resolver.announce_once()

        target_lists: dict[str, list] = {}

        excluded_node_ids = {self.cluster_resolver.origin_node_id}

        for chunk_hash in missing_hashes:
            remote_targets = cluster.hrw_targets_excluding(
                chunk_hash,
                rf=self.rf,
                salt=self.cluster_token,
                excluded_node_ids=excluded_node_ids,
            )
            target_lists[chunk_hash] = remote_targets

            if not remote_targets:
                result_map[chunk_hash] = ChunkUnavailableError(
                    f"El chunk {chunk_hash[:8]} no está localmente y no hay targets remotos HRW utilizables. "
                    + " | ".join(error_map[chunk_hash])
                )

        unresolved = {chunk_hash for chunk_hash in missing_hashes if chunk_hash not in result_map}
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
                        addr=member.address,
                        chunk_hashes=group_hashes,
                    ): (member, group_hashes)
                    for member, group_hashes in groups.values()
                }

                for future in concurrent.futures.as_completed(future_map):
                    member, group_hashes = future_map[future]

                    try:
                        batch_results = future.result()
                    except Exception as exc:
                        if is_rpc_error(exc):
                            message = f"{member.address}: RPC {format_rpc_error(exc)}"
                        else:
                            message = f"{member.address}: {exc}"

                        for chunk_hash in group_hashes:
                            error_map[chunk_hash].append(message)
                        continue

                    for chunk_hash in group_hashes:
                        if chunk_hash not in unresolved:
                            continue

                        item = batch_results.get(chunk_hash)
                        if item is None:
                            error_map[chunk_hash].append(f"{member.address}: sin resultado")
                            continue

                        if not item.is_found(self.remote_pool.retrieve_status_found):
                            error_map[chunk_hash].append(f"{member.address}: {item.detail}")
                            continue

                        try:
                            raw_data = self._validate_compressed_chunk(
                                chunk_hash,
                                item.chunk_data,
                                source_label="chunk remoto",
                            )
                        except Exception as exc:
                            print(
                                f"   Réplica corrupta ignorada: "
                                f"{member.address} chunk={chunk_hash[:8]} -> {exc}"
                            )
                            error_map[chunk_hash].append(
                                f"{member.address}: chunk corrupto: {exc}"
                            )
                            self.stats.remote_chunks_corrupt += 1
                            continue

                        try:
                            self.repo.put_compressed(chunk_hash, item.chunk_data)
                        except Exception as exc:
                            # El chunk ya fue validado en memoria. Si el cache local falla,
                            # aún podemos devolver raw_data y completar el restore.
                            error_map[chunk_hash].append(
                                f"{member.address}: no pude cachear localmente: {exc}"
                            )

                        result_map[chunk_hash] = raw_data
                        self.stats.chunks_from_remote_replication += 1
                        unresolved.discard(chunk_hash)

        for chunk_hash in list(unresolved):
            result_map[chunk_hash] = ChunkUnavailableError(
                f"Ningún target HRW devolvió el bloque {chunk_hash[:8]}. "
                + " | ".join(error_map[chunk_hash])
            )

        return result_map
